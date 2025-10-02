import { performance } from 'node:perf_hooks';
import { Job } from './job';
import { Logger, type LoggerInterface } from './logger';
import type { AddOptions, Queue, ReservedJob } from './queue';

export type BackoffStrategy = (attempt: number) => number; // ms

// Typed event system for Worker
export interface WorkerEvents<T = any>
  extends Record<string, (...args: any[]) => void> {
  error: (error: Error) => void;
  closed: () => void;
  ready: () => void;
  failed: (job: Job<T>) => void;
  completed: (job: Job<T>) => void;
  'ioredis:close': () => void;
  'graceful-timeout': (job: Job<T>) => void;
}

class TypedEventEmitter<
  TEvents extends Record<string, (...args: any[]) => void>,
> {
  private listeners = new Map<keyof TEvents, Array<TEvents[keyof TEvents]>>();

  on<K extends keyof TEvents>(event: K, listener: TEvents[K]): this {
    if (!this.listeners.has(event)) {
      this.listeners.set(event, []);
    }
    this.listeners.get(event)!.push(listener);
    return this;
  }

  off<K extends keyof TEvents>(event: K, listener: TEvents[K]): this {
    const eventListeners = this.listeners.get(event);
    if (eventListeners) {
      const index = eventListeners.indexOf(listener);
      if (index !== -1) {
        eventListeners.splice(index, 1);
      }
    }
    return this;
  }

  emit<K extends keyof TEvents>(
    event: K,
    ...args: Parameters<TEvents[K]>
  ): boolean {
    const eventListeners = this.listeners.get(event);
    if (eventListeners && eventListeners.length > 0) {
      for (const listener of eventListeners) {
        try {
          listener(...args);
        } catch (error) {
          // Don't let listener errors break the emit
          console.error(
            `Error in event listener for '${String(event)}':`,
            error,
          );
        }
      }
      return true;
    }
    return false;
  }

  removeAllListeners<K extends keyof TEvents>(event?: K): this {
    if (event) {
      this.listeners.delete(event);
    } else {
      this.listeners.clear();
    }
    return this;
  }
}

// New API: pass an existing Queue instance; keep old options for BC
export type WorkerOptions<T> = {
  queue: Queue<T>;
  name?: string;
  handler: (job: ReservedJob<T>) => Promise<unknown>;
  heartbeatMs?: number; // default: queue.jobTimeoutMs/3
  onError?: (err: unknown, job?: ReservedJob<T>) => void;
  maxAttempts?: number; // worker-level cap (defaults to queue.maxAttemptsDefault)
  backoff?: BackoffStrategy;
  enableCleanup?: boolean; // default: true
  cleanupIntervalMs?: number; // default: 60s
  schedulerIntervalMs?: number; // default: 1s
  blockingTimeoutSec?: number; // default: 5s
  atomicCompletion?: boolean; // default: true
  logger?: LoggerInterface | true;
  concurrency?: number; // default: 1
};

const defaultBackoff: BackoffStrategy = (attempt) => {
  const base = Math.min(30_000, 2 ** (attempt - 1) * 500);
  const jitter = Math.floor(base * 0.25 * Math.random());
  return base + jitter;
};

class _Worker<T = any> extends TypedEventEmitter<WorkerEvents<T>> {
  private logger: LoggerInterface;
  public readonly name: string;
  private q: Queue<T>;
  private handler: WorkerOptions<T>['handler'];
  private hbMs: number;
  private onError?: WorkerOptions<T>['onError'];
  private stopping = false;
  private ready = false;
  private closed = false;
  private maxAttempts: number;
  private backoff: BackoffStrategy;
  private enableCleanup: boolean;
  private cleanupMs: number;
  private cleanupTimer?: NodeJS.Timeout;
  private schedulerTimer?: NodeJS.Timeout;
  private schedulerMs: number;
  private blockingTimeoutSec: number;
  private atomicCompletion: boolean;
  private currentJob: ReservedJob<T> | null = null;
  private processingStartTime = 0;
  private concurrency: number;
  private blockingClient: import('ioredis').default | null = null;
  private inFlightJobs = new Set<string>();

  // Blocking detection and monitoring
  private lastJobPickupTime = 0;
  private totalJobsProcessed = 0;
  private blockingStats = {
    totalBlockingCalls: 0,
    totalBlockingTimeMs: 0,
    consecutiveEmptyReserves: 0,
    lastActivityTime: Date.now(),
  };
  private stuckDetectionTimer?: NodeJS.Timeout;
  private redisCloseHandler?: () => void;
  private redisErrorHandler?: (error: Error) => void;
  private redisReadyHandler?: () => void;

  constructor(opts: WorkerOptions<T>) {
    super();

    if (!opts.handler || typeof opts.handler !== 'function') {
      throw new Error('Worker handler must be a function');
    }

    this.q = opts.queue;
    this.name =
      opts.name ?? `worker-${Math.random().toString(36).substr(2, 9)}`;
    this.logger =
      typeof opts.logger === 'object'
        ? opts.logger
        : new Logger(!!opts.logger, this.name);
    this.handler = opts.handler;
    const jobTimeoutMs = this.q.jobTimeoutMs ?? 30_000;
    this.hbMs =
      opts.heartbeatMs ?? Math.max(1000, Math.floor(jobTimeoutMs / 3));
    this.onError = opts.onError;
    this.maxAttempts = opts.maxAttempts ?? this.q.maxAttemptsDefault ?? 3;
    this.backoff = opts.backoff ?? defaultBackoff;
    this.enableCleanup = opts.enableCleanup ?? true;
    this.cleanupMs = opts.cleanupIntervalMs ?? 60_000;
    this.schedulerMs = opts.schedulerIntervalMs ?? 1000;
    this.blockingTimeoutSec = opts.blockingTimeoutSec ?? 5; // 5s like BullMQ's drainDelay
    this.atomicCompletion = opts.atomicCompletion ?? true;
    this.concurrency = Math.max(1, opts.concurrency ?? 1);

    // Set up Redis connection event handlers
    this.setupRedisEventHandlers();
  }

  private setupRedisEventHandlers() {
    // Get Redis instance from the queue to monitor connection events
    const redis = this.q.redis;
    if (redis) {
      this.redisCloseHandler = () => {
        this.ready = false;
        this.emit('ioredis:close');
      };
      this.redisErrorHandler = (error: Error) => {
        this.emit('error', error);
      };
      this.redisReadyHandler = () => {
        if (!this.ready && !this.stopping) {
          this.ready = true;
          this.emit('ready');
        }
      };

      redis.on('close', this.redisCloseHandler);
      redis.on('error', this.redisErrorHandler);
      redis.on('ready', this.redisReadyHandler);
    }
  }

  async run() {
    this.logger.info(`🚀 Worker ${this.name} starting...`);
    // Dedicated blocking client per worker with auto-pipelining to reduce contention
    try {
      this.blockingClient = this.q.redis.duplicate({
        enableAutoPipelining: true,
      });

      // Add reconnection handlers for resilience
      this.blockingClient.on('error', (err) => {
        this.logger.warn('Blocking client error:', err);
      });
      this.blockingClient.on('close', () => {
        this.logger.warn(
          'Blocking client disconnected, will reconnect on next operation',
        );
      });
    } catch (_e) {
      this.blockingClient = null; // fall back to queue's blocking client
    }

    // Start cleanup timer if enabled
    if (this.enableCleanup) {
      // Cleanup timer: only runs cleanup, not scheduler
      this.cleanupTimer = setInterval(async () => {
        try {
          await this.q.cleanup();
        } catch (err) {
          this.onError?.(err);
        }
      }, this.cleanupMs);

      // Scheduler timer: promotes delayed jobs and processes cron jobs
      // Runs independently in the background, even when worker is blocked on BZPOPMIN
      // Distributed lock ensures only one worker executes at a time
      const schedulerInterval = Math.min(this.schedulerMs, this.cleanupMs);
      this.schedulerTimer = setInterval(async () => {
        try {
          await this.q.runSchedulerOnce();
        } catch (err) {
          // Ignore errors, this is best-effort
        }
      }, schedulerInterval);
    }

    // Start stuck detection monitoring
    this.startStuckDetection();

    let blockUntil = 0;
    let connectionRetries = 0;
    const maxConnectionRetries = 5;

    let inFlight = 0;

    while (!this.stopping) {
      const loopStartTime = Date.now();

      try {
        this.blockingStats.totalBlockingCalls++;
        const blockingStartTime = Date.now();

        this.logger.info(
          `Attempting to reserve job (call #${this.blockingStats.totalBlockingCalls})...`,
        );

        // If we have capacity and concurrency > 1, try to get a batch first (low latency, fewer round trips)
        const capacity = Math.max(0, this.concurrency - inFlight);
        if (capacity > 0 && this.concurrency > 1) {
          try {
            const batch = await this.q.reserveBatch(Math.min(32, capacity));
            if (batch.length > 0) {
              for (const job of batch) {
                this.totalJobsProcessed++;
                inFlight++;
                this.inFlightJobs.add(job.id);
                void this.processOne(job)
                  .catch((err) => this.logger.error('ProcessOne fatal:', err))
                  .finally(() => {
                    inFlight--;
                    this.inFlightJobs.delete(job.id);
                    this.blockingStats.lastActivityTime = Date.now();
                  });
              }
              // Quickly iterate again to try fill remaining capacity
              continue;
            }
          } catch (e) {
            this.logger.warn(
              'reserveBatch error, falling back to blocking:',
              e,
            );
          }
        }

        // Enhanced blocking with BullMQ-style connection management
        const job = await this.q.reserveBlocking(
          this.blockingTimeoutSec,
          blockUntil,
          this.blockingClient ?? undefined,
        );

        const blockingEndTime = Date.now();
        const blockingDuration = blockingEndTime - blockingStartTime;
        this.blockingStats.totalBlockingTimeMs += blockingDuration;

        if (job) {
          // Reset connection retry count on successful job pickup
          connectionRetries = 0;
          blockUntil = 0; // Reset blockUntil after successful job

          this.lastJobPickupTime = Date.now();
          this.blockingStats.consecutiveEmptyReserves = 0;
          this.blockingStats.lastActivityTime = Date.now();

          this.logger.info(
            `Picked up job ${job.id} from group ${job.groupId} (took ${blockingDuration}ms)`,
          );

          if (this.concurrency > 1) {
            this.totalJobsProcessed++;
            inFlight++;
            this.inFlightJobs.add(job.id);
            void this.processOne(job)
              .catch((err) => this.logger.error('ProcessOne fatal:', err))
              .finally(() => {
                inFlight--;
                this.inFlightJobs.delete(job.id);
              });
          } else {
            this.totalJobsProcessed++;
            this.inFlightJobs.add(job.id);
            await this.processOne(job).catch((err) => {
              this.logger.error(`ProcessOne fatal:`, err);
            });
            this.inFlightJobs.delete(job.id);
          }
        } else {
          // No job available
          this.blockingStats.consecutiveEmptyReserves++;

          this.logger.info(
            `No job available (blocking took ${blockingDuration}ms, consecutive empty: ${this.blockingStats.consecutiveEmptyReserves})`,
          );

          // Log warning if too many consecutive empty reserves (but only if we've processed jobs before and queue seems to have work)
          if (
            this.blockingStats.consecutiveEmptyReserves % 10 === 0 &&
            this.shouldWarnAboutEmptyReserves()
          ) {
            this.logger.warn(
              `Has had ${this.blockingStats.consecutiveEmptyReserves} consecutive empty reserves - potential blocking issue!`,
            );
            await this.logWorkerStatus();
          }

          // Recovery for delayed groups that might be stuck
          // Note: runSchedulerOnce() is handled by the background scheduler timer
          try {
            const recovered = await this.q.recoverDelayedGroups();
            if (recovered > 0) {
              this.logger.info(`Recovered ${recovered} delayed groups`);
            }
          } catch (err) {
            this.logger.warn(`Recovery error:`, err);
          }
        }
      } catch (err) {
        // Distinguish between connection errors (retry) and other errors (log and continue)
        const isConnErr = this.isConnectionError(err);

        if (isConnErr) {
          // Connection error - implement retry logic with backoff
          connectionRetries++;

          this.logger.error(
            `Connection error (retry ${connectionRetries}/${maxConnectionRetries}):`,
            err,
          );

          if (connectionRetries >= maxConnectionRetries) {
            this.logger.error(
              `⚠️  Max connection retries (${maxConnectionRetries}) exceeded! Worker will continue but may be experiencing persistent Redis issues.`,
            );
            this.emit(
              'error',
              new Error(
                `Max connection retries (${maxConnectionRetries}) exceeded - worker continuing with backoff`,
              ),
            );
            await this.delay(5000); // Wait 5 seconds before continuing
            connectionRetries = 0; // Reset to continue trying
          } else {
            // Exponential delay before retry (1s, 2s, 3s, 4s, 5s)
            await this.delay(1000 * connectionRetries);
          }
        } else {
          // Non-connection error (programming error, Lua script error, etc.)
          // Log it, emit it, but don't retry - just continue with next iteration
          this.logger.error(
            `Worker loop error (non-connection, continuing):`,
            err,
          );
          this.emit(
            'error',
            err instanceof Error ? err : new Error(String(err)),
          );

          // Reset connection retries since this wasn't a connection issue
          connectionRetries = 0;

          // Small delay to avoid tight error loops
          await this.delay(100);
        }

        this.onError?.(err);
      }

      const loopDuration = Date.now() - loopStartTime;
      if (loopDuration > 10000) {
        // Log if a single loop takes more than 10 seconds
        this.logger.warn(`Slow loop detected: ${loopDuration}ms`);
      }
    }

    this.logger.info(`Stopped`);
  }

  private async delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  /**
   * Check if an error is a Redis connection error (should retry)
   * vs a programming error (should not retry)
   */
  private isConnectionError(err: any): boolean {
    if (!err) return false;

    const message = err.message || '';
    const code = err.code || '';

    return (
      message.includes('Connection is closed') ||
      message.includes('connect ECONNREFUSED') ||
      message.includes('ENOTFOUND') ||
      message.includes('ECONNRESET') ||
      message.includes('ETIMEDOUT') ||
      message.includes('EPIPE') ||
      message.includes('Socket closed unexpectedly') ||
      code === 'ECONNREFUSED' ||
      code === 'ENOTFOUND' ||
      code === 'ECONNRESET' ||
      code === 'ETIMEDOUT' ||
      code === 'EPIPE'
    );
  }

  /**
   * Read remaining delay for a job from Redis and return it in milliseconds
   */
  private async getDelayMs(jobId: string): Promise<number | undefined> {
    try {
      const jobKey = `${this.q.namespace}:job:${jobId}`;
      const val = await this.q.redis.hget(jobKey, 'delayUntil');
      if (!val) return undefined;
      const delayUntil = parseInt(val, 10);
      const now = Date.now();
      if (Number.isFinite(delayUntil) && delayUntil > now) {
        return delayUntil - now;
      }
      return undefined;
    } catch (_e) {
      return undefined;
    }
  }

  /**
   * Start monitoring for stuck worker conditions
   */
  private startStuckDetection(): void {
    this.stuckDetectionTimer = setInterval(async () => {
      await this.checkForStuckConditions();
    }, 30000); // Check every 30 seconds
  }

  /**
   * Determine if we should warn about empty reserves based on context
   */
  private shouldWarnAboutEmptyReserves(): boolean {
    // Don't warn if stopping
    if (this.stopping || this.closed) return false;

    // Don't warn if we haven't processed any jobs (could be intentionally empty queue)
    if (this.totalJobsProcessed === 0) return false;

    // Don't warn if it's been more than 30 seconds since last job (likely normal completion)
    const timeSinceLastJob =
      this.lastJobPickupTime > 0
        ? Date.now() - this.lastJobPickupTime
        : Date.now();
    if (timeSinceLastJob > 30000) return false;

    // Warn if we've processed jobs recently but are now seeing many empty reserves
    return true;
  }

  /**
   * Check if worker appears to be stuck
   */
  private async checkForStuckConditions(): Promise<void> {
    if (this.stopping || this.closed) return;

    const now = Date.now();
    const timeSinceLastActivity = now - this.blockingStats.lastActivityTime;
    const timeSinceLastJob =
      this.lastJobPickupTime > 0 ? now - this.lastJobPickupTime : now;

    // Check for stuck conditions
    if (timeSinceLastActivity > 60000) {
      // 1 minute without any activity
      this.logger.warn(
        `STUCK WORKER ALERT: No activity for ${Math.round(timeSinceLastActivity / 1000)}s`,
      );
      await this.logWorkerStatus();
    }

    if (this.blockingStats.consecutiveEmptyReserves > 50) {
      // Too many empty reserves
      this.logger.warn(
        `BLOCKING ALERT: ${this.blockingStats.consecutiveEmptyReserves} consecutive empty reserves`,
      );
      await this.logWorkerStatus();
    }

    if (timeSinceLastJob > 120000 && this.totalJobsProcessed > 0) {
      // 2 minutes since last job (but has processed jobs before)
      this.logger.warn(
        `JOB STARVATION ALERT: No jobs for ${Math.round(timeSinceLastJob / 1000)}s`,
      );
      await this.logWorkerStatus();
    }
  }

  /**
   * Log comprehensive worker status for debugging
   */
  private async logWorkerStatus(): Promise<void> {
    try {
      const now = Date.now();
      const [queueStats, uniqueGroupsCount] = await Promise.all([
        this.q.getJobCounts(),
        this.q.getUniqueGroups(),
      ]);

      this.logger.info(`📊 Status Report:`);
      this.logger.info(`  🔢 Jobs Processed: ${this.totalJobsProcessed}`);
      this.logger.info(
        `  ⏱️ Last Job: ${this.lastJobPickupTime > 0 ? Math.round((now - this.lastJobPickupTime) / 1000) : 'never'}s ago`,
      );
      this.logger.info(
        `  🚫 Consecutive Empty Reserves: ${this.blockingStats.consecutiveEmptyReserves}`,
      );
      this.logger.info(
        `  📞 Total Blocking Calls: ${this.blockingStats.totalBlockingCalls}`,
      );
      this.logger.info(
        `  ⏲️ Total Blocking Time: ${Math.round(this.blockingStats.totalBlockingTimeMs / 1000)}s`,
      );
      this.logger.info(
        `  📈 Queue Stats: Active=${queueStats.active}, Waiting=${queueStats.waiting}, Delayed=${queueStats.delayed}, Groups=${uniqueGroupsCount}`,
      );
      this.logger.info(
        `  🔄 Currently Processing: ${this.currentJob?.id || 'none'}`,
      );

      if (this.currentJob) {
        const processingTime = now - this.processingStartTime;
        this.logger.info(
          `  ⚙️ Current Job Processing Time: ${Math.round(processingTime / 1000)}s`,
        );
      }
    } catch (err) {
      this.logger.error(`Failed to log worker status:`, err);
    }
  }

  /**
   * Get worker performance metrics
   */
  getWorkerMetrics() {
    const now = Date.now();
    return {
      name: this.name,
      totalJobsProcessed: this.totalJobsProcessed,
      lastJobPickupTime: this.lastJobPickupTime,
      timeSinceLastJob:
        this.lastJobPickupTime > 0 ? now - this.lastJobPickupTime : null,
      blockingStats: { ...this.blockingStats },
      isProcessing: this.currentJob !== null,
      currentJobId: this.currentJob?.id || null,
      currentJobGroupId: this.currentJob?.groupId || null,
      currentJobProcessingTime: this.currentJob
        ? now - this.processingStartTime
        : 0,
    };
  }

  /**
   * Stop the worker gracefully
   * @param gracefulTimeoutMs Maximum time to wait for current job to finish (default: 30 seconds)
   */
  async close(gracefulTimeoutMs = 30_000): Promise<void> {
    this.stopping = true;

    if (this.cleanupTimer) {
      clearInterval(this.cleanupTimer);
    }

    if (this.schedulerTimer) {
      clearInterval(this.schedulerTimer);
    }

    if (this.stuckDetectionTimer) {
      clearInterval(this.stuckDetectionTimer);
    }

    // Wait for all in-flight jobs to finish or timeout
    const startTime = Date.now();
    while (
      this.inFlightJobs.size > 0 &&
      Date.now() - startTime < gracefulTimeoutMs
    ) {
      await sleep(100);
    }

    if (this.inFlightJobs.size > 0) {
      this.logger.warn(
        `Worker stopped with ${this.inFlightJobs.size} jobs still processing after ${gracefulTimeoutMs}ms timeout. Job IDs: ${Array.from(this.inFlightJobs).join(', ')}`,
      );
      // Emit graceful-timeout event for the current job if one is tracked
      if (this.currentJob) {
        const nowWall = Date.now();
        const elapsedSinceStart = this.processingStartTime
          ? performance.now() - this.processingStartTime
          : 0;
        const processedOnWall = Math.max(
          0,
          Math.floor(nowWall - elapsedSinceStart),
        );
        let delayMs: number | undefined;
        try {
          delayMs = await this.getDelayMs(this.currentJob.id);
        } catch (_e) {
          // ignore
        }
        this.emit(
          'graceful-timeout',
          Job.fromReserved(this.q, this.currentJob, {
            processedOn: processedOnWall,
            finishedOn: nowWall,
            delayMs,
            status: 'active',
          }),
        );
      }
    }

    // Clear tracking
    this.currentJob = null;
    this.processingStartTime = 0;
    this.inFlightJobs.clear();
    this.ready = false;
    this.closed = true;

    // Remove Redis event listeners to avoid leaks
    try {
      const redis = this.q.redis;
      if (redis) {
        if (this.redisCloseHandler)
          redis.off?.('close', this.redisCloseHandler);
        if (this.redisErrorHandler)
          redis.off?.('error', this.redisErrorHandler);
        if (this.redisReadyHandler)
          redis.off?.('ready', this.redisReadyHandler);
      }
    } catch (_e) {
      // ignore listener cleanup errors
    }

    // Emit closed event
    this.emit('closed');
  }

  /**
   * Get information about the currently processing job, if any
   */
  getCurrentJob(): { job: ReservedJob<T>; processingTimeMs: number } | null {
    if (!this.currentJob) {
      return null;
    }

    return {
      job: this.currentJob,
      processingTimeMs: performance.now() - this.processingStartTime,
    };
  }

  /**
   * Check if the worker is currently processing a job
   */
  isProcessing(): boolean {
    return this.currentJob !== null;
  }

  // Proxy to the underlying queue.add with correct data typing inferred from the queue
  async add(opts: AddOptions<T>) {
    return this.q.add(opts);
  }

  private async processOne(job: ReservedJob<T>) {
    // Track current job
    this.currentJob = job;
    this.processingStartTime = performance.now();
    const jobStartWallTime = Date.now();

    let hbTimer: NodeJS.Timeout | undefined;
    const startHeartbeat = () => {
      const minInterval = Math.max(
        this.hbMs,
        Math.floor((this.q.jobTimeoutMs || 30000) / 2),
      );
      hbTimer = setInterval(async () => {
        try {
          await this.q.heartbeat(job);
        } catch (e) {
          this.onError?.(e, job);
          this.emit('error', e instanceof Error ? e : new Error(String(e)));
        }
      }, minInterval);
    };

    try {
      startHeartbeat();
      const handlerResult = await this.handler(job);
      clearInterval(hbTimer!);

      // Try to atomically complete this job and reserve the next one from the same group
      // This prevents race conditions where other workers steal subsequent jobs
      let nextJob: any = null;
      let _jobCompleted = false;

      if (this.atomicCompletion) {
        try {
          nextJob = await this.q.completeAndReserveNext(job.id, job.groupId);
          _jobCompleted = true; // If we reach here, the job was completed atomically
        } catch (err) {
          this.logger.warn(
            `CompleteAndReserveNext failed, falling back to regular complete:`,
            err,
          );
          // Fallback to regular completion
          await this.q.complete(job);
          _jobCompleted = true;
        }
      } else {
        // Use regular completion (no atomic chaining)
        await this.q.complete(job);
        _jobCompleted = true;
      }

      // Timing for completed event
      const finishedAtWall = Date.now();
      let delayMs: number | undefined;
      try {
        delayMs = await this.getDelayMs(job.id);
      } catch (_e) {
        // ignore
      }

      this.emit(
        'completed',
        Job.fromReserved(this.q, job, {
          processedOn: jobStartWallTime,
          finishedOn: finishedAtWall,
          returnvalue: handlerResult,
          delayMs,
          status: 'completed',
        }),
      );
      // Persist completion for inspection
      try {
        await this.q.recordCompleted(
          { id: job.id, groupId: job.groupId },
          handlerResult,
          {
            processedOn: jobStartWallTime,
            finishedOn: finishedAtWall,
            attempts: job.attempts,
            maxAttempts: job.maxAttempts,
            data: job.data,
          },
        );
      } catch (e) {
        // Always log this error, even if logger is disabled
        console.error(
          `💥 CRITICAL: Failed to record completion for job ${job.id}:`,
          e,
        );
        this.logger.warn('Failed to record completion', e);
      }

      // If we got a next job from the same group, process it immediately (maintaining FIFO)
      // But DON'T chain recursively - just process this one job atomically
      if (this.atomicCompletion && nextJob) {
        this.logger.info(
          `Got next job ${nextJob.id} from same group ${nextJob.groupId} atomically`,
        );

        // Process the next job but WITHOUT atomic completion to prevent infinite chaining
        const originalAtomicCompletion = this.atomicCompletion;
        this.atomicCompletion = false; // Temporarily disable to prevent recursion

        try {
          await this.processOne(nextJob).catch((err) => {
            this.logger.error(`ProcessOne (chained) fatal:`, err);
          });
        } finally {
          this.atomicCompletion = originalAtomicCompletion; // Restore original setting
        }

        // After processing the chained job, return to normal competition
        // This allows other workers to compete for subsequent jobs in this group
        return;
      }

      // If no next job in the same group, the normal run() loop will handle getting the next job
    } catch (err) {
      clearInterval(hbTimer!);
      this.onError?.(err, job);

      // Safely emit error event - don't let emit errors break retry logic
      try {
        this.emit('error', err instanceof Error ? err : new Error(String(err)));
      } catch (_emitError) {
        // Silently ignore emit errors to prevent breaking retry logic
      }

      // Create a job-like object with accurate timing in milliseconds for failed event
      const failedAt = Date.now();
      const failedJob = {
        ...job,
        failedReason: err instanceof Error ? err.message : String(err),
        processedOn: jobStartWallTime,
        finishedOn: failedAt,
        data: job.data,
        opts: {
          attempts: job.maxAttempts,
        },
      };

      let delayMs: number | undefined;
      try {
        delayMs = await this.getDelayMs(job.id);
      } catch (_e) {
        // ignore
      }
      this.emit(
        'failed',
        Job.fromReserved(this.q, job, {
          processedOn: jobStartWallTime,
          finishedOn: failedAt,
          failedReason: failedJob.failedReason,
          stacktrace:
            err instanceof Error
              ? err.stack
              : typeof err === 'object' && err !== null
                ? (err as any).stack
                : undefined,
          delayMs,
          status: 'failed',
        }),
      );

      // enforce attempts at worker level too (job-level enforced by Redis)
      const nextAttempt = job.attempts + 1; // after qRetry increment this becomes current
      const backoffMs = this.backoff(nextAttempt);

      if (nextAttempt >= this.maxAttempts) {
        this.logger.info(
          `Dead lettering job ${job.id} from group ${job.groupId} (worker maxAttempts: ${this.maxAttempts}, job attempts: ${job.attempts})`,
        );
        // Record final failure before dead-lettering
        try {
          const errObj = err instanceof Error ? err : new Error(String(err));
          await this.q.recordFinalFailure(
            { id: job.id, groupId: job.groupId },
            { name: errObj.name, message: errObj.message, stack: errObj.stack },
            {
              processedOn: jobStartWallTime,
              finishedOn: failedAt,
              attempts: nextAttempt,
              maxAttempts: job.maxAttempts,
              data: job.data,
            },
          );
        } catch (e) {
          this.logger.warn('Failed to record final failure', e);
        }
        await this.q.deadLetter(job.id, job.groupId);
        return;
      }

      // Retry immediately to re-add to group and lock it with backoff (prevents overtaking)
      const retryResult = await this.q.retry(job.id, backoffMs);
      if (retryResult === -1) {
        // Queue-level max attempts exceeded; record final failure and dead-letter
        try {
          const errObj = err instanceof Error ? err : new Error(String(err));
          await this.q.recordFinalFailure(
            { id: job.id, groupId: job.groupId },
            { name: errObj.name, message: errObj.message, stack: errObj.stack },
            {
              processedOn: jobStartWallTime,
              finishedOn: failedAt,
              attempts: job.maxAttempts,
              maxAttempts: job.maxAttempts,
              data: job.data,
            },
          );
        } catch (e) {
          this.logger.warn('Failed to record final failure', e);
        }
        await this.q.deadLetter(job.id, job.groupId);
        return;
      }

      // Persist last failure attempt info after retry is secured
      try {
        const errObj = err instanceof Error ? err : new Error(String(err));
        await this.q.recordAttemptFailure(
          { id: job.id, groupId: job.groupId },
          { name: errObj.name, message: errObj.message, stack: errObj.stack },
          {
            processedOn: jobStartWallTime,
            finishedOn: failedAt,
            attempts: nextAttempt,
            maxAttempts: job.maxAttempts,
          },
        );
      } catch (e) {
        this.logger.warn('Failed to record attempt failure', e);
      }
    } finally {
      // Clear current job tracking
      this.currentJob = null;
      this.processingStartTime = 0;
    }
  }
}

// Export a value with a generic constructor so T is inferred from opts.queue
export type Worker<T = any> = _Worker<T>;
type WorkerConstructor = new <T>(opts: WorkerOptions<T>) => _Worker<T>;
export const Worker: WorkerConstructor =
  _Worker as unknown as WorkerConstructor;

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}
