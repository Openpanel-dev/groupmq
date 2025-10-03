import { AsyncFifoQueue } from './async-fifo-queue';
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
  private concurrency: number;
  private blockingClient: import('ioredis').default | null = null;

  // Track all jobs in progress (for all concurrency levels)
  private jobsInProgress = new Set<{ job: ReservedJob<T>; ts: number }>();

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
    // With AsyncFifoQueue, we can safely use atomic completion for all concurrency levels
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
        } catch (_err) {
          // Ignore errors, this is best-effort
        }
      }, schedulerInterval);
    }

    // Start stuck detection monitoring
    this.startStuckDetection();

    let blockUntil = 0;
    let connectionRetries = 0;
    const maxConnectionRetries = 5;

    // BullMQ-style async queue for clean promise management
    const asyncFifoQueue = new AsyncFifoQueue<void | ReservedJob<T> | null>(
      true,
    );

    while (!this.stopping || asyncFifoQueue.numTotal() > 0) {
      try {
        // Phase 1: Fetch jobs sequentially until we reach concurrency capacity
        while (!this.stopping && asyncFifoQueue.numTotal() < this.concurrency) {
          this.blockingStats.totalBlockingCalls++;

          this.logger.debug(
            `Fetching job (call #${this.blockingStats.totalBlockingCalls}, queue: ${asyncFifoQueue.numTotal()}/${this.concurrency})...`,
          );

          const fetchedJob = this.retryIfFailed(
            () =>
              this.q.reserveBlocking(
                this.blockingTimeoutSec,
                blockUntil,
                this.blockingClient ?? undefined,
              ),
            this.delay.bind(this),
          );

          asyncFifoQueue.add(fetchedJob);

          // Sequential fetching: wait for this fetch before next (prevents thundering herd)
          const job = await fetchedJob;

          if (job) {
            // Reset connection retry count and empty reserves
            connectionRetries = 0;
            blockUntil = 0;
            this.lastJobPickupTime = Date.now();
            this.blockingStats.consecutiveEmptyReserves = 0;
            this.blockingStats.lastActivityTime = Date.now();

            this.logger.debug(
              `Fetched job ${job.id} from group ${job.groupId}`,
            );
          } else {
            // No more jobs available
            this.blockingStats.consecutiveEmptyReserves++;

            this.logger.debug(
              `No job available (consecutive empty: ${this.blockingStats.consecutiveEmptyReserves})`,
            );

            // Log warning if too many consecutive empty reserves
            if (
              this.blockingStats.consecutiveEmptyReserves % 100 === 0 &&
              this.shouldWarnAboutEmptyReserves()
            ) {
              this.logger.warn(
                `Has had ${this.blockingStats.consecutiveEmptyReserves} consecutive empty reserves - potential blocking issue!`,
              );
              await this.logWorkerStatus();
            }

            // Recovery for delayed groups
            try {
              const recovered = await this.q.recoverDelayedGroups();
              if (recovered > 0) {
                this.logger.debug(`Recovered ${recovered} delayed groups`);
              }
            } catch (err) {
              this.logger.warn(`Recovery error:`, err);
            }

            // No more jobs, break fetch loop if we have jobs processing
            if (asyncFifoQueue.numTotal() > 1) {
              break;
            }
          }

          // Break if blockUntil is set to avoid waiting during processing
          if (blockUntil) {
            break;
          }
        }

        // Phase 2: Wait for a job to become available from the queue
        let job: ReservedJob<T> | void | null;
        do {
          job = await asyncFifoQueue.fetch();
        } while (!job && asyncFifoQueue.numQueued() > 0);

        if (job) {
          this.totalJobsProcessed++;

          // Process the job (may return chained job from atomic completion)
          const processingPromise = this.retryIfFailed(
            () =>
              this.processJob(
                job!,
                () => asyncFifoQueue.numTotal() <= this.concurrency,
                this.jobsInProgress,
              ),
            this.delay.bind(this),
          ).then((chainedJob) => {
            // If atomic completion returned a chained job, return it as the result
            return chainedJob;
          });

          asyncFifoQueue.add(processingPromise);
        }
      } catch (err) {
        // Distinguish between connection errors (retry) and other errors (log and continue)
        const isConnErr = this.q.isConnectionError(err);

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
    }

    this.logger.info(`Stopped`);
  }

  private async delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  private async retryIfFailed<T>(
    fn: () => Promise<T>,
    delayFn: (ms: number) => Promise<void>,
  ): Promise<T> {
    try {
      return await fn();
    } catch (err) {
      // Don't emit here - let AsyncFifoQueue handle it with ignoreErrors
      // The error will be logged at a higher level if needed
      throw err;
    }
  }

  /**
   * Process a job and return the next job if atomic completion succeeds
   * This matches BullMQ's processJob signature
   */
  private async processJob(
    job: ReservedJob<T>,
    fetchNextCallback: () => boolean,
    jobsInProgress: Set<{ job: ReservedJob<T>; ts: number }>,
  ): Promise<void | ReservedJob<T>> {
    const inProgressItem = { job, ts: Date.now() };
    jobsInProgress.add(inProgressItem);

    try {
      const nextJob = await this.processOne(job, fetchNextCallback);
      // If processOne returns a chained job from atomic completion, return it
      // so it can be added back to asyncFifoQueue
      return nextJob;
    } finally {
      jobsInProgress.delete(inProgressItem);
    }
  }

  /**
   * Complete a job and try to atomically get next job from same group
   */
  private async completeJob(
    job: ReservedJob<T>,
    handlerResult: unknown,
    fetchNextCallback?: () => boolean,
  ): Promise<ReservedJob<T> | undefined> {
    if (this.atomicCompletion && fetchNextCallback?.()) {
      // Try atomic completion with next job reservation
      try {
        const nextJob = await this.q.completeAndReserveNext(
          job.id,
          job.groupId,
        );
        if (nextJob) {
          this.logger.debug(
            `Got next job ${nextJob.id} from same group ${nextJob.groupId} atomically`,
          );
          return nextJob;
        }
      } catch (err) {
        this.logger.warn(
          `CompleteAndReserveNext failed, falling back to regular complete:`,
          err,
        );
        // Fallback to regular completion
        await this.q.complete(job);
      }
    } else {
      // Use regular completion (no atomic chaining)
      await this.q.complete(job);
    }

    return undefined;
  }

  /**
   * Record job completion for inspection
   */
  private async recordCompletion(
    job: ReservedJob<T>,
    handlerResult: unknown,
    processedOn: number,
    finishedOn: number,
  ): Promise<void> {
    try {
      await this.q.recordCompleted(
        { id: job.id, groupId: job.groupId },
        handlerResult,
        {
          processedOn,
          finishedOn,
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

    if (
      this.blockingStats.consecutiveEmptyReserves > 50 &&
      this.shouldWarnAboutEmptyReserves()
    ) {
      // Too many empty reserves (but queue might have jobs)
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
        `  🔄 Currently Processing: ${this.jobsInProgress.size} jobs`,
      );
      if (this.jobsInProgress.size > 0) {
        const jobDetails = Array.from(this.jobsInProgress).map((item) => {
          const processingTime = now - item.ts;
          return `${item.job.id} (${Math.round(processingTime / 1000)}s)`;
        });
        this.logger.info(`  📋 Processing: ${jobDetails.join(', ')}`);
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
      isProcessing: this.jobsInProgress.size > 0,
      jobsInProgressCount: this.jobsInProgress.size,
      jobsInProgress: Array.from(this.jobsInProgress).map((item) => ({
        jobId: item.job.id,
        groupId: item.job.groupId,
        processingTimeMs: now - item.ts,
      })),
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

    // Note: asyncFifoQueue and jobsInProgress are local to run() method
    // Worker loop will naturally drain when stopping = true
    // Wait for jobs to finish
    const startTime = Date.now();
    while (
      this.jobsInProgress.size > 0 &&
      Date.now() - startTime < gracefulTimeoutMs
    ) {
      await sleep(100);
    }

    if (this.jobsInProgress.size > 0) {
      this.logger.warn(
        `Worker stopped with ${this.jobsInProgress.size} jobs still processing after ${gracefulTimeoutMs}ms timeout.`,
      );
      // Emit graceful-timeout event for each job still processing
      const nowWall = Date.now();
      for (const item of this.jobsInProgress) {
        this.emit(
          'graceful-timeout',
          Job.fromReserved(this.q, item.job, {
            processedOn: item.ts,
            finishedOn: nowWall,
            status: 'active',
          }),
        );
      }
    }

    // Clear tracking
    this.jobsInProgress.clear();
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
   * Get information about the first currently processing job (if any)
   * For concurrency > 1, returns the oldest job in progress
   */
  getCurrentJob(): { job: ReservedJob<T>; processingTimeMs: number } | null {
    if (this.jobsInProgress.size === 0) {
      return null;
    }

    // Return the oldest job (first one added to the set)
    const oldest = Array.from(this.jobsInProgress)[0];
    const now = Date.now();
    return {
      job: oldest.job,
      processingTimeMs: now - oldest.ts,
    };
  }

  /**
   * Get information about all currently processing jobs
   */
  getCurrentJobs(): Array<{ job: ReservedJob<T>; processingTimeMs: number }> {
    const now = Date.now();
    return Array.from(this.jobsInProgress).map((item) => ({
      job: item.job,
      processingTimeMs: now - item.ts,
    }));
  }

  /**
   * Check if the worker is currently processing any jobs
   */
  isProcessing(): boolean {
    return this.jobsInProgress.size > 0;
  }

  // Proxy to the underlying queue.add with correct data typing inferred from the queue
  async add(opts: AddOptions<T>) {
    return this.q.add(opts);
  }

  private async processOne(
    job: ReservedJob<T>,
    fetchNextCallback?: () => boolean,
  ): Promise<void | ReservedJob<T>> {
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
      // Execute the user's handler
      startHeartbeat();
      const handlerResult = await this.handler(job);
      clearInterval(hbTimer!);

      // Complete the job and optionally get next job from same group
      const nextJob = await this.completeJob(
        job,
        handlerResult,
        fetchNextCallback,
      );

      // Emit completed event
      const finishedAtWall = Date.now();
      this.emit(
        'completed',
        Job.fromReserved(this.q, job, {
          processedOn: jobStartWallTime,
          finishedOn: finishedAtWall,
          returnvalue: handlerResult,
          status: 'completed',
        }),
      );

      // Record completion for inspection
      await this.recordCompletion(
        job,
        handlerResult,
        jobStartWallTime,
        finishedAtWall,
      );

      // Return chained job if available and we have capacity
      return nextJob;
    } catch (err) {
      clearInterval(hbTimer!);
      await this.handleJobFailure(err, job, jobStartWallTime);
    }
  }

  /**
   * Handle job failure: emit events, retry or dead-letter
   */
  private async handleJobFailure(
    err: unknown,
    job: ReservedJob<T>,
    jobStartWallTime: number,
  ): Promise<void> {
    this.onError?.(err, job);

    // Safely emit error event
    try {
      this.emit('error', err instanceof Error ? err : new Error(String(err)));
    } catch (_emitError) {
      // Silently ignore emit errors
    }

    const failedAt = Date.now();

    // Emit failed event
    this.emit(
      'failed',
      Job.fromReserved(this.q, job, {
        processedOn: jobStartWallTime,
        finishedOn: failedAt,
        failedReason: err instanceof Error ? err.message : String(err),
        stacktrace:
          err instanceof Error
            ? err.stack
            : typeof err === 'object' && err !== null
              ? (err as any).stack
              : undefined,
        status: 'failed',
      }),
    );

    // Calculate next attempt and backoff
    const nextAttempt = job.attempts + 1;
    const backoffMs = this.backoff(nextAttempt);

    // Check if we should dead-letter (max attempts reached)
    if (nextAttempt >= this.maxAttempts) {
      await this.deadLetterJob(
        err,
        job,
        jobStartWallTime,
        failedAt,
        nextAttempt,
      );
      return;
    }

    // Retry the job
    const retryResult = await this.q.retry(job.id, backoffMs);
    if (retryResult === -1) {
      // Queue-level max attempts exceeded
      await this.deadLetterJob(
        err,
        job,
        jobStartWallTime,
        failedAt,
        job.maxAttempts,
      );
      return;
    }

    // Record attempt failure
    await this.recordFailureAttempt(
      err,
      job,
      jobStartWallTime,
      failedAt,
      nextAttempt,
    );
  }

  /**
   * Dead-letter a job that exceeded max attempts
   */
  private async deadLetterJob(
    err: unknown,
    job: ReservedJob<T>,
    processedOn: number,
    finishedOn: number,
    attempts: number,
  ): Promise<void> {
    this.logger.info(
      `Dead lettering job ${job.id} from group ${job.groupId} (attempts: ${attempts}/${job.maxAttempts})`,
    );

    const errObj = err instanceof Error ? err : new Error(String(err));

    try {
      await this.q.recordFinalFailure(
        { id: job.id, groupId: job.groupId },
        { name: errObj.name, message: errObj.message, stack: errObj.stack },
        {
          processedOn,
          finishedOn,
          attempts,
          maxAttempts: job.maxAttempts,
          data: job.data,
        },
      );
    } catch (e) {
      this.logger.warn('Failed to record final failure', e);
    }

    await this.q.deadLetter(job.id, job.groupId);
  }

  /**
   * Record a failed attempt (not final)
   */
  private async recordFailureAttempt(
    err: unknown,
    job: ReservedJob<T>,
    processedOn: number,
    finishedOn: number,
    attempts: number,
  ): Promise<void> {
    const errObj = err instanceof Error ? err : new Error(String(err));

    try {
      await this.q.recordAttemptFailure(
        { id: job.id, groupId: job.groupId },
        { name: errObj.name, message: errObj.message, stack: errObj.stack },
        {
          processedOn,
          finishedOn,
          attempts,
          maxAttempts: job.maxAttempts,
        },
      );
    } catch (e) {
      this.logger.warn('Failed to record attempt failure', e);
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
