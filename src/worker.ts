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

/**
 * Configuration options for a GroupMQ Worker
 *
 * @template T The type of data stored in jobs
 */
export type WorkerOptions<T> = {
  /** The queue instance this worker will process jobs from */
  queue: Queue<T>;

  /**
   * Optional worker name for logging and identification
   * @default queue.name
   */
  name?: string;

  /**
   * The function that processes jobs. Must be async and handle job failures gracefully.
   * @param job The reserved job to process
   * @returns Promise that resolves when job is complete
   */
  handler: (job: ReservedJob<T>) => Promise<unknown>;

  /**
   * Heartbeat interval in milliseconds to keep jobs alive during processing.
   * Prevents jobs from timing out during long-running operations.
   *
   * @default Math.max(1000, queue.jobTimeoutMs / 3)
   * @example 5000 // Heartbeat every 5 seconds
   *
   * **When to adjust:**
   * - Long-running jobs: Increase to reduce Redis overhead
   * - Short jobs: Decrease for faster timeout detection
   * - High job volume: Increase to reduce Redis commands
   */
  heartbeatMs?: number;

  /**
   * Error handler called when job processing fails or worker encounters errors
   * @param err The error that occurred
   * @param job The job that failed (if applicable)
   */
  onError?: (err: unknown, job?: ReservedJob<T>) => void;

  /**
   * Maximum number of retry attempts for failed jobs at the worker level.
   * This overrides the queue's default maxAttempts setting.
   *
   * @default queue.maxAttemptsDefault
   * @example 5 // Retry failed jobs up to 5 times
   *
   * **When to adjust:**
   * - Critical jobs: Increase for more retries
   * - Non-critical jobs: Decrease to fail faster
   * - External API calls: Consider network reliability
   */
  maxAttempts?: number;

  /**
   * Backoff strategy for retrying failed jobs. Determines delay between retries.
   *
   * @default Exponential backoff with jitter (500ms, 1s, 2s, 4s, 8s, 16s, 30s max)
   * @example (attempt) => Math.min(10000, attempt * 1000) // Linear backoff
   *
   * **When to adjust:**
   * - Rate-limited APIs: Use longer delays
   * - Database timeouts: Use shorter delays
   * - External services: Consider their retry policies
   */
  backoff?: BackoffStrategy;

  /**
   * Whether to enable automatic cleanup of expired and completed jobs.
   * Cleanup removes old jobs to prevent Redis memory growth.
   *
   * @default true
   * @example false // Disable if you handle cleanup manually
   *
   * **When to disable:**
   * - Manual cleanup: If you have your own cleanup process
   * - Job auditing: If you need to keep all job history
   * - Development: For debugging job states
   */
  enableCleanup?: boolean;

  /**
   * Interval in milliseconds between cleanup operations.
   * Cleanup removes expired jobs and trims completed/failed job retention.
   *
   * @default 300000 (5 minutes)
   * @example 600000 // Cleanup every 10 minutes
   *
   * **When to adjust:**
   * - High job volume: Increase to reduce Redis overhead
   * - Low job volume: Decrease for more frequent cleanup
   * - Memory constraints: Decrease to prevent Redis memory growth
   * - Job retention needs: Adjust based on keepCompleted/keepFailed settings
   */
  cleanupIntervalMs?: number;

  /**
   * Interval in milliseconds between scheduler operations.
   * Scheduler promotes delayed jobs and processes cron/repeating jobs.
   *
   * @default Adaptive: min(5000ms, max(1000ms, schedulerBufferMs * 0.8)) with 1000ms minimum cap
   * @example 1000 // For fast cron jobs (every minute or less)
   * @example 10000 // For slow cron jobs (hourly or daily)
   *
   * **When to adjust:**
   * - Fast cron jobs: Decrease (1000-2000ms) for sub-minute schedules
   * - Slow cron jobs: Increase (10000-60000ms) to reduce Redis overhead
   * - No cron jobs: Increase (5000-10000ms) since only delayed jobs are affected
   * - Scheduler buffering (`orderingMethod: 'scheduler'`): Automatically adjusted to prevent ordering stalls
   *
   * **Important:** The scheduler interval is automatically capped at 80% of the scheduler buffer window
   * (with a 1000ms minimum) to ensure buffered jobs are promoted promptly and don't cause
   * ordering stalls, while preventing excessive Redis load from very frequent scheduler runs.
   */
  schedulerIntervalMs?: number;

  /**
   * Maximum time in seconds to wait for new jobs when queue is empty.
   * Shorter timeouts make workers more responsive but use more Redis resources.
   *
   * @default 2
   * @example 1 // More responsive, higher Redis usage
   * @example 5 // Less responsive, lower Redis usage
   *
   * **When to adjust:**
   * - High job volume: Decrease (1-2s) for faster job pickup
   * - Low job volume: Increase (3-5s) to reduce Redis overhead
   * - Real-time requirements: Decrease for lower latency
   * - Resource constraints: Increase to reduce Redis load
   */
  blockingTimeoutSec?: number;

  /**
   * Whether to use atomic completion for better performance and consistency.
   * Atomic completion completes a job and reserves the next job from the same group in one operation.
   *
   * @default true
   * @example false // Disable for debugging or compatibility
   *
   * **When to disable:**
   * - Debugging: To isolate completion vs reservation issues
   * - Compatibility: If you have custom completion logic
   * - Single concurrency: Less benefit with concurrency=1
   */
  atomicCompletion?: boolean;

  /**
   * Logger configuration for worker operations and debugging.
   *
   * @default false (no logging)
   * @example true // Enable basic logging
   * @example customLogger // Use custom logger instance
   *
   * **When to enable:**
   * - Development: For debugging job processing
   * - Production monitoring: For operational insights
   * - Troubleshooting: When investigating performance issues
   */
  logger?: LoggerInterface | true;

  /**
   * Number of jobs this worker can process concurrently.
   * Higher concurrency increases throughput but uses more memory and CPU.
   *
   * @default 1
   * @example 4 // Process 4 jobs simultaneously
   * @example 8 // For CPU-intensive jobs on multi-core systems
   *
   * **When to adjust:**
   * - CPU-bound jobs: Set to number of CPU cores
   * - I/O-bound jobs: Set to 2-4x number of CPU cores
   * - Memory constraints: Lower concurrency to reduce memory usage
   * - High job volume: Increase for better throughput
   * - Single-threaded requirements: Keep at 1
   */
  concurrency?: number;
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
    consecutiveEmptyReserves: 0,
    lastActivityTime: Date.now(),
  };
  private emptyReserveBackoffMs = 0;
  private stuckDetectionTimer?: NodeJS.Timeout;
  private redisCloseHandler?: () => void;
  private redisErrorHandler?: (error: Error) => void;
  private redisReadyHandler?: () => void;
  private runLoopPromise?: Promise<void>;

  // Track when we started collecting for each group (for in-memory ordering)
  // This ensures we wait the full grace period from when we FIRST picked up a job from the group
  private groupCollectionStartTimes = new Map<string, number>();

  constructor(opts: WorkerOptions<T>) {
    super();

    if (!opts.handler || typeof opts.handler !== 'function') {
      throw new Error('Worker handler must be a function');
    }

    this.q = opts.queue;
    this.name = opts.name ?? this.q.name;
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
    this.cleanupMs = opts.cleanupIntervalMs ?? 60_000; // 1 minutes for high-concurrency production

    // Ensure scheduler runs more frequently than ordering delay to prevent stalls
    // But only if scheduler buffering is meaningful (>= 100ms)
    const defaultSchedulerMs = 5000; // 5 seconds for high-concurrency production
    const minSchedulerMs = 1000; // Minimum 1 second to prevent excessive Redis load
    const schedulerBufferMs = this.q.schedulerBufferMs || 0;
    this.schedulerMs =
      opts.schedulerIntervalMs ??
      (schedulerBufferMs >= 100
        ? Math.min(
            defaultSchedulerMs,
            Math.max(minSchedulerMs, schedulerBufferMs * 0.8),
          )
        : defaultSchedulerMs);

    this.blockingTimeoutSec = opts.blockingTimeoutSec ?? 5; // 5s for high-concurrency production
    // With AsyncFifoQueue, we can safely use atomic completion for all concurrency levels
    this.atomicCompletion = opts.atomicCompletion ?? true;
    this.concurrency = Math.max(1, opts.concurrency ?? 1);

    // Set up Redis connection event handlers
    this.setupRedisEventHandlers();
  }

  get isClosed() {
    return this.closed;
  }

  /**
   * Add jitter to prevent thundering herd problems in high-concurrency environments
   * @param baseInterval The base interval in milliseconds
   * @param jitterPercent Percentage of jitter to add (0-1, default 0.1 for 10%)
   * @returns The interval with jitter applied
   */
  private addJitter(baseInterval: number, jitterPercent = 0.1): number {
    const jitter = Math.random() * baseInterval * jitterPercent;
    return baseInterval + jitter;
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

  async run(): Promise<void> {
    // Store the run loop promise so close() can wait for it
    const runPromise = this._runLoop();
    this.runLoopPromise = runPromise;
    return runPromise;
  }

  private async _runLoop(): Promise<void> {
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
      // Add jitter to prevent all workers from running cleanup simultaneously
      this.cleanupTimer = setInterval(async () => {
        try {
          await this.q.cleanup();
        } catch (err) {
          this.onError?.(err);
        }
      }, this.addJitter(this.cleanupMs));

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
      }, this.addJitter(schedulerInterval));
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
        // Phase 1: Fetch jobs efficiently until we reach concurrency capacity
        while (!this.stopping && asyncFifoQueue.numTotal() < this.concurrency) {
          this.blockingStats.totalBlockingCalls++;

          this.logger.debug(
            `Fetching job (call #${this.blockingStats.totalBlockingCalls}, queue: ${asyncFifoQueue.numTotal()}/${this.concurrency})...`,
          );

          // Try batch reserve first for better efficiency (when concurrency > 1)
          if (this.concurrency > 1 && asyncFifoQueue.numTotal() === 0) {
            const batchSize = Math.min(this.concurrency, 8); // Cap at 8 for efficiency
            const batchJobs = await this.q.reserveBatch(batchSize);

            if (batchJobs.length > 0) {
              this.logger.debug(`Batch reserved ${batchJobs.length} jobs`);
              for (const job of batchJobs) {
                asyncFifoQueue.add(Promise.resolve(job));
              }
              // Reset counters for successful batch
              connectionRetries = 0;
              blockUntil = 0;
              this.lastJobPickupTime = Date.now();
              this.blockingStats.consecutiveEmptyReserves = 0;
              this.blockingStats.lastActivityTime = Date.now();
              this.emptyReserveBackoffMs = 0;
              continue; // Skip individual reserve
            }
          }

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
            this.emptyReserveBackoffMs = 0; // Reset backoff when we get a job

            this.logger.debug(
              `Fetched job ${job.id} from group ${job.groupId}`,
            );
          } else {
            // No more jobs available
            this.blockingStats.consecutiveEmptyReserves++;

            // Only log every 50th empty reserve to reduce spam, or on important milestones
            if (
              this.blockingStats.consecutiveEmptyReserves % 50 === 0 ||
              this.blockingStats.consecutiveEmptyReserves % 100 === 0
            ) {
              this.logger.debug(
                `No job available (consecutive empty: ${this.blockingStats.consecutiveEmptyReserves})`,
              );
            }

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

            // Exponential backoff when no jobs are available to prevent tight polling
            // Start backoff after just 3 consecutive empty reserves (more aggressive than before)
            if (this.blockingStats.consecutiveEmptyReserves > 3) {
              // More aggressive backoff for high-concurrency: start at 100ms, grow faster, cap at 5000ms
              if (this.emptyReserveBackoffMs === 0) {
                this.emptyReserveBackoffMs = 100; // Start at 100ms for high concurrency
              } else {
                this.emptyReserveBackoffMs = Math.min(
                  5000, // Cap at 5 seconds for high concurrency
                  Math.max(100, this.emptyReserveBackoffMs * 1.5), // Grow by 50% each time
                );
              }

              // Only log backoff every 20th time to reduce spam
              if (this.blockingStats.consecutiveEmptyReserves % 20 === 0) {
                this.logger.debug(
                  `Applying backoff: ${Math.round(this.emptyReserveBackoffMs)}ms before next reserve attempt (consecutive empty: ${this.blockingStats.consecutiveEmptyReserves})`,
                );
              }

              await this.delay(this.emptyReserveBackoffMs);
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
    processedOn?: number,
    finishedOn?: number,
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
        // Fallback to regular completion (without metadata - will be added by recordCompletion)
        await this.q.complete(job);
      }
    } else {
      // Use efficient combined completion if we have timing info
      // Note: We'll still call recordCompletion separately for now to maintain compatibility
      // TODO: In next major version, always use completeWithMetadata and remove recordCompletion call
      await this.q.complete(job);
    }

    return undefined;
  }

  /**
   * Record job completion for inspection
   *
   * During graceful shutdown, we continue recording completions for jobs that finish
   * within the timeout window. Only after the graceful timeout expires (closed=true)
   * do we skip recording to avoid connection errors.
   */
  private async recordCompletion(
    job: ReservedJob<T>,
    handlerResult: unknown,
    processedOn: number,
    finishedOn: number,
  ): Promise<void> {
    // Only skip recording if worker is fully closed (after graceful timeout expired)
    // During graceful shutdown (stopping=true, closed=false), we still record completions
    if (this.closed) {
      return;
    }

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
      // Suppress connection errors during shutdown - they're expected when Redis is closed
      const isConnectionError =
        e instanceof Error && e.message.includes('Connection is closed');

      // Only log if it's not a connection error during shutdown
      if (!isConnectionError || !this.stopping) {
        console.error(
          `💥 CRITICAL: Failed to record completion for job ${job.id}:`,
          e,
        );
        this.logger.warn('Failed to record completion', e);
      }
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
    // Give some time if we just received a job
    // Otherwise jobsInProgress will be 0 and we will exit immediately
    await this.delay(100);

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

    // Wait for jobs to finish first
    const startTime = Date.now();
    while (
      this.jobsInProgress.size > 0 &&
      Date.now() - startTime < gracefulTimeoutMs
    ) {
      await sleep(100);
    }

    // Close the blocking client immediately to interrupt any blocking operations
    // This will cause the run loop to exit faster
    if (this.blockingClient) {
      try {
        // Use disconnect() instead of quit() for immediate termination
        this.blockingClient.disconnect();
      } catch (_e) {
        // ignore blocking client close errors
      }
      this.blockingClient = null;
    }

    // Now wait for the run loop to fully exit, but with a much shorter timeout
    // Since we closed the blocking client, the run loop should exit immediately
    if (this.runLoopPromise) {
      const runLoopTimeout =
        this.jobsInProgress.size > 0
          ? gracefulTimeoutMs // If jobs are still running, use full timeout
          : 2000; // Run loop should exit in 2 seconds after blocking client is closed

      const timeoutPromise = new Promise<void>((resolve) => {
        setTimeout(resolve, runLoopTimeout);
      });

      try {
        await Promise.race([this.runLoopPromise, timeoutPromise]);
      } catch (err) {
        this.logger.warn('Error while waiting for run loop to exit:', err);
      }
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

    // Blocking client was already closed earlier to interrupt blocking operations

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

  /**
   * Collect jobs from same group during grace period
   *
   * How it works:
   * 1. Worker picks up first job and holds it in memory
   * 2. Waits for the grace period to allow concurrent enqueue operations to finish
   * 3. When new jobs are detected, reset the timer (with decay) and wait again
   * 4. Repeat until no new jobs arrive for the full grace period
   * 5. Process all collected jobs sorted by orderMs
   *
   * This solves the race condition where jobs are enqueued within 1-2ms:
   * - Worker reserves job 1 while jobs 2-4 are still being written to Redis
   * - By always waiting the grace period, we give concurrent writes time to complete
   * - Then we collect all jobs that finished enqueueing during our wait
   *
   * NOTE: The grace window is measured from when WE picked up the job (Date.now()),
   * NOT from when the job was enqueued. This is critical for handling concurrent enqueues.
   */
  private async collectJobsWithGrace(
    firstJob: ReservedJob<T>,
  ): Promise<ReservedJob<T>[]> {
    const initialGraceMs = this.q.graceCollectionMs;

    if (initialGraceMs <= 0) {
      return [firstJob];
    }

    const groupId = firstJob.groupId;
    const collected: ReservedJob<T>[] = [firstJob];
    const now = Date.now();

    // Check if we're already in a collection window for this group
    let collectionStartTime = this.groupCollectionStartTimes.get(groupId);

    if (!collectionStartTime) {
      // First job from this group - start tracking the collection window
      collectionStartTime = now;
      this.groupCollectionStartTimes.set(groupId, collectionStartTime);

      this.logger.debug(
        `Started collection window for group ${groupId} (${initialGraceMs}ms grace period)`,
      );
    } else {
      // We're continuing an existing collection for this group
      // Check if we've already waited long enough
      const elapsedSinceStart = now - collectionStartTime;
      if (elapsedSinceStart >= initialGraceMs) {
        this.logger.debug(
          `Collection window for group ${groupId} already expired (${elapsedSinceStart}ms elapsed), processing immediately`,
        );
        return [firstJob];
      }

      this.logger.debug(
        `Continuing collection for group ${groupId} (${elapsedSinceStart}ms elapsed)`,
      );
    }

    let lastSeenCount = 0;
    let lastJobArrivalTime = now; // Reset every time we detect new jobs
    let currentGraceMs = initialGraceMs; // Decaying grace window
    const minGraceMs = 20; // Minimum wait time
    const decayFactor = this.q.orderingGracePeriodDecay;

    // ALWAYS wait the grace period when we first pick up a job
    // This gives concurrent enqueue operations time to finish writing to Redis
    // The goal is to collect ALL jobs that exist at the END of the grace period

    // Keep checking for NEW jobs until grace period expires with no new arrivals
    while (Date.now() - lastJobArrivalTime < currentGraceMs) {
      await this.delay(10); // Check every 10ms

      const currentCount = await this.q.getGroupJobCount(groupId);

      if (currentCount > lastSeenCount) {
        // New jobs arrived! Reset the grace period timer with decay
        const newJobsCount = currentCount - lastSeenCount;
        lastSeenCount = currentCount;
        lastJobArrivalTime = Date.now(); // RESET timer to when we detected new jobs

        // Apply decay: reduce the grace window for the next iteration
        currentGraceMs = Math.max(minGraceMs, currentGraceMs * decayFactor);

        this.logger.debug(
          `${newJobsCount} new job(s) detected in group ${groupId}, resetting grace period to ${Math.round(currentGraceMs)}ms (${Math.round((currentGraceMs / initialGraceMs) * 100)}% of original)`,
        );
      }

      // Safety: don't wait forever (max multiplier from queue config)
      const maxWaitMs = initialGraceMs * this.q.orderingMaxWaitMultiplier;
      const totalWaitTime = Date.now() - collectionStartTime;
      if (totalWaitTime > maxWaitMs) {
        this.logger.warn(
          `Grace period exceeded ${this.q.orderingMaxWaitMultiplier}x limit (${maxWaitMs}ms) for group ${groupId} (waited ${totalWaitTime}ms), proceeding`,
        );
        break;
      }
    }

    // Collect ALL remaining jobs in the group (up to configured batch limit)
    // These are the jobs that were there when the grace period ended
    const finalCount = await this.q.getGroupJobCount(groupId);
    const maxBatchSize = this.q.orderingMaxBatchSize;
    const jobsToCollect = Math.min(finalCount, maxBatchSize);

    this.logger.debug(
      `Grace period complete for group ${groupId}, collecting ${jobsToCollect} of ${finalCount} remaining jobs (max batch: ${maxBatchSize})`,
    );

    // Collect remaining jobs from the group (up to max batch size)
    for (let i = 0; i < jobsToCollect; i++) {
      try {
        // Reserve next job from this group, passing first job's ID to bypass lock
        const nextJob = await this.reserveNextFromSameGroup(
          groupId,
          firstJob.id,
        );
        if (!nextJob) break;

        collected.push(nextJob);
      } catch (err) {
        this.logger.warn(`Error collecting additional job: ${err}`);
        break;
      }
    }

    const oldCollected = collected.slice();

    // Sort all collected jobs by orderMs
    // Handle NaN values by treating them as 0 (or using timestamp as fallback)
    collected.sort((a, b) => {
      const aOrder = Number.isNaN(a.orderMs) ? a.timestamp : a.orderMs;
      const bOrder = Number.isNaN(b.orderMs) ? b.timestamp : b.orderMs;
      return aOrder - bOrder;
    });

    if (collected.length > 1) {
      this.logger.info(
        `Collected ${collected.length} jobs from group ${groupId} during ${Date.now() - now}ms grace period`,
        {
          unsorted: oldCollected.map((job) => job.id),
          sorted: collected.map((job, index) => ({
            jobId: job.id,
            order: index,
            groupId: job.groupId,
            orderMs: job.orderMs,
            timestamp: job.timestamp,
          })),
        },
      );
    }

    // Clear the tracking for this group after we've collected the batch
    // This allows the next batch to start fresh
    this.groupCollectionStartTimes.delete(groupId);

    return collected;
  }

  /**
   * Reserve next job from same group (helper for grace collection)
   *
   * Passes the initial job ID to bypass the lock check, allowing us to
   * reserve multiple jobs from the same group during grace collection.
   */
  private async reserveNextFromSameGroup(
    groupId: string,
    allowedJobId: string,
  ): Promise<ReservedJob<T> | null> {
    // Pass the initial job ID - if the lock matches, we can reserve
    return await this.q.reserveAtomic(groupId, allowedJobId);
  }

  private async processOne(
    job: ReservedJob<T>,
    fetchNextCallback?: () => boolean,
  ): Promise<void | ReservedJob<T>> {
    // Collect jobs during grace period if enabled
    const jobsToProcess = await this.collectJobsWithGrace(job);

    // Process each job in order
    let nextJob: ReservedJob<T> | undefined | void;
    for (let i = 0; i < jobsToProcess.length; i++) {
      const currentJob = jobsToProcess[i];
      const isLastJob = i === jobsToProcess.length - 1;

      nextJob = await this.processSingleJob(
        currentJob,
        isLastJob ? fetchNextCallback : undefined,
      );
    }

    // Return the chained job from the last processed job (if any)
    return nextJob;
  }

  private async processSingleJob(
    job: ReservedJob<T>,
    fetchNextCallback?: () => boolean,
  ): Promise<void | ReservedJob<T>> {
    const jobStartWallTime = Date.now();

    let hbTimer: NodeJS.Timeout | undefined;
    let heartbeatDelayTimer: NodeJS.Timeout | undefined;

    const startHeartbeat = () => {
      // More conservative heartbeat: use 1/4 of job timeout instead of 1/2
      // This reduces Redis calls while still providing safety
      const minInterval = Math.max(
        this.hbMs,
        Math.floor((this.q.jobTimeoutMs || 30000) / 4),
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
      // Smart heartbeat: only start for jobs that might actually timeout
      // Skip heartbeat for short jobs (< jobTimeoutMs / 3) to reduce Redis load
      const jobTimeout = this.q.jobTimeoutMs || 30000;
      const heartbeatThreshold = jobTimeout / 3;

      // Start heartbeat after threshold delay for potentially long-running jobs
      heartbeatDelayTimer = setTimeout(() => {
        startHeartbeat();
      }, heartbeatThreshold);

      // Execute the user's handler
      const handlerResult = await this.handler(job);

      // Job finished quickly, cancel delayed heartbeat start
      if (heartbeatDelayTimer) {
        clearTimeout(heartbeatDelayTimer);
      }

      // Clean up heartbeat if it was started
      if (hbTimer) {
        clearInterval(hbTimer);
      }

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
      // Clean up timers
      if (heartbeatDelayTimer) {
        clearTimeout(heartbeatDelayTimer);
      }
      if (hbTimer) {
        clearInterval(hbTimer);
      }
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
