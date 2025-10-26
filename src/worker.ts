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
  stalled: (jobId: string, groupId: string) => void;
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
   * @default 5000 (5 seconds)
   * @example 1000 // For fast cron jobs (every minute or less)
   * @example 10000 // For slow cron jobs (hourly or daily)
   *
   * **When to adjust:**
   * - Fast cron jobs: Decrease (1000-2000ms) for sub-minute schedules
   * - Slow cron jobs: Increase (10000-60000ms) to reduce Redis overhead
   * - No cron jobs: Increase (5000-10000ms) since only delayed jobs are affected
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

  /**
   * Interval in milliseconds between stalled job checks.
   * Stalled jobs are those whose worker crashed or lost connection.
   *
   * @default 30000 (30 seconds)
   * @example 60000 // Check every minute for lower overhead
   * @example 10000 // Check every 10 seconds for faster recovery
   *
   * **When to adjust:**
   * - Fast recovery needed: Decrease (10-20s)
   * - Lower Redis overhead: Increase (60s+)
   * - Unreliable workers: Decrease for faster detection
   */
  stalledInterval?: number;

  /**
   * Maximum number of times a job can become stalled before being failed.
   * A job becomes stalled when its worker crashes or loses connection.
   *
   * @default 1
   * @example 2 // Allow jobs to stall twice before failing
   * @example 0 // Never fail jobs due to stalling (not recommended)
   *
   * **When to adjust:**
   * - Unreliable infrastructure: Increase to tolerate more failures
   * - Critical jobs: Increase to allow more recovery attempts
   * - Quick failure detection: Keep at 1
   */
  maxStalledCount?: number;

  /**
   * Grace period in milliseconds before a job is considered stalled.
   * Jobs are only marked as stalled if their deadline has passed by this amount.
   *
   * @default 0 (no grace period)
   * @example 5000 // 5 second grace period for clock skew
   * @example 1000 // 1 second grace for network latency
   *
   * **When to adjust:**
   * - Clock skew between servers: Add 1-5s grace
   * - Network latency: Add 1-2s grace
   * - Strict timing: Keep at 0
   */
  stalledGracePeriod?: number;
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
  private concurrency: number;
  private blockingClient: import('ioredis').default | null = null;

  // Stalled job detection
  private stalledCheckTimer?: NodeJS.Timeout;
  private stalledInterval: number;
  private maxStalledCount: number;
  private stalledGracePeriod: number;

  // Track all jobs in progress (for all concurrency levels)
  private jobsInProgress = new Set<{ job: ReservedJob<T>; ts: number }>();

  // Blocking detection and monitoring
  private lastJobPickupTime = Date.now(); // Initialize to now so we start in "active" mode
  private totalJobsProcessed = 0;
  private blockingStats = {
    totalBlockingCalls: 0,
    consecutiveEmptyReserves: 0,
    lastActivityTime: Date.now(),
  };
  private emptyReserveBackoffMs = 0;

  private redisCloseHandler?: () => void;
  private redisErrorHandler?: (error: Error) => void;
  private redisReadyHandler?: () => void;
  private runLoopPromise?: Promise<void>;

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

    // Scheduler interval for delayed jobs and cron jobs
    const defaultSchedulerMs = 1000; // 1 second for responsive job processing
    this.schedulerMs = opts.schedulerIntervalMs ?? defaultSchedulerMs;

    this.blockingTimeoutSec = opts.blockingTimeoutSec ?? 5; // 5s to match BullMQ's drainDelay
    // With AsyncFifoQueue, we can safely use atomic completion for all concurrency levels
    this.concurrency = Math.max(1, opts.concurrency ?? 1);

    // Initialize stalled job detection settings
    // BullMQ-inspired: More conservative settings for high concurrency
    this.stalledInterval =
      opts.stalledInterval ?? (this.concurrency > 50 ? 60000 : 30000); // 60s for high concurrency, 30s otherwise
    this.maxStalledCount =
      opts.maxStalledCount ?? (this.concurrency > 50 ? 2 : 1); // Allow 2 stalls for high concurrency
    this.stalledGracePeriod =
      opts.stalledGracePeriod ?? (this.concurrency > 50 ? 5000 : 0); // 5s grace for high concurrency

    // Set up Redis connection event handlers
    this.setupRedisEventHandlers();

    // Auto-start promoter if orderingDelayMs is configured
    if (this.q.orderingDelayMs > 0) {
      this.q.startPromoter().catch((err) => {
        this.logger.error('Failed to start staging promoter:', err);
      });
    }

    this.run();
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
    if (this.runLoopPromise) {
      return this.runLoopPromise;
    }

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
        // Infinite retries for blocking connections to prevent "Max retries exceeded" errors
        maxRetriesPerRequest: null,
        // Exponential backoff retry strategy
        retryStrategy: (times: number) => {
          return Math.max(Math.min(Math.exp(times) * 1000, 20000), 1000);
        },
      });

      // Add reconnection handlers for resilience
      this.blockingClient.on('error', (err) => {
        if (!this.q.isConnectionError(err)) {
          this.logger.error('Blocking client error (non-connection):', err);
        } else {
          this.logger.warn('Blocking client connection error:', err.message);
        }
        this.emit('error', err instanceof Error ? err : new Error(String(err)));
      });

      this.blockingClient.on('close', () => {
        // Only log close if not during shutdown
        if (!this.stopping && !this.closed) {
          this.logger.warn(
            'Blocking client disconnected, will reconnect on next operation',
          );
        }
      });

      this.blockingClient.on('reconnecting', () => {
        if (!this.stopping && !this.closed) {
          this.logger.info('Blocking client reconnecting...');
        }
      });

      this.blockingClient.on('ready', () => {
        if (!this.stopping && !this.closed) {
          this.logger.info('Blocking client ready');
        }
      });
    } catch (err) {
      this.logger.error('Failed to create blocking client:', err);
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

    // Start stalled job checker for automatic recovery
    this.startStalledChecker();

    let connectionRetries = 0;
    const maxConnectionRetries = 10; // Allow more retries with exponential backoff

    // BullMQ-style async queue for clean promise management
    const asyncFifoQueue = new AsyncFifoQueue<void | ReservedJob<T> | null>(
      true,
    );

    while (!this.stopping || asyncFifoQueue.numTotal() > 0) {
      try {
        // Phase 1: Fetch jobs efficiently until we reach concurrency capacity
        while (!this.stopping && asyncFifoQueue.numTotal() < this.concurrency) {
          this.blockingStats.totalBlockingCalls++;

          // Prevent overflow: reset counter after 1 billion calls (keeps number manageable)
          if (this.blockingStats.totalBlockingCalls >= 1_000_000_000) {
            this.blockingStats.totalBlockingCalls = 0;
          }

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
              this.lastJobPickupTime = Date.now();
              this.blockingStats.consecutiveEmptyReserves = 0;
              this.blockingStats.lastActivityTime = Date.now();
              this.emptyReserveBackoffMs = 0;
              continue; // Skip individual reserve
            }
          }

          // BullMQ-style: only perform blocking reserve when truly drained
          // Require 2 consecutive empty reserves before considering queue drained
          // This prevents false positives from worker competition while staying responsive
          const allowBlocking =
            this.blockingStats.consecutiveEmptyReserves >= 2 &&
            asyncFifoQueue.numTotal() === 0;

          // Use a consistent blocking timeout - BullMQ style
          // Job completion resets consecutiveEmptyReserves to 0, ensuring fast pickup
          const adaptiveTimeout = this.blockingTimeoutSec;

          const fetchedJob = allowBlocking
            ? this.q.reserveBlocking(
                adaptiveTimeout,
                undefined, // blockUntil removed (was always 0, dead code)
                this.blockingClient ?? undefined,
              )
            : this.q.reserve();

          asyncFifoQueue.add(fetchedJob);

          // Sequential fetching: wait for this fetch before next (prevents thundering herd)
          const job = await fetchedJob;

          if (job) {
            // Reset connection retry count and empty reserves
            connectionRetries = 0;
            this.lastJobPickupTime = Date.now();
            this.blockingStats.consecutiveEmptyReserves = 0;
            this.blockingStats.lastActivityTime = Date.now();
            this.emptyReserveBackoffMs = 0; // Reset backoff when we get a job

            this.logger.debug(
              `Fetched job ${job.id} from group ${job.groupId}`,
            );
          } else {
            // No more jobs available - increment counter
            this.blockingStats.consecutiveEmptyReserves++;

            // Only log every 50th empty reserve to reduce spam
            if (this.blockingStats.consecutiveEmptyReserves % 50 === 0) {
              this.logger.debug(
                `No job available (consecutive empty: ${this.blockingStats.consecutiveEmptyReserves})`,
              );
            }

            // Only apply exponential backoff when queue is truly empty (no jobs processing)
            // This prevents slowdown during the tail end when a few jobs are still processing
            const backoffThreshold = this.concurrency >= 100 ? 5 : 3;
            if (
              this.blockingStats.consecutiveEmptyReserves > backoffThreshold &&
              asyncFifoQueue.numTotal() === 0 // Critical: only backoff when nothing is processing
            ) {
              // Adaptive backoff based on concurrency level
              const maxBackoff = this.concurrency >= 100 ? 2000 : 5000;
              if (this.emptyReserveBackoffMs === 0) {
                this.emptyReserveBackoffMs = this.concurrency >= 100 ? 100 : 50;
              } else {
                this.emptyReserveBackoffMs = Math.min(
                  maxBackoff,
                  Math.max(100, this.emptyReserveBackoffMs * 1.2),
                );
              }

              // Only log backoff every 20th time to reduce spam
              if (this.blockingStats.consecutiveEmptyReserves % 20 === 0) {
                this.logger.debug(
                  `Applying backoff: ${Math.round(this.emptyReserveBackoffMs)}ms (consecutive empty: ${this.blockingStats.consecutiveEmptyReserves}, jobs in progress: ${asyncFifoQueue.numTotal()})`,
                );
              }

              await this.delay(this.emptyReserveBackoffMs);
            }

            // BullMQ-inspired: Break immediately when no jobs found and queue is idle
            // This prevents tight polling and allows backoff to work properly
            if (asyncFifoQueue.numTotal() === 0) {
              break; // Fully idle - exit fetch loop
            }

            // If we have jobs processing, also break to process them
            if (asyncFifoQueue.numTotal() > 1) {
              break;
            }
          }
        }

        // Phase 2: BullMQ-style - Fetch ONE job and process immediately
        // This is more responsive than batching, especially at high concurrency
        let job: ReservedJob<T> | void;
        do {
          const fetchedJob = await asyncFifoQueue.fetch();
          job = fetchedJob ?? undefined;
        } while (!job && asyncFifoQueue.numQueued() > 0);

        if (job) {
          this.totalJobsProcessed++;
          this.logger.debug(
            `Processing job ${job.id} from group ${job.groupId} immediately`,
          );

          // Add processing promise immediately, don't wait for completion
          const processingPromise = this.processJob(
            job,
            () => asyncFifoQueue.numTotal() <= this.concurrency,
            this.jobsInProgress,
          );

          asyncFifoQueue.add(processingPromise);
        }
        // Note: No delay here - just loop back to Phase 1 immediately
        // The adaptive timeout in Phase 1's blocking reserve handles idle efficiently
      } catch (err) {
        if (this.stopping) {
          return;
        }
        // Distinguish between connection errors (retry) and other errors (log and continue)
        const isConnErr = this.q.isConnectionError(err);

        if (isConnErr) {
          // Connection error - retry with exponential backoff
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
            // Use maximum backoff delay before continuing
            await this.delay(20000);
            connectionRetries = 0; // Reset to continue trying
          } else {
            // Exponential backoff with 1s min, 20s max
            const delayMs = Math.max(
              Math.min(Math.exp(connectionRetries) * 1000, 20000),
              1000,
            );
            this.logger.debug(
              `Waiting ${Math.round(delayMs)}ms before retry (exponential backoff)`,
            );
            await this.delay(delayMs);
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
      const nextJob = await this.processSingleJob(job, fetchNextCallback);
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
    if (fetchNextCallback?.()) {
      // Try atomic completion with next job reservation
      const nextJob = await this.q.completeAndReserveNextWithMetadata(
        job.id,
        job.groupId,
        handlerResult,
        {
          processedOn: processedOn || Date.now(),
          finishedOn: finishedOn || Date.now(),
          attempts: job.attempts,
          maxAttempts: job.maxAttempts,
        },
      );
      if (nextJob) {
        this.logger.debug(
          `Got next job ${nextJob.id} from same group ${nextJob.groupId} atomically`,
        );
        return nextJob;
      }
      // CRITICAL FIX: If atomic completion failed, we need to check if the job was actually completed
      // The job completion happens BEFORE the next job reservation in the Lua script
      // So if it failed, the job might still be completed in Redis
      this.logger.debug(
        `Atomic completion failed for job ${job.id}, checking if job was completed in Redis`,
      );

      // Check if the job is still in processing - if not, it was completed
      const isStillProcessing = await this.q.isJobProcessing(job.id);
      if (!isStillProcessing) {
        this.logger.debug(
          `Job ${job.id} was completed in Redis despite atomic failure, group ${job.groupId} should be unlocked`,
        );
        // Job was completed, just ensure group is unlocked for next job
        await this.q.complete(job);
      } else {
        this.logger.warn(
          `Job ${job.id} is still in processing after atomic failure - this should not happen`,
        );
        // Fallback: complete the job normally
        await this.q.completeWithMetadata(job, handlerResult, {
          processedOn: processedOn || Date.now(),
          finishedOn: finishedOn || Date.now(),
          attempts: job.attempts,
          maxAttempts: job.maxAttempts,
        });
      }

      // CRITICAL: For high concurrency, add a small delay to prevent thundering herd
      // This reduces the chance of multiple workers hitting the same race condition
      if (Math.random() < 0.1) {
        // 10% chance
        await new Promise((resolve) =>
          setTimeout(resolve, Math.random() * 100),
        );
      }
    } else {
      // Use completeWithMetadata for atomic completion with metadata
      await this.q.completeWithMetadata(job, handlerResult, {
        processedOn: processedOn || Date.now(),
        finishedOn: finishedOn || Date.now(),
        attempts: job.attempts,
        maxAttempts: job.maxAttempts,
      });
    }

    return undefined;
  }

  /**
   * Start the stalled job checker
   * Checks periodically for jobs that exceeded their deadline and recovers or fails them
   */
  private startStalledChecker(): void {
    if (this.stalledInterval <= 0) {
      return; // Disabled
    }

    this.stalledCheckTimer = setInterval(async () => {
      try {
        await this.checkStalled();
      } catch (err) {
        this.logger.error('Error in stalled job checker:', err);
        this.emit('error', err instanceof Error ? err : new Error(String(err)));
      }
    }, this.stalledInterval);
  }

  /**
   * Check for stalled jobs and recover or fail them
   * A job is stalled when its worker crashed or lost connection
   */
  private async checkStalled(): Promise<void> {
    if (this.stopping || this.closed) {
      return;
    }

    try {
      const now = Date.now();
      const results = await this.q.checkStalledJobs(
        now,
        this.stalledGracePeriod,
        this.maxStalledCount,
      );

      if (results.length > 0) {
        // Process results in groups of 3: [jobId, groupId, action]
        for (let i = 0; i < results.length; i += 3) {
          const jobId = results[i];
          const groupId = results[i + 1];
          const action = results[i + 2];

          if (action === 'recovered') {
            this.logger.info(
              `Recovered stalled job ${jobId} from group ${groupId}`,
            );
            this.emit('stalled', jobId, groupId);
          } else if (action === 'failed') {
            this.logger.warn(
              `Failed stalled job ${jobId} from group ${groupId} (exceeded max stalled count)`,
            );
            this.emit('stalled', jobId, groupId);
          }
        }
      }
    } catch (err) {
      // Don't throw, just log - stalled checker should be resilient
      this.logger.error('Error checking stalled jobs:', err);
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
    // Give some time if we just received a job
    // Otherwise jobsInProgress will be 0 and we will exit immediately
    await this.delay(100);

    if (this.cleanupTimer) {
      clearInterval(this.cleanupTimer);
    }

    if (this.schedulerTimer) {
      clearInterval(this.schedulerTimer);
    }

    if (this.stalledCheckTimer) {
      clearInterval(this.stalledCheckTimer);
    }

    // Wait for jobs to finish first
    const startTime = Date.now();
    while (
      this.jobsInProgress.size > 0 &&
      Date.now() - startTime < gracefulTimeoutMs
    ) {
      await sleep(100);
    }

    // Close the blocking client to interrupt any blocking operations
    if (this.blockingClient) {
      try {
        if (this.jobsInProgress.size > 0 && gracefulTimeoutMs > 0) {
          // Graceful: use quit() to allow in-flight commands to complete
          this.logger.debug('Gracefully closing blocking client (quit)...');
          await this.blockingClient.quit();
        } else {
          // Force or no jobs: use disconnect() for immediate termination
          this.logger.debug('Force closing blocking client (disconnect)...');
          this.blockingClient.disconnect();
        }
      } catch (err) {
        // Swallow errors during close
        this.logger.debug('Error closing blocking client:', err);
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

  private async processSingleJob(
    job: ReservedJob<T>,
    fetchNextCallback?: () => boolean,
  ): Promise<void | ReservedJob<T>> {
    const jobStartWallTime = Date.now();

    let hbTimer: NodeJS.Timeout | undefined;
    let heartbeatDelayTimer: NodeJS.Timeout | undefined;

    const startHeartbeat = () => {
      // BullMQ-inspired: Adaptive heartbeat interval based on concurrency
      const minInterval = Math.max(
        this.hbMs, // Use the worker's configured heartbeat interval
        Math.floor((this.q.jobTimeoutMs || 30000) / 2),
      );

      this.logger.debug(
        `Starting heartbeat for job ${job.id} (interval: ${minInterval}ms, concurrency: ${this.concurrency})`,
      );

      hbTimer = setInterval(async () => {
        try {
          const result = await this.q.heartbeat(job);
          if (result === 0) {
            // Job no longer exists or is not in processing state
            this.logger.warn(
              `Heartbeat failed for job ${job.id} - job may have been removed or completed elsewhere`,
            );
            // Stop heartbeat since job is gone
            if (hbTimer) {
              clearInterval(hbTimer);
            }
          }
        } catch (e) {
          // Only log heartbeat errors if they're not connection errors during shutdown
          const isConnErr = this.q.isConnectionError(e);
          if (!isConnErr || !this.stopping) {
            this.logger.error(
              `Heartbeat error for job ${job.id}:`,
              e instanceof Error ? e.message : String(e),
            );
          }

          this.onError?.(e, job);

          // Only emit error if not a connection error during shutdown
          if (!isConnErr || !this.stopping) {
            this.emit('error', e instanceof Error ? e : new Error(String(e)));
          }
        }
      }, minInterval);
    };

    try {
      // BullMQ-inspired: Smart heartbeat with adaptive timing
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

      // Capture finish time before completing the job
      const finishedAtWall = Date.now();

      // Complete the job and optionally get next job from same group
      const nextJob = await this.completeJob(
        job,
        handlerResult,
        fetchNextCallback,
        jobStartWallTime,
        finishedAtWall,
      );

      // Reset adaptive timeout after successful job completion
      // This ensures the worker uses the low timeout (0.1s) for the next fetch
      this.blockingStats.consecutiveEmptyReserves = 0;
      this.emptyReserveBackoffMs = 0;

      // Emit completed event
      this.emit(
        'completed',
        Job.fromReserved(this.q, job, {
          processedOn: jobStartWallTime,
          finishedOn: finishedAtWall,
          returnvalue: handlerResult,
          status: 'completed',
        }),
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

    // Reset adaptive timeout after job failure
    // This ensures the worker uses the low timeout (0.1s) for the next fetch
    this.blockingStats.consecutiveEmptyReserves = 0;
    this.emptyReserveBackoffMs = 0;

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
export const Worker = _Worker as unknown as WorkerConstructor;

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}
