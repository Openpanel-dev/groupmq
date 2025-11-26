import { randomUUID } from 'node:crypto';
import CronParser from 'cron-parser';
import type Redis from 'ioredis';
import { type Job, Job as JobEntity } from './job';
import { Logger, type LoggerInterface } from './logger';
import { evalScript } from './lua/loader';
import type { Status } from './status';

/**
 * Options for configuring a GroupMQ queue
 */
export type QueueOptions = {
  /**
   * Logger configuration for queue operations and debugging.
   *
   * @default false (no logging)
   * @example true // Enable basic logging
   * @example customLogger // Use custom logger instance
   *
   * **When to enable:**
   * - Development: For debugging queue operations
   * - Production monitoring: For operational insights
   * - Troubleshooting: When investigating performance issues
   */
  logger?: LoggerInterface | boolean;

  /**
   * Redis client instance for queue operations.
   * Should be a connected ioredis client.
   *
   * @example new Redis('redis://localhost:6379')
   * @example new Redis({ host: 'localhost', port: 6379, db: 0 })
   */
  redis: Redis;

  /**
   * Unique namespace for this queue. Used to separate different queues in the same Redis instance.
   * Should be unique across your application to avoid conflicts.
   *
   * @example 'email-queue'
   * @example 'user-notifications'
   * @example 'data-processing'
   */
  namespace: string;

  /**
   * Maximum time in milliseconds a job can run before being considered failed.
   * Jobs that exceed this timeout will be retried or moved to failed state.
   *
   * @default 30000 (30 seconds)
   * @example 60000 // 1 minute timeout
   * @example 300000 // 5 minute timeout for long-running jobs
   *
   * **When to adjust:**
   * - Long-running jobs: Increase (5-30 minutes)
   * - Short jobs: Decrease (5-15 seconds) for faster failure detection
   * - External API calls: Consider API timeout + buffer
   * - Database operations: Consider query timeout + buffer
   */
  jobTimeoutMs?: number;

  /**
   * Default maximum number of retry attempts for failed jobs.
   * Can be overridden per job or per worker.
   *
   * @default 3
   * @example 5 // Retry failed jobs up to 5 times
   * @example 1 // Fail fast with minimal retries
   *
   * **When to override:**
   * - Critical jobs: Increase retries
   * - Non-critical jobs: Decrease retries
   * - Idempotent operations: Can safely retry more
   * - External API calls: Consider API reliability
   */
  maxAttempts?: number;

  /**
   * Maximum number of groups to scan when looking for available jobs.
   * Higher values may find more jobs but use more Redis resources.
   *
   * @default 20
   * @example 50 // Scan more groups for better job distribution
   * @example 10 // Reduce Redis load for simple queues
   *
   * **When to adjust:**
   * - Many groups: Increase (50-100) for better job distribution
   * - Few groups: Decrease (5-10) to reduce Redis overhead
   * - High job volume: Increase for better throughput
   * - Resource constraints: Decrease to reduce Redis load
   */
  reserveScanLimit?: number;

  /**
   * Maximum number of completed jobs to retain for inspection.
   * Jobs beyond this limit are automatically cleaned up.
   *
   * @default 0 (no retention)
   * @example 100 // Keep last 100 completed jobs
   * @example 1000 // Keep last 1000 completed jobs for analysis
   *
   * **When to adjust:**
   * - Debugging: Increase to investigate issues
   * - Memory constraints: Decrease to reduce Redis memory usage
   * - Compliance: Increase for audit requirements
   */
  keepCompleted?: number;

  /**
   * Maximum number of failed jobs to retain for inspection.
   * Jobs beyond this limit are automatically cleaned up.
   *
   * @default 0 (no retention)
   * @example 1000 // Keep last 1000 failed jobs for analysis
   * @example 10000 // Keep more failed jobs for trend analysis
   *
   * **When to adjust:**
   * - Error analysis: Increase to investigate failure patterns
   * - Memory constraints: Decrease to reduce Redis memory usage
   * - Compliance: Increase for audit requirements
   */
  keepFailed?: number;

  /**
   * TTL for scheduler lock in milliseconds.
   * Prevents multiple schedulers from running simultaneously.
   *
   * @default 1500
   * @example 3000 // 3 seconds for slower environments
   * @example 1000 // 1 second for faster environments
   */
  schedulerLockTtlMs?: number;

  /**
   * Ordering delay in milliseconds. When set, jobs with orderMs will be staged
   * and promoted only after orderMs + orderingDelayMs to ensure proper ordering
   * even when producers are out of sync.
   *
   * @default 0 (no staging, jobs processed immediately)
   * @example 200 // Wait 200ms to ensure all jobs arrive in order
   * @example 1000 // Wait 1 second for strict ordering
   *
   * **When to use:**
   * - Distributed producers with clock drift
   * - Strict timestamp ordering required
   * - Network latency between producers
   *
   * **Note:** Only applies to jobs with orderMs set. Jobs without orderMs
   * are never staged.
   */
  orderingDelayMs?: number;

  /**
   * Enable automatic job batching to reduce Redis load.
   * Jobs are buffered in memory and sent in batches.
   *
   * @default undefined (disabled)
   * @example true // Enable with defaults (size: 10, maxWaitMs: 10)
   * @example { size: 20, maxWaitMs: 5 } // Custom configuration
   *
   * **Trade-offs:**
   * - ✅ 10x fewer Redis calls (huge performance win)
   * - ✅ Higher throughput (5-10x improvement)
   * - ✅ Lower latency per add() call
   * - ⚠️ Jobs buffered in memory briefly before Redis
   * - ⚠️ If process crashes during batch window, those jobs are lost
   *
   * **When to use:**
   * - High job volume (>100 jobs/s)
   * - Using orderingDelayMs (already buffering)
   * - Network latency is a bottleneck
   * - Acceptable risk of losing jobs during crash (e.g., non-critical jobs)
   *
   * **When NOT to use:**
   * - Critical jobs that must be persisted immediately
   * - Very low volume (<10 jobs/s)
   * - Zero tolerance for data loss
   *
   * **Configuration:**
   * - size: Maximum jobs per batch (default: 10)
   * - maxWaitMs: Maximum time to wait before flushing (default: 10)
   *
   * **Safety:**
   * - Keep maxWaitMs small (10ms = very low risk)
   * - Batches are flushed on queue.close()
   * - Consider graceful shutdown handling
   */
  autoBatch?:
    | boolean
    | {
        size?: number;
        maxWaitMs?: number;
      };
};

/**
 * Configuration for repeating jobs
 */
export type RepeatOptions =
  | {
      /**
       * Repeat interval in milliseconds. Job will be created every N milliseconds.
       *
       * @example 60000 // Every minute
       * @example 3600000 // Every hour
       * @example 86400000 // Every day
       *
       * When to use:
       * - Simple intervals: Use for regular, predictable schedules
       * - High frequency: Good for sub-hour intervals
       * - Performance: More efficient than cron for simple intervals
       */
      every: number;
    }
  | {
      /**
       * Cron pattern for complex scheduling. Uses standard cron syntax with seconds.
       * Format: second minute hour day month dayOfWeek
       *
       * When to use:
       * - Complex schedules: Business hours, specific days, etc.
       * - Low frequency: Good for daily, weekly, monthly schedules
       * - Business logic: Align with business requirements
       *
       * Cron format uses standard syntax with seconds precision.
       */
      pattern: string;
    };

/**
 * Options for adding a job to the queue
 *
 * @template T The type of data to store in the job
 */
export type AddOptions<T> = {
  /**
   * Group ID for this job. Jobs with the same groupId are processed sequentially (FIFO).
   * Only one job per group can be processed at a time.
   *
   * @example 'user-123' // All jobs for user 123
   * @example 'email-notifications' // All email jobs
   * @example 'order-processing' // All order-related jobs
   *
   * **Best practices:**
   * - Use meaningful group IDs (user ID, resource ID, etc.)
   * - Keep group IDs consistent for related jobs
   * - Avoid too many unique groups (can impact performance)
   */
  groupId: string;

  /**
   * The data payload for this job. Can be any serializable data.
   *
   * @example { userId: 123, email: 'user@example.com' }
   * @example { orderId: 'order-456', items: [...] }
   * @example 'simple string data'
   */
  data: T;

  /**
   * Custom ordering timestamp in milliseconds. Jobs are processed in orderMs order within each group.
   * If not provided, uses current timestamp (Date.now()).
   *
   * @default Date.now()
   * @example Date.now() + 5000 // Process 5 seconds from now
   * @example 1640995200000 // Specific timestamp
   *
   * **When to use:**
   * - Delayed processing: Set future timestamp
   * - Priority ordering: Use lower timestamps for higher priority
   * - Batch processing: Group related jobs with same timestamp
   */
  orderMs?: number;

  /**
   * Maximum number of retry attempts for this specific job.
   * Overrides the queue's default maxAttempts setting.
   *
   * @default queue.maxAttemptsDefault
   * @example 5 // Retry this job up to 5 times
   * @example 1 // Fail fast with no retries
   *
   * **When to override:**
   * - Critical jobs: Increase retries
   * - Non-critical jobs: Decrease retries
   * - Idempotent operations: Can safely retry more
   * - External API calls: Consider API reliability
   */
  maxAttempts?: number;

  /**
   * Delay in milliseconds before this job becomes available for processing.
   * Alternative to using orderMs for simple delays.
   *
   * @example 5000 // Process after 5 seconds
   * @example 300000 // Process after 5 minutes
   *
   * **When to use:**
   * - Simple delays: Use delay instead of orderMs
   * - Rate limiting: Delay jobs to spread load
   * - Retry backoff: Delay retry attempts
   */
  delay?: number;

  /**
   * Specific time when this job should be processed.
   * Can be a Date object or timestamp in milliseconds.
   *
   * @example new Date('2024-01-01T12:00:00Z')
   * @example Date.now() + 3600000 // 1 hour from now
   *
   * **When to use:**
   * - Scheduled processing: Process at specific time
   * - Business hours: Schedule during working hours
   * - Maintenance windows: Schedule during low-traffic periods
   */
  runAt?: Date | number;

  /**
   * Configuration for repeating jobs (cron or interval-based).
   * Creates a repeating job that generates new instances automatically.
   *
   * @example { every: 60000 } // Every minute
   *
   * When to use:
   * - Periodic tasks: Regular cleanup, reports, etc.
   * - Monitoring: Health checks, metrics collection
   * - Maintenance: Regular database cleanup, cache warming
   */
  repeat?: RepeatOptions;

  /**
   * Custom job ID for idempotence. If a job with this ID already exists,
   * the new job will be ignored (idempotent behavior).
   *
   * @example 'user-123-email-welcome'
   * @example 'order-456-payment-process'
   *
   * **When to use:**
   * - Idempotent operations: Prevent duplicate processing
   * - External system integration: Use external IDs
   * - Retry scenarios: Ensure same job isn't added multiple times
   * - Deduplication: Prevent duplicate jobs from being created
   */
  jobId?: string;
};

export type ReservedJob<T = any> = {
  id: string;
  groupId: string;
  data: T;
  attempts: number;
  maxAttempts: number;
  seq: number;
  timestamp: number; // ms
  orderMs: number;
  score: number;
  deadlineAt: number;
};

function nsKey(ns: string, ...parts: string[]) {
  return [ns, ...parts].join(':');
}

function safeJsonParse(input: string): any {
  try {
    return JSON.parse(input);
  } catch (_e) {
    return null;
  }
}

export class Queue<T = any> {
  private logger: LoggerInterface;
  private r: Redis;
  private rawNs: string;
  private ns: string;
  private vt: number;
  private defaultMaxAttempts: number;
  private scanLimit: number;
  private keepCompleted: number;

  private keepFailed: number;
  private schedulerLockTtlMs: number;
  public orderingDelayMs: number;
  public name: string;

  // Internal tracking for adaptive behavior
  private _consecutiveEmptyReserves = 0;

  // Promoter service for staging system
  private promoterRedis?: Redis;
  private promoterRunning = false;
  private promoterLockId?: string;
  private promoterInterval?: NodeJS.Timeout;

  // Auto-batching for high-throughput scenarios
  private batchConfig?: { size: number; maxWaitMs: number };
  private batchBuffer: Array<{
    groupId: string;
    data: T | null;
    jobId: string;
    maxAttempts: number;
    delayMs?: number;
    orderMs?: number;
    resolve: (job: Job<T>) => void;
    reject: (err: Error) => void;
  }> = [];
  private batchTimer?: NodeJS.Timeout;
  private flushing = false;

  // Inline defineCommand bindings removed; using external Lua via evalsha

  constructor(opts: QueueOptions) {
    // Use the provided Redis client for main operations to preserve connection semantics
    // and a dedicated duplicate for blocking operations.
    this.r = opts.redis;
    this.rawNs = opts.namespace;
    this.name = opts.namespace;
    this.ns = `groupmq:${this.rawNs}`;
    const rawVt = opts.jobTimeoutMs ?? 30_000;
    this.vt = Math.max(1, rawVt); // Minimum 1ms
    this.defaultMaxAttempts = opts.maxAttempts ?? 3;
    this.scanLimit = opts.reserveScanLimit ?? 20;
    this.keepCompleted = Math.max(0, opts.keepCompleted ?? 0);
    this.keepFailed = Math.max(0, opts.keepFailed ?? 0);
    this.schedulerLockTtlMs = opts.schedulerLockTtlMs ?? 1500;
    this.orderingDelayMs = opts.orderingDelayMs ?? 0;

    // Initialize auto-batching if enabled
    if (opts.autoBatch) {
      this.batchConfig =
        typeof opts.autoBatch === 'boolean'
          ? { size: 10, maxWaitMs: 10 }
          : {
              size: opts.autoBatch.size ?? 10,
              maxWaitMs: opts.autoBatch.maxWaitMs ?? 10,
            };
    }

    // Initialize logger first
    this.logger =
      typeof opts.logger === 'object'
        ? opts.logger
        : new Logger(!!opts.logger, this.namespace);

    this.r.on('error', (err) => {
      this.logger.error('Redis error (main):', err);
    });
  }

  get redis(): Redis {
    return this.r;
  }

  get namespace(): string {
    return this.ns;
  }

  get rawNamespace(): string {
    return this.rawNs;
  }

  get jobTimeoutMs(): number {
    return this.vt;
  }

  get maxAttemptsDefault(): number {
    return this.defaultMaxAttempts;
  }

  async add(opts: AddOptions<T>): Promise<JobEntity<T>> {
    const maxAttempts = opts.maxAttempts ?? this.defaultMaxAttempts;
    const orderMs = opts.orderMs ?? Date.now();
    const now = Date.now();
    const jobId = opts.jobId ?? randomUUID();

    if (opts.repeat) {
      // Keep existing behavior for repeating jobs (returns a repeat key string)
      return this.addRepeatingJob({ ...opts, orderMs, maxAttempts });
    }

    // Calculate delay
    let delayMs: number | undefined;
    if (opts.delay !== undefined && opts.delay > 0) {
      delayMs = opts.delay;
    } else if (opts.runAt !== undefined) {
      const runAtTimestamp =
        opts.runAt instanceof Date ? opts.runAt.getTime() : opts.runAt;
      delayMs = Math.max(0, runAtTimestamp - now);
    }

    // Handle undefined data by converting to null for consistent JSON serialization
    const data = opts.data === undefined ? null : (opts.data as T);

    // Use batching if enabled
    if (this.batchConfig) {
      return new Promise((resolve, reject) => {
        this.batchBuffer.push({
          groupId: opts.groupId,
          data,
          jobId,
          maxAttempts,
          delayMs,
          orderMs,
          resolve,
          reject,
        });

        // Flush if batch is full
        if (this.batchBuffer.length >= this.batchConfig!.size) {
          this.flushBatch();
        } else if (!this.batchTimer) {
          // Start timer for partial batch
          this.batchTimer = setTimeout(
            () => this.flushBatch(),
            this.batchConfig!.maxWaitMs,
          );
        }
      });
    }

    // Non-batched path (original logic)
    return this.addSingle({
      ...opts,
      data,
      jobId,
      maxAttempts,
      orderMs,
      delayMs,
    });
  }

  private async addSingle(opts: {
    groupId: string;
    data: T | null;
    jobId: string;
    maxAttempts: number;
    orderMs: number;
    delayMs?: number;
  }): Promise<JobEntity<T>> {
    const now = Date.now();

    // Calculate delay timestamp
    let delayUntil = 0;
    if (opts.delayMs !== undefined && opts.delayMs > 0) {
      delayUntil = now + opts.delayMs;
    }

    const serializedPayload = JSON.stringify(opts.data);

    const result = await evalScript<string[] | string>(
      this.r,
      'enqueue',
      [
        this.ns,
        opts.groupId,
        serializedPayload,
        String(opts.maxAttempts),
        String(opts.orderMs),
        String(delayUntil),
        String(opts.jobId),
        String(this.keepCompleted),
        String(now), // Pass client timestamp for accurate timing calculations
        String(this.orderingDelayMs), // Pass orderingDelayMs for staging logic
      ],
      1,
    );

    // Handle new array format that includes job data (avoids race condition)
    // Format: [jobId, groupId, data, attempts, maxAttempts, timestamp, orderMs, delayUntil, status]
    if (Array.isArray(result)) {
      const [
        returnedJobId,
        returnedGroupId,
        returnedData,
        attempts,
        returnedMaxAttempts,
        timestamp,
        returnedOrderMs,
        returnedDelayUntil,
        status,
      ] = result;

      return JobEntity.fromRawHash<T>(
        this,
        returnedJobId,
        {
          id: returnedJobId,
          groupId: returnedGroupId,
          data: returnedData,
          attempts,
          maxAttempts: returnedMaxAttempts,
          timestamp,
          orderMs: returnedOrderMs,
          delayUntil: returnedDelayUntil,
          status,
        },
        status as any,
      );
    }

    // Fallback for old format (just jobId string) - this shouldn't happen with updated Lua script
    // but kept for backwards compatibility during rollout
    return this.getJob(result);
  }

  private async flushBatch(): Promise<void> {
    // Clear timer
    if (this.batchTimer) {
      clearTimeout(this.batchTimer);
      this.batchTimer = undefined;
    }

    if (this.batchBuffer.length === 0 || this.flushing) return;

    this.flushing = true;
    const batch = this.batchBuffer.splice(0); // Take all pending jobs

    try {
      this.logger.debug(`Flushing batch of ${batch.length} jobs`);
      const now = Date.now();

      // Prepare batch data for Lua script
      const jobsData = batch.map((job) => ({
        jobId: job.jobId,
        groupId: job.groupId,
        data: JSON.stringify(job.data),
        maxAttempts: job.maxAttempts,
        orderMs: job.orderMs,
        delayMs: job.delayMs,
      }));

      // Call batch enqueue Lua script
      // Returns array of job data arrays: [[jobId, groupId, data, attempts, maxAttempts, timestamp, orderMs, delayUntil, status], ...]
      const jobDataArrays = await evalScript<string[][]>(
        this.r,
        'enqueue-batch',
        [
          this.ns,
          JSON.stringify(jobsData),
          String(this.keepCompleted),
          String(now),
          String(this.orderingDelayMs),
        ],
        1,
      );

      // Resolve all promises with job entities
      for (let i = 0; i < batch.length; i++) {
        const job = batch[i];
        const jobDataArray = jobDataArrays[i];

        try {
          if (jobDataArray && jobDataArray.length >= 9) {
            const [
              returnedJobId,
              returnedGroupId,
              returnedData,
              attempts,
              returnedMaxAttempts,
              timestamp,
              returnedOrderMs,
              returnedDelayUntil,
              status,
            ] = jobDataArray;

            const jobEntity = JobEntity.fromRawHash<T>(
              this,
              returnedJobId,
              {
                id: returnedJobId,
                groupId: returnedGroupId,
                data: returnedData,
                attempts,
                maxAttempts: returnedMaxAttempts,
                timestamp,
                orderMs: returnedOrderMs,
                delayUntil: returnedDelayUntil,
                status,
              },
              status as any,
            );
            job.resolve(jobEntity);
          } else {
            throw new Error('Invalid job data returned from batch enqueue');
          }
        } catch (err) {
          job.reject(err instanceof Error ? err : new Error(String(err)));
        }
      }
    } catch (err) {
      // Reject all promises on error
      for (const job of batch) {
        job.reject(err instanceof Error ? err : new Error(String(err)));
      }
    } finally {
      this.flushing = false;

      // If there are jobs that accumulated during flush, flush them now
      if (this.batchBuffer.length > 0) {
        // Use setImmediate to avoid deep recursion
        setImmediate(() => this.flushBatch());
      }
    }
  }

  async reserve(): Promise<ReservedJob<T> | null> {
    const now = Date.now();

    const raw = await evalScript<string | null>(
      this.r,
      'reserve',
      [this.ns, String(now), String(this.vt), String(this.scanLimit)],
      1,
    );

    if (!raw) return null;

    const parts = raw.split('|||');
    if (parts.length !== 10) return null;

    let data: T;
    try {
      data = JSON.parse(parts[2]);
    } catch (err) {
      this.logger.warn(
        `Failed to parse job data: ${(err as Error).message}, raw: ${parts[2]}`,
      );
      data = null as T;
    }

    const parsedOrderMs = Number.parseInt(parts[7], 10);
    const job = {
      id: parts[0],
      groupId: parts[1],
      data,
      attempts: Number.parseInt(parts[3], 10),
      maxAttempts: Number.parseInt(parts[4], 10),
      seq: Number.parseInt(parts[5], 10),
      timestamp: Number.parseInt(parts[6], 10),
      orderMs: Number.isNaN(parsedOrderMs)
        ? Number.parseInt(parts[6], 10)
        : parsedOrderMs, // Fallback to timestamp if orderMs is NaN
      score: Number(parts[8]),
      deadlineAt: Number.parseInt(parts[9], 10),
    } as ReservedJob<T>;

    return job;
  }

  /**
   * Check how many jobs are waiting in a specific group
   */
  async getGroupJobCount(groupId: string): Promise<number> {
    const gZ = `${this.ns}:g:${groupId}`;
    return await this.r.zcard(gZ);
  }

  /**
   * Complete a job by removing from processing and unlocking the group.
   * Note: Job metadata recording is handled separately by recordCompleted().
   *
   * @deprecated Use completeWithMetadata() for internal operations. This method
   * is kept for backward compatibility and testing only.
   */
  async complete(job: { id: string; groupId: string }) {
    await evalScript<number>(
      this.r,
      'complete',
      [this.ns, job.id, job.groupId],
      1,
    );
  }

  /**
   * Complete a job AND record metadata in a single atomic operation.
   * This is the efficient internal method used by workers.
   */
  public async completeWithMetadata(
    job: { id: string; groupId: string },
    result: unknown,
    meta: {
      processedOn: number;
      finishedOn: number;
      attempts: number;
      maxAttempts: number;
    },
  ): Promise<void> {
    await evalScript<number>(
      this.r,
      'complete-with-metadata',
      [
        this.ns,
        job.id,
        job.groupId,
        'completed',
        String(meta.finishedOn),
        JSON.stringify(result ?? null),
        String(this.keepCompleted),
        String(this.keepFailed),
        String(meta.processedOn),
        String(meta.finishedOn),
        String(meta.attempts),
        String(meta.maxAttempts),
      ],
      1,
    );
  }

  /**
   * Atomically complete a job and try to reserve the next job from the same group
   * This prevents race conditions where other workers can steal subsequent jobs from the same group
   */

  /**
   * Atomically complete a job with metadata and reserve the next job from the same group.
   */
  async completeAndReserveNextWithMetadata(
    completedJobId: string,
    groupId: string,
    handlerResult: unknown,
    meta: {
      processedOn: number;
      finishedOn: number;
      attempts: number;
      maxAttempts: number;
    },
  ): Promise<ReservedJob<T> | null> {
    const now = Date.now();

    try {
      const result = await evalScript<string | null>(
        this.r,
        'complete-and-reserve-next-with-metadata',
        [
          this.ns,
          completedJobId,
          groupId,
          'completed',
          String(meta.finishedOn),
          JSON.stringify(handlerResult ?? null),
          String(this.keepCompleted),
          String(this.keepFailed),
          String(meta.processedOn),
          String(meta.finishedOn),
          String(meta.attempts),
          String(meta.maxAttempts),
          String(now),
          String(this.jobTimeoutMs),
        ],
        1,
      );

      if (!result) {
        return null;
      }

      // Parse the result (same format as reserve methods)
      const parts = result.split('|||');
      if (parts.length !== 10) {
        this.logger.error(
          'Queue completeAndReserveNextWithMetadata: unexpected result format:',
          result,
        );
        return null;
      }

      const [
        id,
        ,
        data,
        attempts,
        maxAttempts,
        seq,
        enq,
        orderMs,
        score,
        deadline,
      ] = parts;

      return {
        id,
        groupId,
        data: JSON.parse(data),
        attempts: parseInt(attempts, 10),
        maxAttempts: parseInt(maxAttempts, 10),
        seq: parseInt(seq, 10),
        timestamp: parseInt(enq, 10),
        orderMs: parseInt(orderMs, 10),
        score: parseFloat(score),
        deadlineAt: parseInt(deadline, 10),
      };
    } catch (error) {
      this.logger.error(
        'Queue completeAndReserveNextWithMetadata error:',
        error,
      );
      return null;
    }
  }

  /**
   * Check if a job is currently in processing state
   */
  async isJobProcessing(jobId: string): Promise<boolean> {
    const score = await this.r.zscore(`${this.ns}:processing`, jobId);
    return score !== null;
  }

  async retry(jobId: string, backoffMs = 0) {
    return evalScript<number>(
      this.r,
      'retry',
      [this.ns, jobId, String(backoffMs)],

      1,
    );
  }

  /**
   * Dead letter a job (remove from group and optionally store in dead letter queue)
   */
  async deadLetter(jobId: string, groupId: string) {
    return evalScript<number>(
      this.r,
      'dead-letter',
      [this.ns, jobId, groupId],
      1,
    );
  }

  /**
   * Record a successful completion for retention and inspection
   * Uses consolidated Lua script for atomic operation with retention management
   */
  async recordCompleted(
    job: { id: string; groupId: string },
    result: unknown,
    meta: {
      processedOn?: number;
      finishedOn?: number;
      attempts?: number;
      maxAttempts?: number;
      data?: unknown; // legacy
    },
  ): Promise<void> {
    const processedOn = meta.processedOn ?? Date.now();
    const finishedOn = meta.finishedOn ?? Date.now();
    const attempts = meta.attempts ?? 0;
    const maxAttempts = meta.maxAttempts ?? this.defaultMaxAttempts;

    try {
      await evalScript<number>(
        this.r,
        'record-job-result',
        [
          this.ns,
          job.id,
          'completed',
          String(finishedOn),
          JSON.stringify(result ?? null),
          String(this.keepCompleted),
          String(this.keepFailed),
          String(processedOn),
          String(finishedOn),
          String(attempts),
          String(maxAttempts),
        ],
        1,
      );
    } catch (error) {
      this.logger.error(`Error recording completion for job ${job.id}:`, error);
      throw error;
    }
  }

  /**
   * Record a failure attempt (non-final), storing last error for visibility
   */
  async recordAttemptFailure(
    job: { id: string; groupId: string },
    error: { message?: string; name?: string; stack?: string } | string,
    meta: {
      processedOn?: number;
      finishedOn?: number;
      attempts?: number;
      maxAttempts?: number;
    },
  ): Promise<void> {
    const jobKey = `${this.ns}:job:${job.id}`;
    const processedOn = meta.processedOn ?? Date.now();
    const finishedOn = meta.finishedOn ?? Date.now();

    const message =
      typeof error === 'string' ? error : (error.message ?? 'Error');
    const name = typeof error === 'string' ? 'Error' : (error.name ?? 'Error');
    const stack = typeof error === 'string' ? '' : (error.stack ?? '');

    await this.r.hset(
      jobKey,
      'lastErrorMessage',
      message,
      'lastErrorName',
      name,
      'lastErrorStack',
      stack,
      'processedOn',
      String(processedOn),
      'finishedOn',
      String(finishedOn),
    );
  }

  /**
   * Record a final failure (dead-lettered) for retention and inspection
   * Uses consolidated Lua script for atomic operation
   */
  async recordFinalFailure(
    job: { id: string; groupId: string },
    error: { message?: string; name?: string; stack?: string } | string,
    meta: {
      processedOn?: number;
      finishedOn?: number;
      attempts?: number;
      maxAttempts?: number;
      data?: unknown;
    },
  ): Promise<void> {
    const processedOn = meta.processedOn ?? Date.now();
    const finishedOn = meta.finishedOn ?? Date.now();
    const attempts = meta.attempts ?? 0;
    const maxAttempts = meta.maxAttempts ?? this.defaultMaxAttempts;

    const message =
      typeof error === 'string' ? error : (error.message ?? 'Error');
    const name = typeof error === 'string' ? 'Error' : (error.name ?? 'Error');
    const stack = typeof error === 'string' ? '' : (error.stack ?? '');

    // Package error info as JSON for Lua script
    const errorInfo = JSON.stringify({ message, name, stack });

    try {
      await evalScript<number>(
        this.r,
        'record-job-result',
        [
          this.ns,
          job.id,
          'failed',
          String(finishedOn),
          errorInfo,
          String(this.keepCompleted),
          String(this.keepFailed),
          String(processedOn),
          String(finishedOn),
          String(attempts),
          String(maxAttempts),
        ],
        1,
      );
    } catch (err) {
      this.logger.error(
        `Error recording final failure for job ${job.id}:`,
        err,
      );
      throw err;
    }
  }

  async getCompleted(limit = this.keepCompleted): Promise<
    Array<{
      id: string;
      groupId: string;
      data: any;
      returnvalue: any;
      processedOn?: number;
      finishedOn?: number;
      attempts: number;
      maxAttempts: number;
    }>
  > {
    const completedKey = `${this.ns}:completed`;
    const ids = await this.r.zrevrange(completedKey, 0, Math.max(0, limit - 1));
    if (ids.length === 0) return [];
    const pipe = this.r.multi();
    for (const id of ids) {
      pipe.hmget(
        `${this.ns}:job:${id}`,
        'groupId',
        'data',
        'returnvalue',
        'processedOn',
        'finishedOn',
        'attempts',
        'maxAttempts',
      );
    }
    const rows = (await pipe.exec()) ?? [];
    return ids.map((id, idx) => {
      const row = rows[idx]?.[1] as Array<string | null>;
      const [
        groupId,
        dataStr,
        retStr,
        processedOn,
        finishedOn,
        attempts,
        maxAttempts,
      ] = row || [];
      return {
        id,
        groupId: groupId || '',
        data: dataStr ? safeJsonParse(dataStr) : null,
        returnvalue: retStr ? safeJsonParse(retStr) : null,
        processedOn: processedOn ? parseInt(processedOn, 10) : undefined,
        finishedOn: finishedOn ? parseInt(finishedOn, 10) : undefined,
        attempts: attempts ? parseInt(attempts, 10) : 0,
        maxAttempts: maxAttempts
          ? parseInt(maxAttempts, 10)
          : this.defaultMaxAttempts,
      };
    });
  }

  async getFailed(limit = this.keepFailed): Promise<
    Array<{
      id: string;
      groupId: string;
      data: any;
      failedReason: string;
      stacktrace?: string;
      processedOn?: number;
      finishedOn?: number;
      attempts: number;
      maxAttempts: number;
    }>
  > {
    const failedKey = `${this.ns}:failed`;
    const ids = await this.r.zrevrange(failedKey, 0, Math.max(0, limit - 1));
    if (ids.length === 0) return [];
    const pipe = this.r.multi();
    for (const id of ids) {
      pipe.hmget(
        `${this.ns}:job:${id}`,
        'groupId',
        'data',
        'failedReason',
        'stacktrace',
        'processedOn',
        'finishedOn',
        'attempts',
        'maxAttempts',
      );
    }
    const rows = (await pipe.exec()) ?? [];
    return ids.map((id, idx) => {
      const row = rows[idx]?.[1] as Array<string | null>;
      const [
        groupId,
        dataStr,
        failedReason,
        stacktrace,
        processedOn,
        finishedOn,
        attempts,
        maxAttempts,
      ] = row || [];
      return {
        id,
        groupId: groupId || '',
        data: dataStr ? safeJsonParse(dataStr) : null,
        failedReason: failedReason || '',
        stacktrace: stacktrace || undefined,
        processedOn: processedOn ? parseInt(processedOn, 10) : undefined,
        finishedOn: finishedOn ? parseInt(finishedOn, 10) : undefined,
        attempts: attempts ? parseInt(attempts, 10) : 0,
        maxAttempts: maxAttempts
          ? parseInt(maxAttempts, 10)
          : this.defaultMaxAttempts,
      };
    });
  }

  /**
   * Convenience: return completed jobs as Job entities (non-breaking, new API)
   */
  async getCompletedJobs(
    limit = this.keepCompleted,
  ): Promise<Array<JobEntity<T>>> {
    const completedKey = `${this.ns}:completed`;
    const ids = await this.r.zrevrange(completedKey, 0, Math.max(0, limit - 1));
    if (ids.length === 0) return [];

    // Atomically fetch all job hashes in one pipeline
    const pipe = this.r.multi();
    for (const id of ids) {
      pipe.hgetall(`${this.ns}:job:${id}`);
    }
    const rows = await pipe.exec();

    // Construct jobs directly from pipeline data (atomic, no race condition)
    const jobs: Array<JobEntity<T>> = [];
    for (let i = 0; i < ids.length; i++) {
      const id = ids[i];
      const raw = (rows?.[i]?.[1] as Record<string, string>) || {};

      // Skip jobs that were already cleaned up
      if (!raw || Object.keys(raw).length === 0) {
        this.logger.warn(
          `Skipping completed job ${id} - not found (likely cleaned up)`,
        );
        continue;
      }

      const job = JobEntity.fromRawHash<T>(this, id, raw, 'completed');
      jobs.push(job);
    }
    return jobs;
  }

  /**
   * Convenience: return failed jobs as Job entities (non-breaking, new API)
   */
  async getFailedJobs(limit = this.keepFailed): Promise<Array<JobEntity<T>>> {
    const failedKey = `${this.ns}:failed`;
    const ids = await this.r.zrevrange(failedKey, 0, Math.max(0, limit - 1));
    if (ids.length === 0) return [];

    // Atomically fetch all job hashes in one pipeline
    const pipe = this.r.multi();
    for (const id of ids) {
      pipe.hgetall(`${this.ns}:job:${id}`);
    }
    const rows = await pipe.exec();

    // Construct jobs directly from pipeline data (atomic, no race condition)
    const jobs: Array<JobEntity<T>> = [];
    for (let i = 0; i < ids.length; i++) {
      const id = ids[i];
      const raw = (rows?.[i]?.[1] as Record<string, string>) || {};

      // Skip jobs that were already cleaned up
      if (!raw || Object.keys(raw).length === 0) {
        this.logger.warn(
          `Skipping failed job ${id} - not found (likely cleaned up)`,
        );
        continue;
      }

      const job = JobEntity.fromRawHash<T>(this, id, raw, 'failed');
      jobs.push(job);
    }
    return jobs;
  }

  async getCompletedCount(): Promise<number> {
    return this.r.zcard(`${this.ns}:completed`);
  }

  async getFailedCount(): Promise<number> {
    return this.r.zcard(`${this.ns}:failed`);
  }
  async heartbeat(job: { id: string; groupId: string }, extendMs = this.vt) {
    return evalScript<number>(
      this.r,
      'heartbeat',
      [this.ns, job.id, job.groupId, String(extendMs)],
      1,
    );
  }

  /**
   * Clean up expired jobs and stale data.
   * Uses distributed lock to ensure only one worker runs cleanup at a time,
   * similar to scheduler lock pattern.
   */
  async cleanup(): Promise<number> {
    // Try to acquire cleanup lock (similar to scheduler lock)
    const cleanupLockKey = `${this.ns}:cleanup:lock`;
    const ttlMs = 60000; // 60 seconds - longer than typical cleanup duration

    try {
      const acquired = await (this.r as any).set(
        cleanupLockKey,
        '1',
        'PX',
        ttlMs,
        'NX',
      );

      if (acquired !== 'OK') {
        // Another worker is running cleanup
        return 0;
      }

      // We have the lock, run cleanup
      const now = Date.now();
      return evalScript<number>(this.r, 'cleanup', [this.ns, String(now)], 1);
    } catch (_e) {
      return 0;
    }
  }

  /**
   * Calculate adaptive blocking timeout like BullMQ
   * Returns timeout in seconds
   *
   * Inspiration by BullMQ ⭐️
   */
  private getBlockTimeout(maxTimeout: number, blockUntil?: number): number {
    const minimumBlockTimeout = 0.001; // 1ms like BullMQ for fast job pickup
    const maximumBlockTimeout = 5; // 5s max to reduce idle CPU usage

    // Handle delayed jobs case (when we know exactly when next job should be processed)
    if (blockUntil) {
      const blockDelay = blockUntil - Date.now();

      // If we've reached the time to get new jobs
      if (blockDelay <= 0) {
        return minimumBlockTimeout; // Process immediately
      } else if (blockDelay < minimumBlockTimeout * 1000) {
        return minimumBlockTimeout; // Very short delay, use minimum
      } else {
        // Block until the delayed job is ready, but cap at maximum
        return Math.min(blockDelay / 1000, maximumBlockTimeout);
      }
    }

    // Use maxTimeout when draining (similar to BullMQ's drainDelay), but clamp to minimum
    // This keeps the worker responsive while balancing Redis load
    return Math.max(
      minimumBlockTimeout,
      Math.min(maxTimeout, maximumBlockTimeout),
    );
  }

  /**
   * Check if an error is a Redis connection error (should retry)
   * Conservative approach: only connection closed and ECONNREFUSED
   */
  isConnectionError(err: any): boolean {
    if (!err) return false;

    const message = `${err.message || ''}`;

    return (
      message === 'Connection is closed.' || message.includes('ECONNREFUSED')
    );
  }

  async reserveBlocking(
    timeoutSec = 5,
    blockUntil?: number,
    blockingClient?: import('ioredis').default,
  ): Promise<ReservedJob<T> | null> {
    const startTime = Date.now();

    // Short-circuit if paused
    if (await this.isPaused()) {
      await sleep(50);
      return null;
    }

    // Fast path optimization: Skip immediate reserve if we recently had empty reserves
    // This avoids wasteful Lua script calls when queue is idle
    // After 3 consecutive empty reserves, go straight to blocking for better performance
    const skipImmediateReserve = this._consecutiveEmptyReserves >= 3;

    if (!skipImmediateReserve) {
      // Fast path: try immediate reserve first (avoids blocking when jobs are available)
      const immediateJob = await this.reserve();
      if (immediateJob) {
        this.logger.debug(
          `Immediate reserve successful (${Date.now() - startTime}ms)`,
        );
        // Reset consecutive empty reserves counter when we get a job via fast path
        this._consecutiveEmptyReserves = 0;
        return immediateJob;
      }
    }

    // Use BullMQ-style adaptive timeout with delayed job consideration
    const adaptiveTimeout = this.getBlockTimeout(timeoutSec, blockUntil);

    // Only log blocking operations every 10th time to reduce spam
    if (this._consecutiveEmptyReserves % 10 === 0) {
      this.logger.debug(
        `Starting blocking operation (timeout: ${adaptiveTimeout}s, consecutive empty: ${this._consecutiveEmptyReserves})`,
      );
    }

    // Use ready queue for blocking behavior (more reliable than marker system)
    const readyKey = nsKey(this.ns, 'ready');

    try {
      // Avoid extra zcard during every blocking call to reduce Redis CPU

      // Use dedicated blocking connection to avoid interfering with other operations
      const bzpopminStart = Date.now();
      const client = blockingClient ?? this.r;
      const result = await client.bzpopmin(readyKey, adaptiveTimeout);
      const bzpopminDuration = Date.now() - bzpopminStart;

      if (!result || result.length < 3) {
        this.logger.debug(
          `Blocking timeout/empty (took ${bzpopminDuration}ms)`,
        );
        // Track consecutive empty reserves for adaptive timeout
        this._consecutiveEmptyReserves = this._consecutiveEmptyReserves + 1;
        return null; // Timeout or no result
      }

      const [, groupId, score] = result;

      // Only log blocking results every 10th time to reduce spam
      if (this._consecutiveEmptyReserves % 10 === 0) {
        this.logger.debug(
          `Blocking result: group=${groupId}, score=${score} (took ${bzpopminDuration}ms)`,
        );
      }

      // Try to reserve atomically from the specific group to eliminate race conditions
      const reserveStart = Date.now();
      const job = await this.reserveAtomic(groupId);
      const reserveDuration = Date.now() - reserveStart;

      if (job) {
        this.logger.debug(
          `Successful job reserve after blocking: ${job.id} from group ${job.groupId} (reserve took ${reserveDuration}ms)`,
        );
        // Reset consecutive empty reserves counter
        this._consecutiveEmptyReserves = 0;
      } else {
        this.logger.warn(
          `Blocking found group but reserve failed: group=${groupId} (reserve took ${reserveDuration}ms)`,
        );

        // Check if group actually has jobs before restoring to prevent infinite loops
        // This prevents poisoned groups (empty groups in ready queue) from being restored
        try {
          const groupKey = `${this.ns}:g:${groupId}`;
          const jobCount = await this.r.zcard(groupKey);

          if (jobCount > 0) {
            // Group has jobs, restore it to ready queue
            await this.r.zadd(readyKey, Number(score), groupId);
            this.logger.debug(
              `Restored group ${groupId} to ready with score ${score} after failed atomic reserve (${jobCount} jobs)`,
            );
          } else {
            // Group is empty (poisoned), don't restore it
            this.logger.warn(
              `Not restoring empty group ${groupId} - preventing poisoned group loop`,
            );
          }
        } catch (_e) {
          // If check fails, err on the side of not restoring to prevent infinite loops
          this.logger.warn(
            `Failed to check group ${groupId} job count, not restoring`,
          );
        }

        // Increment consecutive empty reserves and fall back to general reserve scan
        this._consecutiveEmptyReserves = this._consecutiveEmptyReserves + 1;
        return this.reserve();
      }
      return job;
    } catch (err) {
      const errorDuration = Date.now() - startTime;
      this.logger.error(`Blocking error after ${errorDuration}ms:`, err);

      // Enhanced error handling - check if it's a connection error
      if (this.isConnectionError(err)) {
        this.logger.error(`Connection error detected - rethrowing`);
        // For connection errors, don't fall back immediately
        throw err;
      }
      // For other errors, fall back to regular reserve
      this.logger.warn(`Falling back to regular reserve due to error`);
      return this.reserve();
    } finally {
      const totalDuration = Date.now() - startTime;
      if (totalDuration > 1000) {
        this.logger.debug(`ReserveBlocking completed in ${totalDuration}ms`);
      }
    }
  }

  /**
   * Reserve a job from a specific group atomically (eliminates race conditions)
   * @param groupId - The group to reserve from
   */
  async reserveAtomic(groupId: string): Promise<ReservedJob<T> | null> {
    const now = Date.now();

    const result = await evalScript<string | null>(
      this.r,
      'reserve-atomic',
      [this.ns, String(now), String(this.vt), String(groupId)],
      1,
    );
    if (!result) return null;

    // Parse the delimited string response (same format as regular reserve)
    const parts = result.split('|||');
    if (parts.length < 10) return null;

    const [
      id,
      groupIdRaw,
      data,
      attempts,
      maxAttempts,
      seq,
      timestamp,
      orderMs,
      score,
      deadline,
    ] = parts;

    const parsedTimestamp = parseInt(timestamp, 10);
    const parsedOrderMs = parseInt(orderMs, 10);
    return {
      id,
      groupId: groupIdRaw,
      data: JSON.parse(data),
      attempts: parseInt(attempts, 10),
      maxAttempts: parseInt(maxAttempts, 10),
      seq: parseInt(seq, 10),
      timestamp: parsedTimestamp,
      orderMs: Number.isNaN(parsedOrderMs) ? parsedTimestamp : parsedOrderMs, // Fallback to timestamp if orderMs is NaN
      score: parseFloat(score),
      deadlineAt: parseInt(deadline, 10),
    };
  }

  /**
   * Reserve up to maxBatch jobs (one per available group) atomically in Lua.
   */
  async reserveBatch(maxBatch = 16): Promise<Array<ReservedJob<T>>> {
    const now = Date.now();
    const results = await evalScript<Array<string | null>>(
      this.r,
      'reserve-batch',
      [this.ns, String(now), String(this.vt), String(Math.max(1, maxBatch))],
      1,
    );
    const out: Array<ReservedJob<T>> = [];
    for (const r of results || []) {
      if (!r) continue;
      const parts = r.split('|||');
      if (parts.length !== 10) continue;
      out.push({
        id: parts[0],
        groupId: parts[1],
        data: safeJsonParse(parts[2]),
        attempts: parseInt(parts[3], 10),
        maxAttempts: parseInt(parts[4], 10),
        seq: parseInt(parts[5], 10),
        timestamp: parseInt(parts[6], 10),
        orderMs: parseInt(parts[7], 10),
        score: parseFloat(parts[8]),
        deadlineAt: parseInt(parts[9], 10),
      } as ReservedJob<T>);
    }
    return out;
  }

  /**
   * Get the number of jobs currently being processed (active jobs)
   */
  async getActiveCount(): Promise<number> {
    return evalScript<number>(this.r, 'get-active-count', [this.ns], 1);
  }

  /**
   * Get the number of jobs waiting to be processed
   */
  async getWaitingCount(): Promise<number> {
    return evalScript<number>(this.r, 'get-waiting-count', [this.ns], 1);
  }

  /**
   * Get the number of jobs delayed due to backoff
   */
  async getDelayedCount(): Promise<number> {
    return evalScript<number>(this.r, 'get-delayed-count', [this.ns], 1);
  }

  /**
   * Get list of active job IDs
   */
  async getActiveJobs(): Promise<string[]> {
    return evalScript<string[]>(this.r, 'get-active-jobs', [this.ns], 1);
  }

  /**
   * Get list of waiting job IDs
   */
  async getWaitingJobs(): Promise<string[]> {
    return evalScript<string[]>(this.r, 'get-waiting-jobs', [this.ns], 1);
  }

  /**
   * Get list of delayed job IDs
   */
  async getDelayedJobs(): Promise<string[]> {
    return evalScript<string[]>(this.r, 'get-delayed-jobs', [this.ns], 1);
  }

  /**
   * Get list of unique group IDs that have jobs
   */
  async getUniqueGroups(): Promise<string[]> {
    return evalScript<string[]>(this.r, 'get-unique-groups', [this.ns], 1);
  }

  /**
   * Get count of unique groups that have jobs
   */
  async getUniqueGroupsCount(): Promise<number> {
    return evalScript<number>(this.r, 'get-unique-groups-count', [this.ns], 1);
  }

  /**
   * Fetch a single job by ID with enriched fields for UI/inspection.
   * Attempts to mimic BullMQ's Job shape for fields commonly used by BullBoard.
   */
  async getJob(id: string): Promise<JobEntity<T>> {
    return JobEntity.fromStore<T>(this, id);
  }

  /**
   * Fetch jobs by statuses, emulating BullMQ's Queue.getJobs API used by BullBoard.
   * Only getter functionality; ordering is best-effort.
   *
   * Optimized with pagination to reduce Redis load - especially important for BullBoard polling.
   */
  async getJobsByStatus(
    jobStatuses: Array<Status>,
    start = 0,
    end = -1,
  ): Promise<Array<JobEntity<T>>> {
    // Calculate actual limit to fetch (with some buffer for deduplication)
    const requestedCount = end >= 0 ? end - start + 1 : 100; // Default to 100 if unbounded
    const fetchLimit = Math.min(requestedCount * 2, 500); // Cap at 500 to prevent excessive fetches

    // Map to track which status each job belongs to (for known status optimization)
    const idToStatus = new Map<string, Status>();
    const idSets: string[] = [];

    // Optimized helper that respects pagination
    const pushZRange = async (key: string, status: Status, reverse = false) => {
      try {
        // Fetch only what we need (with buffer), not everything
        const ids = reverse
          ? await this.r.zrevrange(key, 0, fetchLimit - 1)
          : await this.r.zrange(key, 0, fetchLimit - 1);
        for (const id of ids) {
          idToStatus.set(id, status);
        }
        idSets.push(...ids);
      } catch (_e) {
        // ignore
      }
    };

    const statuses = new Set(jobStatuses);

    if (statuses.has('active')) {
      await pushZRange(`${this.ns}:processing`, 'active');
    }
    if (statuses.has('delayed')) {
      await pushZRange(`${this.ns}:delayed`, 'delayed');
    }
    if (statuses.has('completed')) {
      await pushZRange(`${this.ns}:completed`, 'completed', true);
    }
    if (statuses.has('failed')) {
      await pushZRange(`${this.ns}:failed`, 'failed', true);
    }
    if (statuses.has('waiting')) {
      // Aggregate waiting jobs with limits to prevent scanning all groups
      try {
        const groupIds = await this.r.smembers(`${this.ns}:groups`);
        if (groupIds.length > 0) {
          // Limit groups to scan (prevent excessive iteration)
          const groupsToScan = groupIds.slice(
            0,
            Math.min(100, groupIds.length),
          );
          const pipe = this.r.multi();

          // Fetch only first few jobs from each group (most are at the head anyway)
          const jobsPerGroup = Math.max(
            1,
            Math.ceil(fetchLimit / groupsToScan.length),
          );
          for (const gid of groupsToScan) {
            pipe.zrange(`${this.ns}:g:${gid}`, 0, jobsPerGroup - 1);
          }

          const rows = await pipe.exec();
          for (const r of rows || []) {
            const arr = (r?.[1] as string[]) || [];
            for (const id of arr) {
              idToStatus.set(id, 'waiting');
            }
            idSets.push(...arr);
          }
        }
      } catch (_e) {
        // ignore
      }
    }

    // paused, waiting-children, prioritized are not supported; return empty

    // De-duplicate keeping first occurrence
    const seen = new Set<string>();
    const uniqueIds: string[] = [];
    for (const id of idSets) {
      if (!seen.has(id)) {
        seen.add(id);
        uniqueIds.push(id);
      }
    }

    const slice =
      end >= 0 ? uniqueIds.slice(start, end + 1) : uniqueIds.slice(start);
    if (slice.length === 0) return [];

    // Atomically fetch all job hashes in one pipeline
    const pipe = this.r.multi();
    for (const id of slice) {
      pipe.hgetall(`${this.ns}:job:${id}`);
    }
    const rows = await pipe.exec();

    // Construct jobs directly from pipeline data (atomic, no race condition)
    const jobs: Array<JobEntity<T>> = [];
    for (let i = 0; i < slice.length; i++) {
      const id = slice[i];
      const raw = (rows?.[i]?.[1] as Record<string, string>) || {};

      // Skip jobs that were already cleaned up
      if (!raw || Object.keys(raw).length === 0) {
        this.logger.warn(
          `Skipping job ${id} - not found (likely cleaned up by retention)`,
        );
        continue;
      }

      // Use the known status from the index we fetched from
      const knownStatus = idToStatus.get(id);
      const job = JobEntity.fromRawHash<T>(this, id, raw, knownStatus);
      jobs.push(job);
    }
    return jobs;
  }

  /**
   * Provide counts structured like BullBoard expects.
   */
  async getJobCounts(): Promise<
    Record<
      | 'active'
      | 'waiting'
      | 'delayed'
      | 'completed'
      | 'failed'
      | 'paused'
      | 'waiting-children'
      | 'prioritized',
      number
    >
  > {
    const [active, waiting, delayed, completed, failed] = await Promise.all([
      this.getActiveCount(),
      this.getWaitingCount(),
      this.getDelayedCount(),
      this.getCompletedCount(),
      this.getFailedCount(),
    ]);

    return {
      active,
      waiting,
      delayed,
      completed,
      failed,
      paused: 0,
      'waiting-children': 0,
      prioritized: 0,
    };
  }

  /**
   * Check for stalled jobs and recover or fail them
   * Returns array of [jobId, groupId, action] tuples
   */
  async checkStalledJobs(
    now: number,
    gracePeriod: number,
    maxStalledCount: number,
  ): Promise<string[]> {
    try {
      const results = await evalScript<string[]>(
        this.r,
        'check-stalled',
        [this.ns, String(now), String(gracePeriod), String(maxStalledCount)],
        1,
      );
      return results || [];
    } catch (error) {
      this.logger.error('Error checking stalled jobs:', error);
      return [];
    }
  }

  /**
   * Start the promoter service for staging system.
   * Promoter listens to Redis keyspace notifications and promotes staged jobs when ready.
   * This is idempotent - calling multiple times has no effect if already running.
   */
  async startPromoter(): Promise<void> {
    if (this.promoterRunning || this.orderingDelayMs <= 0) {
      return; // Already running or not needed
    }

    this.promoterRunning = true;
    this.promoterLockId = randomUUID();

    try {
      // Create duplicate Redis connection for pub/sub
      this.promoterRedis = this.r.duplicate();

      // Try to enable keyspace notifications
      try {
        await this.promoterRedis.config('SET', 'notify-keyspace-events', 'Ex');
        this.logger.debug(
          'Enabled Redis keyspace notifications for staging promoter',
        );
      } catch (err) {
        this.logger.warn(
          'Failed to enable keyspace notifications. Promoter will use polling fallback.',
          err,
        );
      }

      // Get Redis database number for keyspace event channel
      const db = this.promoterRedis.options.db ?? 0;

      const timerKey = `${this.ns}:stage:timer`;
      const expiredChannel = `__keyevent@${db}__:expired`;

      // Subscribe to keyspace expiration events
      await this.promoterRedis.subscribe(expiredChannel, (err) => {
        if (err) {
          this.logger.error('Failed to subscribe to keyspace events:', err);
        } else {
          this.logger.debug(`Subscribed to ${expiredChannel}`);
        }
      });

      // Handle expiration events
      this.promoterRedis.on('message', async (channel, message) => {
        if (channel === expiredChannel && message === timerKey) {
          await this.runPromotion();
        }
      });

      // Fallback: polling interval (100ms) in case keyspace notifications fail
      this.promoterInterval = setInterval(async () => {
        await this.runPromotion();
      }, 100);

      // Initial promotion check
      await this.runPromotion();

      this.logger.debug('Staging promoter started');
    } catch (err) {
      this.logger.error('Failed to start promoter:', err);
      this.promoterRunning = false;
      await this.stopPromoter();
    }
  }

  /**
   * Run a single promotion cycle with distributed locking
   */
  private async runPromotion(): Promise<void> {
    if (!this.promoterRunning) {
      return;
    }

    const lockKey = `${this.ns}:promoter:lock`;
    const lockTtl = 30000; // 30 seconds

    try {
      // Try to acquire lock
      const acquired = await this.r.set(
        lockKey,
        this.promoterLockId!,
        'PX',
        lockTtl,
        'NX',
      );

      if (acquired === 'OK') {
        try {
          // Promote staged jobs
          const promoted = await evalScript<number>(
            this.r,
            'promote-staged',
            [
              this.ns,
              String(Date.now()),
              String(100), // Limit per batch
            ],
            1,
          );

          if (promoted > 0) {
            this.logger.debug(`Promoted ${promoted} staged jobs`);
          }
        } finally {
          // Release lock (only if it's still ours)
          const currentLockValue = await this.r.get(lockKey);
          if (currentLockValue === this.promoterLockId) {
            await this.r.del(lockKey);
          }
        }
      }
    } catch (err) {
      this.logger.error('Error during promotion:', err);
    }
  }

  /**
   * Stop the promoter service
   */
  async stopPromoter(): Promise<void> {
    if (!this.promoterRunning) return;

    this.promoterRunning = false;

    // Clear interval
    if (this.promoterInterval) {
      clearInterval(this.promoterInterval);
      this.promoterInterval = undefined;
    }

    // Close promoter Redis connection
    if (this.promoterRedis) {
      try {
        await this.promoterRedis.unsubscribe();
        await this.promoterRedis.quit();
      } catch (_err) {
        try {
          this.promoterRedis.disconnect();
        } catch (_e) {}
      }
      this.promoterRedis = undefined;
    }

    this.logger.debug('Staging promoter stopped');
  }

  /**
   * Close underlying Redis connections
   */
  async close(): Promise<void> {
    // Flush any pending batched jobs before closing
    if (this.batchConfig && this.batchBuffer.length > 0) {
      this.logger.debug(
        `Flushing ${this.batchBuffer.length} pending batched jobs before close`,
      );
      await this.flushBatch();
    }

    // Stop promoter
    await this.stopPromoter();

    try {
      await this.r.quit();
    } catch (_e) {
      try {
        this.r.disconnect();
      } catch (_e2) {}
    }
  }

  // --------------------- Pause/Resume ---------------------
  private get pausedKey(): string {
    return `${this.ns}:paused`;
  }

  async pause(): Promise<void> {
    await this.r.set(this.pausedKey, '1');
  }

  async resume(): Promise<void> {
    await this.r.del(this.pausedKey);
  }

  async isPaused(): Promise<boolean> {
    const v = await this.r.get(this.pausedKey);
    return v !== null;
  }

  /**
   * Wait for the queue to become empty (no active jobs)
   * @param timeoutMs Maximum time to wait in milliseconds (default: 60 seconds)
   * @returns true if queue became empty, false if timeout reached
   */
  async waitForEmpty(timeoutMs = 60_000): Promise<boolean> {
    const startTime = Date.now();

    while (Date.now() - startTime < timeoutMs) {
      try {
        // Single atomic Lua script checks all queue structures
        const isEmpty = await evalScript<number>(
          this.r,
          'is-empty',
          [this.ns],
          1,
        );

        if (isEmpty === 1) {
          await sleep(0);
          return true;
        }

        await sleep(200);
      } catch (err) {
        // Handle connection errors gracefully - Redis might be temporarily unavailable
        if (this.isConnectionError(err)) {
          this.logger.warn(
            'Redis connection error in waitForEmpty, retrying...',
          );
          // Wait longer before retry on connection errors
          await sleep(1000);
          continue;
        }
        // For non-connection errors, rethrow
        throw err;
      }
    }

    return false; // Timeout reached
  }

  // Track cleanup calls per group to throttle excessive checking
  private _groupCleanupTracking = new Map<string, number>();

  /**
   * Remove problematic groups from ready queue to prevent infinite loops
   * Handles both poisoned groups (only failed/expired jobs) and locked groups
   *
   * Throttled to 1% sampling rate to reduce Redis overhead
   */
  private async cleanupPoisonedGroup(groupId: string): Promise<string> {
    // Throttle: only check 1% of the time to reduce Redis load
    // This is called frequently when workers compete for groups
    if (Math.random() > 0.01) {
      return 'skipped';
    }

    // Additional throttle: max once per 10 seconds per group
    const lastCheck = this._groupCleanupTracking.get(groupId) || 0;
    const now = Date.now();
    if (now - lastCheck < 10000) {
      return 'throttled';
    }
    this._groupCleanupTracking.set(groupId, now);

    // Periodically clean old tracking entries (keep map bounded)
    if (this._groupCleanupTracking.size > 1000) {
      const cutoff = now - 60000; // 1 minute ago
      for (const [gid, ts] of this._groupCleanupTracking.entries()) {
        if (ts < cutoff) {
          this._groupCleanupTracking.delete(gid);
        }
      }
    }

    try {
      const result = await evalScript<string>(
        this.r,
        'cleanup-poisoned-group',
        [this.ns, groupId, String(now)],
        1,
      );
      if (result === 'poisoned') {
        this.logger.warn(`Removed poisoned group ${groupId} from ready queue`);
      } else if (result === 'empty') {
        this.logger.warn(`Removed empty group ${groupId} from ready queue`);
      } else if (result === 'locked') {
        // Only log locked group warnings occasionally
        if (Math.random() < 0.1) {
          this.logger.debug(
            `Detected group ${groupId} is locked by another worker (this is normal with high concurrency)`,
          );
        }
      }
      return result as string;
    } catch (error) {
      this.logger.error(`Error cleaning up group ${groupId}:`, error);
      return 'error';
    }
  }

  /**
   * Distributed one-shot scheduler: promotes delayed jobs and processes repeating jobs.
   * Only proceeds if a short-lived scheduler lock can be acquired.
   */
  private schedulerLockKey(): string {
    return `${this.ns}:sched:lock`;
  }

  async acquireSchedulerLock(ttlMs = 1500): Promise<boolean> {
    try {
      const res = (await (this.r as any).set(
        this.schedulerLockKey(),
        '1',
        'PX',
        ttlMs,
        'NX',
      )) as string | null;
      return res === 'OK';
    } catch (_e) {
      return false;
    }
  }

  async runSchedulerOnce(now = Date.now()): Promise<void> {
    const ok = await this.acquireSchedulerLock(this.schedulerLockTtlMs);
    if (!ok) return;
    // Reduced limits for faster execution: process a few jobs per tick instead of hundreds
    await this.promoteDelayedJobsBounded(32, now);
    await this.processRepeatingJobsBounded(16, now);
  }

  /**
   * Promote up to `limit` delayed jobs that are due now. Uses a small Lua to move one item per tick.
   */
  async promoteDelayedJobsBounded(
    limit = 256,
    now = Date.now(),
  ): Promise<number> {
    let moved = 0;
    for (let i = 0; i < limit; i++) {
      try {
        const n = await evalScript<number>(
          this.r,
          'promote-delayed-one',
          [this.ns, String(now)],
          1,
        );
        if (!n || n <= 0) break;
        moved += n;
      } catch (_e) {
        break;
      }
    }
    return moved;
  }

  /**
   * Process up to `limit` repeating job ticks.
   * Intentionally small per-tick work to keep Redis CPU flat.
   */
  async processRepeatingJobsBounded(
    limit = 128,
    now = Date.now(),
  ): Promise<number> {
    const scheduleKey = `${this.ns}:repeat:schedule`;
    let processed = 0;
    for (let i = 0; i < limit; i++) {
      // Get one due entry
      const due = await this.r.zrangebyscore(
        scheduleKey,
        0,
        now,
        'LIMIT',
        0,
        1,
      );
      if (!due || due.length === 0) break;
      const repeatKey = due[0];

      try {
        const repeatJobKey = `${this.ns}:repeat:${repeatKey}`;
        const repeatJobDataStr = await this.r.get(repeatJobKey);

        if (!repeatJobDataStr) {
          await this.r.zrem(scheduleKey, repeatKey);
          continue;
        }

        const repeatJobData = JSON.parse(repeatJobDataStr);
        if (repeatJobData.removed) {
          await this.r.zrem(scheduleKey, repeatKey);
          await this.r.del(repeatJobKey);
          continue;
        }

        // Remove from schedule first to prevent duplicates
        await this.r.zrem(scheduleKey, repeatKey);

        // Compute next run
        let nextRunTime: number;
        if ('every' in repeatJobData.repeat) {
          nextRunTime = now + repeatJobData.repeat.every;
        } else {
          nextRunTime = this.getNextCronTime(repeatJobData.repeat.pattern, now);
        }

        repeatJobData.nextRunTime = nextRunTime;
        repeatJobData.lastRunTime = now;
        await this.r.set(repeatJobKey, JSON.stringify(repeatJobData));
        await this.r.zadd(scheduleKey, nextRunTime, repeatKey);

        // Enqueue the instance
        await evalScript<string>(
          this.r,
          'enqueue',
          [
            this.ns,
            repeatJobData.groupId,
            JSON.stringify(repeatJobData.data),
            String(repeatJobData.maxAttempts ?? this.defaultMaxAttempts),
            String(repeatJobData.orderMs ?? now),
            String(0),
            String(randomUUID()),
            String(this.keepCompleted),
          ],
          1,
        );

        processed++;
      } catch (error) {
        this.logger.error(
          `Error processing repeating job ${repeatKey}:`,
          error,
        );
        await this.r.zrem(scheduleKey, repeatKey);
      }
    }
    return processed;
  }

  /**
   * Promote delayed jobs that are now ready to be processed
   * This should be called periodically to move jobs from delayed set to ready queue
   */
  async promoteDelayedJobs(): Promise<number> {
    try {
      return await evalScript<number>(
        this.r,
        'promote-delayed-jobs',
        [this.ns, String(Date.now())],
        1,
      );
    } catch (error) {
      this.logger.error(`Error promoting delayed jobs:`, error);
      return 0;
    }
  }

  /**
   * Change the delay of a specific job
   */
  async changeDelay(jobId: string, newDelay: number): Promise<boolean> {
    const newDelayUntil = newDelay > 0 ? Date.now() + newDelay : 0;

    try {
      const result = await evalScript<number>(
        this.r,
        'change-delay',
        [this.ns, jobId, String(newDelayUntil), String(Date.now())],
        1,
      );
      return result === 1;
    } catch (error) {
      this.logger.error(`Error changing delay for job ${jobId}:`, error);
      return false;
    }
  }

  /**
   * Promote a delayed job to be ready immediately
   */
  async promote(jobId: string): Promise<boolean> {
    return this.changeDelay(jobId, 0);
  }

  /**
   * Remove a job from the queue regardless of state (waiting, delayed, processing)
   */
  async remove(jobId: string): Promise<boolean> {
    try {
      const result = await evalScript<number>(
        this.r,
        'remove',
        [this.ns, jobId],
        1,
      );
      return result === 1;
    } catch (error) {
      this.logger.error(`Error removing job ${jobId}:`, error);
      return false;
    }
  }

  /**
   * Clean jobs of a given status older than graceTimeMs
   * @param graceTimeMs Remove jobs with finishedOn <= now - graceTimeMs (for completed/failed)
   * @param limit Max number of jobs to clean in one call
   * @param status Either 'completed' or 'failed'
   */
  async clean(
    graceTimeMs: number,
    limit: number,
    status: 'completed' | 'failed' | 'delayed',
  ): Promise<number> {
    const graceAt = Date.now() - graceTimeMs;
    try {
      const removed = await evalScript<number>(
        this.r,
        'clean-status',
        [
          this.ns,
          status,
          String(graceAt),
          String(Math.max(0, Math.min(limit, 100000))),
        ],
        1,
      );
      return removed ?? 0;
    } catch (error) {
      console.log('HERE?', error);

      this.logger.error(`Error cleaning ${status} jobs:`, error);
      return 0;
    }
  }

  /**
   * Update a job's data payload (BullMQ-style)
   */
  async updateData(jobId: string, data: T): Promise<void> {
    const jobKey = `${this.ns}:job:${jobId}`;
    const exists = await this.r.exists(jobKey);
    if (!exists) {
      throw new Error(`Job ${jobId} not found`);
    }
    const serialized = JSON.stringify(data === undefined ? null : data);
    await this.r.hset(jobKey, 'data', serialized);
  }

  /**
   * Add a repeating job (cron job)
   */
  private async addRepeatingJob(opts: AddOptions<T>): Promise<JobEntity> {
    if (!opts.repeat) {
      throw new Error('Repeat options are required for repeating jobs');
    }

    const now = Date.now();
    // Make repeatKey unique by including a timestamp and random component
    const repeatKey = `${opts.groupId}:${JSON.stringify(opts.repeat)}:${now}:${Math.random().toString(36).slice(2)}`;

    // Calculate next run time
    let nextRunTime: number;

    if ('every' in opts.repeat) {
      // Simple interval-based repeat
      nextRunTime = now + opts.repeat.every;
    } else {
      // Cron pattern-based repeat
      nextRunTime = this.getNextCronTime(opts.repeat.pattern, now);
    }

    // Store repeating job metadata
    const repeatJobData = {
      groupId: opts.groupId,
      data: opts.data === undefined ? null : opts.data,
      maxAttempts: opts.maxAttempts ?? this.defaultMaxAttempts,
      orderMs: opts.orderMs,
      repeat: opts.repeat,
      nextRunTime,
      lastRunTime: null as number | null,
      removed: false, // Track if this repeat job has been removed
    };

    // Store in Redis (metadata JSON)
    const repeatJobKey = `${this.ns}:repeat:${repeatKey}`;
    await this.r.set(repeatJobKey, JSON.stringify(repeatJobData));

    // Add to repeating jobs sorted set for efficient scheduling
    await this.r.zadd(`${this.ns}:repeat:schedule`, nextRunTime, repeatKey);

    // Create a reverse mapping for easier removal
    const lookupKey = `${this.ns}:repeat:lookup:${opts.groupId}:${JSON.stringify(opts.repeat)}`;
    await this.r.set(lookupKey, repeatKey);

    // Persist a synthetic Job entity for this repeating definition so that
    // Queue.add consistently returns a Job. Use a special repeat id namespace.
    const repeatId = `repeat:${repeatKey}`;
    const jobHashKey = `${this.ns}:job:${repeatId}`;
    try {
      await this.r.hmset(
        jobHashKey,
        'id',
        repeatId,
        'groupId',
        repeatJobData.groupId,
        'data',
        JSON.stringify(repeatJobData.data),
        'attempts',
        '0',
        'maxAttempts',
        String(repeatJobData.maxAttempts),
        'seq',
        '0',
        'timestamp',
        String(Date.now()),
        'orderMs',
        String(repeatJobData.orderMs ?? now),
        'status',
        'waiting',
      );
    } catch (_e) {
      // best-effort; even if this fails, the repeat metadata exists
    }

    // Don't schedule the first job immediately - let the cron processor handle it
    // Return the persisted Job entity handle for the repeating definition
    return JobEntity.fromStore<T>(this as any, repeatId);
  }

  /**
   * Compute next execution time using cron-parser (BullMQ-style)
   */
  private getNextCronTime(pattern: string, fromTime: number): number {
    try {
      const interval = CronParser.parseExpression(pattern, {
        currentDate: new Date(fromTime),
      });
      return interval.next().getTime();
    } catch (_e) {
      throw new Error(`Invalid cron pattern: ${pattern}`);
    }
  }

  /**
   * Remove a repeating job
   */
  async removeRepeatingJob(
    groupId: string,
    repeat: RepeatOptions,
  ): Promise<boolean> {
    try {
      // Use the lookup key to find the actual repeatKey
      const lookupKey = `${this.ns}:repeat:lookup:${groupId}:${JSON.stringify(repeat)}`;
      const repeatKey = await this.r.get(lookupKey);

      if (!repeatKey) {
        // No such repeating job exists
        return false;
      }

      const repeatJobKey = `${this.ns}:repeat:${repeatKey}`;
      const scheduleKey = `${this.ns}:repeat:schedule`;

      // Get the repeat job data before modifying
      const repeatJobDataStr = await this.r.get(repeatJobKey);

      if (!repeatJobDataStr) {
        // Clean up orphaned lookup
        await this.r.del(lookupKey);
        return false;
      }

      const repeatJobData = JSON.parse(repeatJobDataStr);

      // Mark as removed to prevent future scheduling
      repeatJobData.removed = true;
      await this.r.set(repeatJobKey, JSON.stringify(repeatJobData));

      // Remove from future schedule (but keep the metadata for cleanup)
      await this.r.zrem(scheduleKey, repeatKey);

      // Clean up the lookup key
      await this.r.del(lookupKey);

      // Note: Cleanup of existing job instances is best-effort and not critical.
      // Jobs will naturally complete or be cleaned up by the retention policies.

      // Remove the synthetic repeat job hash persisted at creation time
      try {
        const repeatId = `repeat:${repeatKey}`;
        await this.r.del(`${this.ns}:job:${repeatId}`);
      } catch (_e) {
        // best-effort cleanup
      }

      return true;
    } catch (error) {
      this.logger.error(`Error removing repeating job:`, error);
      return false;
    }
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
