import { randomUUID } from 'node:crypto';
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
 * Options for adding a job to the queue
 *
 * @template T The type of data to store in the job
 */
export type AddOptions<T> = {
  /**
   * Group ID for this job. Jobs with the same groupId are processed sequentially (FIFO).
   * Only one job per group can be processed at a time.
   * If omitted or empty string, job uses fast simple queue (no group locking, faster).
   *
   * @example 'user-123' // All jobs for user 123
   * @example 'email-notifications' // All email jobs
   * @example undefined // Fast path for non-grouped events
   *
   * **Best practices:**
   * - Use meaningful group IDs (user ID, resource ID, etc.) when ordering matters
   * - Omit groupId for events that don't need ordering guarantees (faster)
   * - Keep group IDs consistent for related jobs
   * - Avoid too many unique groups (can impact performance)
   */
  groupId?: string;

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
  public orderingDelayMs: number;
  public name: string;

  // Internal tracking for adaptive behavior
  private _consecutiveEmptyReserves = 0;

  // Promoter service for staging system
  private promoterRedis?: Redis;
  private promoterRunning = false;
  private promoterLockId?: string;
  private promoterInterval?: NodeJS.Timeout;
  private promoterBackoffMs = 100;

  // Auto-batching for high-throughput scenarios
  private batchConfig?: { size: number; maxWaitMs: number };
  private batchBuffer: Array<{
    groupId?: string;
    data: T | null;
    jobId: string;
    maxAttempts: number;
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
    const jobId = opts.jobId ?? randomUUID();

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
    });
  }

  private async addSingle(opts: {
    groupId?: string;
    data: T | null;
    jobId: string;
    maxAttempts: number;
    orderMs: number;
  }): Promise<JobEntity<T>> {

    const serializedPayload = JSON.stringify(opts.data);

    // Use simple enqueue for non-grouped jobs (faster path)
    if (!opts.groupId || opts.groupId === '') {
      const now = Date.now();
      const result = await evalScript<string[] | string>(
        this.r,
        'enqueue-simple',
        [
          this.ns,
          String(opts.jobId),
          serializedPayload,
          String(opts.orderMs),
          String(opts.maxAttempts),
          String(this.keepCompleted),
          String(now),
        ],
        1,
      );

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
            groupId: returnedGroupId || '',
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

      // Defensive fallback: current Lua scripts always return an array (even on
      // dedup), but if a stale script ever returns just a jobId, build the Job
      // synthetically from the input opts rather than re-reading the hash —
      // retention may have already trimmed it, and re-reading would throw.
      return this.buildSyntheticDedupJob(result, opts, serializedPayload);
    }

    // Grouped job path (with staging support)
    const now = Date.now();
    const result = await evalScript<string[] | string>(
      this.r,
      'enqueue',
      [
        this.ns,
        opts.groupId!,
        serializedPayload,
        String(opts.maxAttempts),
        String(opts.orderMs),
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

    // Defensive fallback: same race avoidance as the simple path above.
    return this.buildSyntheticDedupJob(result, opts, serializedPayload);
  }

  private buildSyntheticDedupJob(
    id: string,
    opts: {
      groupId?: string;
      jobId: string;
      maxAttempts: number;
      orderMs: number;
    },
    serializedPayload: string,
  ): JobEntity<T> {
    return JobEntity.fromRawHash<T>(
      this,
      id,
      {
        id,
        groupId: opts.groupId ?? '',
        data: serializedPayload,
        attempts: '0',
        maxAttempts: String(opts.maxAttempts),
        timestamp: String(Date.now()),
        orderMs: String(opts.orderMs),
        delayUntil: '0',
        status: 'unknown',
      },
      'unknown',
    );
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

  /**
   * Reserve a job from the simple queue (non-grouped jobs, faster path)
   */
  async reserveSimple(): Promise<ReservedJob<T> | null> {
    const now = Date.now();

    const raw = await evalScript<string | null>(
      this.r,
      'reserve-simple',
      [this.ns, String(now), String(this.vt)],
      1,
    );

    if (!raw) return null;

    const parts = raw.split('||GROUPMQ||');
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
      groupId: parts[1] || '', // Empty for simple jobs
      data,
      attempts: Number.parseInt(parts[3], 10),
      maxAttempts: Number.parseInt(parts[4], 10),
      seq: Number.parseInt(parts[5], 10),
      timestamp: Number.parseInt(parts[6], 10),
      orderMs: Number.isNaN(parsedOrderMs)
        ? Number.parseInt(parts[6], 10)
        : parsedOrderMs,
      score: Number(parts[8]),
      deadlineAt: Number.parseInt(parts[9], 10),
    } as ReservedJob<T>;

    return job;
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

    const parts = raw.split('||GROUPMQ||');
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
      const parts = result.split('||GROUPMQ||');
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
   * Uses distributed lock to ensure only one worker runs cleanup at a time.
   */
  async cleanup(): Promise<number> {
    // Try to acquire cleanup lock
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
  private getBlockTimeout(maxTimeout: number): number {
    const minimumBlockTimeout = 0.001; // 1ms like BullMQ for fast job pickup
    const maximumBlockTimeout = 5; // 5s max to reduce idle CPU usage

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

    // Use BullMQ-style adaptive timeout, but cap to next delayed job's due time
    // so we don't block for the full timeout when a retried job is pending
    let adaptiveTimeout = this.getBlockTimeout(timeoutSec);
    const nextDelayed = await this.r.zrange(
      nsKey(this.ns, 'delayed'),
      0,
      0,
      'WITHSCORES',
    );
    if (nextDelayed.length >= 2) {
      const msUntilDue = Math.max(0, Number(nextDelayed[1]) - Date.now());
      const secUntilDue = Math.max(0.001, msUntilDue / 1000);
      if (secUntilDue < adaptiveTimeout) {
        adaptiveTimeout = secUntilDue;
      }
    }

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
        this.logger.debug(
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
            this.logger.debug(
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
    const parts = result.split('||GROUPMQ||');
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
      const parts = r.split('||GROUPMQ||');
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
    const [active, waiting, completed, failed] = await Promise.all([
      this.getActiveCount(),
      this.getWaitingCount(),
      this.getCompletedCount(),
      this.getFailedCount(),
    ]);

    return {
      active,
      waiting,
      delayed: 0,
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
          const promoted = await this.runPromotion();
          if (promoted > 0) {
            this.promoterBackoffMs = 100;
          }
        }
      });

      // Fallback: adaptive polling in case keyspace notifications miss events.
      // Backs off up to 5s when idle, resets to 100ms when jobs are found.
      this.promoterBackoffMs = 100;
      const schedulePromoterPoll = () => {
        this.promoterInterval = setTimeout(async () => {
          if (!this.promoterRunning) return;
          const promoted = await this.runPromotion();
          if (promoted > 0) {
            this.promoterBackoffMs = 100;
          } else {
            this.promoterBackoffMs = Math.min(
              Math.ceil(this.promoterBackoffMs * 1.5),
              5000,
            );
          }
          schedulePromoterPoll();
        }, this.promoterBackoffMs);
      };
      schedulePromoterPoll();

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
   * Run a single promotion cycle with distributed locking.
   * Returns the number of jobs promoted (used for adaptive backoff).
   */
  private async runPromotion(): Promise<number> {
    if (!this.promoterRunning) {
      return 0;
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
          return promoted ?? 0;
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
    return 0;
  }

  /**
   * Stop the promoter service
   */
  async stopPromoter(): Promise<void> {
    if (!this.promoterRunning) return;

    this.promoterRunning = false;

    // Clear pending poll timeout
    if (this.promoterInterval) {
      clearTimeout(this.promoterInterval);
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
   * Remove a job from the queue regardless of state (waiting, processing)
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
    status: 'completed' | 'failed',
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

}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
