import { randomUUID } from 'node:crypto';
import CronParser from 'cron-parser';
import type Redis from 'ioredis';
import { Job as JobEntity } from './job';
import { Logger } from './logger';
import { evalScript } from './lua/loader';
import type { Status } from './status';

export type QueueOptions = {
  logger?: Logger | boolean;
  redis: Redis;
  namespace: string;
  jobTimeoutMs?: number;
  maxAttempts?: number;
  reserveScanLimit?: number;
  orderingDelayMs?: number;
  keepCompleted?: number;
  keepFailed?: number;
};

export type RepeatOptions =
  | {
      every: number;
    }
  | {
      pattern: string;
    };

export type AddOptions<T> = {
  groupId: string;
  data: T;
  orderMs?: number;
  maxAttempts?: number;
  delay?: number;
  runAt?: Date | number;
  repeat?: RepeatOptions;
  jobId?: string; // Optional custom job ID for idempotence
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
  private logger: Logger;
  private r: Redis;
  private blockingR: Redis; // Separate connection for blocking operations
  private rawNs: string;
  private ns: string;
  private vt: number;
  private defaultMaxAttempts: number;
  private scanLimit: number;
  private orderingDelayMs: number;
  private keepCompleted: number;
  private keepFailed: number;

  // Inline defineCommand bindings removed; using external Lua via evalsha

  constructor(opts: QueueOptions) {
    // Use the provided Redis client for main operations to preserve connection semantics
    // and a dedicated duplicate for blocking operations.
    this.r = opts.redis;
    this.blockingR = opts.redis.duplicate();
    this.rawNs = opts.namespace;
    this.ns = `groupmq:${this.rawNs}`;
    const rawVt = opts.jobTimeoutMs ?? 30_000;
    this.vt = Math.max(1, rawVt); // Minimum 1ms
    this.defaultMaxAttempts = opts.maxAttempts ?? 3;
    this.scanLimit = opts.reserveScanLimit ?? 20;
    this.orderingDelayMs = opts.orderingDelayMs ?? 0;
    this.keepCompleted = Math.max(0, opts.keepCompleted ?? 0);
    this.keepFailed = Math.max(0, opts.keepFailed ?? 0);
    // Using external Lua scripts via evalsha; no inline defineCommand
    this.logger =
      typeof opts.logger === 'object'
        ? opts.logger
        : new Logger(!!opts.logger, this.namespace);

    this.r.on('error', (err) => {
      this.logger.error('Redis error (main):', err);
    });

    this.blockingR.on('error', (err) => {
      this.logger.error('Redis error (blocking):', err);
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

  get orderingDelay(): number {
    return this.orderingDelayMs;
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

    // Calculate delay timestamp
    let delayUntil = 0;
    if (opts.delay !== undefined && opts.delay > 0) {
      delayUntil = now + opts.delay;
    } else if (opts.runAt !== undefined) {
      const runAtTimestamp =
        opts.runAt instanceof Date ? opts.runAt.getTime() : opts.runAt;
      delayUntil = Math.max(runAtTimestamp, now); // Don't allow past dates
    }

    // Handle undefined data by converting to null for consistent JSON serialization
    const data = opts.data === undefined ? null : opts.data;
    const serializedPayload = JSON.stringify(data);

    const enqId = await evalScript<string>(this.r, 'enqueue', [
      this.ns,
      opts.groupId,
      serializedPayload,
      String(maxAttempts),
      String(orderMs),
      String(delayUntil),
      String(jobId),
      String(this.keepCompleted),
    ]);
    return this.getJob(enqId);
  }

  async reserve(): Promise<ReservedJob<T> | null> {
    const now = Date.now();

    const raw = await evalScript<string | null>(this.r, 'reserve', [
      this.ns,
      String(now),
      String(this.vt),
      String(this.scanLimit),
      String(this.orderingDelayMs),
    ]);

    if (!raw) return null;

    const parts = raw.split('||DELIMITER||');
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

    const job = {
      id: parts[0],
      groupId: parts[1],
      data,
      attempts: Number.parseInt(parts[3], 10),
      maxAttempts: Number.parseInt(parts[4], 10),
      seq: Number.parseInt(parts[5], 10),
      timestamp: Number.parseInt(parts[6], 10),
      orderMs: Number.parseInt(parts[7], 10),
      score: Number(parts[8]),
      deadlineAt: Number.parseInt(parts[9], 10),
    } as ReservedJob<T>;

    (this as any)._lastJobTime = Date.now();

    return job;
  }

  async complete(job: { id: string; groupId: string }) {
    await evalScript<number>(this.r, 'complete', [
      this.ns,
      job.id,
      job.groupId,
      String(this.keepCompleted),
    ]);
  }

  /**
   * Atomically complete a job and try to reserve the next job from the same group
   * This prevents race conditions where other workers can steal subsequent jobs from the same group
   */
  async completeAndReserveNext(
    completedJobId: string,
    groupId: string,
  ): Promise<ReservedJob<T> | null> {
    const now = Date.now();

    try {
      const result = await evalScript<string | null>(
        this.r,
        'complete-and-reserve-next',
        [
          this.ns,
          completedJobId,
          groupId,
          String(now),
          String(this.vt),
          String(this.orderingDelayMs),
          String(this.keepCompleted),
        ],
      );

      if (!result) {
        return null;
      }

      // Parse the result (same format as reserve methods)
      const parts = result.split('||DELIMITER||');
      if (parts.length !== 10) {
        this.logger.error(
          'Queue completeAndReserveNext: unexpected result format:',
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
        enqueuedAt,
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
        timestamp: parseInt(enqueuedAt, 10),
        orderMs: parseInt(orderMs, 10),
        score: parseFloat(score),
        deadlineAt: parseInt(deadline, 10),
      };
    } catch (error) {
      this.logger.error('Queue completeAndReserveNext error:', error);
      return null;
    }
  }

  async retry(jobId: string, backoffMs = 0) {
    return evalScript<number>(this.r, 'retry', [
      this.ns,
      jobId,
      String(backoffMs),
    ]);
  }

  /**
   * Dead letter a job (remove from group and optionally store in dead letter queue)
   */
  async deadLetter(jobId: string, groupId: string) {
    const script = `
local ns = "${this.ns}"
local jobId = ARGV[1]
local groupId = ARGV[2]
local gZ = ns .. ":g:" .. groupId
local readyKey = ns .. ":ready"

-- Remove job from group
redis.call("ZREM", gZ, jobId)

-- Remove from processing if it's there
redis.call("DEL", ns .. ":processing:" .. jobId)
redis.call("ZREM", ns .. ":processing", jobId)

-- Remove idempotence mapping to allow reuse
redis.call("DEL", ns .. ":unique:" .. jobId)

-- Remove group lock if this job holds it
local lockKey = ns .. ":lock:" .. groupId
local lockValue = redis.call("GET", lockKey)
if lockValue == jobId then
  redis.call("DEL", lockKey)
end

-- Check if group is now empty or should be removed from ready queue
local jobCount = redis.call("ZCARD", gZ)
if jobCount == 0 then
  -- Group is empty, remove from ready queue
  redis.call("ZREM", readyKey, groupId)
else
  -- Group still has jobs, update ready queue with new head
  local head = redis.call("ZRANGE", gZ, 0, 0, "WITHSCORES")
  if head and #head >= 2 then
    local headScore = tonumber(head[2])
    redis.call("ZADD", readyKey, headScore, groupId)
  end
end

-- Optionally store in dead letter queue (uncomment if needed)
-- redis.call("LPUSH", ns .. ":dead", jobId)

return 1
    `;

    try {
      return await this.r.eval(script, 0, jobId, groupId);
    } catch (error) {
      this.logger.error(
        `Queue ${this.ns} error dead lettering job ${jobId}:`,
        error,
      );
      throw error;
    }
  }

  /**
   * Record a successful completion for retention and inspection
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
    const jobKey = `${this.ns}:job:${job.id}`;
    const completedKey = `${this.ns}:completed`;

    const processedOn = meta.processedOn ?? Date.now();
    const finishedOn = meta.finishedOn ?? Date.now();
    const attempts = meta.attempts ?? 0;
    const maxAttempts = meta.maxAttempts ?? this.defaultMaxAttempts;

    const pipe = this.r.multi();
    pipe.hset(
      jobKey,
      'status',
      'completed',
      'processedOn',
      String(processedOn),
      'finishedOn',
      String(finishedOn),
      'attempts',
      String(attempts),
      'maxAttempts',
      String(maxAttempts),
      'returnvalue',
      JSON.stringify(result ?? null),
    );
    pipe.zadd(completedKey, finishedOn, job.id);

    // Trim to retention
    if (this.keepCompleted >= 0) {
      // Remove older than keepCompleted (keep newest)
      // Calculate how many to remove: zcard - keep
      pipe.zcard(completedKey);
    }

    const replies = await pipe.exec();

    // If not keeping completed jobs at all, drop idempotence mapping immediately
    if (this.keepCompleted === 0) {
      try {
        await this.r.del(`${this.ns}:unique:${job.id}`);
      } catch (_e) {
        // ignore
      }
    }

    if (this.keepCompleted >= 0) {
      const zcardReply = replies?.[replies.length - 1]?.[1] as
        | number
        | undefined;
      if (typeof zcardReply === 'number') {
        const toRemove = Math.max(0, zcardReply - this.keepCompleted);
        if (toRemove > 0) {
          const oldIds = await this.r.zrange(completedKey, 0, toRemove - 1);
          if (oldIds.length > 0) {
            const delPipe = this.r.multi();
            delPipe.zremrangebyrank(completedKey, 0, toRemove - 1);
            for (const oldId of oldIds) {
              delPipe.del(`${this.ns}:job:${oldId}`);
              // Also remove idempotence mapping so the same jobId can be reused
              delPipe.del(`${this.ns}:unique:${oldId}`);
            }
            await delPipe.exec();
          }
        }
      }
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
    const jobKey = `${this.ns}:job:${job.id}`;
    const failedKey = `${this.ns}:failed`;

    const processedOn = meta.processedOn ?? Date.now();
    const finishedOn = meta.finishedOn ?? Date.now();
    const attempts = meta.attempts ?? 0;
    const maxAttempts = meta.maxAttempts ?? this.defaultMaxAttempts;

    const message =
      typeof error === 'string' ? error : (error.message ?? 'Error');
    const name = typeof error === 'string' ? 'Error' : (error.name ?? 'Error');
    const stack = typeof error === 'string' ? '' : (error.stack ?? '');

    const pipe = this.r.multi();
    pipe.hset(
      jobKey,
      'status',
      'failed',
      'failedReason',
      message,
      'failedName',
      name,
      'stacktrace',
      stack,
      'processedOn',
      String(processedOn),
      'finishedOn',
      String(finishedOn),
      'attempts',
      String(attempts),
      'maxAttempts',
      String(maxAttempts),
    );
    pipe.zadd(failedKey, finishedOn, job.id);

    if (this.keepFailed >= 0) {
      pipe.zcard(failedKey);
    }

    const replies = await pipe.exec();

    if (this.keepFailed >= 0) {
      const zcardReply = replies?.[replies.length - 1]?.[1] as
        | number
        | undefined;
      if (typeof zcardReply === 'number') {
        const toRemove = Math.max(0, zcardReply - this.keepFailed);
        if (toRemove > 0) {
          const oldIds = await this.r.zrange(failedKey, 0, toRemove - 1);
          if (oldIds.length > 0) {
            const delPipe = this.r.multi();
            delPipe.zremrangebyrank(failedKey, 0, toRemove - 1);
            for (const oldId of oldIds) {
              delPipe.del(`${this.ns}:job:${oldId}`);
              // Remove idempotence mapping to allow reuse
              delPipe.del(`${this.ns}:unique:${oldId}`);
            }
            await delPipe.exec();
          }
        }
      }
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
    const jobs: Array<JobEntity<T>> = [];
    for (const id of ids) {
      const j = await this.getJob(id);
      if (j) jobs.push(j);
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
    const jobs: Array<JobEntity<T>> = [];
    for (const id of ids) {
      const j = await this.getJob(id);
      if (j) jobs.push(j);
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
    return evalScript<number>(this.r, 'heartbeat', [
      this.ns,
      job.id,
      job.groupId,
      String(extendMs),
    ]);
  }

  async cleanup(): Promise<number> {
    const now = Date.now();
    return evalScript<number>(this.r, 'cleanup', [this.ns, String(now)]);
  }

  /**
   * Calculate adaptive blocking timeout like BullMQ
   * Returns timeout in seconds
   *
   * Inspiration by BullMQ ⭐️
   */
  private getBlockTimeout(maxTimeout: number, blockUntil?: number): number {
    const minimumBlockTimeout = 0.001; // 1ms like BullMQ
    const maximumBlockTimeout = 10; // 10s max like BullMQ

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

    // Factor in ordering delays for smarter blocking
    if (this.orderingDelayMs > 0) {
      const nextEligibleTime = this.calculateNextEligibleJobTime();
      if (nextEligibleTime > 0) {
        const delayUntilEligible = nextEligibleTime - Date.now();
        if (
          delayUntilEligible > 0 &&
          delayUntilEligible < maximumBlockTimeout * 1000
        ) {
          return Math.max(minimumBlockTimeout, delayUntilEligible / 1000);
        }
      }
    }

    // Enhanced activity-based adaptive timeout with worker context
    const now = Date.now();
    const recentActivity = (this as any)._lastJobTime || 0;
    const timeSinceLastJob = now - recentActivity;

    // Track failed reserves for more intelligent adaptation
    const consecutiveEmptyReserves =
      (this as any)._consecutiveEmptyReserves || 0;

    if (timeSinceLastJob < 500) {
      // Very recent activity - stay highly responsive
      return minimumBlockTimeout;
    }

    if (timeSinceLastJob < 2000) {
      // Recent activity (< 2s) - responsive but slightly longer for efficiency
      return Math.min(0.01, maxTimeout); // 10ms
    }

    if (timeSinceLastJob < 10000) {
      // Moderate activity (< 10s) - balance responsiveness and efficiency
      return Math.min(0.1, maxTimeout); // 100ms
    }

    if (timeSinceLastJob < 60000) {
      // Some recent activity (< 1min) - lean towards efficiency
      return Math.min(1.0, maxTimeout); // 1s
    }

    // No recent activity but check if we've been seeing empty reserves
    if (consecutiveEmptyReserves > 5) {
      // If we're consistently getting empty reserves, use longer timeout to reduce Redis load
      return Math.min(Math.max(2.0, maxTimeout * 0.5), maximumBlockTimeout); // At least 2s, up to half max
    }

    // No recent activity - use full timeout for efficiency
    return Math.min(maxTimeout, maximumBlockTimeout);
  }

  /**
   * Calculate when the next job will be eligible for processing
   * considering ordering delays
   */
  private calculateNextEligibleJobTime(): number {
    // This would require tracking delayed groups, for now return 0
    // Could be enhanced with a separate delayed jobs timeline
    return 0;
  }

  /**
   * Check if an error is a Redis connection error
   */
  private isConnectionError(err: any): boolean {
    // Check for common Redis connection error patterns
    if (!err) return false;

    const message = err.message || '';
    const code = err.code || '';

    return (
      message.includes('Connection is closed') ||
      message.includes('connect ECONNREFUSED') ||
      message.includes('ENOTFOUND') ||
      message.includes('ECONNRESET') ||
      message.includes('ETIMEDOUT') ||
      code === 'ECONNREFUSED' ||
      code === 'ENOTFOUND' ||
      code === 'ECONNRESET' ||
      code === 'ETIMEDOUT'
    );
  }

  async reserveBlocking(
    timeoutSec = 5,
    blockUntil?: number,
  ): Promise<ReservedJob<T> | null> {
    const startTime = Date.now();

    // Short-circuit if paused
    if (await this.isPaused()) {
      await sleep(50);
      return null;
    }

    // First try immediate reserve (fast path)
    const immediateJob = await this.reserve();
    if (immediateJob) {
      this.logger.info(
        `Immediate reserve successful (${Date.now() - startTime}ms)`,
      );
      return immediateJob;
    }

    // Use BullMQ-style adaptive timeout with delayed job consideration
    const adaptiveTimeout = this.getBlockTimeout(timeoutSec, blockUntil);
    this.logger.info(
      `Starting blocking operation (timeout: ${adaptiveTimeout}s)`,
    );

    // Use ready queue for blocking behavior (more reliable than marker system)
    const readyKey = nsKey(this.ns, 'ready');

    let connectionTimeout: NodeJS.Timeout | undefined;

    try {
      // Set up connection timeout like BullMQ to handle Redis connection issues
      if (adaptiveTimeout > 0) {
        connectionTimeout = setTimeout(
          () => {
            this.logger.warn(
              `Connection timeout triggered after ${adaptiveTimeout}s - forcing disconnect`,
            );
            // Disconnect and reconnect the blocking connection if no response
            this.blockingR.disconnect(false);
            // Proactively reconnect to avoid a stuck disconnected state
            try {
              void (this.blockingR as any).connect?.();
            } catch (_e) {
              // ignore reconnect errors here; reserveBlocking will retry
            }
          },
          adaptiveTimeout * 1000 + 1000,
        ); // Extra 1 second buffer
      }

      // Log queue state before blocking
      const readyCount = await this.r.zcard(readyKey);
      this.logger.info(`Blocking state: ${readyCount} groups in ready queue`);

      // Use dedicated blocking connection to avoid interfering with other operations
      const bzpopminStart = Date.now();
      const result = await this.blockingR.bzpopmin(readyKey, adaptiveTimeout);
      const bzpopminDuration = Date.now() - bzpopminStart;

      if (!result || result.length < 3) {
        this.logger.info(`Blocking timeout/empty (took ${bzpopminDuration}ms)`);
        // Track consecutive empty reserves for adaptive timeout
        (this as any)._consecutiveEmptyReserves =
          ((this as any)._consecutiveEmptyReserves || 0) + 1;
        return null; // Timeout or no result
      }

      const [, groupId, score] = result;
      this.logger.info(
        `Blocking result: group=${groupId}, score=${score} (took ${bzpopminDuration}ms)`,
      );

      // Try to reserve directly from the specific group first to reduce contention
      const reserveStart = Date.now();
      let job = await this.reserveFromGroup(groupId);

      if (!job) {
        // Group might be empty or locked, put it back and try general reserve
        await this.r.zadd(readyKey, score, groupId);
        job = await this.reserve();
      }
      const reserveDuration = Date.now() - reserveStart;

      if (job) {
        this.logger.info(
          `Successful job reserve after blocking: ${job.id} from group ${job.groupId} (reserve took ${reserveDuration}ms)`,
        );
        // Track job activity for adaptive timeout
        (this as any)._lastJobTime = Date.now();
        // Reset consecutive empty reserves counter
        (this as any)._consecutiveEmptyReserves = 0;
      } else {
        this.logger.warn(
          `Blocking found group but reserve failed: group=${groupId} (reserve took ${reserveDuration}ms)`,
        );

        // CRITICAL FIX: Handle both poisoned groups and temporarily locked groups
        const cleanupResult = await this.cleanupPoisonedGroup(groupId);

        if (cleanupResult === 'locked') {
          // Group is temporarily locked by another worker, back off to avoid spinning
          this.logger.info(
            `Group ${groupId} is locked, backing off to prevent infinite loop`,
          );
          // Use a short delay to prevent immediate retry of the same locked group
          await new Promise((resolve) => setTimeout(resolve, 50));
        }
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
      // Clear the connection timeout
      if (connectionTimeout) {
        clearTimeout(connectionTimeout);
      }

      const totalDuration = Date.now() - startTime;
      if (totalDuration > 1000) {
        this.logger.info(`ReserveBlocking completed in ${totalDuration}ms`);
      }
    }
  }

  /**
   * Reserve a job from a specific group (optimized for blocking operations)
   */
  async reserveFromGroup(groupId: string): Promise<ReservedJob<T> | null> {
    const now = Date.now();

    const result = await evalScript<string | null>(
      this.r,
      'reserve-from-group',
      [
        this.ns,
        String(now),
        String(this.vt),
        String(groupId),
        String(this.orderingDelayMs || 0),
      ],
    );
    if (!result) return null;

    // Parse the delimited string response (same format as regular reserve)
    const parts = result.split('||DELIMITER||');
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

    return {
      id,
      groupId: groupIdRaw,
      data: JSON.parse(data),
      attempts: parseInt(attempts, 10),
      maxAttempts: parseInt(maxAttempts, 10),
      seq: parseInt(seq, 10),
      timestamp: parseInt(timestamp, 10),
      orderMs: parseInt(orderMs, 10),
      score: parseFloat(score),
      deadlineAt: parseInt(deadline, 10),
    };
  }

  /**
   * Get the number of jobs currently being processed (active jobs)
   */
  async getActiveCount(): Promise<number> {
    return evalScript<number>(this.r, 'get-active-count', [this.ns]);
  }

  /**
   * Get the number of jobs waiting to be processed
   */
  async getWaitingCount(): Promise<number> {
    return evalScript<number>(this.r, 'get-waiting-count', [this.ns]);
  }

  /**
   * Get the number of jobs delayed due to backoff
   */
  async getDelayedCount(): Promise<number> {
    return evalScript<number>(this.r, 'get-delayed-count', [this.ns]);
  }

  /**
   * Get list of active job IDs
   */
  async getActiveJobs(): Promise<string[]> {
    return evalScript<string[]>(this.r, 'get-active-jobs', [this.ns]);
  }

  /**
   * Get list of waiting job IDs
   */
  async getWaitingJobs(): Promise<string[]> {
    return evalScript<string[]>(this.r, 'get-waiting-jobs', [this.ns]);
  }

  /**
   * Get list of delayed job IDs
   */
  async getDelayedJobs(): Promise<string[]> {
    return evalScript<string[]>(this.r, 'get-delayed-jobs', [this.ns]);
  }

  /**
   * Get list of unique group IDs that have jobs
   */
  async getUniqueGroups(): Promise<string[]> {
    return evalScript<string[]>(this.r, 'get-unique-groups', [this.ns]);
  }

  /**
   * Get count of unique groups that have jobs
   */
  async getUniqueGroupsCount(): Promise<number> {
    return evalScript<number>(this.r, 'get-unique-groups-count', [this.ns]);
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
   */
  async getJobsByStatus(
    jobStatuses: Array<Status>,
    start = 0,
    end = -1,
  ): Promise<Array<JobEntity<T>>> {
    const idSets: string[] = [];

    // Helper to push a range from a sorted set
    const pushZRange = async (key: string, reverse = false) => {
      try {
        const ids = reverse
          ? await this.r.zrevrange(key, 0, -1)
          : await this.r.zrange(key, 0, -1);
        idSets.push(...ids);
      } catch (_e) {
        // ignore
      }
    };

    const statuses = new Set(jobStatuses);

    if (statuses.has('active')) {
      await pushZRange(`${this.ns}:processing`);
    }
    if (statuses.has('delayed')) {
      await pushZRange(`${this.ns}:delayed`);
    }
    if (statuses.has('completed')) {
      await pushZRange(`${this.ns}:completed`, true);
    }
    if (statuses.has('failed')) {
      await pushZRange(`${this.ns}:failed`, true);
    }
    if (statuses.has('waiting')) {
      // Aggregate waiting jobs by iterating known groups (no KEYS scan)
      try {
        const groupIds = await this.r.smembers(`${this.ns}:groups`);
        if (groupIds.length > 0) {
          const pipe = this.r.multi();
          for (const gid of groupIds) pipe.zrange(`${this.ns}:g:${gid}`, 0, -1);
          const rows = await pipe.exec();
          for (const r of rows || []) {
            const arr = (r?.[1] as string[]) || [];
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

    const pipe = this.r.multi();
    for (const id of slice) {
      pipe.hgetall(`${this.ns}:job:${id}`);
    }
    const rows = await pipe.exec();

    const jobs: Array<JobEntity<T>> = [];
    for (let i = 0; i < slice.length; i++) {
      const id = slice[i];
      const raw = (rows?.[i]?.[1] as Record<string, string>) || {};
      if (!raw || Object.keys(raw).length === 0) continue;
      const built = await this.getJob(id);
      if (built) jobs.push(built);
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
   * Close underlying Redis connections
   */
  async close(): Promise<void> {
    try {
      await this.blockingR.quit();
    } catch (_e) {
      try {
        this.blockingR.disconnect();
      } catch (_e2) {}
    }
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
      const [activeCount, waitingCount, delayedCount] = await Promise.all([
        this.getActiveCount(),
        this.getWaitingCount(),
        this.getDelayedCount(),
      ]);
      if (activeCount === 0 && waitingCount === 0 && delayedCount === 0) {
        return true;
      }

      // Wait a bit before checking again
      await sleep(100);
    }

    return false; // Timeout reached
  }

  /**
   * Remove problematic groups from ready queue to prevent infinite loops
   * Handles both poisoned groups (only failed/expired jobs) and locked groups
   */
  private async cleanupPoisonedGroup(groupId: string): Promise<string> {
    try {
      const result = await evalScript<string>(
        this.r,
        'cleanup-poisoned-group',
        [this.ns, groupId, String(Date.now())],
      );
      if (result === 'poisoned') {
        this.logger.warn(`Removed poisoned group ${groupId} from ready queue`);
      } else if (result === 'empty') {
        this.logger.warn(`Removed empty group ${groupId} from ready queue`);
      } else if (result === 'locked') {
        this.logger.warn(
          `Detected group ${groupId} is locked by another worker`,
        );
      }
      return result as string;
    } catch (error) {
      this.logger.error(`Error cleaning up group ${groupId}:`, error);
      return 'error';
    }
  }

  /**
   * Check for groups that might be ready after their ordering delay has expired.
   * This is a recovery mechanism for groups that were delayed but not re-added to ready queue.
   */
  async recoverDelayedGroups(): Promise<number> {
    if (this.orderingDelayMs <= 0) {
      return 0;
    }

    try {
      const result = await evalScript<number>(
        this.r,
        'recover-delayed-groups',
        [this.ns, String(Date.now()), String(this.orderingDelayMs)],
      );
      return result || 0;
    } catch (error) {
      this.logger.warn('Error in recoverDelayedGroups:', error);
      return 0;
    }
  }

  /**
   * Promote delayed jobs that are now ready to be processed
   * This should be called periodically to move jobs from delayed set to ready queue
   */
  async promoteDelayedJobs(): Promise<number> {
    try {
      return await evalScript<number>(this.r, 'promote-delayed-jobs', [
        this.ns,
        String(Date.now()),
      ]);
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
      const result = await evalScript<number>(this.r, 'change-delay', [
        this.ns,
        jobId,
        String(newDelayUntil),
        String(Date.now()),
      ]);
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
      const result = await evalScript<number>(this.r, 'remove', [
        this.ns,
        jobId,
      ]);
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
      const removed = await evalScript<number>(this.r, 'clean-status', [
        this.ns,
        status,
        String(graceAt),
        String(Math.max(0, limit)),
      ]);
      return removed ?? 0;
    } catch (error) {
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
   * Process repeating jobs that are due to run
   */
  async processRepeatingJobs(): Promise<number> {
    const now = Date.now();
    const scheduleKey = `${this.ns}:repeat:schedule`;

    // Get jobs that are ready to run
    const readyJobs = await this.r.zrangebyscore(scheduleKey, 0, now);

    let processedCount = 0;

    for (const repeatKey of readyJobs) {
      try {
        const repeatJobKey = `${this.ns}:repeat:${repeatKey}`;
        const repeatJobDataStr = await this.r.get(repeatJobKey);

        if (!repeatJobDataStr) {
          // Clean up orphaned entry
          await this.r.zrem(scheduleKey, repeatKey);
          continue;
        }

        const repeatJobData = JSON.parse(repeatJobDataStr);

        // Check if this repeat job has been removed
        if (repeatJobData.removed) {
          // Don't process removed jobs, just clean up
          await this.r.zrem(scheduleKey, repeatKey);
          await this.r.del(repeatJobKey);
          continue;
        }

        // CRITICAL: Remove from schedule FIRST to prevent duplicate processing
        await this.r.zrem(scheduleKey, repeatKey);

        // Calculate next run time
        let nextRunTime: number;
        if ('every' in repeatJobData.repeat) {
          nextRunTime = now + repeatJobData.repeat.every;
        } else {
          nextRunTime = this.getNextCronTime(repeatJobData.repeat.pattern, now);
        }

        // Update repeat job data
        repeatJobData.nextRunTime = nextRunTime;
        repeatJobData.lastRunTime = now;

        // Save updated data
        await this.r.set(repeatJobKey, JSON.stringify(repeatJobData));

        // Schedule next occurrence
        await this.r.zadd(scheduleKey, nextRunTime, repeatKey);

        // Enqueue the job for now - preserve original orderMs to maintain FIFO
        await evalScript<string>(this.r, 'enqueue', [
          this.ns,
          repeatJobData.groupId,
          JSON.stringify(repeatJobData.data),
          String(repeatJobData.maxAttempts),
          String(repeatJobData.orderMs),
          String(0),
          String(randomUUID()),
          String(this.keepCompleted),
        ]);

        processedCount++;
      } catch (error) {
        this.logger.error(
          `Error processing repeating job ${repeatKey}:`,
          error,
        );
        // Remove problematic job
        await this.r.zrem(scheduleKey, repeatKey);
      }
    }

    return processedCount;
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

      // Clean up any existing jobs that match this repeat pattern
      await this.cleanupRepeatingJobInstances(groupId, repeatJobData);

      // Also remove the synthetic repeat job hash persisted at creation time
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

  /**
   * Clean up existing job instances from a removed repeating job
   */
  private async cleanupRepeatingJobInstances(
    groupId: string,
    repeatJobData: any,
  ): Promise<void> {
    const delayedKey = `${this.ns}:delayed`;
    const readyKey = `${this.ns}:ready`;
    const groupKey = `${this.ns}:g:${groupId}`;

    // Clean up any delayed jobs
    const allDelayedJobs = await this.r.zrange(delayedKey, 0, -1);

    for (const jobId of allDelayedJobs) {
      const jobKey = `${this.ns}:job:${jobId}`;
      const jobData = await this.r.hmget(jobKey, 'groupId', 'data', 'data');

      const rawData = jobData[1] ?? jobData[2];
      if (jobData[0] === groupId && rawData) {
        try {
          const data = JSON.parse(rawData);
          if (JSON.stringify(data) === JSON.stringify(repeatJobData.data)) {
            await this.r.zrem(delayedKey, jobId);
            await this.r.zrem(groupKey, jobId);
            await this.r.del(jobKey);
            await this.r.del(`${this.ns}:unique:${jobId}`);
          }
        } catch (_e) {
          // ignore
        }
      }
    }

    // Clean up any ready jobs in the group that match this pattern
    const allGroupJobs = await this.r.zrange(groupKey, 0, -1);

    for (const jobId of allGroupJobs) {
      const jobKey = `${this.ns}:job:${jobId}`;
      const jobData = await this.r.hmget(jobKey, 'groupId', 'data', 'data');

      const rawData2 = jobData[1] ?? jobData[2];
      if (jobData[0] === groupId && rawData2) {
        try {
          const data = JSON.parse(rawData2);
          if (JSON.stringify(data) === JSON.stringify(repeatJobData.data)) {
            await this.r.zrem(groupKey, jobId);
            await this.r.del(jobKey);
            await this.r.del(`${this.ns}:unique:${jobId}`);
          }
        } catch (_e) {
          // ignore
        }
      }
    }

    // Check if group needs to be removed from ready queue
    const groupSize = await this.r.zcard(groupKey);
    if (groupSize === 0) {
      await this.r.zrem(readyKey, groupId);
    }
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
