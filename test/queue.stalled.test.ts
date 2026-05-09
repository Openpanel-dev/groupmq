import Redis from 'ioredis';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { Queue } from '../src/queue';
import { Worker } from '../src/worker';

/**
 * Stalled Job Detection Tests
 *
 * These tests document the stalled job detection feature.
 * Stalled jobs occur when a worker crashes or loses connection while processing.
 *
 * How it works:
 * - Background checker runs every `stalledInterval` ms (default: 30s)
 * - Detects jobs in processing state past their deadline + grace period
 * - Recovers jobs (moves back to waiting) if stalledCount < maxStalledCount
 * - Fails jobs permanently if stalledCount >= maxStalledCount
 *
 * Configuration:
 * - stalledInterval: How often to check (ms)
 * - maxStalledCount: Max stalls before failing
 * - stalledGracePeriod: Grace period before marking as stalled (ms)
 *
 * Events:
 * - 'stalled': Emitted when a stalled job is detected (jobId, groupId)
 */
describe('Stalled Job Detection Tests', () => {
  let redis: Redis;
  let queue: Queue;
  let workers: Worker[] = [];
  let namespace: string;

  beforeEach(async () => {
    namespace = `test-stalled-${Date.now()}-${Math.random().toString(36).slice(2)}`;

    redis = new Redis({
      host: 'localhost',
      port: 6379,
      maxRetriesPerRequest: null,
    });

    const keys = await redis.keys(`groupmq:${namespace}:*`);
    if (keys.length > 0) {
      await redis.del(...keys);
    }

    queue = new Queue({
      redis: redis.duplicate(),
      namespace,
      jobTimeoutMs: 5000,
    });
  });

  afterEach(async () => {
    await Promise.all(workers.map((w) => w.close(0)));
    workers = [];

    const keys = await redis.keys(`groupmq:${namespace}:*`);
    if (keys.length > 0) {
      await redis.del(...keys);
    }

    await queue.close();
    await redis.quit();
  });

  it('should support stalled job detection configuration', () => {
    // This test documents the API and configuration options
    const worker = new Worker({
      queue: queue,
      handler: async (job) => {
        return { processed: job.data };
      },
      stalledInterval: 30000, // Check every 30 seconds
      maxStalledCount: 1, // Fail after 1 stall
      stalledGracePeriod: 0, // No grace period
    });

    // Event handler for stalled jobs
    worker.on('stalled', (jobId, groupId) => {
      console.log(`Job ${jobId} from group ${groupId} was stalled`);
    });

    workers.push(worker);

    expect(worker).toBeDefined();
  });

  it('should not interfere with normally completing jobs', async () => {
    const completedJobs: any[] = [];
    const stalledEvents: any[] = [];

    // Add multiple jobs
    for (let i = 0; i < 5; i++) {
      await queue.add({
        groupId: `group-${i}`,
        data: { id: i },
      });
    }

    const worker = new Worker({
      queue: queue,
      handler: async (job) => {
        // Normal job processing - completes quickly
        await new Promise((resolve) => setTimeout(resolve, 50));
        return { processed: job.data };
      },
      stalledInterval: 200, // Check frequently
      maxStalledCount: 1,
    });

    worker.on('completed', (job) => {
      completedJobs.push(job);
    });

    worker.on('stalled', (jobId, groupId) => {
      stalledEvents.push({ jobId, groupId });
    });

    workers.push(worker);

    worker.run().catch(() => {});

    // Wait for all jobs to complete
    await new Promise((resolve) => setTimeout(resolve, 1000));

    // All jobs should complete normally
    expect(completedJobs.length).toBe(5);

    // No stalled events should be emitted for normally completing jobs
    expect(stalledEvents.length).toBe(0);

    // No jobs should be in active state
    const activeCount = await queue.getActiveCount();
    expect(activeCount).toBe(0);
  });

  it('self-heals zombie processing entries with no job hash', async () => {
    const ns = `groupmq:${namespace}`;
    const jobId = 'zombie-no-hash';
    const expiredDeadline = Date.now() - 60_000;

    // Simulate the leak shape: processing zset has an entry but the job hash
    // was deleted (e.g. via retention trim or manual cleanup).
    await redis.zadd(`${ns}:processing`, expiredDeadline, jobId);

    const before = await redis.zscore(`${ns}:processing`, jobId);
    expect(before).not.toBeNull();

    const results = await queue.checkStalledJobs(Date.now(), 0, 1);

    // The entry should be gone from the processing zset.
    const after = await redis.zscore(`${ns}:processing`, jobId);
    expect(after).toBeNull();

    // And reported as orphaned so callers can observe the cleanup.
    const idx = results.indexOf(jobId);
    expect(idx).toBeGreaterThanOrEqual(0);
    expect(results[idx + 2]).toBe('orphaned');
  });

  it('self-heals processing entries whose status is not "processing"', async () => {
    const ns = `groupmq:${namespace}`;
    const jobId = 'zombie-status-mismatch';
    const groupId = 'g-zombie';
    const expiredDeadline = Date.now() - 60_000;

    // Job hash exists with a non-"processing" status (e.g. fix-orphaned-active
    // set it back to "waiting" but failed to clean up the processing entry in
    // older versions of the script).
    await redis.hset(`${ns}:job:${jobId}`, {
      groupId,
      status: 'waiting',
      score: '0',
    });
    await redis.zadd(`${ns}:processing`, expiredDeadline, jobId);
    // And it's leaking in the active list too — verify we clear that.
    await redis.lpush(`${ns}:g:${groupId}:active`, jobId);

    const results = await queue.checkStalledJobs(Date.now(), 0, 1);

    const stillProcessing = await redis.zscore(`${ns}:processing`, jobId);
    expect(stillProcessing).toBeNull();

    const stillActive = await redis.lrange(`${ns}:g:${groupId}:active`, 0, -1);
    expect(stillActive).not.toContain(jobId);

    const idx = results.indexOf(jobId);
    expect(idx).toBeGreaterThanOrEqual(0);
    expect(results[idx + 1]).toBe(groupId);
    expect(results[idx + 2]).toBe('orphaned');
  });

  it('should expose queue.checkStalledJobs method for manual checking', async () => {
    // This documents the manual checking API
    const now = Date.now();
    const gracePeriod = 1000;
    const maxStalledCount = 1;

    // The method exists and can be called
    const results = await queue.checkStalledJobs(
      now,
      gracePeriod,
      maxStalledCount,
    );

    // Returns an array (empty if no stalled jobs)
    expect(Array.isArray(results)).toBe(true);
  });
});
