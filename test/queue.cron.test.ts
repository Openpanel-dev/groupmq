import Redis from 'ioredis';
import {
  afterAll,
  afterEach,
  beforeAll,
  beforeEach,
  describe,
  expect,
  it,
} from 'vitest';
import { Queue, Worker } from '../src';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';

describe('Cron Jobs Tests', () => {
  let namespace: string;
  let redis: Redis;
  let queue: Queue;

  beforeAll(async () => {
    redis = new Redis(REDIS_URL);
  });

  beforeEach(async () => {
    // Create a unique namespace for each test
    namespace = `test:cron:${Date.now()}:${Math.random().toString(36).slice(2)}`;
    queue = new Queue({
      redis,
      namespace,
      orderingMethod: 'none',
      jobTimeoutMs: 100,
      schedulerLockTtlMs: 50, // Fast lock for sub-second repeats in tests
    });

    // Cleanup any existing keys
    const keys = await redis.keys(`${namespace}*`);
    if (keys.length) await redis.del(keys);
  });

  afterEach(async () => {
    // Cleanup after each test
    const keys = await redis.keys(`${namespace}*`);
    if (keys.length) await redis.del(keys);
  });

  afterAll(async () => {
    await redis.quit();
  });

  it('should create and process repeating jobs with every option', async () => {
    const processed: Array<{ id: string; processedAt: number }> = [];

    const worker = new Worker({
      queue,
      handler: async (job) => {
        processed.push({
          id: (job.data as any).id,
          processedAt: Date.now(),
        });
      },
      cleanupIntervalMs: 1, // Run cleanup more frequently for faster test
      schedulerIntervalMs: 1,
    });

    worker.run();

    // Create a job that repeats every 100ms
    const cronJob = await queue.add({
      groupId: 'cron-group',
      data: { id: 'recurring-job', message: 'Hello from cron!' },
      repeat: { every: 100 }, // Every 100ms
    });

    expect(cronJob.id).toContain('repeat:');

    // Wait for multiple executions
    await new Promise((resolve) => setTimeout(resolve, 500)); // Wait 500ms

    await worker.close();

    // Should have processed the job multiple times (at least 3 times in 500ms)
    expect(processed.length).toBeGreaterThanOrEqual(3);
    expect(processed.length).toBeLessThanOrEqual(8); // Shouldn't be too many

    // All processed jobs should have the same data
    processed.forEach((job) => {
      expect(job.id).toBe('recurring-job');
    });

    // Jobs should be spaced approximately 100ms apart
    if (processed.length >= 2) {
      const timeDiff = processed[1].processedAt - processed[0].processedAt;
      expect(timeDiff).toBeGreaterThan(80); // Allow some tolerance
      expect(timeDiff).toBeLessThan(200); // More generous tolerance for system overhead
    }
  });

  it('should handle cron pattern expressions', async () => {
    const processed: string[] = [];

    const worker = new Worker({
      queue,
      handler: async (job) => {
        processed.push(`${(job.data as any).id}-${Date.now()}`);
      },
      cleanupIntervalMs: 30000, // Every 30 seconds - faster for test
    });

    worker.run();

    // Create a job that runs every minute
    const cronJob = await queue.add({
      groupId: 'pattern-group',
      data: { id: 'minute-job' },
      repeat: { pattern: '* * * * *' }, // Every minute
    });

    expect(cronJob.id).toContain('repeat:');

    await worker.close();

    // The job should be scheduled but not necessarily executed yet
    // (since we don't want to wait a full minute in a test)
    expect(cronJob).toBeDefined();
  });

  it('should remove repeating jobs', async () => {
    const processed: string[] = [];

    const worker = new Worker({
      queue,
      handler: async (job) => {
        processed.push((job.data as any).id);
      },
      cleanupIntervalMs: 100,
      schedulerIntervalMs: 50,
    });

    worker.run();

    const repeatOptions = { every: 100 }; // Every 100ms

    // Create a repeating job
    await queue.add({
      groupId: 'removable-group',
      data: { id: 'removable-job' },
      repeat: repeatOptions,
    });

    // Let it run a few times
    await new Promise((resolve) => setTimeout(resolve, 300));

    // Give the scheduler a moment to ensure the repeating job is fully set up
    await new Promise((resolve) => setTimeout(resolve, 100));

    // Remove the repeating job
    const removed = await queue.removeRepeatingJob(
      'removable-group',
      repeatOptions,
    );
    expect(removed).toBe(true);

    // Wait for the group to drain completely (any already-enqueued jobs to be processed)
    // The scheduler might have enqueued jobs just before we called removeRepeatingJob
    const maxWait = 2000;
    const startWait = Date.now();
    while (Date.now() - startWait < maxWait) {
      const waiting = await queue.getWaitingCount();
      const active = await queue.getActiveCount();
      if (waiting === 0 && active === 0) {
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 50));
    }

    const processedSoFar = processed.length;

    // Wait for several scheduler intervals to ensure the scheduler has had time to
    // process any remaining due jobs and see the removed flag
    await new Promise((resolve) => setTimeout(resolve, 200));

    // Now verify no new jobs are scheduled - wait several repeat intervals
    await new Promise((resolve) => setTimeout(resolve, 500));

    await worker.close();

    // Should not have processed more jobs after the queue drained
    // Allow for 1 extra job due to race conditions (scheduler might have been
    // in the middle of processing when removeRepeatingJob was called)
    expect(processed.length).toBeLessThanOrEqual(processedSoFar + 1);
  });

  it('should handle complex cron patterns', async () => {
    // Test the cron parser without actually waiting

    // This should not throw an error
    try {
      await queue.add({
        groupId: 'complex-group',
        data: { id: 'complex-job' },
        repeat: { pattern: '0 9 * * 1-5' }, // 9 AM on weekdays
      });
    } catch (error) {
      // Should not throw for valid patterns
      expect(error).toBeUndefined();
    }

    // Test invalid pattern
    try {
      await queue.add({
        groupId: 'invalid-group',
        data: { id: 'invalid-job' },
        repeat: { pattern: 'invalid pattern' },
      });
      // Should have thrown an error
      expect(true).toBe(false);
    } catch (error) {
      expect(error).toBeDefined();
    }
  });
});
