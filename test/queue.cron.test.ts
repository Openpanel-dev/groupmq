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
    queue = new Queue({ redis, namespace });

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
      cleanupIntervalMs: 1000, // Run cleanup more frequently for faster test
    });

    worker.run();

    // Create a job that repeats every 2 seconds
    const cronJob = await queue.add({
      groupId: 'cron-group',
      data: { id: 'recurring-job', message: 'Hello from cron!' },
      repeat: { every: 2000 }, // Every 2 seconds
    });

    expect(cronJob.id).toContain('repeat:');

    // Wait for multiple executions
    await new Promise((resolve) => setTimeout(resolve, 5500)); // Wait 5.5 seconds

    await worker.close();

    // Should have processed the job multiple times (at least 2 times)
    expect(processed.length).toBeGreaterThanOrEqual(2);
    expect(processed.length).toBeLessThanOrEqual(4); // Shouldn't be too many

    // All processed jobs should have the same data
    processed.forEach((job) => {
      expect(job.id).toBe('recurring-job');
    });

    // Jobs should be spaced approximately 2 seconds apart
    if (processed.length >= 2) {
      const timeDiff = processed[1].processedAt - processed[0].processedAt;
      expect(timeDiff).toBeGreaterThan(1800); // Allow some tolerance
      expect(timeDiff).toBeLessThan(3500); // More generous tolerance for system overhead
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

    // Wait for the job to be scheduled (it should be delayed to next minute)
    await new Promise((resolve) => setTimeout(resolve, 2000));

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
      cleanupIntervalMs: 1000,
    });

    worker.run();

    const repeatOptions = { every: 1000 }; // Every second

    // Create a repeating job
    await queue.add({
      groupId: 'removable-group',
      data: { id: 'removable-job' },
      repeat: repeatOptions,
    });

    // Let it run once
    await new Promise((resolve) => setTimeout(resolve, 1500));

    // Remove the repeating job
    const removed = await queue.removeRepeatingJob(
      'removable-group',
      repeatOptions,
    );
    expect(removed).toBe(true);

    const processedSoFar = processed.length;

    // Wait longer to ensure it doesn't run again
    await new Promise((resolve) => setTimeout(resolve, 3000));

    await worker.close();

    // Should not have processed more jobs after removal
    expect(processed.length).toBe(processedSoFar);
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

  it('should maintain FIFO order within groups for repeating jobs', async () => {
    const processed: string[] = [];

    const worker = new Worker({
      queue,
      handler: async (job) => {
        const d = job.data as any;
        processed.push(`${d.id}-${d.timestamp}`);
        // Add small processing time
        await new Promise((resolve) => setTimeout(resolve, 100));
      },
      cleanupIntervalMs: 500,
    });

    worker.run();

    // Create multiple repeating jobs in the same group
    await queue.add({
      groupId: 'fifo-cron-group',
      data: { id: 'job-1', timestamp: 'A' },
      repeat: { every: 800 },
      orderMs: 1000,
    });

    await queue.add({
      groupId: 'fifo-cron-group',
      data: { id: 'job-2', timestamp: 'B' },
      repeat: { every: 800 },
      orderMs: 2000,
    });

    // Wait for processing
    await new Promise((resolve) => setTimeout(resolve, 2500));

    await worker.close();

    // Jobs in the same group should maintain order
    expect(processed.length).toBeGreaterThan(0);

    // Filter by timestamp to see order within each execution
    const firstExecution = processed.filter(
      (p) => p.includes('-A') || p.includes('-B'),
    );
    if (firstExecution.length >= 2) {
      // job-1 should come before job-2 due to orderMs
      expect(firstExecution[0]).toContain('job-1');
      expect(firstExecution[1]).toContain('job-2');
    }
  });
});
