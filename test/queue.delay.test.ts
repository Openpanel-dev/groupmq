import Redis from 'ioredis';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { Queue, Worker } from '../src';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';

describe('Delay Jobs Tests', () => {
  const namespace = `test:delay:${Date.now()}`;
  let redis: Redis;
  let queue: Queue;

  beforeAll(async () => {
    redis = new Redis(REDIS_URL);
    queue = new Queue({ redis, namespace });

    // Cleanup
    const keys = await redis.keys(`${namespace}*`);
    if (keys.length) await redis.del(keys);
  });

  afterAll(async () => {
    await redis.quit();
  });

  it('should delay jobs and process them after delay expires', async () => {
    const processed: Array<{ id: string; processedAt: number }> = [];

    const worker = new Worker({
      queue,
      handler: async (job) => {
        processed.push({
          id: (job.data as any).id,
          processedAt: Date.now(),
        });
      },
    });

    worker.run();

    const startTime = Date.now();
    const delayMs = 1000; // 1 second delay

    // Add delayed job
    await queue.add({
      groupId: 'delay-group',
      data: { id: 'delayed-job' },
      delay: delayMs,
    });

    // Add immediate job for comparison
    await queue.add({
      groupId: 'immediate-group',
      data: { id: 'immediate-job' },
    });

    // Wait for processing
    await new Promise((resolve) => setTimeout(resolve, 2000));

    await worker.close();

    // Verify both jobs were processed
    expect(processed).toHaveLength(2);

    const immediateJob = processed.find((p) => p.id === 'immediate-job');
    const delayedJob = processed.find((p) => p.id === 'delayed-job');

    expect(immediateJob).toBeDefined();
    expect(delayedJob).toBeDefined();

    // Verify delayed job was processed after the delay
    const delayedJobProcessTime = delayedJob!.processedAt - startTime;
    expect(delayedJobProcessTime).toBeGreaterThanOrEqual(delayMs - 100); // Allow some tolerance

    // Verify immediate job was processed quickly
    const immediateJobProcessTime = immediateJob!.processedAt - startTime;
    expect(immediateJobProcessTime).toBeLessThan(500);
  });

  it('should handle runAt scheduling', async () => {
    const processed: Array<{ id: string; processedAt: number }> = [];

    const worker = new Worker({
      queue,
      handler: async (job) => {
        processed.push({
          id: (job.data as any).id,
          processedAt: Date.now(),
        });
      },
      cleanupIntervalMs: 100, // Promote delayed jobs more frequently for test
    });

    worker.run();

    const runAt = new Date(Date.now() + 800); // Run in 800ms

    await queue.add({
      groupId: 'scheduled-group',
      data: { id: 'scheduled-job' },
      runAt,
    });

    // Wait for processing
    await new Promise((resolve) => setTimeout(resolve, 1500));

    await worker.close();

    expect(processed).toHaveLength(1);
    expect(processed[0].id).toBe('scheduled-job');

    // Verify job was processed at approximately the right time
    const actualRunTime = processed[0].processedAt;
    const expectedRunTime = runAt.getTime();
    const timeDiff = Math.abs(actualRunTime - expectedRunTime);

    expect(timeDiff).toBeLessThan(300); // Allow 300ms tolerance
  });

  it('should not allow past dates for runAt', async () => {
    const processed: string[] = [];

    const worker = new Worker({
      queue,
      handler: async (job) => {
        processed.push((job.data as any).id);
      },
    });

    worker.run();

    // Try to schedule in the past
    const pastDate = new Date(Date.now() - 5000); // 5 seconds ago

    await queue.add({
      groupId: 'past-group',
      data: { id: 'past-job' },
      runAt: pastDate,
    });

    // Wait for processing
    await new Promise((resolve) => setTimeout(resolve, 500));

    await worker.close();

    // Job should be processed immediately since past dates are clamped to now
    expect(processed).toContain('past-job');
  });

  it('should support changeDelay functionality', async () => {
    const processed: Array<{ id: string; processedAt: number }> = [];

    const worker = new Worker({
      queue,
      handler: async (job) => {
        processed.push({
          id: (job.data as any).id,
          processedAt: Date.now(),
        });
      },
      cleanupIntervalMs: 100, // Promote delayed jobs more frequently for test
    });

    worker.run();

    const startTime = Date.now();

    // Add job with 2 second delay
    const job = await queue.add({
      groupId: 'change-delay-group',
      data: { id: 'changeable-job' },
      delay: 2000,
    });

    // Wait 500ms then change delay to 100ms (so it should run soon)
    await new Promise((resolve) => setTimeout(resolve, 500));
    const changeSuccess = await job.changeDelay(100);
    expect(changeSuccess).toBe(true);

    // Wait for processing
    await new Promise((resolve) => setTimeout(resolve, 1000));

    await worker.close();

    expect(processed).toHaveLength(1);
    expect(processed[0].id).toBe('changeable-job');

    // Job should have been processed around 600ms (500ms + 100ms new delay)
    const actualProcessTime = processed[0].processedAt - startTime;
    expect(actualProcessTime).toBeGreaterThan(500);
    expect(actualProcessTime).toBeLessThan(1000); // Much less than original 2 second delay
  });

  it('should maintain FIFO order within groups even with delays', async () => {
    const processed: string[] = [];

    const worker = new Worker({
      queue,
      handler: async (job) => {
        processed.push((job.data as any).id);
      },
      cleanupIntervalMs: 100, // Promote delayed jobs more frequently for test
    });

    worker.run();

    // Add jobs with different delays but same group
    await queue.add({
      groupId: 'fifo-delay-group',
      data: { id: 'job1' },
      delay: 500,
      orderMs: 1000, // Earlier order
    });

    await queue.add({
      groupId: 'fifo-delay-group',
      data: { id: 'job2' },
      delay: 300, // Shorter delay but later order
      orderMs: 2000,
    });

    // Wait for processing
    await new Promise((resolve) => setTimeout(resolve, 1000));

    await worker.close();

    expect(processed).toHaveLength(2);
    // Even though job2 had shorter delay, job1 should be processed first due to orderMs
    expect(processed).toEqual(['job1', 'job2']);
  });
});
