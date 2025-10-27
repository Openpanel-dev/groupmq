import Redis from 'ioredis';
import { afterAll, describe, expect, it } from 'vitest';
import { Queue, Worker } from '../src';

describe('Queue Auto-Batch Simple', () => {
  let redisCleanup: Redis;

  afterAll(async () => {
    // Clean up with a fresh connection
    redisCleanup = new Redis({ maxRetriesPerRequest: null });
    const keys = await redisCleanup.keys('groupmq:test-autobatch-simple:*');
    if (keys.length > 0) {
      await redisCleanup.del(...keys);
    }
    await redisCleanup.quit();
  });

  it('should add jobs with autoBatch enabled', async () => {
    const redis = new Redis({ maxRetriesPerRequest: null });

    // Clean up first
    const keys = await redis.keys('groupmq:test-autobatch-simple:*');
    if (keys.length > 0) {
      await redis.del(...keys);
    }

    const queue = new Queue({
      redis,
      namespace: 'test-autobatch-simple',
      autoBatch: true,
    });

    // Add 5 jobs
    const jobs = await Promise.all([
      queue.add({ groupId: 'g1', data: { index: 0 } }),
      queue.add({ groupId: 'g2', data: { index: 1 } }),
      queue.add({ groupId: 'g3', data: { index: 2 } }),
      queue.add({ groupId: 'g4', data: { index: 3 } }),
      queue.add({ groupId: 'g5', data: { index: 4 } }),
    ]);

    console.log(
      'Jobs added:',
      jobs.map((j) => ({ id: j.id, groupId: j.groupId })),
    );

    // Verify jobs were added
    expect(jobs.length).toBe(5);
    for (const job of jobs) {
      expect(job).toBeDefined();
      expect(job.id).toBeDefined();
      expect(job.groupId).toBeDefined();
    }

    // Verify jobs are in Redis
    const waitingCount = await queue.getWaitingCount();
    console.log('Waiting count:', waitingCount);
    expect(waitingCount).toBe(5);

    await queue.close();
  }, 10000);

  it('should process batched jobs', async () => {
    const redis = new Redis({ maxRetriesPerRequest: null });

    // Clean up first
    const keys = await redis.keys('groupmq:test-autobatch-simple:*');
    if (keys.length > 0) {
      await redis.del(...keys);
    }

    const queue = new Queue({
      redis,
      namespace: 'test-autobatch-simple',
      autoBatch: true,
    });

    const processed: string[] = [];

    const worker = new Worker({
      queue,
      handler: async (job) => {
        processed.push(job.id);
      },
      concurrency: 3,
    });

    // Add 10 jobs
    await Promise.all(
      Array.from({ length: 10 }, (_, i) =>
        queue.add({ groupId: `g${i}`, data: { index: i } }),
      ),
    );

    // Wait for processing
    await new Promise((resolve) => setTimeout(resolve, 3000));

    console.log('Processed:', processed.length);
    expect(processed.length).toBe(10);

    await worker.close();
    await queue.close();
  }, 10000);

  it('should work without autoBatch (baseline)', async () => {
    const redis = new Redis({ maxRetriesPerRequest: null });

    // Clean up first
    const keys = await redis.keys('groupmq:test-autobatch-simple:*');
    if (keys.length > 0) {
      await redis.del(...keys);
    }

    const queue = new Queue({
      redis,
      namespace: 'test-autobatch-simple',
      // No autoBatch
    });

    // Add 5 jobs
    const jobs = await Promise.all([
      queue.add({ groupId: 'g1', data: { index: 0 } }),
      queue.add({ groupId: 'g2', data: { index: 1 } }),
      queue.add({ groupId: 'g3', data: { index: 2 } }),
      queue.add({ groupId: 'g4', data: { index: 3 } }),
      queue.add({ groupId: 'g5', data: { index: 4 } }),
    ]);

    // Verify jobs were added
    expect(jobs.length).toBe(5);
    for (const job of jobs) {
      expect(job).toBeDefined();
      expect(job.id).toBeDefined();
    }

    // Verify jobs are in Redis
    const waitingCount = await queue.getWaitingCount();
    expect(waitingCount).toBe(5);

    await queue.close();
  }, 10000);
});
