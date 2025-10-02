import Redis from 'ioredis';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { Queue, Worker } from '../src';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';

describe('grouping', () => {
  let redis: Redis;
  let namespace: string;

  beforeEach(async () => {
    // Create fresh Redis connection and namespace for each test
    redis = new Redis(REDIS_URL);
    namespace = `test:q1:${Date.now()}:${Math.random().toString(36).substring(7)}`;

    // flush only this namespace keys (best-effort)
    const keys = await redis.keys(`${namespace}*`);
    if (keys.length) await redis.del(keys);
  });

  afterEach(async () => {
    // Clean up after each test
    const keys = await redis.keys(`${namespace}*`);
    if (keys.length) await redis.del(keys);
    await redis.quit();
  });

  it('process jobs in correct order based on orderMs', async () => {
    const q = new Queue({ redis, namespace, jobTimeoutMs: 5000 });

    const order: Array<string> = [];
    const worker = new Worker<{ n: number }>({
      queue: q,
      handler: async (job) => {
        order.push(`${job.groupId}:${(job.data as any).n}`);
        await wait(50);
      },
    });
    const jobs = [
      {
        groupId: 'g1',
        data: { n: 2 },
        orderMs: new Date('2025-01-01 00:00:00.500').getTime(),
      },
      {
        groupId: 'g1',
        data: { n: 4 },
        orderMs: new Date('2025-01-01 00:01:01.000').getTime(),
      },
      {
        groupId: 'g1',
        data: { n: 3 },
        orderMs: new Date('2025-01-01 00:00:00.800').getTime(),
      },
      {
        groupId: 'g1',
        data: { n: 1 },
        orderMs: new Date('2025-01-01 00:00:00.000').getTime(),
      },
    ];

    for (const job of jobs) {
      await q.add(job);
    }

    worker.run();

    await q.waitForEmpty();

    expect(order).toEqual(
      jobs
        .slice()
        .sort((a, b) => a.orderMs - b.orderMs)
        .map((j) => `${j.groupId}:${j.data.n}`),
    );

    await worker.close();
  });

  it('should handle ordering delay for late events', async () => {
    const orderingDelayMs = 1000; // 1 second delay (shorter for faster test)
    const q = new Queue({
      redis,
      namespace: `${namespace}:delay`,
      orderingDelayMs,
    });

    const order: Array<string> = [];
    const worker = new Worker<{ n: number }>({
      queue: q,
      handler: async (job) => {
        order.push(`${job.groupId}:${job.data.n}`);
        await wait(10);
      },
      blockingTimeoutSec: 1,
    });

    const now = Date.now();

    // Scenario: Events arrive out of order, but we want to process them in order
    // Enqueue jobs with timestamps in a way that tests the delay
    await q.add({
      groupId: 'delay-group',
      data: { n: 3 },
      orderMs: now + 1500, // Future timestamp, should be delayed
    });

    await q.add({
      groupId: 'delay-group',
      data: { n: 1 },
      orderMs: now - 5000, // Past timestamp, should be processed immediately
    });

    await q.add({
      groupId: 'delay-group',
      data: { n: 2 },
      orderMs: now - 1000, // Past timestamp, between job 1 and 3
    });

    // Start worker
    worker.run();

    // Wait for processing to complete (longer wait to ensure future job is processed)
    await wait(3500);

    // Should process in correct chronological order
    expect(order.length).toBe(3);
    expect(order).toEqual(['delay-group:1', 'delay-group:2', 'delay-group:3']);

    await worker.close();
  }, 5000); // Timeout for the 3.5s wait + buffer
});

async function wait(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}
