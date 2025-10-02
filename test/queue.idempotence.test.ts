import Redis from 'ioredis';
import { afterAll, describe, expect, it } from 'vitest';
import { Queue, Worker } from '../src';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';

describe('Idempotent enqueue with optional jobId', () => {
  const namespace = `test:idempotence:${Date.now()}`;

  afterAll(async () => {
    const redis = new Redis(REDIS_URL);
    const keys = await redis.keys(`${namespace}*`);
    if (keys.length) await redis.del(keys);
    await redis.quit();
  });

  it('should ignore duplicate adds with the same jobId and return same id', async () => {
    const redis = new Redis(REDIS_URL);
    const q = new Queue({ redis, namespace: `${namespace}:dedupe` });

    const customId = 'my-fixed-id';

    const job1 = await q.add({
      groupId: 'g1',
      data: { n: 1 },
      jobId: customId,
    });
    const job2 = await q.add({
      groupId: 'g1',
      data: { n: 2 },
      jobId: customId,
    });

    expect(job1.id).toBe(customId);
    expect(job2.id).toBe(customId);

    // Process and ensure only one job is executed
    const processed: any[] = [];
    const worker = new Worker({
      queue: q,
      blockingTimeoutSec: 1,
      handler: async (job) => {
        processed.push(job.data as any);
      },
    });
    worker.run();

    await q.waitForEmpty();

    expect(processed.length).toBe(1);
    expect(processed[0]).toEqual({ n: 1 });

    await worker.close();
    await redis.quit();
  });

  it('should generate a UUID when jobId is not provided', async () => {
    const redis = new Redis(REDIS_URL);
    const q = new Queue({ redis, namespace: `${namespace}:uuid` });

    const job = await q.add({ groupId: 'g1', data: { a: 1 } });

    // UUID v4 shape check (8-4-4-4-12 hex)
    expect(job.id).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i,
    );

    const processed: any[] = [];
    const worker = new Worker({
      queue: q,
      blockingTimeoutSec: 1,
      handler: async (job) => {
        processed.push(job.id);
      },
    });
    worker.run();

    await q.waitForEmpty();

    expect(processed).toEqual([job.id]);

    await worker.close();
    await redis.quit();
  });

  it('should allow reuse of jobId after job is removed by retention', async () => {
    const redis = new Redis(REDIS_URL);
    const q = new Queue({
      redis,
      namespace: `${namespace}:reuse`,
      keepCompleted: 0,
      keepFailed: 0,
    });

    const customId = 'reusable-id';

    const job = await q.add({
      groupId: 'g1',
      data: { n: 1 },
      jobId: customId,
    });
    expect(job.id).toBe(customId);

    // Process first job
    const processed: any[] = [];
    const worker1 = new Worker({
      name: 'worker1',
      queue: q,
      blockingTimeoutSec: 1,
      handler: async (job) => {
        processed.push({
          ...job.data,
          worker: 'worker1',
        });
      },
    });
    worker1.run();

    await q.waitForEmpty();
    await worker1.close();
    await new Promise((resolve) => setTimeout(resolve, 1000));

    // At this point, keepCompleted:0 should have removed job and unique mapping
    const job2 = await q.add({
      groupId: 'g1',
      data: { n: 2 },
      jobId: customId,
    });
    expect(job2.id).toBe(customId);

    // Process second job
    const worker2 = new Worker({
      name: 'worker2',
      queue: q,
      blockingTimeoutSec: 1,
      handler: async (job) => {
        processed.push({
          ...job.data,
          worker: 'worker2',
        });
      },
    });
    worker2.run();

    await q.waitForEmpty();
    await worker2.close();

    // Verify both jobs were processed correctly
    expect(processed).toEqual([
      { n: 1, worker: 'worker1' },
      { n: 2, worker: 'worker2' },
    ]);

    await redis.quit();
  });
});
