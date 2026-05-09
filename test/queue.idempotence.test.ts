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

  // Regression: the dedup branch used to return a bare jobId from Lua, after
  // which JS did a follow-up HGETALL via getJob(). Under high throughput with
  // small `keepCompleted`, retention could trim the hash between Lua returning
  // and getJob running, causing add() to throw "Job not found" even though the
  // original add succeeded. The fix reads the existing job inside the Lua
  // script so the response is atomic with the dedup check.
  it('dedup returns the existing job data atomically (grouped path)', async () => {
    const redis = new Redis(REDIS_URL);
    const q = new Queue({
      redis,
      namespace: `${namespace}:dedup-atomic-grouped`,
      keepCompleted: 1,
    });

    const id = 'dedup-atomic-grouped-id';
    const job1 = await q.add({ groupId: 'g1', data: { n: 1 }, jobId: id });
    expect(job1.id).toBe(id);
    expect(job1.data).toEqual({ n: 1 });

    // Job is still in the group zset (no worker), so this hits the
    // "exists & in-group" dedup branch — exactly the path that used to
    // return a bare jobId.
    const job2 = await q.add({ groupId: 'g1', data: { n: 2 }, jobId: id });
    expect(job2).toBeDefined();
    expect(job2.id).toBe(id);
    // Dedup must return the *existing* job's data, not the second caller's,
    // proving the response was sourced from Redis inside the script.
    expect(job2.data).toEqual({ n: 1 });

    await redis.quit();
  });

  it('dedup returns the existing job data atomically (simple path)', async () => {
    const redis = new Redis(REDIS_URL);
    const q = new Queue({
      redis,
      namespace: `${namespace}:dedup-atomic-simple`,
      keepCompleted: 1,
    });

    const id = 'dedup-atomic-simple-id';
    const job1 = await q.add({ data: { n: 1 }, jobId: id });
    expect(job1.id).toBe(id);
    expect(job1.data).toEqual({ n: 1 });

    const job2 = await q.add({ data: { n: 2 }, jobId: id });
    expect(job2).toBeDefined();
    expect(job2.id).toBe(id);
    expect(job2.data).toEqual({ n: 1 });

    await redis.quit();
  });

  // Stronger guarantee: even if retention trims the hash *after* the dedup
  // script has already returned, add() must not throw. Old code would do a
  // separate HGETALL here and blow up.
  it('does not throw if retention trims the hash after dedup returns', async () => {
    const redis = new Redis(REDIS_URL);
    const q = new Queue({
      redis,
      namespace: `${namespace}:retention-race`,
      keepCompleted: 1,
    });

    const id = 'retention-race-id';
    await q.add({ groupId: 'g1', data: { n: 1 }, jobId: id });

    // Wrap eval to delete the hash immediately after the dedup script
    // resolves — the worst-case timing the original bug exploited.
    const origEval = redis.evalsha.bind(redis);
    const jobKey = `${namespace}:retention-race:job:${id}`;
    (redis as any).evalsha = async (...args: any[]) => {
      const result = await (origEval as any)(...args);
      // Only trim after a dedup-shaped reply for our id
      if (Array.isArray(result) && result[0] === id) {
        await redis.del(jobKey);
      }
      return result;
    };

    await expect(
      q.add({ groupId: 'g1', data: { n: 2 }, jobId: id }),
    ).resolves.toBeDefined();

    (redis as any).evalsha = origEval;
    await redis.quit();
  });
});
