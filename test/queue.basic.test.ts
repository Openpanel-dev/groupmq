import Redis from 'ioredis';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { Queue, Worker } from '../src';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';

describe('basic per-group FIFO and parallelism', () => {
  const redis = new Redis(REDIS_URL);
  const namespace = `test:q1:${Date.now()}`;

  beforeAll(async () => {
    // flush only this namespace keys (best-effort)
    const keys = await redis.keys(`${namespace}*`);
    if (keys.length) await redis.del(keys);
  });

  afterAll(async () => {
    await redis.quit();
  });

  it('should have a name', () => {
    const q = new Queue({ redis, namespace, jobTimeoutMs: 5000 });
    expect(q.name).toBe(namespace);
  });

  it('processes FIFO within group by orderMs and in parallel across groups', async () => {
    const q = new Queue({
      redis,
      namespace,
      jobTimeoutMs: 5000,
      orderingMethod: 'in-memory',
      orderingWindowMs: 200, // 200ms grace period for network jitter
    });

    const seen: Array<string> = [];
    const worker = new Worker<{ n: number }>({
      queue: q,
      handler: async (job) => {
        seen.push(`${job.groupId}:${(job.data as any).n}`);
        await wait(50);
      },
    });
    worker.run();

    // Add jobs with timing that tests grace period:
    // - gB jobs arrive close together (within 200ms grace)
    // - They should be collected and ordered by orderMs
    await q.add({ groupId: 'gA', data: { n: 1 }, orderMs: 1000 });
    await wait(50);
    await q.add({ groupId: 'gB', data: { n: 3 }, orderMs: 1600 });
    await wait(100); // Within 200ms grace - should be collected
    await q.add({ groupId: 'gB', data: { n: 2 }, orderMs: 1500 });
    await wait(50);
    await q.add({ groupId: 'gA', data: { n: 4 }, orderMs: 2000 });

    await q.waitForEmpty();

    // Check FIFO inside each group (should be ordered by orderMs due to buffering)
    const aIndices = seen.filter((s) => s.startsWith('gA:'));
    const bIndices = seen.filter((s) => s.startsWith('gB:'));
    console.log(aIndices);
    console.log(bIndices);
    expect(aIndices).toEqual(['gA:1', 'gA:4']);
    expect(bIndices).toEqual(['gB:2', 'gB:3']); // Correct order due to 200ms buffer

    // Ensure we processed all items
    expect(seen.length).toBe(4);

    await worker.close();
  });
});

async function wait(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}
