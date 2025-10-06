import Redis from 'ioredis';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { Queue, Worker } from '../src';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';

describe('Ordering Grace Period (orderingGraceMs)', () => {
  const redis = new Redis(REDIS_URL);
  const namespace = `test:grace:${Date.now()}`;

  beforeAll(async () => {
    const keys = await redis.keys(`${namespace}*`);
    if (keys.length) await redis.del(keys);
  });

  afterAll(async () => {
    await redis.quit();
  });

  it('should handle jobs arriving just a few ms out of order with grace period', async () => {
    const q = new Queue({
      redis,
      namespace: `${namespace}:small-delay`,
      jobTimeoutMs: 5000,
      orderingMethod: 'in-memory',
      orderingWindowMs: 50, // Wait up to 50ms for late arrivals
    });

    const processed: number[] = [];

    const worker = new Worker<{ seq: number }>({
      queue: q,
      handler: async (job) => {
        processed.push(job.data.seq);
        await wait(10);
      },
    });
    worker.run();

    const baseTime = Date.now();

    // Simulate jobs arriving slightly out of order (10-20ms jitter)
    await q.add({
      groupId: 'device-1',
      data: { seq: 2 },
      orderMs: baseTime + 200,
    });
    await wait(15);
    await q.add({
      groupId: 'device-1',
      data: { seq: 1 },
      orderMs: baseTime + 100,
    }); // Late!
    await wait(10);
    await q.add({
      groupId: 'device-1',
      data: { seq: 3 },
      orderMs: baseTime + 300,
    });

    await q.waitForEmpty();
    await wait(100);

    // With grace period, should wait for seq:1 to arrive before processing seq:2
    expect(processed).toEqual([1, 2, 3]);

    console.log('✅ Grace period handled out-of-order arrivals');

    await worker.close();
  });

  it('should work with multiple groups independently', async () => {
    const q = new Queue({
      redis,
      namespace: `${namespace}:multi-group`,
      jobTimeoutMs: 5000,
      orderingMethod: 'in-memory',
      orderingWindowMs: 40,
    });

    const processedByGroup: Record<string, number[]> = {
      'group-A': [],
      'group-B': [],
    };

    const worker = new Worker<{ seq: number }>({
      queue: q,
      handler: async (job) => {
        processedByGroup[job.groupId].push(job.data.seq);
        await wait(10);
      },
    });
    worker.run();

    const baseTime = Date.now();

    // Group A: jobs arrive out of order
    await q.add({
      groupId: 'group-A',
      data: { seq: 2 },
      orderMs: baseTime + 200,
    });
    await wait(10);
    await q.add({
      groupId: 'group-A',
      data: { seq: 1 },
      orderMs: baseTime + 100,
    });

    // Group B: also out of order
    await q.add({
      groupId: 'group-B',
      data: { seq: 3 },
      orderMs: baseTime + 300,
    });
    await wait(15);
    await q.add({
      groupId: 'group-B',
      data: { seq: 2 },
      orderMs: baseTime + 200,
    });
    await wait(10);
    await q.add({
      groupId: 'group-B',
      data: { seq: 1 },
      orderMs: baseTime + 100,
    });

    await q.waitForEmpty();
    await wait(100);

    expect(processedByGroup['group-A']).toEqual([1, 2]);
    expect(processedByGroup['group-B']).toEqual([1, 2, 3]);

    console.log('✅ Grace period works independently for multiple groups');

    await worker.close();
  });

  it('should have low overhead when no out-of-order jobs', async () => {
    const qNormal = new Queue({
      redis,
      namespace: `${namespace}:normal`,
      jobTimeoutMs: 5000,
    });

    const qGrace = new Queue({
      redis,
      namespace: `${namespace}:grace`,
      jobTimeoutMs: 5000,
      orderingMethod: 'in-memory',
      orderingWindowMs: 10, // Smaller grace for performance test
    });

    // Both should process same number of jobs with similar performance
    const baseTime = Date.now();

    // Add jobs in correct order (no waiting needed)
    for (let i = 0; i < 10; i++) {
      await qNormal.add({
        groupId: 'test',
        data: { seq: i },
        orderMs: baseTime + i * 100,
      });
      await qGrace.add({
        groupId: 'test',
        data: { seq: i },
        orderMs: baseTime + i * 100,
      });
    }

    const processedNormal: number[] = [];
    const processedGrace: number[] = [];

    const workerNormal = new Worker<{ seq: number }>({
      queue: qNormal,
      handler: async (job) => {
        processedNormal.push(job.data.seq);
      },
    });

    const workerGrace = new Worker<{ seq: number }>({
      queue: qGrace,
      handler: async (job) => {
        processedGrace.push(job.data.seq);
      },
    });

    const startNormal = Date.now();
    workerNormal.run();
    await qNormal.waitForEmpty();
    const durationNormal = Date.now() - startNormal;

    const startGrace = Date.now();
    workerGrace.run();
    await qGrace.waitForEmpty();
    const durationGrace = Date.now() - startGrace;

    // When jobs are in order, grace period shouldn't add much overhead
    expect(processedNormal).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    expect(processedGrace).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);

    // With 10ms grace and 10 jobs, expect ~100ms overhead max
    // Grace version should be within 2x (10 jobs * 10ms grace = 100ms theoretical overhead)
    expect(durationGrace).toBeLessThan(durationNormal * 2);

    console.log(
      `✅ Performance: Normal=${durationNormal}ms, Grace=${durationGrace}ms`,
    );

    await workerNormal.close();
    await workerGrace.close();
  });

  it('should work without scheduler - pure on-demand', async () => {
    const q = new Queue({
      redis,
      namespace: `${namespace}:no-scheduler`,
      jobTimeoutMs: 5000,
      orderingMethod: 'in-memory',
      orderingWindowMs: 60,
    });

    const processed: number[] = [];

    // Worker with scheduler DISABLED to prove grace period works without it
    const worker = new Worker<{ seq: number }>({
      queue: q,
      handler: async (job) => {
        processed.push(job.data.seq);
      },
      enableCleanup: false, // Disable scheduler
    });
    worker.run();

    const baseTime = Date.now();

    // Add jobs out of order
    await q.add({ groupId: 'test', data: { seq: 3 }, orderMs: baseTime + 300 });
    await wait(20);
    await q.add({ groupId: 'test', data: { seq: 1 }, orderMs: baseTime + 100 });
    await wait(15);
    await q.add({ groupId: 'test', data: { seq: 2 }, orderMs: baseTime + 200 });

    await q.waitForEmpty();

    // Should still be in order despite no scheduler
    expect(processed).toEqual([1, 2, 3]);

    console.log('✅ Works without scheduler - pure on-demand ordering');

    await worker.close();
  });
});

async function wait(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}
