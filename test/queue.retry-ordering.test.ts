import Redis from 'ioredis';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { Queue, Worker } from '../src';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';

describe('retry keeps failed job as head and respects backoff', () => {
  const redis = new Redis(REDIS_URL);
  const namespace = `test:q2:${Date.now()}`;

  beforeAll(async () => {
    const keys = await redis.keys(`${namespace}*`);
    if (keys.length) await redis.del(keys);
  });

  afterAll(async () => {
    await redis.quit();
  });

  it('retries a failing job up to maxAttempts and never lets later jobs overtake', async () => {
    const q = new Queue({ redis, namespace, jobTimeoutMs: 800 });

    // add 2 jobs in same group; first will fail 2 times then succeed
    const _j1 = await q.add({
      groupId: 'gX',
      data: { id: 'A' },
      orderMs: 1000,
      maxAttempts: 3,
    });
    const _j2 = await q.add({
      groupId: 'gX',
      data: { id: 'B' },
      orderMs: 2000,
      maxAttempts: 3,
    });

    let aFailures = 0;
    const processed: string[] = [];

    const worker = new Worker<{ id: string }>({
      queue: q,
      blockingTimeoutSec: 1,
      backoff: (_attempt) => 100,
      handler: async (job) => {
        if ((job.data as any).id === 'A' && aFailures < 2) {
          aFailures++;
          throw new Error('boom');
        }
        processed.push((job.data as any).id);
      },
    });
    worker.run();

    await wait(1500);

    // A must be processed before B, despite retries
    expect(processed[0]).toBe('A');
    expect(processed[1]).toBe('B');

    // Ensure A failed twice before success
    expect(aFailures).toBe(2);

    await worker.close();
  });

  it('visibility timeout reclaim works (no heartbeat)', async () => {
    const ns = `${namespace}:vt:${Date.now()}`;
    const r2 = new Redis(REDIS_URL);
    const q = new Queue({ redis: r2, namespace: ns, jobTimeoutMs: 200 });

    await q.add({ groupId: 'g1', data: { n: 1 }, orderMs: 1 });
    await q.add({ groupId: 'g1', data: { n: 2 }, orderMs: 2 });

    // Worker that reserves then crashes (simulate by not completing)
    const job = await q.reserve();
    expect(job).toBeTruthy();

    // Wait for visibility to expire so the group becomes eligible again
    await wait(300);

    const processed: number[] = [];
    const worker = new Worker<{ n: number }>({
      queue: q,
      blockingTimeoutSec: 1,
      handler: async (j) => {
        processed.push((j.data as any).n);
      },
    });
    worker.run();

    await wait(500);

    // We expect item 1 to be retried (at-least-once) and then item 2
    expect(processed[0]).toBe(1);
    expect(processed[1]).toBe(2);

    await worker.close();
    await r2.quit();
  });

  it('processes events in chronological order even when added out of order', async () => {
    const ns = `${namespace}:chrono:${Date.now()}`;
    const r2 = new Redis(REDIS_URL);
    const q = new Queue({
      redis: r2,
      namespace: ns,
      jobTimeoutMs: 1000,
      orderingDelayMs: 100,
    });

    // Add events in the same problematic order from the user's logs
    const baseTime = new Date('2025-09-19T21:01:21.000Z').getTime();

    // Events added in this order (mimicking user's log):
    await q.add({
      groupId: 'device1',
      data: { timestamp: '21:01:21.100Z' },
      orderMs: baseTime + 100,
    });
    await q.add({
      groupId: 'device1',
      data: { timestamp: '21:01:21.103Z' },
      orderMs: baseTime + 103,
    });
    await q.add({
      groupId: 'device1',
      data: { timestamp: '21:01:21.102Z' },
      orderMs: baseTime + 102,
    });
    await q.add({
      groupId: 'device1',
      data: { timestamp: '21:01:21.095Z' },
      orderMs: baseTime + 95,
    }); // Earlier!
    await q.add({
      groupId: 'device1',
      data: { timestamp: '21:01:21.102Z' },
      orderMs: baseTime + 102,
    });

    const processed: string[] = [];
    const worker = new Worker<{ timestamp: string }>({
      queue: q,
      blockingTimeoutSec: 1,
      handler: async (job) => {
        processed.push((job.data as any).timestamp);
      },
    });
    worker.run();

    // Wait for all events to be processed
    await wait(1000);

    // They should be processed in chronological order (by orderMs):
    // 095, 100, 102, 102, 103
    expect(processed[0]).toBe('21:01:21.095Z'); // Earliest should be first
    expect(processed[1]).toBe('21:01:21.100Z');
    expect(processed[2]).toBe('21:01:21.102Z'); // First 102
    expect(processed[3]).toBe('21:01:21.102Z'); // Second 102
    expect(processed[4]).toBe('21:01:21.103Z'); // Latest should be last

    await worker.close();
    await r2.quit();
  });

  it('demonstrates ordering issue with multiple workers on same queue', async () => {
    const ns = `${namespace}:multi:${Date.now()}`;
    const r2 = new Redis(REDIS_URL);
    const q = new Queue({
      redis: r2,
      namespace: ns,
      jobTimeoutMs: 1000,
      orderingDelayMs: 50, // Short delay to see the issue
    });

    // Add sequential events to same group
    const baseTime = Date.now();
    for (let i = 0; i < 10; i++) {
      await q.add({
        groupId: 'device1',
        data: { order: i },
        orderMs: baseTime + i,
      });
    }

    const processed: number[] = [];

    // Create TWO workers (like the user's setup)
    const worker1 = new Worker<{ order: number }>({
      queue: q,
      blockingTimeoutSec: 1,
      handler: async (job) => {
        await wait(10);
        processed.push((job.data as any).order);
      },
    });

    const worker2 = new Worker<{ order: number }>({
      queue: q,
      blockingTimeoutSec: 1,
      handler: async (job) => {
        await wait(10);
        processed.push((job.data as any).order);
      },
    });

    worker1.run();
    worker2.run();

    // Wait for all jobs to process
    await wait(1500);

    // With multiple workers, order might be violated
    // This test documents the problem rather than expecting perfect order
    expect(processed.length).toBe(10);

    // Check if order was maintained (this might fail with multiple workers)
    const isOrdered = processed.every(
      (val, i) => i === 0 || val >= processed[i - 1],
    );
    if (!isOrdered) {
      console.log(
        '⚠️  Ordering violated with multiple workers! Events processed out of order.',
      );
    }

    await worker1.close();
    await worker2.close();
    await r2.quit();
  });
});

async function wait(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}
