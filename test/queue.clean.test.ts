import Redis from 'ioredis';
import { afterAll, describe, expect, it } from 'vitest';
import { Queue, Worker } from '../src';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';

describe('Queue.clean', () => {
  const namespace = `test:clean:${Date.now()}`;

  afterAll(async () => {
    const redis = new Redis(REDIS_URL);
    const keys = await redis.keys(`${namespace}*`);
    if (keys.length) await redis.del(keys);
    await redis.quit();
  });

  it('cleans completed jobs older than grace time', async () => {
    const redis = new Redis(REDIS_URL);
    const q = new Queue<{ n: number }>({
      redis,
      namespace: `${namespace}:completed`,
      keepCompleted: 100,
    });

    // Add and process two jobs
    const w = new Worker<{ n: number }>({
      queue: q,
      handler: async () => 'ok',
    });
    w.run();
    await q.add({ groupId: 'g1', data: { n: 1 } });
    await q.add({ groupId: 'g1', data: { n: 2 } });
    await q.waitForEmpty();

    // Ensure there are completed jobs
    const before = await q.getCompletedCount();
    expect(before).toBeGreaterThanOrEqual(2);

    // Clean all completed (grace 0 => older than now)
    const cleaned = await q.clean(0, Number.MAX_SAFE_INTEGER, 'completed');
    expect(cleaned).toBeGreaterThanOrEqual(2);

    const after = await q.getCompletedCount();
    expect(after).toBe(0);

    await w.close();
    await redis.quit();
  });

  it('cleans failed jobs older than grace time', async () => {
    const redis = new Redis(REDIS_URL);
    const q = new Queue<{ n: number }>({
      redis,
      namespace: `${namespace}:failed`,
      keepFailed: 100,
    });

    const w = new Worker<{ n: number }>({
      maxAttempts: 1,
      queue: q,
      handler: async () => {
        throw new Error('fail');
      },
    });
    w.run();

    await q.add({ groupId: 'g1', data: { n: 1 } });
    await q.add({ groupId: 'g1', data: { n: 2 } });
    await q.waitForEmpty();

    const before = await q.getFailedCount();
    expect(before).toBeGreaterThanOrEqual(2);

    const cleaned = await q.clean(0, Number.MAX_SAFE_INTEGER, 'failed');
    expect(cleaned).toBeGreaterThanOrEqual(2);

    const after = await q.getFailedCount();
    expect(after).toBe(0);

    await w.close();
    await redis.quit();
  });

  it('cleans delayed jobs older than grace time', async () => {
    const redis = new Redis(REDIS_URL);
    const q = new Queue<{ n: number }>({
      redis,
      namespace: `${namespace}:delayed`,
      keepCompleted: 100,
      keepFailed: 100,
    });

    // Add two delayed jobs 10 minutes in the future
    await q.add({ groupId: 'g1', data: { n: 1 }, delay: 600_000 });
    await q.add({ groupId: 'g1', data: { n: 2 }, delay: 600_000 });

    // There should be no processing, just delayed
    const beforeDelayed = await q.getDelayedCount();
    expect(beforeDelayed).toBeGreaterThanOrEqual(2);

    // Clean all delayed (grace 0 => score <= now)
    // Since delayed scores are > now, use a large grace to include them artificially: graceAt = now - (-infinity)
    // Instead, we simulate by cleaning with graceAt far in the future: implement uses now-grace, so pass negative to include future
    const cleaned = await q.clean(
      -1 * 24 * 60 * 60 * 1000,
      Number.MAX_SAFE_INTEGER,
      'delayed',
    );
    expect(cleaned).toBeGreaterThanOrEqual(2);

    const afterDelayed = await q.getDelayedCount();
    expect(afterDelayed).toBe(0);

    await redis.quit();
  });
});
