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

describe('Ordering with Staging Tests', () => {
  let namespace: string;
  let redis: Redis;
  let queue: Queue;

  beforeAll(async () => {
    redis = new Redis(REDIS_URL);
  });

  beforeEach(async () => {
    // Create unique namespace for each test to avoid interference
    namespace = `test:ordering:${Date.now()}:${Math.random().toString(36).slice(2)}`;
  });

  afterEach(async () => {
    // Cleanup after each test
    if (queue) {
      // Stop promoter but don't close main Redis connection (shared with tests)
      await queue.stopPromoter();
    }
    const keys = await redis.keys(`groupmq:${namespace}*`);
    if (keys.length) await redis.del(keys);
  });

  afterAll(async () => {
    await redis.quit();
  });

  it('should process jobs in correct orderMs order when arriving out of order', async () => {
    // Create queue with orderingDelayMs to enable staging
    queue = new Queue({
      redis,
      namespace,
      orderingDelayMs: 150, // Wait 150ms to ensure all jobs arrive
    });

    const processed: Array<{ id: number; orderMs: number }> = [];
    let resolveComplete: () => void;
    const completePromise = new Promise<void>((resolve) => {
      resolveComplete = resolve;
    });

    const worker = new Worker({
      queue,
      handler: async (job) => {
        const data = job.data as { id: number };
        processed.push({ id: data.id, orderMs: job.orderMs! });
        if (processed.length === 3) {
          resolveComplete();
        }
      },
    });

    // Add jobs with orderMs timestamps in reverse order of arrival
    const now = Date.now();

    // Job 3 arrives first but has latest orderMs
    await queue.add({
      groupId: 'test-group',
      data: { id: 3 },
      orderMs: now + 100, // Latest timestamp
    });

    // Wait 50ms before adding next job
    await new Promise((resolve) => setTimeout(resolve, 50));

    // Job 2 arrives second
    await queue.add({
      groupId: 'test-group',
      data: { id: 2 },
      orderMs: now + 50, // Middle timestamp
    });

    // Wait 50ms before adding next job
    await new Promise((resolve) => setTimeout(resolve, 50));

    // Job 1 arrives last but has earliest orderMs
    await queue.add({
      groupId: 'test-group',
      data: { id: 1 },
      orderMs: now, // Earliest timestamp
    });

    // Wait for all jobs to be processed
    await completePromise;

    await worker.close();

    // Jobs should be processed in orderMs order, not arrival order
    expect(processed).toHaveLength(3);
    expect(processed[0].id).toBe(1); // Earliest orderMs (now)
    expect(processed[1].id).toBe(2); // Middle orderMs (now+50)
    expect(processed[2].id).toBe(3); // Latest orderMs (now+100)
    expect(processed[0].orderMs).toBe(now);
    expect(processed[1].orderMs).toBe(now + 50);
    expect(processed[2].orderMs).toBe(now + 100);
  });

  it('should not stage jobs when orderMs is not provided', async () => {
    queue = new Queue({
      redis,
      namespace,
      orderingDelayMs: 150,
    });

    const processed: string[] = [];
    let resolveComplete: () => void;
    const completePromise = new Promise<void>((resolve) => {
      resolveComplete = resolve;
    });

    const worker = new Worker({
      queue,
      handler: async (job) => {
        const data = job.data as { id: string };
        processed.push(data.id);
        if (processed.length === 2) {
          resolveComplete();
        }
      },
    });

    // Add jobs without orderMs - should process immediately (not staged)
    await queue.add({
      groupId: 'test-group',
      data: { id: 'first' },
    });

    await queue.add({
      groupId: 'test-group',
      data: { id: 'second' },
    });

    await completePromise;
    await worker.close();

    // Without orderMs, jobs should be processed in arrival order
    expect(processed).toEqual(['first', 'second']);
  });

  it('should not stage jobs when orderingDelayMs is 0', async () => {
    queue = new Queue({
      redis,
      namespace,
      orderingDelayMs: 0, // No staging
    });

    const processed: Array<{ id: number; orderMs: number }> = [];
    let resolveComplete: () => void;
    const completePromise = new Promise<void>((resolve) => {
      resolveComplete = resolve;
    });

    const worker = new Worker({
      queue,
      handler: async (job) => {
        const data = job.data as { id: number };
        processed.push({ id: data.id, orderMs: job.orderMs! });
        if (processed.length === 3) {
          resolveComplete();
        }
      },
    });

    const now = Date.now();

    // Add jobs simultaneously (no delays) with orderMs in reverse order
    const [job3, job2, job1] = await Promise.all([
      queue.add({
        groupId: 'test-group',
        data: { id: 3 },
        orderMs: now + 100,
      }),
      queue.add({
        groupId: 'test-group',
        data: { id: 2 },
        orderMs: now + 50,
      }),
      queue.add({
        groupId: 'test-group',
        data: { id: 1 },
        orderMs: now,
      }),
    ]);

    await completePromise;
    await worker.close();

    // With orderingDelayMs = 0, jobs process in orderMs order (score-based) from ZSET
    expect(processed).toHaveLength(3);
    expect(processed[0].id).toBe(1);
    expect(processed[1].id).toBe(2);
    expect(processed[2].id).toBe(3);
  });

  it('should handle staging for multiple groups independently', async () => {
    queue = new Queue({
      redis,
      namespace,
      orderingDelayMs: 150,
    });

    const processedA: number[] = [];
    const processedB: number[] = [];
    let resolveComplete: () => void;
    const completePromise = new Promise<void>((resolve) => {
      resolveComplete = resolve;
    });

    const worker = new Worker({
      queue,
      handler: async (job) => {
        const data = job.data as { id: number };
        if (job.groupId === 'group-a') {
          processedA.push(data.id);
        } else {
          processedB.push(data.id);
        }
        if (processedA.length === 2 && processedB.length === 2) {
          resolveComplete();
        }
      },
    });

    // Add jobs for group A in reverse order
    await queue.add({
      groupId: 'group-a',
      data: { id: 2 },
      orderMs: 200,
    });

    await queue.add({
      groupId: 'group-a',
      data: { id: 1 },
      orderMs: 100,
    });

    // Add jobs for group B in reverse order
    await queue.add({
      groupId: 'group-b',
      data: { id: 4 },
      orderMs: 400,
    });

    await queue.add({
      groupId: 'group-b',
      data: { id: 3 },
      orderMs: 300,
    });

    await completePromise;
    await worker.close();

    // Each group should process in correct order
    expect(processedA).toEqual([1, 2]);
    expect(processedB).toEqual([3, 4]);
  });

  it('should handle promoter being started multiple times (idempotent)', async () => {
    queue = new Queue({
      redis,
      namespace,
      orderingDelayMs: 150,
    });

    // Start promoter multiple times - should not cause issues
    await queue.startPromoter();
    await queue.startPromoter();
    await queue.startPromoter();

    const processed: number[] = [];
    let resolveComplete: () => void;
    const completePromise = new Promise<void>((resolve) => {
      resolveComplete = resolve;
    });

    const worker = new Worker({
      queue,
      handler: async (job) => {
        const data = job.data as { id: number };
        processed.push(data.id);
        if (processed.length === 2) {
          resolveComplete();
        }
      },
    });

    await queue.add({
      groupId: 'test-group',
      data: { id: 2 },
      orderMs: 200,
    });

    await queue.add({
      groupId: 'test-group',
      data: { id: 1 },
      orderMs: 100,
    });

    await completePromise;
    await worker.close();

    expect(processed).toEqual([1, 2]);
  });
});
