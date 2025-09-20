import Redis from 'ioredis';
import { afterAll, describe, expect, it } from 'vitest';
import { Queue, type ReservedJob, Worker } from '../src';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';

describe('Edge Cases and Error Handling Tests', () => {
  const namespace = `test:edge:${Date.now()}`;

  afterAll(async () => {
    const redis = new Redis(REDIS_URL);
    const keys = await redis.keys(`${namespace}*`);
    if (keys.length) await redis.del(keys);
    await redis.quit();
  });

  it('should handle empty payloads and null values', async () => {
    const redis = new Redis(REDIS_URL);
    const q = new Queue({ redis, namespace: `${namespace}:empty` });

    // Test various empty/null payloads
    const testCases = [
      { id: 1, data: null },
      { id: 2, data: undefined },
      { id: 3, data: {} },
      { id: 4, data: [] },
      { id: 5, data: '' },
      { id: 6, data: 0 },
      { id: 7, data: false },
    ];

    // Enqueue all test cases with different groups for parallel processing
    for (const testCase of testCases) {
      await q.add({
        groupId: `empty-group-${testCase.id}`, // Different groups = parallel processing
        data: testCase.data,
        orderMs: testCase.id,
      });
    }

    const processed: any[] = [];

    const worker = new Worker({
      queue: q,
      blockingTimeoutSec: 1,
      handler: async (job) => {
        processed.push(job.data as any);
      },
    });

    worker.run();

    await new Promise((resolve) => setTimeout(resolve, 2000)); // More time for processing

    expect(processed.length).toBe(testCases.length);

    // Verify payloads are preserved correctly (undefined becomes null)
    expect(processed).toContain(null);
    expect(processed).toEqual([null, null, {}, [], '', 0, false]); // undefined -> null

    await worker.close();
    await redis.quit();
  });

  it('should handle extremely large payloads', async () => {
    const redis = new Redis(REDIS_URL);
    const q = new Queue({ redis, namespace: `${namespace}:large` });

    // Create large payload (1MB)
    const largePayload = {
      id: 'large-payload',
      data: 'x'.repeat(1024 * 1024),
      metadata: {
        timestamp: Date.now(),
        nested: {
          array: new Array(1000).fill('item'),
          object: Object.fromEntries(
            Array.from({ length: 100 }, (_, i) => [`key${i}`, `value${i}`]),
          ),
        },
      },
    };

    await q.add({
      groupId: 'large-group',
      data: largePayload,
    });

    let processedData: any = null;

    const worker = new Worker({
      queue: q,
      blockingTimeoutSec: 1,
      handler: async (job) => {
        processedData = job.data;
      },
    });

    worker.run();

    await new Promise((resolve) => setTimeout(resolve, 2000));

    expect(processedData).not.toBeNull();
    expect(processedData.id).toBe('large-payload');
    expect(processedData.data.length).toBe(1024 * 1024);
    expect(processedData.metadata.nested.array.length).toBe(1000);

    await worker.close();
    await redis.quit();
  });

  it('should handle special characters and unicode in payloads', async () => {
    const redis = new Redis(REDIS_URL);
    const q = new Queue({ redis, namespace: `${namespace}:unicode` });

    const specialPayloads = [
      { id: 1, text: 'Hello 🌍 World! 你好世界 🚀' },
      { id: 2, text: 'Special chars: !@#$%^&*()_+-=[]{}|;:,.<>?' },
      { id: 3, text: 'Emojis: 😀😃😄😁😆😅😂🤣☺️😊' },
      { id: 4, text: 'Multi-line\nstring\nwith\ttabs' },
      { id: 5, text: 'Quotes: "double" \'single\' `backtick`' },
      { id: 6, text: 'JSON-like: {"key": "value", "number": 123}' },
      { id: 7, text: 'Arabic: مرحبا بالعالم' },
      { id: 8, text: 'Russian: Привет мир' },
      { id: 9, text: 'Japanese: こんにちは世界' },
    ];

    for (const payload of specialPayloads) {
      await q.add({
        groupId: `unicode-group-${payload.id}`, // Different groups for parallel processing
        data: payload,
        orderMs: payload.id,
      });
    }

    const processed: any[] = [];

    const worker = new Worker({
      queue: q,
      blockingTimeoutSec: 1,
      handler: async (job) => {
        processed.push(job.data as any);
      },
    });

    worker.run();

    // Wait until all jobs are processed or timeout
    const startTime = Date.now();
    while (
      processed.length < specialPayloads.length &&
      Date.now() - startTime < 5000
    ) {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    // Logging removed for clean test output

    expect(processed.length).toBe(specialPayloads.length);

    // Verify all special characters are preserved
    processed.forEach((payload, index) => {
      expect(payload.text).toBe(specialPayloads[index].text);
    });

    await worker.close();
    await redis.quit();
  });

  it('should handle malformed or corrupted data gracefully', async () => {
    const redis = new Redis(REDIS_URL);
    const q = new Queue({ redis, namespace: `${namespace}:corrupted` });

    // Manually insert corrupted data into Redis
    // Need to use the same namespace prefix as the queue (which auto-prefixes with 'groupmq:')
    const queueNamespace = `groupmq:${namespace}:corrupted`;
    const jobKey = `${queueNamespace}:job:corrupted-job`;
    const groupKey = `${queueNamespace}:g:corrupted-group`;
    const readyKey = `${queueNamespace}:ready`;

    // Insert malformed job data
    await redis.hmset(jobKey, {
      id: 'corrupted-job',
      groupId: 'corrupted-group',
      data: 'invalid-json{malformed',
      attempts: 'not-a-number',
      maxAttempts: '3',
      seq: '1',
      timestamp: 'invalid-timestamp',
      orderMs: '1',
      score: 'not-a-score',
    });

    await redis.zadd(groupKey, 1, 'corrupted-job');
    await redis.zadd(readyKey, 1, 'corrupted-group');

    const errors: string[] = [];
    const processed: any[] = [];

    const worker = new Worker({
      queue: q,
      blockingTimeoutSec: 1,
      handler: async (job) => {
        processed.push(job.data as any);
      },
      onError: (err) => {
        errors.push((err as Error).message);
      },
    });

    worker.run();

    await new Promise((resolve) => setTimeout(resolve, 2000));

    // With graceful JSON parsing, corrupted job should be processed with null payload
    expect(processed.length).toBe(1);
    expect(processed[0]).toBeNull(); // Corrupted JSON becomes null payload

    await worker.close();
    await redis.quit();
  });

  it('should handle extremely long group IDs and job IDs', async () => {
    const redis = new Redis(REDIS_URL);
    const q = new Queue({ redis, namespace: `${namespace}:long` });

    // Create very long group ID (just under Redis key length limit)
    const longGroupId = `group-${'x'.repeat(500)}`;
    const longData = {
      veryLongProperty: 'y'.repeat(1000),
      id: 'long-test',
    };

    await q.add({
      groupId: longGroupId,
      data: longData,
    });

    let processedJob: ReservedJob | null = null;

    const worker = new Worker({
      queue: q,
      blockingTimeoutSec: 1,
      handler: async (job) => {
        processedJob = job;
      },
    });

    worker.run();

    await new Promise((resolve) => setTimeout(resolve, 1000));

    expect(processedJob).not.toBeNull();
    expect(processedJob!.groupId).toBe(longGroupId);
    expect(processedJob!.data.veryLongProperty.length).toBe(1000);

    await worker.close();
    await redis.quit();
  });

  it('should handle rapid worker start/stop cycles', async () => {
    const redis = new Redis(REDIS_URL);
    const q = new Queue({ redis, namespace: `${namespace}:rapid` });

    // Enqueue some jobs
    for (let i = 0; i < 10; i++) {
      await q.add({
        groupId: 'rapid-group',
        data: { id: i },
        orderMs: i,
      });
    }

    const processed: number[] = [];

    // Rapidly start and stop workers
    for (let cycle = 0; cycle < 5; cycle++) {
      const worker = new Worker({
        queue: q,
        blockingTimeoutSec: 1,
        handler: async (job) => {
          processed.push((job.data as any).id);
          await new Promise((resolve) => setTimeout(resolve, 50));
        },
      });

      worker.run();

      // Very short runtime
      await new Promise((resolve) => setTimeout(resolve, 100));

      await worker.close();
    }

    // Final worker to clean up remaining jobs
    const finalWorker = new Worker({
      queue: q,
      blockingTimeoutSec: 1,
      handler: async (job) => {
        processed.push((job.data as any).id);
      },
    });

    finalWorker.run();
    await new Promise((resolve) => setTimeout(resolve, 2000));
    await finalWorker.close();

    // All jobs should eventually be processed
    expect(processed.length).toBe(10);
    expect(new Set(processed).size).toBe(10); // No duplicates

    await redis.quit();
  });

  it('should handle clock skew and time-based edge cases', async () => {
    const redis = new Redis(REDIS_URL);
    const q = new Queue({ redis, namespace: `${namespace}:time` });

    // Test jobs with timestamps far in the past and future
    const timeTestCases = [
      { id: 1, orderMs: 0 }, // Unix epoch
      { id: 2, orderMs: Date.now() - 86400000 }, // 24 hours ago
      { id: 3, orderMs: Date.now() }, // Now
      { id: 4, orderMs: Date.now() + 86400000 }, // 24 hours from now
      { id: 5, orderMs: Number.MAX_SAFE_INTEGER }, // Far future
    ];

    for (const testCase of timeTestCases) {
      await q.add({
        groupId: 'time-group',
        data: { id: testCase.id },
        orderMs: testCase.orderMs,
      });
    }

    const processed: number[] = [];

    const worker = new Worker({
      queue: q,
      blockingTimeoutSec: 1,
      handler: async (job) => {
        processed.push((job.data as any).id);
      },
    });

    worker.run();

    await new Promise((resolve) => setTimeout(resolve, 1000));

    // Should process all jobs in chronological order
    expect(processed.length).toBe(5);
    expect(processed).toEqual([1, 2, 3, 4, 5]);

    await worker.close();
    await redis.quit();
  });

  it('should handle circular references in payloads', async () => {
    const redis = new Redis(REDIS_URL);
    const q = new Queue({ redis, namespace: `${namespace}:circular` });

    // Create object with circular reference
    const circularObj: any = { id: 'circular-test' };
    circularObj.self = circularObj;

    let enqueueFailed = false;
    try {
      await q.add({
        groupId: 'circular-group',
        data: circularObj,
      });
    } catch (err) {
      enqueueFailed = true;
      expect((err as Error).message).toContain('circular'); // JSON.stringify should fail
    }

    expect(enqueueFailed).toBe(true);

    await redis.quit();
  });

  it('should handle zero and negative visibility timeouts', async () => {
    const redis = new Redis(REDIS_URL);

    // Test with zero visibility timeout
    const q1 = new Queue({
      redis,
      namespace: `${namespace}:zero-vt`,
      jobTimeoutMs: 1,
    });

    await q1.add({ groupId: 'zero-group', data: { test: 'zero' } });

    const job1 = await q1.reserve();
    expect(job1).not.toBeNull();

    // Test with negative visibility timeout (should use default)
    const q2 = new Queue({
      redis: redis.duplicate(),
      namespace: `${namespace}:neg-vt`,
      jobTimeoutMs: -1000,
    });

    await q2.add({ groupId: 'neg-group', data: { test: 'negative' } });

    const job2 = await q2.reserve();
    expect(job2).not.toBeNull();

    await redis.quit();
  });

  it('should handle queue operations on disconnected Redis', async () => {
    const redis = new Redis(REDIS_URL);
    const q = new Queue({ redis, namespace: `${namespace}:disconnected` });

    // Disconnect Redis
    await redis.disconnect();

    let enqueueError = null;
    let reserveError = null;

    try {
      await q.add({ groupId: 'disc-group', data: { test: 'disconnected' } });
    } catch (err) {
      enqueueError = err;
    }

    try {
      await q.reserve();
    } catch (err) {
      reserveError = err;
    }

    expect(enqueueError).not.toBeNull();
    expect(reserveError).not.toBeNull();

    // Reconnect should work
    await redis.connect();

    // Now operations should work
    await q.add({
      groupId: 'reconnected-group',
      data: { test: 'reconnected' },
    });
    const job = await q.reserve();
    expect(job).not.toBeNull();

    await redis.quit();
  });
});

async function _wait(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
