import Redis from 'ioredis';
import { afterAll, beforeAll, describe, expect, test } from 'vitest';
import { Queue, Worker } from '../src/index';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';

describe('orderingDelayMs', () => {
  let redis: Redis;

  beforeAll(async () => {
    redis = new Redis(REDIS_URL);
  });

  test('should process jobs in chronological order with orderingDelayMs', async () => {
    const namespace = `test:basic:${Date.now()}`;

    const queue = new Queue({
      redis,
      namespace,
      orderingDelayMs: 100,
    });

    const processedJobs: any[] = [];

    const worker = new Worker({
      queue,
      name: 'test-worker',
      handler: async (job) => {
        processedJobs.push({
          id: job.id,
          order: job.data.order,
          orderMs: job.orderMs,
          groupId: job.groupId,
        });
      },
    });

    worker.run();

    // Wait for worker to be ready
    await new Promise((resolve) => setTimeout(resolve, 200));

    const now = Date.now();

    // Add jobs in reverse chronological order
    await queue.add({
      groupId: 'group1',
      data: { order: 2 },
      orderMs: now + 100, // Later timestamp
    });

    await queue.add({
      groupId: 'group1',
      data: { order: 1 },
      orderMs: now, // Earlier timestamp
    });

    // Wait for all jobs to be processed
    const isEmpty = await queue.waitForEmpty(3000);
    expect(isEmpty).toBe(true);

    expect(processedJobs).toHaveLength(2);
    // Should process in chronological order despite addition order
    expect(processedJobs[0].order).toBe(1); // Earlier timestamp first
    expect(processedJobs[1].order).toBe(2); // Later timestamp second
  });

  test('should handle sleep between adds and wrong order', async () => {
    const namespace = `test:sleep:${Date.now()}`;

    const queue = new Queue({
      redis,
      namespace,
      orderingDelayMs: 100,
    });

    const processedJobs: any[] = [];

    const worker = new Worker({
      queue,
      name: 'test-worker-sleep',
      handler: async (job) => {
        processedJobs.push({
          id: job.id,
          order: job.data.order,
          orderMs: job.orderMs,
          groupId: job.groupId,
        });
      },
    });

    worker.run();
    await new Promise((resolve) => setTimeout(resolve, 200));

    const now = Date.now();

    // Add jobs with delays and in wrong order
    await queue.add({
      groupId: 'group1',
      data: { order: 2 },
      orderMs: now + 200, // Latest timestamp
    });

    await new Promise((resolve) => setTimeout(resolve, 100)); // Sleep 100ms

    await queue.add({
      groupId: 'group1',
      data: { order: 1 },
      orderMs: now + 100, // Middle timestamp
    });

    await new Promise((resolve) => setTimeout(resolve, 200)); // Sleep 200ms

    await queue.add({
      groupId: 'group1',
      data: { order: 3 },
      orderMs: now + 300, // Latest timestamp
    });

    await new Promise((resolve) => setTimeout(resolve, 150)); // Sleep 150ms

    await queue.add({
      groupId: 'group1',
      data: { order: 4 },
      orderMs: now + 400, // Latest timestamp
    });

    // Wait for all jobs to be processed
    const isEmpty = await queue.waitForEmpty(3000);
    expect(isEmpty).toBe(true);

    expect(processedJobs).toHaveLength(4);
    // Should process in chronological order despite delays and wrong addition order
    expect(processedJobs[0].order).toBe(1); // First timestamp
    expect(processedJobs[1].order).toBe(2); // Second timestamp
    expect(processedJobs[2].order).toBe(3); // Third timestamp
    expect(processedJobs[3].order).toBe(4); // Fourth timestamp
  });

  test('should handle multiple groups with random order and sleep between adds', async () => {
    const namespace = `test:multi:${Date.now()}`;

    const queue = new Queue({
      redis,
      namespace,
      orderingDelayMs: 100,
    });

    const processedJobs: any[] = [];

    const worker = new Worker({
      queue,
      name: 'test-worker-multi',
      concurrency: 5,
      handler: async (job) => {
        processedJobs.push({
          id: job.id,
          groupId: job.groupId,
          order: job.data.order,
          orderMs: job.orderMs,
        });
      },
    });

    worker.run();
    await new Promise((resolve) => setTimeout(resolve, 200));

    const now = Date.now();

    // Add jobs to multiple groups in random order with sleep between adds
    await queue.add({
      groupId: 'groupA',
      data: { order: 2 },
      orderMs: now + 200,
    });
    await new Promise((resolve) => setTimeout(resolve, 150)); // Sleep

    await queue.add({
      groupId: 'groupB',
      data: { order: 1 },
      orderMs: now + 150,
    });
    await new Promise((resolve) => setTimeout(resolve, 100)); // Sleep

    await queue.add({
      groupId: 'groupA',
      data: { order: 1 },
      orderMs: now,
    });
    await new Promise((resolve) => setTimeout(resolve, 200)); // Sleep

    await queue.add({
      groupId: 'groupC',
      data: { order: 2 },
      orderMs: now + 300,
    });
    await new Promise((resolve) => setTimeout(resolve, 120)); // Sleep

    await queue.add({
      groupId: 'groupB',
      data: { order: 2 },
      orderMs: now + 250,
    });
    await new Promise((resolve) => setTimeout(resolve, 80)); // Sleep

    await queue.add({
      groupId: 'groupC',
      data: { order: 1 },
      orderMs: now + 100,
    });

    // Wait for all jobs to be processed
    const isEmpty = await queue.waitForEmpty(3000);
    expect(isEmpty).toBe(true);

    expect(processedJobs).toHaveLength(6);

    // Verify ordering within each group
    const groupAJobs = processedJobs.filter((j) => j.groupId === 'groupA');
    const groupBJobs = processedJobs.filter((j) => j.groupId === 'groupB');
    const groupCJobs = processedJobs.filter((j) => j.groupId === 'groupC');

    expect(groupAJobs).toHaveLength(2);
    expect(groupBJobs).toHaveLength(2);
    expect(groupCJobs).toHaveLength(2);

    // Each group should have jobs in chronological order
    expect(groupAJobs[0].order).toBe(1); // Earlier timestamp
    expect(groupAJobs[1].order).toBe(2); // Later timestamp

    expect(groupBJobs[0].order).toBe(1); // Earlier timestamp
    expect(groupBJobs[1].order).toBe(2); // Later timestamp

    expect(groupCJobs[0].order).toBe(1); // Earlier timestamp
    expect(groupCJobs[1].order).toBe(2); // Later timestamp
  });

  test('should handle complex scenario with past, present, and future timestamps', async () => {
    const namespace = `test:complex:${Date.now()}`;

    const queue = new Queue({
      redis,
      namespace,
      orderingDelayMs: 100,
    });

    const processedJobs: any[] = [];

    const worker = new Worker({
      queue,
      name: 'test-worker-complex',
      handler: async (job) => {
        processedJobs.push({
          id: job.id,
          groupId: job.groupId,
          order: job.data.order,
          orderMs: job.orderMs,
        });
      },
    });

    worker.run();
    await new Promise((resolve) => setTimeout(resolve, 200));

    const now = Date.now();

    // Mix of past, present, and future timestamps with delays
    await queue.add({
      groupId: 'group1',
      data: { order: 3 },
      orderMs: now + 300, // Future
    });
    await new Promise((resolve) => setTimeout(resolve, 100));

    await queue.add({
      groupId: 'group1',
      data: { order: 1 },
      orderMs: now - 200, // Past
    });
    await new Promise((resolve) => setTimeout(resolve, 150));

    await queue.add({
      groupId: 'group2',
      data: { order: 2 },
      orderMs: now + 150, // Future
    });
    await new Promise((resolve) => setTimeout(resolve, 80));

    await queue.add({
      groupId: 'group1',
      data: { order: 2 },
      orderMs: now, // Present
    });
    await new Promise((resolve) => setTimeout(resolve, 120));

    await queue.add({
      groupId: 'group2',
      data: { order: 1 },
      orderMs: now - 100, // Past
    });

    // Wait for all jobs to be processed
    const isEmpty = await queue.waitForEmpty(3000);
    expect(isEmpty).toBe(true);

    expect(processedJobs).toHaveLength(5);

    // Verify ordering within each group
    const group1Jobs = processedJobs.filter((j) => j.groupId === 'group1');
    const group2Jobs = processedJobs.filter((j) => j.groupId === 'group2');

    expect(group1Jobs).toHaveLength(3);
    expect(group2Jobs).toHaveLength(2);

    // Group1: past, present, future - should be in chronological order
    expect(group1Jobs[0].order).toBe(1); // Past (now-200)
    expect(group1Jobs[1].order).toBe(2); // Present (now)
    expect(group1Jobs[2].order).toBe(3); // Future (now+300)

    // Group2: past, future - should be in chronological order
    expect(group2Jobs[0].order).toBe(1); // Past (now-100)
    expect(group2Jobs[1].order).toBe(2); // Future (now+150)
  });
});
