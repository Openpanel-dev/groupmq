import Redis from 'ioredis';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { Queue, Worker } from '../src';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';

describe('Job Ordering with orderMs and scheduler method', () => {
  const redis = new Redis(REDIS_URL);
  const namespace = `test:ordering:${Date.now()}`;

  beforeAll(async () => {
    // flush only this namespace keys (best-effort)
    const keys = await redis.keys(`${namespace}*`);
    if (keys.length) await redis.del(keys);
  });

  afterAll(async () => {
    await redis.quit();
  });

  it('should process jobs in orderMs order across multiple groups with random arrival times', async () => {
    const q = new Queue({
      redis,
      namespace,
      jobTimeoutMs: 5000,
      orderingMethod: 'scheduler',
      orderingWindowMs: 1000, // 1 second buffer window to collect out-of-order jobs
    });

    const numGroups = 5;
    const jobsPerGroup = 10;
    const totalJobs = numGroups * jobsPerGroup;

    // Track processing order for each group
    const processedByGroup: Record<
      string,
      Array<{ orderMs: number; jobId: string }>
    > = {};
    for (let g = 0; g < numGroups; g++) {
      processedByGroup[`group-${g}`] = [];
    }

    // Create 3 workers to process jobs concurrently
    const workers: Worker<{ orderMs: number; jobId: string }>[] = [];
    for (let w = 0; w < 3; w++) {
      const worker = new Worker({
        queue: q,
        name: `worker-${w}`,
        handler: async (job) => {
          processedByGroup[job.groupId].push({
            orderMs: job.data.orderMs,
            jobId: job.data.jobId,
          });
          // Simulate some processing time
          await wait(10);
        },
      });
      worker.run();
      workers.push(worker);
    }

    // Generate jobs with sequential orderMs but add them in random order
    const baseTime = Date.now();
    const jobsToAdd: Array<{
      groupId: string;
      orderMs: number;
      jobId: string;
      addDelay: number;
    }> = [];

    for (let g = 0; g < numGroups; g++) {
      for (let j = 0; j < jobsPerGroup; j++) {
        jobsToAdd.push({
          groupId: `group-${g}`,
          orderMs: baseTime + j * 100, // Each job 100ms apart in orderMs
          jobId: `g${g}-j${j}`,
          addDelay: Math.random() * 250, // Random delay 0-250ms when adding
        });
      }
    }

    // Shuffle jobs to simulate out-of-order arrival
    const shuffled = jobsToAdd.sort(() => Math.random() - 0.5);

    // Add jobs with random delays
    console.log(`Adding ${totalJobs} jobs with random arrival times...`);
    const addPromises = shuffled.map(async (job) => {
      await wait(job.addDelay);
      await q.add({
        groupId: job.groupId,
        data: { orderMs: job.orderMs, jobId: job.jobId },
        orderMs: job.orderMs,
      });
    });

    await Promise.all(addPromises);
    console.log('All jobs added, waiting for completion...');

    // Wait for all jobs to complete
    await q.waitForEmpty();
    await wait(200); // Extra buffer to ensure all processing is complete

    // Verify each group processed jobs in correct orderMs order
    for (let g = 0; g < numGroups; g++) {
      const groupId = `group-${g}`;
      const processed = processedByGroup[groupId];

      expect(processed.length).toBe(jobsPerGroup);

      // Check that jobs are in ascending orderMs order
      for (let i = 1; i < processed.length; i++) {
        expect(processed[i].orderMs).toBeGreaterThanOrEqual(
          processed[i - 1].orderMs,
        );
      }

      // Verify exact orderMs values
      const expectedOrderMs = Array.from(
        { length: jobsPerGroup },
        (_, i) => baseTime + i * 100,
      );
      const actualOrderMs = processed.map((p) => p.orderMs);
      expect(actualOrderMs).toEqual(expectedOrderMs);
    }

    console.log('✅ All groups processed jobs in correct order');

    // Close workers
    await Promise.all(workers.map((w) => w.close()));
  });

  it('should handle late arrivals within orderingDelayMs window', async () => {
    const q = new Queue({
      redis,
      namespace: `${namespace}:late-arrivals`,
      jobTimeoutMs: 5000,
      orderingMethod: 'scheduler',
      orderingWindowMs: 1000,
    });

    const processed: Array<{ groupId: string; orderMs: number; seq: number }> =
      [];

    const worker = new Worker<{ seq: number; orderMs: number }>({
      queue: q,
      handler: async (job) => {
        processed.push({
          groupId: job.groupId,
          orderMs: job.data.orderMs,
          seq: job.data.seq,
        });
      },
    });
    worker.run();

    const baseTime = Date.now();
    const groupId = 'device-1';

    // Add jobs out of order, simulating network delays
    // Job 5 arrives first
    await q.add({
      groupId,
      data: { seq: 5, orderMs: baseTime + 500 },
      orderMs: baseTime + 500,
    });

    await wait(50);

    // Job 3 arrives
    await q.add({
      groupId,
      data: { seq: 3, orderMs: baseTime + 300 },
      orderMs: baseTime + 300,
    });

    await wait(50);

    // Job 1 arrives (earliest)
    await q.add({
      groupId,
      data: { seq: 1, orderMs: baseTime + 100 },
      orderMs: baseTime + 100,
    });

    await wait(50);

    // Job 4 arrives
    await q.add({
      groupId,
      data: { seq: 4, orderMs: baseTime + 400 },
      orderMs: baseTime + 400,
    });

    await wait(50);

    // Job 2 arrives
    await q.add({
      groupId,
      data: { seq: 2, orderMs: baseTime + 200 },
      orderMs: baseTime + 200,
    });

    // Wait for all jobs to complete
    await q.waitForEmpty();
    await wait(100);

    // Should be processed in orderMs order: 1, 2, 3, 4, 5
    expect(processed.length).toBe(5);
    expect(processed.map((p) => p.seq)).toEqual([1, 2, 3, 4, 5]);

    console.log('✅ Late arrivals handled correctly');

    await worker.close();
  });

  it('should handle multiple groups with interleaved out-of-order arrivals', async () => {
    const q = new Queue({
      redis,
      namespace: `${namespace}:interleaved`,
      jobTimeoutMs: 5000,
      orderingMethod: 'scheduler',
      orderingWindowMs: 1000,
    });

    const processedByGroup: Record<string, number[]> = {
      'group-A': [],
      'group-B': [],
      'group-C': [],
    };

    // Create 2 workers
    const workers: Worker<{ seq: number }>[] = [];
    for (let i = 0; i < 2; i++) {
      const worker = new Worker<{ seq: number }>({
        queue: q,
        name: `worker-${i}`,
        handler: async (job) => {
          processedByGroup[job.groupId].push(job.data.seq);
          await wait(20);
        },
      });
      worker.run();
      workers.push(worker);
    }

    const baseTime = Date.now();

    // Create jobs for 3 groups, each with 8 jobs
    const jobs = [
      // Group A jobs
      { group: 'group-A', seq: 1, orderMs: baseTime + 100, delay: 150 },
      { group: 'group-A', seq: 2, orderMs: baseTime + 200, delay: 80 },
      { group: 'group-A', seq: 3, orderMs: baseTime + 300, delay: 200 },
      { group: 'group-A', seq: 4, orderMs: baseTime + 400, delay: 10 },
      { group: 'group-A', seq: 5, orderMs: baseTime + 500, delay: 120 },
      { group: 'group-A', seq: 6, orderMs: baseTime + 600, delay: 30 },
      { group: 'group-A', seq: 7, orderMs: baseTime + 700, delay: 180 },
      { group: 'group-A', seq: 8, orderMs: baseTime + 800, delay: 5 },

      // Group B jobs
      { group: 'group-B', seq: 1, orderMs: baseTime + 150, delay: 100 },
      { group: 'group-B', seq: 2, orderMs: baseTime + 250, delay: 190 },
      { group: 'group-B', seq: 3, orderMs: baseTime + 350, delay: 40 },
      { group: 'group-B', seq: 4, orderMs: baseTime + 450, delay: 160 },
      { group: 'group-B', seq: 5, orderMs: baseTime + 550, delay: 20 },
      { group: 'group-B', seq: 6, orderMs: baseTime + 650, delay: 130 },
      { group: 'group-B', seq: 7, orderMs: baseTime + 750, delay: 70 },
      { group: 'group-B', seq: 8, orderMs: baseTime + 850, delay: 200 },

      // Group C jobs
      { group: 'group-C', seq: 1, orderMs: baseTime + 120, delay: 110 },
      { group: 'group-C', seq: 2, orderMs: baseTime + 220, delay: 170 },
      { group: 'group-C', seq: 3, orderMs: baseTime + 320, delay: 15 },
      { group: 'group-C', seq: 4, orderMs: baseTime + 420, delay: 140 },
      { group: 'group-C', seq: 5, orderMs: baseTime + 520, delay: 90 },
      { group: 'group-C', seq: 6, orderMs: baseTime + 620, delay: 50 },
      { group: 'group-C', seq: 7, orderMs: baseTime + 720, delay: 25 },
      { group: 'group-C', seq: 8, orderMs: baseTime + 820, delay: 155 },
    ];

    // Add all jobs with their specified delays
    const addPromises = jobs.map(async (job) => {
      await wait(job.delay);
      await q.add({
        groupId: job.group,
        data: { seq: job.seq },
        orderMs: job.orderMs,
      });
    });

    await Promise.all(addPromises);
    console.log('All interleaved jobs added');

    // Wait for all jobs to complete
    await q.waitForEmpty();
    await wait(150);

    // Verify each group processed in correct order
    expect(processedByGroup['group-A']).toEqual([1, 2, 3, 4, 5, 6, 7, 8]);
    expect(processedByGroup['group-B']).toEqual([1, 2, 3, 4, 5, 6, 7, 8]);
    expect(processedByGroup['group-C']).toEqual([1, 2, 3, 4, 5, 6, 7, 8]);

    console.log(
      '✅ All groups maintained correct order despite interleaved arrivals',
    );

    await Promise.all(workers.map((w) => w.close()));
  });

  it('should handle edge case: job arriving after buffering window started', async () => {
    const q = new Queue({
      redis,
      namespace: `${namespace}:edge-case`,
      jobTimeoutMs: 5000,
      orderingMethod: 'scheduler',
      orderingWindowMs: 1000,
    });

    const processed: number[] = [];

    const worker = new Worker<{ seq: number }>({
      queue: q,
      handler: async (job) => {
        processed.push(job.data.seq);
      },
    });
    worker.run();

    const baseTime = Date.now();
    const groupId = 'edge-group';

    // Add first job (starts buffering window)
    await q.add({
      groupId,
      data: { seq: 3 },
      orderMs: baseTime + 300,
    });

    // Wait 200ms (still within 250ms buffer)
    await wait(200);

    // Add earlier job - should still be buffered together
    await q.add({
      groupId,
      data: { seq: 1 },
      orderMs: baseTime + 100,
    });

    // Add another
    await wait(30);
    await q.add({
      groupId,
      data: { seq: 2 },
      orderMs: baseTime + 200,
    });

    // Wait for processing
    await q.waitForEmpty();
    await wait(100);

    // Should process in order: 1, 2, 3
    expect(processed).toEqual([1, 2, 3]);

    console.log('✅ Jobs arriving within buffer window are correctly ordered');

    await worker.close();
  });

  it('should work correctly when orderingDelayMs is 0 (no buffering)', async () => {
    const q = new Queue({
      redis,
      namespace: `${namespace}:no-buffering`,
      jobTimeoutMs: 5000,
      orderingMethod: 'none', // No buffering
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
    const groupId = 'no-buffer-group';

    // Add jobs in specific order
    await q.add({ groupId, data: { seq: 1 }, orderMs: baseTime + 100 });
    await wait(20);
    await q.add({ groupId, data: { seq: 2 }, orderMs: baseTime + 200 });
    await wait(20);
    await q.add({ groupId, data: { seq: 3 }, orderMs: baseTime + 300 });

    await q.waitForEmpty();
    await wait(50);

    // Without buffering, jobs are still ordered by their position in the sorted set
    // They should still be processed in orderMs order since they're in the group's sorted set
    expect(processed).toEqual([1, 2, 3]);

    console.log('✅ Works correctly without buffering (orderingDelayMs=0)');

    await worker.close();
  });
});

async function wait(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}
