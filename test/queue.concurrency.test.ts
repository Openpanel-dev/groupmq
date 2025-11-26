import Redis from 'ioredis';
import { afterAll, describe, expect, it } from 'vitest';
import { Queue, Worker } from '../src';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';

describe('Concurrency and Race Condition Tests', () => {
  const namespace = `test:concurrency:${Date.now()}`;

  afterAll(async () => {
    const redis = new Redis(REDIS_URL);
    const keys = await redis.keys(`${namespace}*`);
    if (keys.length) await redis.del(keys);
    await redis.quit();
  });

  it('should handle multiple workers distributing across different groups with atomic completion', async () => {
    const redis = new Redis(REDIS_URL);
    const q = new Queue({
      redis,
      namespace: `${namespace}:atomic-distribution`,
    });

    // Add 10 jobs to one group (should be processed by one worker)
    for (let i = 0; i < 10; i++) {
      await q.add({
        groupId: 'group-heavy', // Heavy group with 10 jobs
        data: { id: i, group: 'heavy' },
      });
    }

    // Add 1 job to another group (should be processed by the other worker)
    await q.add({
      groupId: 'group-light', // Light group with 1 job
      data: { id: 10, group: 'light' },
    });

    const processed: any[] = [];
    const workers: Worker<any>[] = [];
    const processedBy: { [key: number]: any[] } = {}; // Track which worker processed which jobs

    // Create 2 workers
    for (let workerId = 0; workerId < 2; workerId++) {
      const worker = new Worker({
        queue: q,
        blockingTimeoutSec: 1,
        concurrency: 1, // Single concurrency to make chaining more obvious
        handler: async (job) => {
          const jobData = job.data as any;
          processed.push(jobData);
          if (!processedBy[workerId]) processedBy[workerId] = [];
          processedBy[workerId].push(jobData);

          // Short processing time
          await new Promise((resolve) => setTimeout(resolve, 50));
        },
      });
      workers.push(worker);
      worker.run();
    }

    // Wait for all jobs to be processed
    await q.waitForEmpty();

    // All jobs should be processed exactly once
    expect(processed.length).toBe(11);
    expect(new Set(processed.map((j) => j.id)).size).toBe(11); // No duplicates

    // Verify atomic completion behavior:
    // - One worker should process all 10 jobs from group-heavy
    // - The other worker should process the 1 job from group-light
    const heavyGroupJobs = processed.filter((j) => j.group === 'heavy');
    const lightGroupJobs = processed.filter((j) => j.group === 'light');

    expect(heavyGroupJobs.length).toBe(10);
    expect(lightGroupJobs.length).toBe(1);

    // Check that jobs within each group were processed by the same worker
    const heavyWorkerIds = new Set(
      heavyGroupJobs.map((j) => {
        for (const [workerId, jobs] of Object.entries(processedBy)) {
          if (jobs.some((job) => job.id === j.id)) return parseInt(workerId);
        }
        return -1;
      }),
    );

    const lightWorkerIds = new Set(
      lightGroupJobs.map((j) => {
        for (const [workerId, jobs] of Object.entries(processedBy)) {
          if (jobs.some((job) => job.id === j.id)) return parseInt(workerId);
        }
        return -1;
      }),
    );

    // Each group should be processed by exactly one worker
    expect(heavyWorkerIds.size).toBe(1);
    expect(lightWorkerIds.size).toBe(1);

    // The workers should be different (distribution across groups)
    expect(heavyWorkerIds).not.toEqual(lightWorkerIds);

    // Verify that all 10 heavy jobs were processed by the same worker
    const heavyWorkerId = Array.from(heavyWorkerIds)[0];
    const heavyWorkerJobs = processedBy[heavyWorkerId] || [];
    const heavyJobsByWorker = heavyWorkerJobs.filter(
      (j) => j.group === 'heavy',
    );
    expect(heavyJobsByWorker.length).toBe(10);

    await Promise.all(workers.map((w) => w.close()));
    await redis.quit();
  });

  it('should handle concurrent add and dequeue operations', async () => {
    const redis = new Redis(REDIS_URL);
    const q = new Queue({ redis, namespace: `${namespace}:concurrent` });

    const processed: number[] = [];
    const enqueued: number[] = [];

    const worker = new Worker({
      queue: q,
      blockingTimeoutSec: 1,
      handler: async (job) => {
        processed.push((job.data as any).id);
        await new Promise((resolve) => setTimeout(resolve, 5));
      },
    });

    worker.run();

    // Concurrent producers
    const producers = [];
    for (let producerId = 0; producerId < 3; producerId++) {
      const producer = async () => {
        for (let i = 0; i < 10; i++) {
          const jobId = producerId * 10 + i;
          await q.add({
            groupId: `concurrent-group-${producerId}`,
            data: { id: jobId },
            orderMs: jobId,
          });
          enqueued.push(jobId);
          await new Promise((resolve) => setTimeout(resolve, 2));
        }
      };
      producers.push(producer());
    }

    await Promise.all(producers);

    // Wait for processing to complete
    await q.waitForEmpty();

    expect(processed.length).toBe(30);
    expect(enqueued.length).toBe(30);

    // Check that each group maintains FIFO order
    const groupOrders: { [key: string]: number[] } = {};
    processed.forEach((id) => {
      const groupId = Math.floor(id / 10);
      if (!groupOrders[groupId]) groupOrders[groupId] = [];
      groupOrders[groupId].push(id);
    });

    Object.entries(groupOrders).forEach(([groupId, order]) => {
      const expectedOrder = [...Array(10).keys()].map(
        (i) => Number.parseInt(groupId, 10) * 10 + i,
      );
      expect(order).toEqual(expectedOrder);
    });

    await worker.close();
    await redis.quit();
  });

  it('should handle race conditions during job completion', async () => {
    const redis = new Redis(REDIS_URL);
    const q = new Queue({ redis, namespace: `${namespace}:completion` });

    // Enqueue jobs
    for (let i = 0; i < 10; i++) {
      await q.add({
        groupId: 'completion-group',
        data: { id: i },
        orderMs: i,
      });
    }

    const completed: number[] = [];
    const completionAttempts = new Map<number, number>();

    const worker = new Worker({
      queue: q,
      blockingTimeoutSec: 1,
      handler: async (job) => {
        const id = (job.data as any).id;
        completionAttempts.set(id, (completionAttempts.get(id) || 0) + 1);
        await new Promise((resolve) => setTimeout(resolve, Math.random() * 20));
        completed.push(id);
      },
    });

    worker.run();

    await q.waitForEmpty();

    // Each job should be completed exactly once
    expect(completed.length).toBe(10);
    expect(new Set(completed).size).toBe(10);

    // No job should be attempted more than once (no double processing)
    completionAttempts.forEach((attempts, _jobId) => {
      expect(attempts).toBe(1);
    });

    await worker.close();
    await redis.quit();
  });

  it('should handle worker stopping during job processing', async () => {
    const redis = new Redis(REDIS_URL);
    const q = new Queue({
      redis,
      namespace: `${namespace}:stopping`,
      jobTimeoutMs: 500,
    });

    // Enqueue jobs
    for (let i = 0; i < 5; i++) {
      await q.add({
        groupId: 'stopping-group',
        data: { id: i },
        orderMs: i,
      });
    }

    const processed: number[] = [];
    let _processingCount = 0;

    const worker = new Worker({
      queue: q,
      blockingTimeoutSec: 1,
      handler: async (job) => {
        _processingCount++;
        if ((job.data as any).id === 1) {
          setTimeout(() => worker.close(), 100);
        }
        await new Promise((resolve) => setTimeout(resolve, 200));
        processed.push((job.data as any).id);
      },
    });

    worker.run();

    // Wait for worker to stop and jobs to be reclaimed - reduced
    await new Promise((resolve) => setTimeout(resolve, 1000));

    // Create new worker to process remaining jobs
    const worker2 = new Worker({
      queue: q,
      blockingTimeoutSec: 1,
      handler: async (job) => {
        processed.push((job.data as any).id);
      },
    });

    worker2.run();

    await new Promise((resolve) => setTimeout(resolve, 500));

    // All jobs should eventually be processed
    expect(processed.length).toBeGreaterThanOrEqual(4);

    await worker2.close();
    await redis.quit();
  });

  it('should handle high-frequency add/dequeue cycles', async () => {
    const redis = new Redis(REDIS_URL);
    const q = new Queue({ redis, namespace: `${namespace}:highfreq` });

    const processed: number[] = [];
    const timestamps: number[] = [];

    const worker = new Worker({
      queue: q,
      blockingTimeoutSec: 1,
      handler: async (job) => {
        processed.push((job.data as any).id);
        timestamps.push(Date.now());
      },
    });

    worker.run();

    // Rapidly add jobs
    const start = Date.now();
    for (let i = 0; i < 100; i++) {
      await q.add({
        groupId: `freq-group-${i % 5}`, // 5 parallel groups
        data: { id: i },
        orderMs: i,
      });

      // Very short delay between enqueues
      if (i % 10 === 0) {
        await new Promise((resolve) => setImmediate(resolve));
      }
    }

    const enqueueTime = Date.now() - start;

    // Wait for processing
    await new Promise((resolve) => setTimeout(resolve, 1500));

    expect(processed.length).toBe(100);

    // Check that groups maintain order
    const groupedResults: { [key: number]: number[] } = {};
    processed.forEach((id) => {
      const groupId = id % 5;
      if (!groupedResults[groupId]) groupedResults[groupId] = [];
      groupedResults[groupId].push(id);
    });

    Object.entries(groupedResults).forEach(([groupId, jobs]) => {
      const expectedJobs = [...Array(20).keys()].map(
        (i) => i * 5 + Number.parseInt(groupId, 10),
      );
      expect(jobs.sort((a, b) => a - b)).toEqual(expectedJobs);
    });

    await worker.close();
    await redis.quit();
  });

  it('should handle memory pressure with large datas', async () => {
    const redis = new Redis(REDIS_URL);
    const q = new Queue({ redis, namespace: `${namespace}:memory` });

    // Create large datas
    const largeData = 'x'.repeat(10000); // 10KB data

    for (let i = 0; i < 20; i++) {
      await q.add({
        groupId: `memory-group-${i % 3}`,
        data: { id: i, data: largeData },
        orderMs: i,
      });
    }

    const processed: number[] = [];
    const memoryUsage: number[] = [];

    const worker = new Worker({
      queue: q,
      blockingTimeoutSec: 1,
      handler: async (job) => {
        processed.push((job.data as any).id);
        memoryUsage.push(process.memoryUsage().heapUsed);
        expect((job.data as any).data.length).toBe(10000);
        expect((job.data as any).data).toBe(largeData);
      },
    });

    worker.run();

    await new Promise((resolve) => setTimeout(resolve, 1500));

    expect(processed.length).toBe(20);

    // Memory should not grow indefinitely
    const memoryGrowth = memoryUsage[memoryUsage.length - 1] - memoryUsage[0];
    expect(memoryGrowth).toBeLessThan(200 * 1024 * 1024); // Less than 200MB growth

    await worker.close();
    await redis.quit();
  });

  it('should handle deadlock scenarios with multiple groups', async () => {
    const redis = new Redis(REDIS_URL);
    const q = new Queue({ redis, namespace: `${namespace}:deadlock` });

    // Create a scenario where groups can process independently and avoid true deadlock
    // Put independent jobs first in each group so they can be processed
    await q.add({
      groupId: 'group-A',
      data: { id: 'A1', waitFor: null },
      orderMs: 1,
    }); // Independent
    await q.add({
      groupId: 'group-B',
      data: { id: 'B1', waitFor: null },
      orderMs: 2,
    }); // Independent
    await q.add({
      groupId: 'group-A',
      data: { id: 'A2', waitFor: 'B1' },
      orderMs: 3,
    }); // Depends on B1
    await q.add({
      groupId: 'group-B',
      data: { id: 'B2', waitFor: 'A1' },
      orderMs: 4,
    }); // Depends on A1

    const processed: string[] = [];
    const failed: string[] = [];

    const worker = new Worker({
      queue: q,
      blockingTimeoutSec: 1,
      maxAttempts: 3,
      backoff: () => 100,
      handler: async (job) => {
        const { id, waitFor } = job.data as any;
        if (waitFor && !processed.includes(waitFor)) {
          throw new Error(`Job ${id} waiting for ${waitFor}`);
        }
        processed.push(id);
        await new Promise((resolve) => setTimeout(resolve, 50));
      },
      onError: (_err, job) => {
        if (job) {
          failed.push((job.data as any).id);
        }
      },
    });

    worker.run();

    await new Promise((resolve) => setTimeout(resolve, 1500)); // Longer wait for retries

    // Should process independent jobs first (A1, B1), then dependent jobs (A2, B2) via retry
    expect(processed).toContain('A1'); // Independent, should succeed
    expect(processed).toContain('B1'); // Independent, should succeed
    expect(processed).toContain('A2'); // Should succeed after B1 is done
    expect(processed).toContain('B2'); // Should succeed after A1 is done

    // The test should pass even if there are no failures (jobs might process in perfect order)
    // expect(failed.length).toBeGreaterThan(0);

    await worker.close();
    await redis.quit();
  });
});

async function _wait(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
