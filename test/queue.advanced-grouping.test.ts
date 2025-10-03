import Redis from 'ioredis';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { Queue } from '../src/queue';
import { Worker } from '../src/worker';

describe('Advanced Grouping Tests', () => {
  let redis: Redis;
  let queue: Queue;
  let namespace: string;

  beforeEach(async () => {
    namespace = `test-grouping-${Date.now()}`;
    redis = new Redis({
      host: 'localhost',
      port: 6379,
      maxRetriesPerRequest: null,
    });
    queue = new Queue({ redis, namespace });
    await redis.flushdb(); // Clean Redis before each test
  });

  afterEach(async () => {
    await redis.quit();
  });

  it.only('should process groups in FIFO order with proper worker distribution', async () => {
    const processingLog: Array<{
      workerId: string;
      groupId: string;
      jobId: string;
      startTime: number;
      endTime?: number;
    }> = [];

    // Add jobs to two groups
    // g1: first job is long-running (5s), second job is fast
    await queue.add({
      groupId: 'g1',
      data: { id: 'g1-job1', type: 'long', duration: 500 },
      orderMs: Date.now() + 1000, // Process second
    });

    await queue.add({
      groupId: 'g1',
      data: { id: 'g1-job2', type: 'fast', duration: 100 },
      orderMs: Date.now() + 2000, // Process after g1-job1
    });

    // g2: both jobs are fast
    await queue.add({
      groupId: 'g2',
      data: { id: 'g2-job1', type: 'fast', duration: 100 },
      orderMs: Date.now(), // Process first
    });

    await queue.add({
      groupId: 'g2',
      data: { id: 'g2-job2', type: 'fast', duration: 100 },
      orderMs: Date.now() + 1500, // Process after g2-job1
    });

    const createWorker = (workerId: string) => {
      return new Worker({
        queue: queue,
        blockingTimeoutSec: 1,
        handler: async (job) => {
          const startTime = performance.now();
          processingLog.push({
            workerId,
            groupId: job.groupId,
            jobId: (job.data as any).id,
            startTime,
          });
          // Simulate processing time
          await new Promise((resolve) =>
            setTimeout(resolve, (job.data as any).duration),
          );

          const endTime = performance.now();
          const logEntry = processingLog.find(
            (entry) =>
              entry.workerId === workerId &&
              entry.jobId === (job.data as any).id &&
              !entry.endTime,
          );
          if (logEntry) {
            logEntry.endTime = endTime;
          }
        },
      });
    };

    // Create two workers
    const worker1 = createWorker('worker1');
    const worker2 = createWorker('worker2');

    // Start both workers
    const worker1Promise = worker1.run();
    const worker2Promise = worker2.run();

    await queue.waitForEmpty();

    // Stop workers
    await worker1.close();
    await worker2.close();
    await worker1Promise;
    await worker2Promise;

    // Verify all jobs were processed
    expect(processingLog).toHaveLength(4);

    // Sort processing log by start time to see the order
    const sortedLog = processingLog.sort((a, b) => a.startTime - b.startTime);

    // Test expectations:
    // 1. g2-job1 should be processed first (earliest orderMs)
    expect(sortedLog[0].jobId).toBe('g2-job1');
    expect(sortedLog[0].groupId).toBe('g2');

    // 2. g1-job1 should be processed second (long-running)
    expect(sortedLog[1].jobId).toBe('g1-job1');
    expect(sortedLog[1].groupId).toBe('g1');

    // 3. While g1-job1 is running, g2-job2 should be processed by the other worker
    // (This should start before g1-job1 finishes)
    const g1Job1Entry = sortedLog.find((entry) => entry.jobId === 'g1-job1')!;
    const g2Job2Entry = sortedLog.find((entry) => entry.jobId === 'g2-job2')!;

    expect(g2Job2Entry).toBeDefined();
    expect(g2Job2Entry.startTime).toBeGreaterThan(g1Job1Entry.startTime);

    // g2-job2 should start while g1-job1 is still running (before it ends)
    expect(g2Job2Entry.startTime).toBeLessThan(g1Job1Entry.endTime!);

    // 4. g1-job2 should be processed last (after g1-job1 completes)
    const g1Job2Entry = sortedLog.find((entry) => entry.jobId === 'g1-job2')!;
    expect(g1Job2Entry).toBeDefined();
    expect(g1Job2Entry.startTime).toBeGreaterThan(g1Job1Entry.endTime!);

    // 5. Verify that jobs within each group are processed in FIFO order
    const g1Jobs = sortedLog.filter((entry) => entry.groupId === 'g1');
    const g2Jobs = sortedLog.filter((entry) => entry.groupId === 'g2');

    expect(g1Jobs[0].jobId).toBe('g1-job1');
    expect(g1Jobs[1].jobId).toBe('g1-job2');
    expect(g2Jobs[0].jobId).toBe('g2-job1');
    expect(g2Jobs[1].jobId).toBe('g2-job2');

    // 6. Verify that both workers were utilized
    const workersUsed = new Set(processingLog.map((entry) => entry.workerId));
    expect(workersUsed.size).toBe(2);
    expect(workersUsed.has('worker1')).toBe(true);
    expect(workersUsed.has('worker2')).toBe(true);

    // 7. Verify timing: g1-job1 should take significantly longer than others
    const g1Job1Duration = g1Job1Entry.endTime! - g1Job1Entry.startTime;
    const g2Job1Duration =
      sortedLog.find((entry) => entry.jobId === 'g2-job1')!.endTime! -
      sortedLog.find((entry) => entry.jobId === 'g2-job1')!.startTime;

    expect(g1Job1Duration).toBeGreaterThan(400); // Should be ~5000ms
    expect(g2Job1Duration).toBeLessThan(200); // Should be ~100ms
  }, 20000); // 20 second timeout for the test

  it('should ensure second worker remains idle when no cross-group work is available', async () => {
    const workerActivity: Array<{
      workerId: string;
      action: 'started' | 'processing' | 'completed' | 'idle';
      jobId?: string;
      timestamp: number;
    }> = [];

    // Add only one group with two jobs - both to the same group
    await queue.add({
      groupId: 'single-group',
      data: { id: 'job1', duration: 3000 }, // 3 second job
      orderMs: Date.now(),
    });

    await queue.add({
      groupId: 'single-group',
      data: { id: 'job2', duration: 1000 }, // 1 second job
      orderMs: Date.now() + 1000,
    });

    const createWorker = (workerId: string) => {
      return new Worker({
        queue: queue,
        blockingTimeoutSec: 2, // Longer timeout to see idle periods
        handler: async (job) => {
          workerActivity.push({
            workerId,
            action: 'processing',
            jobId: (job.data as any).id,
            timestamp: Date.now(),
          });

          await new Promise((resolve) =>
            setTimeout(resolve, (job.data as any).duration),
          );

          workerActivity.push({
            workerId,
            action: 'completed',
            jobId: job.data.id,
            timestamp: Date.now(),
          });
        },
      });
    };

    const worker1 = createWorker('worker1');
    const worker2 = createWorker('worker2');

    workerActivity.push({
      workerId: 'worker1',
      action: 'started',
      timestamp: Date.now(),
    });
    workerActivity.push({
      workerId: 'worker2',
      action: 'started',
      timestamp: Date.now(),
    });

    const worker1Promise = worker1.run();
    const worker2Promise = worker2.run();

    // Wait for jobs to complete
    let allJobsProcessed = false;
    const maxWaitTime = 10000;
    const startTime = Date.now();

    while (!allJobsProcessed && Date.now() - startTime < maxWaitTime) {
      const queueStats = await queue.getJobCounts();
      if (queueStats.waiting === 0 && queueStats.active === 0) {
        allJobsProcessed = true;
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 500));
    }

    await worker1.close();
    await worker2.close();
    await worker1Promise;
    await worker2Promise;

    // Verify expectations
    expect(allJobsProcessed).toBe(true);

    const worker1Jobs = workerActivity.filter(
      (a) => a.workerId === 'worker1' && a.action === 'processing',
    );
    const worker2Jobs = workerActivity.filter(
      (a) => a.workerId === 'worker2' && a.action === 'processing',
    );

    // One worker should process both jobs (due to FIFO within group constraint)
    // The other worker should remain idle
    expect(worker1Jobs.length + worker2Jobs.length).toBe(2);

    if (worker1Jobs.length === 2) {
      expect(worker2Jobs.length).toBe(0);
    } else if (worker2Jobs.length === 2) {
      expect(worker1Jobs.length).toBe(0);
    } else {
      throw new Error(
        `Jobs incorrectly split: Worker1=${worker1Jobs.length}, Worker2=${worker2Jobs.length}`,
      );
    }
  }, 15000);
});
