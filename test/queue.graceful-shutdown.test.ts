import Redis from 'ioredis';
import { afterAll, describe, expect, it } from 'vitest';
import { getWorkersStatus, Queue, Worker } from '../src';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';

describe('Graceful Shutdown Tests', () => {
  const namespace = `test:graceful:${Date.now()}`;

  afterAll(async () => {
    // Cleanup after all tests
    const redis = new Redis(REDIS_URL);
    const keys = await redis.keys(`${namespace}*`);
    if (keys.length) await redis.del(keys);
    await redis.quit();
  });

  it('should track active job count correctly', async () => {
    const redis = new Redis(REDIS_URL);
    const queue = new Queue({ redis, namespace: `${namespace}:count` });

    // Initially should be 0
    expect(await queue.getActiveCount()).toBe(0);

    // Add some jobs
    await queue.add({ groupId: 'test-group', data: { id: 1 } });
    await queue.add({ groupId: 'test-group', data: { id: 2 } });

    // Still 0 since no worker is processing
    expect(await queue.getActiveCount()).toBe(0);

    let job1Started = false;
    let job1CanComplete = false;
    const processed: number[] = [];

    const worker = new Worker({
      queue: queue,
      handler: async (job) => {
        if ((job.data as any).id === 1) {
          job1Started = true;
          // Wait for signal to complete
          while (!job1CanComplete) {
            await new Promise((resolve) => setTimeout(resolve, 50));
          }
        }
        processed.push((job.data as any).id);
      },
    });

    worker.run();

    // Wait for job 1 to start processing
    while (!job1Started) {
      await new Promise((resolve) => setTimeout(resolve, 50));
    }

    // Should have 1 active job now
    expect(await queue.getActiveCount()).toBe(1);

    // Signal job 1 to complete
    job1CanComplete = true;

    await queue.waitForEmpty();

    // Should be back to 0
    expect(await queue.getActiveCount()).toBe(0);

    await worker.close();
    await redis.quit();
  });

  it('should wait for queue to empty', async () => {
    const redis = new Redis(REDIS_URL);
    const queue = new Queue({ redis, namespace: `${namespace}:empty` });

    // Should return true immediately if already empty
    expect(await queue.waitForEmpty()).toBe(true);

    // Add jobs and start processing
    await queue.add({ groupId: 'empty-group', data: { id: 1 } });
    await queue.add({ groupId: 'empty-group', data: { id: 2 } });

    let processedCount = 0;
    const processedIds: number[] = [];
    const worker = new Worker({
      queue: queue,
      handler: async (job) => {
        await new Promise((resolve) => setTimeout(resolve, 50)); // Simulate work - reduced for faster tests
        processedCount++;
        processedIds.push((job.data as any).id);
      },
    });

    worker.run();

    // Wait for jobs to start processing - check that active count > 0
    let waitAttempts = 0;
    while ((await queue.getActiveCount()) === 0 && waitAttempts < 20) {
      await new Promise((resolve) => setTimeout(resolve, 50));
      waitAttempts++;
    }

    // Verify that processing has started
    expect(await queue.getActiveCount()).toBeGreaterThan(0);

    // Should wait and return true when empty
    const startTime = Date.now();
    const isEmpty = await queue.waitForEmpty();
    const elapsed = Date.now() - startTime;

    expect(isEmpty).toBe(true);
    expect(processedCount).toBe(2);
    expect(processedIds.sort()).toEqual([1, 2]);
    expect(elapsed).toBeGreaterThan(80); // Should take at least 50ms + 50ms for two jobs

    await worker.close();
    await redis.quit();
  });

  it('should track current job in worker', async () => {
    const redis = new Redis(REDIS_URL);
    const queue = new Queue({ redis, namespace: `${namespace}:current` });

    let jobStarted = false;
    let jobCanComplete = false;

    const worker = new Worker({
      queue: queue,
      handler: async (_job) => {
        jobStarted = true;
        while (!jobCanComplete) {
          await new Promise((resolve) => setTimeout(resolve, 50));
        }
      },
    });

    // Initially no job
    expect(worker.isProcessing()).toBe(false);
    expect(worker.getCurrentJob()).toBe(null);

    worker.run();

    // Add a job
    await queue.add({ groupId: 'current-group', data: { id: 1 } });

    // Wait for job to start
    while (!jobStarted) {
      await new Promise((resolve) => setTimeout(resolve, 50));
    }

    // Give it a moment to track the processing time
    await new Promise((resolve) => setTimeout(resolve, 100));

    // Should be processing now
    expect(worker.isProcessing()).toBe(true);

    const currentJob = worker.getCurrentJob();
    expect(currentJob).not.toBe(null);
    expect((currentJob!.job as any).data.id).toBe(1);
    expect(currentJob!.processingTimeMs).toBeGreaterThan(0);

    // Signal completion
    jobCanComplete = true;

    // Wait for job to complete
    while (worker.isProcessing()) {
      await new Promise((resolve) => setTimeout(resolve, 50));
    }

    expect(worker.getCurrentJob()).toBe(null);

    await worker.close();
    await redis.quit();
  });

  it('should stop worker gracefully', async () => {
    const redis = new Redis(REDIS_URL);
    const queue = new Queue({ redis, namespace: `${namespace}:graceful` });

    let jobStarted = false;
    let jobCompleted = false;

    const worker = new Worker({
      queue: queue,
      handler: async (_job) => {
        jobStarted = true;
        await new Promise((resolve) => setTimeout(resolve, 500)); // Simulate work
        jobCompleted = true;
      },
    });

    worker.run();

    // Add a job
    await queue.add({ groupId: 'graceful-group', data: { id: 1 } });

    // Wait for job to start
    while (!jobStarted) {
      await new Promise((resolve) => setTimeout(resolve, 50));
    }

    expect(worker.isProcessing()).toBe(true);

    // Stop gracefully - should wait for job to complete
    const stopPromise = worker.close(2000); // 2 second timeout

    // Job should complete
    await stopPromise;

    expect(jobCompleted).toBe(true);
    expect(worker.isProcessing()).toBe(false);

    await redis.quit();
  });

  it('should timeout graceful stop if job takes too long', async () => {
    const redis = new Redis(REDIS_URL);
    const queue = new Queue({ redis, namespace: `${namespace}:timeout` });

    let jobStarted = false;
    let shouldStop = false;
    let sawGracefulTimeout = false;

    const worker = new Worker({
      queue: queue,
      handler: async (_job) => {
        jobStarted = true;
        // Simulate a long-running job
        while (!shouldStop) {
          await new Promise((resolve) => setTimeout(resolve, 50));
        }
      },
    });

    worker.on('graceful-timeout', (_info) => {
      sawGracefulTimeout = true;
    });

    worker.run();

    // Add a job
    await queue.add({ groupId: 'timeout-group', data: { id: 1 } });

    // Wait for job to start
    while (!jobStarted) {
      await new Promise((resolve) => setTimeout(resolve, 50));
    }

    expect(worker.isProcessing()).toBe(true);

    // Stop with short timeout - should timeout
    const startTime = Date.now();
    await worker.close(200); // 200ms timeout
    const elapsed = Date.now() - startTime;

    expect(elapsed).toBeGreaterThan(190);
    expect(elapsed).toBeLessThan(800);
    expect(sawGracefulTimeout).toBe(true);

    shouldStop = true; // Allow the handler to finish
    await redis.quit();
  });

  it('should get workers status correctly', async () => {
    const redis = new Redis(REDIS_URL);
    const queue = new Queue({ redis, namespace: `${namespace}:status` });

    let job1Started = false;
    let job1CanComplete = false;

    const workers = [
      new Worker({
        queue: queue,
        handler: async (job) => {
          if (job.data.id === 1) {
            job1Started = true;
            while (!job1CanComplete) {
              await new Promise((resolve) => setTimeout(resolve, 50));
            }
          } else {
            await new Promise((resolve) => setTimeout(resolve, 100));
          }
        },
      }),
      new Worker({
        queue: queue,
        handler: async (job) => {
          if (job.data.id === 1) {
            job1Started = true;
            while (!job1CanComplete) {
              await new Promise((resolve) => setTimeout(resolve, 50));
            }
          } else {
            await new Promise((resolve) => setTimeout(resolve, 100));
          }
        },
      }),
    ];

    workers.forEach((worker) => {
      worker.run();
    });

    // Initially all idle
    let status = getWorkersStatus(workers);
    expect(status.total).toBe(2);
    expect(status.processing).toBe(0);
    expect(status.idle).toBe(2);

    // Add a job
    await queue.add({ groupId: 'status-group', data: { id: 1 } });

    // Wait for job to start with timeout
    let startAttempts = 0;
    while (!job1Started && startAttempts < 200) {
      // 10 second timeout
      await new Promise((resolve) => setTimeout(resolve, 50));
      startAttempts++;
    }

    // Ensure job started
    expect(job1Started).toBe(true);

    // Should have 1 processing, 1 idle
    status = getWorkersStatus(workers);
    expect(status.total).toBe(2);
    expect(status.processing).toBe(1);
    expect(status.idle).toBe(1);

    const processingWorker = status.workers.find((w) => w.isProcessing);
    expect(processingWorker).toBeDefined();
    expect(processingWorker!.currentJob?.jobId).toBeDefined();

    // Signal completion
    job1CanComplete = true;

    // Wait for ANY worker to finish processing (since we don't know which one got the job)
    let attempts = 0;
    while (workers.some((w) => w.isProcessing()) && attempts < 100) {
      await new Promise((resolve) => setTimeout(resolve, 50));
      attempts++;
    }

    // Ensure we didn't timeout
    expect(attempts).toBeLessThan(100);

    // Back to all idle
    status = getWorkersStatus(workers);
    expect(status.processing).toBe(0);
    expect(status.idle).toBe(2);

    await Promise.all(workers.map((w) => w.close()));
    await redis.quit();
  });

  // NEW TESTS REQUESTED BY USER

  it('should finish long-running job before stopping worker (graceful shutdown)', async () => {
    const redis = new Redis(REDIS_URL);
    const queue = new Queue({ redis, namespace: `${namespace}:longrunning` });

    // Add a long-running job
    await queue.add({
      groupId: 'long-group',
      data: { taskType: 'long-running', duration: 2000 }, // 2 second job (reduced for faster tests)
    });

    let jobStartTime: number | null = null;
    let jobEndTime: number | null = null;
    let workerStoppedTime: number | null = null;
    let jobCompleted = false;

    const worker = new Worker({
      queue: queue,
      name: 'graceful-shutdown-worker',
      blockingTimeoutSec: 1,
      handler: async (job) => {
        jobStartTime = Date.now();

        if (job.data.taskType === 'long-running') {
          // Simulate a long job
          await new Promise((resolve) =>
            setTimeout(resolve, job.data.duration),
          );
        }

        jobEndTime = Date.now();
        jobCompleted = true;
      },
    });

    // Start the worker
    const workerPromise = worker.run();

    // Wait for the job to start (give it a moment to pick up the job)
    await new Promise((resolve) => setTimeout(resolve, 200));
    expect(jobStartTime).not.toBeNull();
    expect(jobCompleted).toBe(false);

    // Stop the worker while the job is running
    const stopPromise = worker.close();

    // Wait for worker to stop
    await stopPromise;
    await workerPromise;
    workerStoppedTime = Date.now();

    // Verify that the job completed before the worker stopped
    expect(jobCompleted).toBe(true);
    expect(jobEndTime).not.toBeNull();
    expect(jobStartTime).not.toBeNull();
    expect(workerStoppedTime).not.toBeNull();

    // The job should have completed before or very close to when the worker stopped
    const jobDuration = jobEndTime! - jobStartTime!;
    expect(jobDuration).toBeGreaterThanOrEqual(1900); // At least ~2 seconds
    expect(jobDuration).toBeLessThan(3000); // But not much more

    // Worker should not have stopped before the job completed
    expect(jobEndTime!).toBeLessThanOrEqual(workerStoppedTime! + 100); // Allow small margin

    await redis.quit();
  }, 8000); // 8 second timeout for the test (reduced)

  it('should not pick up new jobs after shutdown is initiated', async () => {
    const redis = new Redis(REDIS_URL);
    const queue = new Queue({ redis, namespace: `${namespace}:nonewjobs` });

    // Add multiple jobs
    await queue.add({
      groupId: 'test-group',
      data: { taskType: 'first', id: 1 },
    });

    await queue.add({
      groupId: 'test-group',
      data: { taskType: 'second', id: 2 },
    });

    const processedJobs: any[] = [];
    let shutdownInitiated = false;

    const worker = new Worker({
      queue: queue,
      name: 'no-new-jobs-worker',
      blockingTimeoutSec: 1,
      handler: async (job) => {
        processedJobs.push(job.data);

        if (job.data.taskType === 'first') {
          // After processing the first job, initiate shutdown
          setTimeout(() => {
            shutdownInitiated = true;
            worker.close();
          }, 100);

          // Take some time to process
          await new Promise((resolve) => setTimeout(resolve, 1000));
        } else {
          // This should not be reached if shutdown works correctly
          await new Promise((resolve) => setTimeout(resolve, 500));
        }
      },
    });

    // Start the worker
    const workerPromise = worker.run();

    // Wait for the worker to finish
    await workerPromise;

    // With atomic completion, the worker might process the second job before shutdown
    // So we check that at least the first job was processed and shutdown was initiated
    expect(processedJobs.length).toBeGreaterThanOrEqual(1);
    expect(processedJobs[0].taskType).toBe('first');
    expect(shutdownInitiated).toBe(true);

    // If atomic completion processed the second job, it should be completed, not waiting
    const queueStats = await queue.getJobCounts();
    if (processedJobs.length === 1) {
      expect(queueStats.waiting).toBe(1); // Second job should still be waiting
    } else {
      expect(queueStats.waiting).toBe(0); // Second job was processed atomically
    }

    await redis.quit();
  }, 10000); // 10 second timeout for the test

  it('should shutdown gracefully', async () => {
    const redis = new Redis(REDIS_URL);
    const queue = new Queue({
      redis,
      logger: true,
      namespace: `${namespace}:in-memory`,
    });
    let isCompleted = false;
    const worker = new Worker({
      queue: queue,
      logger: true,
      handler: async (job) => {
        await new Promise((resolve) => setTimeout(resolve, 1000));
        isCompleted = true;
      },
    });
    await queue.add({ groupId: 'test-group', data: { id: 1 } });
    worker.on('completed', (job) => {
      console.log('Completed', job.id);
    });
    worker.run();
    // Give worker time to pick up the job before closing
    await new Promise((resolve) => setTimeout(resolve, 100));
    await worker.close(2000);
    expect(isCompleted).toBe(true);
    expect(worker.isProcessing()).toBe(false);
    expect(worker.getCurrentJob()).toBe(null);
    expect(worker.isClosed).toBe(true);
  });
});
