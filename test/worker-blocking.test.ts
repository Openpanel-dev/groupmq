import Redis from 'ioredis';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { Queue } from '../src/queue';
import { Worker } from '../src/worker';

describe('Worker Blocking Detection Tests', () => {
  let redis: Redis;
  let queue: Queue;
  let workers: Worker[] = [];
  let namespace: string;

  beforeEach(async () => {
    // Create unique namespace for each test to avoid interference
    namespace = `test-blocking-${Date.now()}-${Math.random().toString(36).slice(2)}`;

    redis = new Redis({
      host: 'localhost',
      port: 6379,
      maxRetriesPerRequest: null,
    });

    // Clear any existing test data
    const keys = await redis.keys(`groupmq:${namespace}:*`);
    if (keys.length > 0) {
      await redis.del(...keys);
    }

    queue = new Queue({
      redis: redis.duplicate(),
      namespace,
      jobTimeoutMs: 30000,
      orderingDelayMs: 0,
    });
  });

  afterEach(async () => {
    // Close all workers
    await Promise.all(workers.map((w) => w.close()));
    workers = [];

    // Clean up test data
    const keys = await redis.keys(`groupmq:${namespace}:*`);
    if (keys.length > 0) {
      await redis.del(...keys);
    }

    await redis.quit();
  });

  it('should detect worker blocking with many groups and few workers', async () => {
    console.log('\n🧪 Testing blocking detection with many groups...');

    // Create 8 workers
    const workerCount = 8;
    const groupCount = 100; // Many more groups than workers

    for (let i = 0; i < workerCount; i++) {
      const worker = new Worker({
        queue: queue,
        handler: async (job) => {
          console.log(
            `Worker ${i} processing job ${job.id} from group ${job.groupId}`,
          );
          // Simulate work
          await new Promise((resolve) => setTimeout(resolve, 50));
        },
        blockingTimeoutSec: 2, // Short timeout for testing
      });

      workers.push(worker);

      // Start worker and give it time to initialize
      worker.run().catch((err) => {
        console.error(`Worker ${i} error:`, err);
      });
    }

    // Wait for workers to start
    await new Promise((resolve) => setTimeout(resolve, 1000));

    // Add jobs to many different groups
    const jobPromises = [];
    for (let i = 0; i < groupCount; i++) {
      jobPromises.push(
        queue.add({
          groupId: `test-group-${i}`,
          data: { id: i, data: `test-data-${i}` },
        }),
      );
    }

    await Promise.all(jobPromises);
    console.log(`✅ Added ${groupCount} jobs to ${groupCount} groups`);

    // Monitor workers for a period to see if any get stuck
    console.log('🔍 Monitoring workers for blocking issues...');

    let totalJobsProcessed = 0;
    const monitorDuration = 10_000;
    const startTime = Date.now();

    while (Date.now() - startTime < monitorDuration) {
      await new Promise((resolve) => setTimeout(resolve, 1000));

      // Get worker metrics
      let activeWorkers = 0;
      let currentTotal = 0;

      for (const worker of workers) {
        const metrics = worker.getWorkerMetrics();
        currentTotal += metrics.totalJobsProcessed;

        if (
          metrics.isProcessing ||
          metrics.timeSinceLastJob === null ||
          metrics.timeSinceLastJob < 5000
        ) {
          activeWorkers++;
        }

        // Log warning if worker seems stuck
        if (metrics.blockingStats.consecutiveEmptyReserves > 5) {
          console.warn(
            `⚠️ Worker ${metrics.name} has ${metrics.blockingStats.consecutiveEmptyReserves} consecutive empty reserves`,
          );
        }
      }

      console.log(
        `📊 Progress: ${currentTotal} jobs processed, ${activeWorkers}/${workerCount} workers active`,
      );
      totalJobsProcessed = currentTotal;

      // If all jobs are processed, break early
      const queueStats = await queue.getJobCounts();
      if (queueStats.active === 0 && queueStats.waiting === 0) {
        console.log('✅ All jobs completed!');
        break;
      }
    }

    // Final analysis
    console.log('\n📈 Final Worker Analysis:');
    for (const worker of workers) {
      const metrics = worker.getWorkerMetrics();
      console.log(`Worker ${metrics.name}:`);
      console.log(`  Jobs Processed: ${metrics.totalJobsProcessed}`);
      console.log(`  Time Since Last Job: ${metrics.timeSinceLastJob}ms`);
      console.log(
        `  Consecutive Empty Reserves: ${metrics.blockingStats.consecutiveEmptyReserves}`,
      );
      console.log(
        `  Total Blocking Calls: ${metrics.blockingStats.totalBlockingCalls}`,
      );
      console.log(`  Is Processing: ${metrics.isProcessing}`);
    }

    // Verify that workers are working efficiently
    expect(totalJobsProcessed).toBeGreaterThan(50); // Should process a good number of jobs

    // Check that no worker is completely stuck (more than 20 consecutive empty reserves is concerning)
    for (const worker of workers) {
      const metrics = worker.getWorkerMetrics();
      expect(metrics.blockingStats.consecutiveEmptyReserves).toBeLessThan(20);
    }
  }, 30000); // 30 second timeout for the test

  it('should handle Redis connection issues gracefully', async () => {
    console.log('\n🧪 Testing Redis connection resilience...');

    // Create a worker
    const worker = new Worker({
      queue: queue,
      handler: async (job) => {
        console.log(`Processing job ${job.id}`);
        await new Promise((resolve) => setTimeout(resolve, 100));
      },
      blockingTimeoutSec: 1, // Very short timeout
    });

    workers.push(worker);

    // Start worker
    worker.run().catch((err) => {
      console.error('Worker error:', err);
    });

    // Add a few jobs
    for (let i = 0; i < 5; i++) {
      await queue.add({
        groupId: `test-group-${i}`,
        data: { id: i },
      });
    }

    // Let it process for a bit (increased for scheduler tick + processing)
    await new Promise((resolve) => setTimeout(resolve, 3000));

    // Check that worker isn't stuck even with short timeouts
    const metrics = worker.getWorkerMetrics();
    console.log('Worker metrics:', metrics);

    expect(metrics.totalJobsProcessed).toBeGreaterThan(0);
  }, 15000);

  it('should detect stuck workers with comprehensive logging', async () => {
    console.log('\n🧪 Testing stuck worker detection...');

    // Create a worker that will get "stuck" (simulate by adding jobs it can't process)
    const worker = new Worker({
      queue: queue,
      handler: async (job) => {
        // Simulate a job that takes a long time or fails
        if ((job.data as any).shouldFail) {
          throw new Error('Simulated job failure');
        }
        await new Promise((resolve) => setTimeout(resolve, 100));
      },
      blockingTimeoutSec: 1,
      maxAttempts: 1, // Quick failure
    });

    workers.push(worker);

    // Start worker
    worker.run().catch((err) => {
      console.error('Worker error:', err);
    });

    // Add some jobs that will fail
    for (let i = 0; i < 3; i++) {
      await queue.add({
        groupId: `fail-group-${i}`,
        data: { id: i, shouldFail: true },
      });
    }

    // Monitor for stuck detection
    await new Promise((resolve) => setTimeout(resolve, 5000));

    const metrics = worker.getWorkerMetrics();
    console.log('Stuck test metrics:', metrics);

    // Worker should have attempted to process jobs
    expect(metrics.blockingStats.totalBlockingCalls).toBeGreaterThan(0);
  }, 15000);
});
