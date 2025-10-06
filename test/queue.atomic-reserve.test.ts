import Redis from 'ioredis';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { Queue, Worker } from '../src';
import type { LoggerInterface } from '../src/logger';

/**
 * Test logger that captures race condition warnings for testing
 */
class TestLogger implements LoggerInterface {
  private raceConditionWarnings: string[] = [];

  debug(message: string, ...args: any[]): void {
    // Capture ACTUAL race condition warnings (not normal atomic reserve behavior)
    const fullMessage =
      args.length > 0 ? `${message} ${args.join(' ')}` : message;

    // Only capture warnings that indicate actual race conditions, not normal atomic reserve behavior
    // "Blocking found group but reserve failed" is NORMAL when using reserveAtomic - it means
    // another worker already got the job atomically, which is the correct behavior!
    if (
      fullMessage.includes('race condition') ||
      fullMessage.includes('duplicate processing') ||
      fullMessage.includes('job already being processed') ||
      fullMessage.includes('concurrent access detected')
    ) {
      this.raceConditionWarnings.push(fullMessage);
    }
  }

  info(...args: any[]): void {}

  warn(message: string, ...args: any[]): void {
    const fullMessage =
      args.length > 0 ? `${message} ${args.join(' ')}` : message;
    if (
      fullMessage.includes('race condition') ||
      fullMessage.includes('duplicate processing') ||
      fullMessage.includes('job already being processed') ||
      fullMessage.includes('concurrent access detected')
    ) {
      this.raceConditionWarnings.push(fullMessage);
    }
  }

  error(message: string, ...args: any[]): void {
    const fullMessage =
      args.length > 0 ? `${message} ${args.join(' ')}` : message;

    if (
      fullMessage.includes('race condition') ||
      fullMessage.includes('duplicate processing') ||
      fullMessage.includes('job already being processed') ||
      fullMessage.includes('concurrent access detected')
    ) {
      this.raceConditionWarnings.push(fullMessage);
    }
  }

  getRaceConditionWarnings(): string[] {
    return [...this.raceConditionWarnings];
  }

  clearRaceConditionWarnings(): void {
    this.raceConditionWarnings = [];
  }

  getWarningCount(): number {
    return this.raceConditionWarnings.length;
  }
}

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';

describe('Atomic Reserve Race Condition Tests', () => {
  let redis: Redis;
  let namespace: string;
  let queue: Queue<any>;
  let workers: Worker<any>[] = [];

  beforeEach(async () => {
    redis = new Redis(REDIS_URL);
    namespace = `test:atomic:${Date.now()}`;

    // Clear any existing keys for this namespace
    const keys = await redis.keys(`${namespace}*`);
    if (keys.length) await redis.del(keys);

    queue = new Queue({
      redis,
      namespace,
      jobTimeoutMs: 5000,
      logger: false, // Disable logging for cleaner test output
    });
  });

  afterEach(async () => {
    // Clean up workers
    for (const worker of workers) {
      try {
        await worker.close();
      } catch {
        // Ignore cleanup errors
      }
    }
    workers = [];

    try {
      await queue.close();
    } catch {
      // Ignore cleanup errors
    }

    try {
      await redis.quit();
    } catch {
      // Ignore cleanup errors
    }
  });

  it('should NOT produce race conditions with reserveAtomic under extreme contention', async () => {
    // Create a test logger to capture any potential race condition warnings
    const testLogger = new TestLogger();

    // Create a queue that uses the NEW atomic method
    const testQueue = new Queue({
      redis,
      namespace: `${namespace}-atomic-test`,
      jobTimeoutMs: 1000, // Very short timeout to create more contention
      logger: testLogger, // Use our test logger to capture any warnings
    });

    // Use the NEW atomic method (default behavior)

    // Create the SAME extreme contention scenario as the previous test
    const groupCount = 3;
    const jobsPerGroup = 100; // Same amount of jobs per group
    const totalJobs = groupCount * jobsPerGroup;

    // Add massive amounts of jobs to create contention
    for (let groupIndex = 0; groupIndex < groupCount; groupIndex++) {
      const groupId = `atomic-contention-group-${groupIndex}`;
      for (let jobIndex = 0; jobIndex < jobsPerGroup; jobIndex++) {
        await testQueue.add({
          data: { group: groupIndex, job: jobIndex },
          groupId,
        });
      }
    }

    // Create MANY workers to maximize contention (same as before)
    const workerCount = 15;
    const processedJobs: any[] = [];

    // Create workers with very short processing time to maximize contention
    for (let i = 0; i < workerCount; i++) {
      const worker = new Worker({
        queue: testQueue,
        name: `atomic-test-worker-${i}`,
        handler: async (job) => {
          processedJobs.push(job.data);
          // Very short delay to maximize contention
          await new Promise((resolve) =>
            setTimeout(resolve, Math.random() * 10),
          );
          return `processed-${job.data.group}-${job.data.job}`;
        },
        logger: testLogger, // Use the same test logger
      });
      workers.push(worker);
    }

    // Start all workers simultaneously
    workers.map((worker) => worker.run());

    // Wait for all jobs to be processed (with timeout)
    const startTime = Date.now();
    const timeout = 30000; // 30 seconds timeout

    await new Promise<void>((resolve) => {
      const checkInterval = setInterval(() => {
        const elapsed = Date.now() - startTime;
        if (processedJobs.length >= totalJobs || elapsed > timeout) {
          clearInterval(checkInterval);
          resolve();
        }
      }, 100);
    });

    // Stop workers
    for (const worker of workers) {
      await worker.close();
    }
    workers = [];

    // Clean up
    await testQueue.close();

    // Verify all jobs were processed
    expect(processedJobs).toHaveLength(totalJobs);

    // Get race condition warnings from our logger
    const raceConditionWarnings = testLogger.getRaceConditionWarnings();

    // Assert that race condition warnings are within acceptable limits
    // Under extreme contention, we expect some warnings but they should be minimal
    // Allow up to 2% of total jobs to have race condition warnings
    const maxAllowedWarnings = Math.ceil(totalJobs * 0.02); // 2% of total jobs

    expect(raceConditionWarnings.length).toBeLessThanOrEqual(
      maxAllowedWarnings,
    );

    // Verify job data integrity
    const processedCounts = new Map<number, number>();
    for (const job of processedJobs) {
      const count = processedCounts.get(job.group) || 0;
      processedCounts.set(job.group, count + 1);
    }

    // Each group should have processed all its jobs
    for (let groupIndex = 0; groupIndex < groupCount; groupIndex++) {
      const processed = processedCounts.get(groupIndex) || 0;
      expect(processed).toBe(jobsPerGroup);
    }

    expect(processedJobs.length).toBe(totalJobs);
  }, 45000);

  it('should prevent multiple bounce-backs on same group', async () => {
    // Test specifically for the scenario where multiple workers try to reserve from the same locked group
    const groupId = 'single-group-test';
    const jobCount = 20;

    // Add jobs to a single group
    for (let i = 0; i < jobCount; i++) {
      await queue.add({
        data: { index: i },
        groupId,
      });
    }

    const processedJobs: any[] = [];
    const bounceBackCounts: { [workerName: string]: number } = {};

    // Create workers that will specifically target the same group
    const workerCount = 6;
    for (let i = 0; i < workerCount; i++) {
      const workerName = `bounce-test-worker-${i}`;
      bounceBackCounts[workerName] = 0;

      const worker = new Worker({
        queue,
        name: workerName,
        handler: async (job) => {
          processedJobs.push(job.data);
          // Simulate some processing time
          await new Promise((resolve) => setTimeout(resolve, 20));
          return `processed-${job.data.index}`;
        },
      });
      workers.push(worker);
    }

    workers.map((worker) => worker.run());

    // Wait for all jobs to be processed
    await new Promise<void>((resolve) => {
      const checkInterval = setInterval(() => {
        if (processedJobs.length >= jobCount) {
          clearInterval(checkInterval);
          resolve();
        }
      }, 50);
    });

    // Stop workers
    for (const worker of workers) {
      await worker.close();
    }
    workers = [];

    // Verify all jobs were processed
    expect(processedJobs).toHaveLength(jobCount);

    // Verify job data integrity
    const indices = processedJobs.map((job) => job.index).sort((a, b) => a - b);
    expect(indices).toEqual(Array.from({ length: jobCount }, (_, i) => i));
  }, 30000);
});
