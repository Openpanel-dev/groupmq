import Redis from 'ioredis';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { Queue, Worker } from '../src';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';

describe('Ready Queue Cleanup Tests', () => {
  let redis: Redis;
  let namespace: string;
  let queue: Queue<any>;
  let workers: Worker<any>[] = [];

  beforeEach(async () => {
    redis = new Redis(REDIS_URL);
    namespace = `test-ready-cleanup-${Date.now()}-${Math.random().toString(36).slice(2)}`;
    queue = new Queue({
      redis,
      namespace,
    });
  });

  afterEach(async () => {
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

  it('should remove empty groups from ready queue after completion', async () => {
    // Add jobs to multiple groups
    const groupCount = 10;
    const jobsPerGroup = 5;

    // Add jobs first
    for (let g = 0; g < groupCount; g++) {
      for (let j = 0; j < jobsPerGroup; j++) {
        await queue.add({
          groupId: `group-${g}`,
          data: { group: g, job: j },
        });
      }
    }

    // Process all jobs
    const processed: any[] = [];
    const worker = new Worker({
      queue,
      handler: async (job) => {
        processed.push(job.data);
        await new Promise((resolve) => setTimeout(resolve, 10));
      },
    });
    workers.push(worker);
    worker.run();

    // Wait for completion
    await queue.waitForEmpty();

    // Give a moment for cleanup to propagate
    await new Promise((resolve) => setTimeout(resolve, 100));

    // Check that ready queue is now empty (this is the critical check!)
    // Without the fix, this would have stale group entries
    const finalReadyCount = await redis.zcard(`${namespace}:ready`);
    expect(finalReadyCount).toBe(0); // All completed groups should be removed from ready queue

    // Verify all jobs were processed
    expect(processed.length).toBe(groupCount * jobsPerGroup);
  });

  it('should not deadlock with stale ready queue entries', async () => {
    // This test specifically checks the bug we fixed:
    // If we complete all jobs in many groups, the ready queue should be clean
    // and new jobs should be processed immediately without deadlock

    const groupCount = 100;
    const jobsPerGroup = 3;

    // Round 1: Add and process many groups
    for (let g = 0; g < groupCount; g++) {
      for (let j = 0; j < jobsPerGroup; j++) {
        await queue.add({
          groupId: `round1-group-${g}`,
          data: { round: 1, group: g, job: j },
        });
      }
    }

    const processed: any[] = [];
    const worker = new Worker({
      queue,
      handler: async (job) => {
        processed.push(job.data);
      },
    });
    workers.push(worker);
    worker.run();

    await queue.waitForEmpty();

    // Check ready queue state after round 1
    const readyCountAfterRound1 = await redis.zcard(`${namespace}:ready`);
    expect(readyCountAfterRound1).toBe(0); // Should be empty, not full of stale entries!

    // Round 2: Add NEW jobs to NEW groups
    // If the ready queue has stale entries, workers will get stuck fetching those
    // instead of processing the new jobs, causing a deadlock/timeout
    const round2Groups = 10;
    for (let g = 0; g < round2Groups; g++) {
      await queue.add({
        groupId: `round2-group-${g}`,
        data: { round: 2, group: g, job: 0 },
      });
    }

    // This should complete quickly. If it times out, we have stale ready queue entries
    const startTime = Date.now();
    await queue.waitForEmpty();
    const duration = Date.now() - startTime;

    // Should complete in under 5 seconds (allowing generous buffer)
    // If we have the bug, this would timeout or take 30+ seconds
    expect(duration).toBeLessThan(5000);

    // Verify all jobs from both rounds were processed
    const round1Jobs = processed.filter((j) => j.round === 1);
    const round2Jobs = processed.filter((j) => j.round === 2);
    expect(round1Jobs.length).toBe(groupCount * jobsPerGroup);
    expect(round2Jobs.length).toBe(round2Groups);

    // Final check: ready queue should still be empty
    const finalReadyCount = await redis.zcard(`${namespace}:ready`);
    expect(finalReadyCount).toBe(0);
  });

  it('should handle mixed group lifecycle without ready queue leaks', async () => {
    // Test a realistic scenario: some groups complete, some get more jobs added

    const worker = new Worker({
      queue,
      handler: async (job) => {
        await new Promise((resolve) => setTimeout(resolve, 50));
      },
    });
    workers.push(worker);
    worker.run();

    // Add initial jobs
    await queue.add({ groupId: 'group-A', data: { seq: 1 } });
    await queue.add({ groupId: 'group-A', data: { seq: 2 } });
    await queue.add({ groupId: 'group-B', data: { seq: 1 } });

    // Wait a bit for some processing
    await new Promise((resolve) => setTimeout(resolve, 200));

    // Add more jobs to one group, let another finish
    await queue.add({ groupId: 'group-A', data: { seq: 3 } });
    await queue.add({ groupId: 'group-C', data: { seq: 1 } });

    // Wait for all to complete
    await queue.waitForEmpty();
    await new Promise((resolve) => setTimeout(resolve, 100));

    // Ready queue should be clean
    const readyCount = await redis.zcard(`${namespace}:ready`);
    expect(readyCount).toBe(0);

    // Groups set should be empty
    const groupsCount = await redis.scard(`${namespace}:groups`);
    expect(groupsCount).toBe(0);
  });
});
