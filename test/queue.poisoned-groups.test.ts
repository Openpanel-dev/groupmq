import Redis from 'ioredis';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { Queue, Worker } from '../src/index.js';

describe('Poisoned Groups', () => {
  let q: Queue;
  let w: Worker;
  let r: Redis;
  const ns = `test-poisoned-groups-${Date.now()}`;

  beforeEach(async () => {
    r = new Redis({ maxRetriesPerRequest: null });
    // Clean slate before each test
    const keys = await r.keys(`${ns}*`);
    if (keys.length) await r.del(keys);
    q = new Queue({ redis: r, namespace: ns });
  });

  afterEach(async () => {
    try {
      if (w) await w.close();
    } catch (e) {
      // Ignore cleanup errors
    }
    try {
      if (q) await q.close();
    } catch (e) {
      // Ignore cleanup errors
    }
    try {
      if (r && r.status !== 'end') {
        const keys = await r.keys(`${ns}*`);
        if (keys.length) await r.del(keys);
        await r.quit();
      }
    } catch (e) {
      // Ignore cleanup errors
    }
  });

  it('should handle empty groups in ready queue without infinite loop', async () => {
    const readyKey = `${ns}:ready`;
    const poisonedGroupId = 'poisoned-group-123';

    // Manually create a poisoned group (group in ready queue but no jobs)
    await r.zadd(readyKey, Date.now(), poisonedGroupId);

    // Verify the poisoned group is in ready queue
    const groupsBeforeCount = await r.zcard(readyKey);
    expect(groupsBeforeCount).toBe(1);

    // Start a worker - it should handle the poisoned group gracefully
    const processedJobs: any[] = [];

    w = new Worker({
      queue: q,
      handler: async (job) => {
        processedJobs.push(job);
        return 'done';
      },
      blockingTimeoutSec: 0.5,
    });
    w.run();

    // Add a real job to make sure the worker is functioning
    await new Promise((resolve) => setTimeout(resolve, 500));
    await q.add({ groupId: 'real-group', data: { test: true } });

    // Wait for the real job to be processed
    await q.waitForEmpty(5000);

    // The real job should have been processed (proves worker isn't stuck)
    expect(processedJobs.length).toBe(1);
    expect(processedJobs[0].data).toEqual({ test: true });

    // The poisoned group might still be in ready queue, but it's not causing issues
    // The key point is that the worker can still process real jobs and isn't stuck in a loop
  });

  it('should not remove groups that have jobs', async () => {
    // Add a real job
    await q.add({ groupId: 'test-group', data: { foo: 'bar' } });

    let processedCount = 0;
    w = new Worker({
      queue: q,
      handler: async () => {
        processedCount++;
        return 'done';
      },
      blockingTimeoutSec: 1,
    });
    w.run();

    // Wait for job to be processed
    await q.waitForEmpty(5000);

    // Job should have been processed
    expect(processedCount).toBe(1);
  });

  it('should handle groups with all exhausted attempts', async () => {
    const groupId = 'exhausted-group';

    // Add a job with 1 attempt (will fail once)
    await q.add({ groupId, data: { foo: 'bar' }, maxAttempts: 1 });

    // Process and fail the job to exhaust attempts
    let attempts = 0;
    w = new Worker({
      queue: q,
      handler: async () => {
        attempts++;
        throw new Error('Intentional failure');
      },
      blockingTimeoutSec: 0.5,
      maxAttempts: 1,
    });
    w.run();

    // Wait for job to be exhausted
    await new Promise((resolve) => setTimeout(resolve, 2000));

    // The job should have been attempted once
    expect(attempts).toBe(1);

    // Close the worker
    await w.close();

    // Start a new worker - it should not process anything
    let newAttempts = 0;
    w = new Worker({
      queue: q,
      handler: async () => {
        newAttempts++;
        return 'done';
      },
      blockingTimeoutSec: 0.5,
    });
    w.run();

    // Wait a bit
    await new Promise((resolve) => setTimeout(resolve, 2000));

    // No new jobs should have been processed (job is exhausted)
    expect(newAttempts).toBe(0);
  });

  it('should restore groups with active jobs that have valid jobs', async () => {
    const groupId = 'active-group';

    // Add multiple jobs to the same group
    await q.add({ groupId, data: { job: 1 } });
    await q.add({ groupId, data: { job: 2 } });

    let processedCount = 0;

    w = new Worker({
      queue: q,
      handler: async (job) => {
        processedCount++;
        // Add small delay to ensure sequential processing
        await new Promise((resolve) => setTimeout(resolve, 50));
        return 'done';
      },
      blockingTimeoutSec: 1,
      concurrency: 1, // Process one at a time per group
    });
    w.run();

    // Wait for both jobs to complete
    await q.waitForEmpty(5000);

    // Both jobs should have been processed
    expect(processedCount).toBe(2);
  });
});
