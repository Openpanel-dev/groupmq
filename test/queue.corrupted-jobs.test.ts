import Redis from 'ioredis';
import { afterEach, beforeEach, describe, expect, test } from 'vitest';
import { Queue } from '../src/queue';

describe('Corrupted/Missing Job Hash Tests', () => {
  let queue: Queue;
  let redis: Redis;

  beforeEach(async () => {
    redis = new Redis({ maxRetriesPerRequest: null });
    const ns = `test:corrupted:${Date.now()}`;
    queue = new Queue({
      redis,
      namespace: ns,
      jobTimeoutMs: 5000,
    });
  });

  afterEach(async () => {
    await queue.close();
    // Don't quit redis separately - queue owns it and closes it
  });

  test('should handle missing job hash gracefully without concatenation error', async () => {
    const groupId = 'test-group';
    const fakeJobId = 'fake-job-id-12345';

    // Manually create a corrupted state: job ID in group sorted set but no job hash
    const gZ = `${queue.prefix}:g:${groupId}`;
    const readyKey = `${queue.prefix}:ready`;

    // Add job ID to group sorted set
    await redis.zadd(gZ, 1000, fakeJobId);

    // Add group to ready queue
    await redis.zadd(readyKey, 1000, groupId);

    // Try to reserve - should not crash with "attempt to concatenate" error
    let errorOccurred = false;
    let concatenationError = false;

    try {
      // This should handle the missing job hash gracefully
      const result = await queue['reserve']();
      // Result should be null since job hash doesn't exist
      expect(result).toBeNull();
    } catch (error: any) {
      errorOccurred = true;
      if (error.message && error.message.includes('attempt to concatenate')) {
        concatenationError = true;
      }
    }

    // The critical assertion: no concatenation error should occur
    expect(concatenationError).toBe(false);

    // Verify queue is still functional after handling corruption
    const validJobId = await queue.add({ groupId, data: { test: 'valid' } });
    expect(validJobId).toBeTruthy();

    // Should be able to reserve the valid job
    const validJob = await queue['reserve']();
    expect(validJob).not.toBeNull();
    // validJobId is a string, validJob is an object with id property
    expect(typeof validJob?.id).toBe('string');
    expect(validJob?.groupId).toBe(groupId);
  });

  test('should handle missing job hash in reserveAtomic', async () => {
    const groupId = 'atomic-group';
    const fakeJobId = 'fake-atomic-job';

    // Create corrupted state
    const gZ = `${queue.prefix}:g:${groupId}`;
    await redis.zadd(gZ, 1000, fakeJobId);

    // Add to ready queue
    const readyKey = `${queue.prefix}:ready`;
    await redis.zadd(readyKey, 1000, groupId);

    let concatenationError = false;

    try {
      // Should handle gracefully
      const result = await queue['reserveAtomic'](groupId, null);
      // Should return null
      expect(result).toBeNull();
    } catch (error: any) {
      if (error.message && error.message.includes('attempt to concatenate')) {
        concatenationError = true;
      }
    }

    expect(concatenationError).toBe(false);

    // Verify queue still works
    await queue.add({ groupId, data: { test: 'valid' } });
    const validJob = await queue['reserveAtomic'](groupId, null);
    expect(validJob).not.toBeNull();
    expect(validJob?.groupId).toBe(groupId);
  });
});
