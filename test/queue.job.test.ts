import Redis from 'ioredis';
import { afterAll, describe, expect, it } from 'vitest';
import { type Job, Queue, Worker } from '../src';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';

describe('Job Tests', () => {
  const namespace = `test:job:${Date.now()}`;

  afterAll(async () => {
    const redis = new Redis(REDIS_URL);
    const keys = await redis.keys(`${namespace}*`);
    if (keys.length) await redis.del(keys);
    await redis.quit();
  });

  it('should always return a job entity', async () => {
    const redis = new Redis(REDIS_URL);
    const q = new Queue({
      redis,
      namespace: `${namespace}:dedupe`,
      keepCompleted: 1,
    });
    const job = await q.add({ groupId: 'g1', data: { n: 1 } });

    expect(job).toBeDefined();
    expect(job.id).toBeDefined();
    expect(job.groupId).toBe('g1');
    expect(job.data).toBeDefined();
    expect(job.data.n).toBe(1);
    expect(job.status).toBe('waiting');
    expect(job.processedOn).toBeUndefined();
    expect(job.finishedOn).toBeUndefined();
    expect(job.failedReason).toBeUndefined();
    expect(job.returnvalue).toBeUndefined();
    expect(job.timestamp).toBeDefined();
    expect(job.orderMs).toBeDefined();
    expect(job.attemptsMade).toBe(0);
    expect(job.opts.attempts).toBe(3);
    expect(job.opts.delay).toBeUndefined();

    const worker = new Worker({
      queue: q,
      handler: async () => {
        return 'return value from worker';
      },
    });
    worker.run();
    let eventJob: Job | undefined;
    worker.on('completed', (job) => {
      eventJob = job as Job;
    });

    await q.waitForEmpty(2000);

    if (eventJob) {
      expect(eventJob.processedOn).toBeDefined();
      expect(eventJob.finishedOn).toBeDefined();
      const now = Date.now();
      expect(typeof (eventJob.processedOn as number)).toBe('number');
      expect(typeof (eventJob.finishedOn as number)).toBe('number');
      expect((eventJob.processedOn as number) > 1e12).toBe(true);
      expect((eventJob.finishedOn as number) > 1e12).toBe(true);
      expect((eventJob.processedOn as number) <= now).toBe(true);
      expect((eventJob.finishedOn as number) <= now).toBe(true);
      expect(
        (eventJob.finishedOn as number) >= (eventJob.processedOn as number),
      ).toBe(true);
      expect(eventJob.failedReason).toBeUndefined();
      expect(eventJob.data).toEqual({ n: 1 });
      expect(eventJob.returnvalue).toEqual('return value from worker');
    } else {
      throw new Error('Completed job event not received');
    }

    const completedJob = await q.getJob(job.id);
    expect(completedJob).toBeDefined();
    expect(completedJob.processedOn).toBeDefined();
    expect(completedJob.finishedOn).toBeDefined();
    const now2 = Date.now();
    expect(typeof (completedJob.processedOn as number)).toBe('number');
    expect(typeof (completedJob.finishedOn as number)).toBe('number');
    expect((completedJob.processedOn as number) > 1e12).toBe(true);
    expect((completedJob.finishedOn as number) > 1e12).toBe(true);
    expect((completedJob.processedOn as number) <= now2).toBe(true);
    expect((completedJob.finishedOn as number) <= now2).toBe(true);
    expect(
      (completedJob.finishedOn as number) >=
        (completedJob.processedOn as number),
    ).toBe(true);
    expect(completedJob.failedReason).toBeUndefined();
    expect(completedJob.data).toEqual({ n: 1 });
    expect(completedJob.returnvalue).toEqual('return value from worker');

    await worker.close();
    await redis.quit();
  });

  it('should give the error for a failed job', async () => {
    const redis = new Redis(REDIS_URL);
    const q = new Queue({
      redis,
      namespace: `${namespace}:failed`,
      keepFailed: 1,
    });
    const job = await q.add({ groupId: 'g1', data: { n: 1 } });

    const worker = new Worker({
      queue: q,
      handler: async () => {
        throw new Error('Failed job');
      },
    });

    worker.run();

    await q.waitForEmpty(2000);

    const failedJob = await q.getJob(job.id);
    expect(failedJob).toBeDefined();
    expect(failedJob.failedReason).toEqual('Failed job');
    expect(failedJob.stacktrace).toBeDefined();
    expect(String(failedJob.stacktrace)).toMatch(/^Error: Failed job\n/);
    expect(String(failedJob.stacktrace)).toMatch(
      /at (\w+)\.(\w+) \(.*\.ts:\d+:\d+\)/,
    );
    // Validate timestamps for failed jobs as Unix ms
    const now3 = Date.now();
    expect(failedJob.processedOn).toBeDefined();
    expect(failedJob.finishedOn).toBeDefined();
    expect(typeof (failedJob.processedOn as number)).toBe('number');
    expect(typeof (failedJob.finishedOn as number)).toBe('number');
    expect((failedJob.processedOn as number) > 1e12).toBe(true);
    expect((failedJob.finishedOn as number) > 1e12).toBe(true);
    expect((failedJob.processedOn as number) <= now3).toBe(true);
    expect((failedJob.finishedOn as number) <= now3).toBe(true);
    expect(
      (failedJob.finishedOn as number) >= (failedJob.processedOn as number),
    ).toBe(true);
  });
});
