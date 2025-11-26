import Redis from 'ioredis';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { Queue, Worker } from '../src';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';

describe('basic per-group FIFO and parallelism', () => {
  const redis = new Redis(REDIS_URL);
  const namespace = `test:q1:${Date.now()}`;

  beforeAll(async () => {
    // flush only this namespace keys (best-effort)
    const keys = await redis.keys(`${namespace}*`);
    if (keys.length) await redis.del(keys);
  });

  afterAll(async () => {
    await redis.quit();
  });

  it('should have a name', () => {
    const q = new Queue({ redis, namespace, jobTimeoutMs: 5000 });
    expect(q.name).toBe(namespace);
  });
});
