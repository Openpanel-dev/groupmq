import Redis from 'ioredis';
import pino from 'pino';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import winston from 'winston';
import { Queue, Worker } from '../src';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';

describe('logger', () => {
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

  it('pino', () => {
    const logger = pino();
    const q = new Queue({ redis, namespace, jobTimeoutMs: 5000 });
    const worker = new Worker({
      logger,
      queue: q,
      handler: async () => {
        return 'return value from worker';
      },
    });
    worker.run();
  });

  it('winston', () => {
    const logger = winston.createLogger();
    const q = new Queue({ redis, namespace, jobTimeoutMs: 5000 });
    const worker = new Worker({
      logger,
      queue: q,
      handler: async () => {
        return 'return value from worker';
      },
    });
    worker.run();
  });
});
