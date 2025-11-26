#!/usr/bin/env node

import { spawn } from 'node:child_process';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { performance } from 'node:perf_hooks';
import * as BullMQ from 'bullmq';
import { Command } from 'commander';
import Redis from 'ioredis';
import pidusage from 'pidusage';
import * as GroupMQ from '../src/index';

// CLI setup
const program = new Command();
program
  .requiredOption(
    '--mq <bullmq|groupmq|both>',
    'Queue implementation to benchmark',
  )
  .option(
    '--jobs <n>',
    'Number of jobs to process',
    (v) => parseInt(v, 10),
    100,
  )
  .option('--workers <n>', 'Number of workers', (v) => parseInt(v, 10), 4)
  .option('--job-type <cpu|io|empty>', 'Type of job workload', 'cpu')
  .option(
    '--multi-process',
    'Use separate processes for workers (better CPU parallelization)',
    false,
  )
  .option('--output <file>', 'Output file for results', '')
  .option(
    '--db <local|redis|dragonfly>',
    'Redis database to use (local=6379, redis=6384, dragonfly=6385)',
    'local',
  )
  .parse();

type BenchmarkOptions = {
  mq: 'bullmq' | 'groupmq' | 'both';
  jobs: number;
  workers: number;
  jobType: 'cpu' | 'io' | 'empty';
  multiProcess: boolean;
  output: string;
  db: 'local' | 'redis' | 'dragonfly';
};
const cliOpts = program.opts() as BenchmarkOptions;

// Map database option to Redis connection details
function getRedisConfig(db: string): { host: string; port: number } {
  switch (db) {
    case 'dragonfly':
      return { host: 'localhost', port: 6385 };
    default:
      return { host: 'localhost', port: 6384 };
  }
}

// Types
interface JobMetrics {
  id: string;
  enqueuedAt: number;
  startedAt: number;
  completedAt: number;
  pickupMs: number;
  processingMs: number;
  totalMs: number;
  // Additional breakdown for analysis
  workerPickupTime?: number; // When worker actually picked up the job
  handlerStartTime?: number; // When job handler started executing
  handlerEndTime?: number; // When job handler finished executing
  workerCompleteTime?: number; // When worker completed the job
}

interface SystemMetrics {
  timestamp: number;
  cpu: number;
  memoryMB: number;
}

interface BenchmarkResult {
  timestamp: number;
  queueType: string;
  jobType: string;
  totalJobs: number;
  workersCount: number;
  completedJobs: number;
  durationMs: number;
  throughputJobsPerSec: number;
  avgPickupMs: number;
  avgProcessingMs: number;
  avgTotalMs: number;
  p95PickupMs: number;
  p95ProcessingMs: number;
  p95TotalMs: number;
  peakCpuPercent: number;
  peakMemoryMB: number;
  avgCpuPercent: number;
  avgMemoryMB: number;
  settings: BenchmarkSettings;
  redisStats?: RedisStats;
}

interface RedisStats {
  commandstats: Record<string, CommandStat>;
  slowlog: any[];
  latency: any[];
  info: {
    used_memory: number;
    used_memory_human: string;
    used_memory_peak: number;
    used_memory_peak_human: string;
    total_commands_processed: number;
    instantaneous_ops_per_sec: number;
    total_net_input_bytes: number;
    total_net_output_bytes: number;
    keyspace_hits: number;
    keyspace_misses: number;
  };
  summary: {
    totalCalls: number;
    totalUsec: number;
    avgUsecPerCall: number;
    commandCount: number;
    topCommands: CommandStat[];
  };
}

interface CommandStat {
  command?: string;
  calls: number;
  usec: number;
  usec_per_call: number;
  rejected_calls: number;
  failed_calls: number;
}

interface BenchmarkSettings {
  mq: string;
  jobs: number;
  workers: number;
  jobType: string;
  multiProcess: boolean;
}

const CONCURRENCY = 8;

// Job workloads
async function cpuIntensiveJob(): Promise<void> {
  // PBKDF2 to simulate CPU load (increased iterations for longer processing)
  const salt = crypto.randomBytes(16);
  crypto.pbkdf2Sync('benchmark-job', salt, 200000, 64, 'sha512');
}

async function ioIntensiveJob(): Promise<void> {
  // File operations to simulate I/O load
  const tmpFile = path.join(
    '/tmp',
    `benchmark-${crypto.randomBytes(8).toString('hex')}`,
  );
  const data = crypto.randomBytes(64 * 1024); // 64KB

  await fs.promises.writeFile(tmpFile, data);
  await fs.promises.readFile(tmpFile);
  await fs.promises.unlink(tmpFile).catch(() => {});
}

async function emptyJob(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 0));
}

// Utility functions
function percentile(values: number[], p: number): number {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const index = Math.ceil(sorted.length * p) - 1;
  return sorted[Math.max(0, index)];
}

function average(values: number[]): number {
  return values.length > 0
    ? values.reduce((a, b) => a + b, 0) / values.length
    : 0;
}

// Redis stats collection
async function resetRedisStats(redis: Redis): Promise<void> {
  await redis.config('RESETSTAT');
  await redis.slowlog('RESET');
}

async function collectRedisStats(redis: Redis): Promise<RedisStats> {
  // Get command stats
  const infoCommandstats = await redis.info('commandstats');
  const commandstats: Record<string, CommandStat> = {};

  const lines = infoCommandstats.split('\n');
  for (const line of lines) {
    if (line.startsWith('cmdstat_')) {
      const parts = line.split(':');
      const command = parts[0].replace('cmdstat_', '');
      const stats = parts[1];

      const calls = parseInt(stats.match(/calls=(\d+)/)?.[1] || '0');
      const usec = parseInt(stats.match(/usec=(\d+)/)?.[1] || '0');
      const usec_per_call = parseFloat(
        stats.match(/usec_per_call=([\d.]+)/)?.[1] || '0',
      );
      const rejected_calls = parseInt(
        stats.match(/rejected_calls=(\d+)/)?.[1] || '0',
      );
      const failed_calls = parseInt(
        stats.match(/failed_calls=(\d+)/)?.[1] || '0',
      );

      commandstats[command] = {
        calls,
        usec,
        usec_per_call,
        rejected_calls,
        failed_calls,
      };
    }
  }

  // Get slowlog
  const slowlog = (await redis.slowlog('GET', 100)) as any[];

  // Get latency events
  const latency: any[] = [];

  // Get general info
  const infoStats = await redis.info('stats');
  const infoMemory = await redis.info('memory');

  const parseInfoValue = (info: string, key: string): string => {
    const match = info.match(new RegExp(`${key}:(.+)`));
    return match ? match[1].trim() : '0';
  };

  const info = {
    used_memory: parseInt(parseInfoValue(infoMemory, 'used_memory')),
    used_memory_human: parseInfoValue(infoMemory, 'used_memory_human'),
    used_memory_peak: parseInt(parseInfoValue(infoMemory, 'used_memory_peak')),
    used_memory_peak_human: parseInfoValue(
      infoMemory,
      'used_memory_peak_human',
    ),
    total_commands_processed: parseInt(
      parseInfoValue(infoStats, 'total_commands_processed'),
    ),
    instantaneous_ops_per_sec: parseInt(
      parseInfoValue(infoStats, 'instantaneous_ops_per_sec'),
    ),
    total_net_input_bytes: parseInt(
      parseInfoValue(infoStats, 'total_net_input_bytes'),
    ),
    total_net_output_bytes: parseInt(
      parseInfoValue(infoStats, 'total_net_output_bytes'),
    ),
    keyspace_hits: parseInt(parseInfoValue(infoStats, 'keyspace_hits')),
    keyspace_misses: parseInt(parseInfoValue(infoStats, 'keyspace_misses')),
  };

  // Calculate summary
  const totalCalls = Object.values(commandstats).reduce(
    (sum, stat) => sum + stat.calls,
    0,
  );
  const totalUsec = Object.values(commandstats).reduce(
    (sum, stat) => sum + stat.usec,
    0,
  );
  const avgUsecPerCall = totalCalls > 0 ? totalUsec / totalCalls : 0;
  const commandCount = Object.keys(commandstats).length;

  const topCommands = Object.entries(commandstats)
    .map(([command, stat]) => ({ command, ...stat }))
    .sort((a, b) => b.calls - a.calls)
    .slice(0, 10);

  return {
    commandstats,
    slowlog,
    latency,
    info,
    summary: {
      totalCalls,
      totalUsec,
      avgUsecPerCall,
      commandCount,
      topCommands,
    },
  };
}

// System monitoring
class SystemMonitor {
  private metrics: SystemMetrics[] = [];
  private monitoring = false;
  private interval?: NodeJS.Timeout;

  start(): void {
    this.monitoring = true;
    this.interval = setInterval(async () => {
      if (!this.monitoring) return;

      try {
        const usage = await pidusage(process.pid);
        this.metrics.push({
          timestamp: Date.now(),
          cpu: usage.cpu,
          memoryMB: usage.memory / (1024 * 1024),
        });
      } catch (_error) {
        // Ignore monitoring errors
      }
    }, 100); // Sample every 100ms
  }

  stop(): void {
    this.monitoring = false;
    if (this.interval) {
      clearInterval(this.interval);
    }
  }

  getStats(): {
    peakCpu: number;
    peakMemory: number;
    avgCpu: number;
    avgMemory: number;
  } {
    if (this.metrics.length === 0) {
      return { peakCpu: 0, peakMemory: 0, avgCpu: 0, avgMemory: 0 };
    }

    const cpuValues = this.metrics.map((m) => m.cpu);
    const memoryValues = this.metrics.map((m) => m.memoryMB);

    return {
      peakCpu: Math.max(...cpuValues),
      peakMemory: Math.max(...memoryValues),
      avgCpu: average(cpuValues),
      avgMemory: average(memoryValues),
    };
  }
}

// Queue adapters
abstract class QueueAdapter {
  abstract setup(): Promise<void>;
  abstract enqueueJobs(count: number): Promise<void>;
  abstract startWorkers(
    count: number,
    jobHandler: () => Promise<void>,
    multiProcess?: boolean,
  ): Promise<void>;
  abstract waitForCompletion(timeoutMs?: number): Promise<void>;
  abstract cleanup(): Promise<void>;
  abstract getCompletedJobs(): JobMetrics[];
  abstract getRedisInstance(): Redis;
}

class BullMQAdapter extends QueueAdapter {
  private redis!: Redis;
  private queue!: BullMQ.Queue;
  private workers: BullMQ.Worker[] = [];
  private workerProcesses: any[] = [];
  private completedJobs: JobMetrics[] = [];
  private queueName: string;
  private opts: BenchmarkOptions;

  constructor(opts: BenchmarkOptions) {
    super();
    this.opts = opts;
    this.queueName = `benchmark-${Date.now()}-${Math.random().toString(36).slice(2)}`;
  }

  async setup(): Promise<void> {
    const redisConfig = getRedisConfig(this.opts.db);
    this.redis = new Redis({
      host: redisConfig.host,
      port: redisConfig.port,
      maxRetriesPerRequest: null,
    });

    this.queue = new BullMQ.Queue(this.queueName, {
      connection: this.redis.duplicate(),
    });

    await this.queue.waitUntilReady();
  }

  async enqueueJobs(count: number): Promise<void> {
    console.log(`Enqueueing ${count} jobs...`);

    for (let i = 0; i < count; i++) {
      await this.queue.add('benchmark-job', {
        id: `job-${i}`,
        enqueuedAt: Date.now(),
      });
    }

    console.log(`✅ Enqueued ${count} jobs`);
  }

  async startWorkers(
    count: number,
    jobHandler: () => Promise<void>,
    multiProcess = false,
  ): Promise<void> {
    if (multiProcess) {
      return this.startWorkerProcesses(count);
    }

    console.log(`Starting ${count} BullMQ workers...`);

    for (let i = 0; i < count; i++) {
      const worker = new BullMQ.Worker(
        this.queueName,
        async (job) => {
          const startTime = performance.now();
          const enqueuedAt = job.data.enqueuedAt;

          await jobHandler();

          const completedAt = performance.now();
          const pickupMs = startTime - enqueuedAt;
          const processingMs = completedAt - startTime;

          this.completedJobs.push({
            id: job.data.id,
            enqueuedAt,
            startedAt: startTime,
            completedAt,
            pickupMs,
            processingMs,
            totalMs: pickupMs + processingMs,
          });
        },
        {
          connection: this.redis.duplicate(),
          concurrency: CONCURRENCY,
        },
      );

      this.workers.push(worker);
      await worker.waitUntilReady();
    }

    console.log(`✅ Started ${count} workers`);
  }

  private async startWorkerProcesses(count: number): Promise<void> {
    console.log(`Starting ${count} BullMQ worker processes...`);

    for (let i = 0; i < count; i++) {
      const workerProcess = spawn(
        'npx',
        [
          'jiti',
          'benchmark/worker-process.ts',
          'bullmq',
          this.queueName,
          cliOpts.jobType,
          i.toString(),
          cliOpts.db,
        ],
        {
          stdio: ['pipe', 'pipe', 'pipe'],
          cwd: process.cwd(),
        },
      );

      // Parse job completion messages from worker stdout
      workerProcess.stdout?.on('data', (data) => {
        const output = data.toString();
        const lines = output.split('\n');

        for (const line of lines) {
          if (line.startsWith('COMPLETED:')) {
            const parts = line.split(':');
            if (parts.length === 7) {
              // New format: COMPLETED:jobId:enqueuedAt:startTime:endTime:pickupMs:processingMs
              const [
                ,
                jobId,
                enqueuedAt,
                startTime,
                endTime,
                pickupMs,
                processingMs,
              ] = parts;

              this.completedJobs.push({
                id: jobId,
                enqueuedAt: parseFloat(enqueuedAt),
                startedAt: parseFloat(startTime),
                completedAt: parseFloat(endTime),
                pickupMs: parseFloat(pickupMs),
                processingMs: parseFloat(processingMs),
                totalMs: parseFloat(pickupMs) + parseFloat(processingMs),
              });
            } else {
              // Fallback to old format: COMPLETED:jobId:startTime:endTime
              const [, jobId, startTime, endTime] = parts;
              const start = parseFloat(startTime);
              const end = parseFloat(endTime);

              this.completedJobs.push({
                id: jobId,
                enqueuedAt: 0,
                startedAt: start,
                completedAt: end,
                pickupMs: 0,
                processingMs: end - start,
                totalMs: end - start,
              });
            }
          } else if (
            line.trim() &&
            !line.includes('Worker') &&
            !line.includes('ready')
          ) {
            console.log(`Worker ${i}:`, line.trim());
          }
        }
      });

      workerProcess.stderr?.on('data', (data) => {
        console.error(`Worker ${i} error:`, data.toString());
      });

      this.workerProcesses.push(workerProcess);
    }

    // Give workers time to start
    await new Promise((resolve) => setTimeout(resolve, 1000));
    console.log(`✅ Started ${count} worker processes`);
  }

  async waitForCompletion(timeoutMs = 60000): Promise<void> {
    const startTime = Date.now();

    while (Date.now() - startTime < timeoutMs) {
      const waiting = await this.queue.getWaiting();
      const active = await this.queue.getActive();
      const delayed = await this.queue.getDelayed();

      if (waiting.length === 0 && active.length === 0) {
        console.log('✅ All jobs completed');
        return;
      }

      if ((Date.now() - startTime) % 2000 < 1000) {
        console.log(
          `⏳ Time: ${new Date().toISOString()} Progress: ${this.completedJobs.length} completed, ${active.length} active, ${waiting.length} waiting, ${delayed.length} delayed`,
        );
      }

      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    console.log('⚠️ Timeout waiting for completion');
  }

  async cleanup(): Promise<void> {
    console.log('🧹 Cleaning up BullMQ...');

    // Close worker processes
    for (const workerProcess of this.workerProcesses) {
      try {
        workerProcess.kill('SIGTERM');
      } catch (err) {
        console.warn('Warning: Worker process kill error:', err);
      }
    }

    await new Promise((resolve) => setTimeout(resolve, 5000));

    // Close in-process workers
    await Promise.all(
      this.workers.map(async (w) => {
        try {
          await w.close();
        } catch (err) {
          console.warn('Warning: Worker close error:', err);
        }
      }),
    );

    // Close queue
    try {
      await this.queue.close();
    } catch (err) {
      console.warn('Warning: Queue close error:', err);
    }

    // Close Redis connections
    try {
      await this.redis.quit();
    } catch (err) {
      console.warn('Warning: Redis close error:', err);
    }

    // Force close any remaining connections
    this.redis.disconnect();
  }

  getCompletedJobs(): JobMetrics[] {
    return this.completedJobs;
  }

  getRedisInstance(): Redis {
    return this.redis;
  }
}

class GroupMQAdapter extends QueueAdapter {
  private redis!: Redis;
  private queue!: GroupMQ.Queue<{ id: string; enqueuedAt: number }>;
  private workers: any[] = [];
  private workerProcesses: any[] = [];
  private completedJobs: JobMetrics[] = [];
  private namespace: string;
  private opts: BenchmarkOptions;

  constructor(opts: BenchmarkOptions) {
    super();
    this.namespace = `benchmark-${Date.now()}-${Math.random().toString(36).slice(2)}}`;
    this.opts = opts;
  }

  async setup(): Promise<void> {
    const redisConfig = getRedisConfig(this.opts.db);
    this.redis = new Redis({
      host: redisConfig.host,
      port: redisConfig.port,
      maxRetriesPerRequest: null,
    });

    this.queue = new GroupMQ.Queue({
      redis: this.redis.duplicate(),
      namespace: this.namespace,
      keepCompleted: 1,
    });
  }

  async enqueueJobs(count: number): Promise<void> {
    console.log(`Enqueueing ${count} jobs...`);
    // Create groups with ~10 jobs each to enable parallelism
    // This matches typical real-world usage where jobs are grouped by user/tenant/session
    // but there are many concurrent groups
    const totalNumOfGroups = Math.max(1, Math.floor(this.opts.jobs / 10));
    for (let i = 0; i < count; i++) {
      await this.queue.add({
        groupId: `group-${i % totalNumOfGroups}`,
        data: {
          id: `job-${i}`,
          enqueuedAt: Date.now(),
        },
      });
    }

    console.log(`✅ Enqueued ${count} jobs`);
  }

  async startWorkers(
    count: number,
    jobHandler: () => Promise<void>,
    multiProcess = false,
  ): Promise<void> {
    if (multiProcess) {
      return this.startWorkerProcesses(count);
    }

    console.log(`Starting ${count} GroupMQ workers...`);

    for (let i = 0; i < count; i++) {
      const worker = new GroupMQ.Worker({
        concurrency: CONCURRENCY,
        queue: this.queue,
        name: `worker-${i}`,
        handler: async (job) => {
          const startTime = performance.now();
          const enqueuedAt = job.data.enqueuedAt;

          await jobHandler();

          const completedAt = performance.now();
          const pickupMs = startTime - enqueuedAt;
          const processingMs = completedAt - startTime;

          this.completedJobs.push({
            id: job.data.id,
            enqueuedAt,
            startedAt: startTime,
            completedAt,
            pickupMs,
            processingMs,
            totalMs: pickupMs + processingMs,
          });
        },
      });

      this.workers.push(worker);

      // Start worker in background
      worker.run().catch((err: any) => {
        console.error(`Worker ${i} error:`, err);
      });
    }

    // Give workers time to start
    await new Promise((resolve) => setTimeout(resolve, 500));
    console.log(`✅ Started ${count} workers`);
  }

  private async startWorkerProcesses(count: number): Promise<void> {
    console.log(`Starting ${count} GroupMQ worker processes...`);

    for (let i = 0; i < count; i++) {
      const workerProcess = spawn(
        'npx',
        [
          'jiti',
          'benchmark/worker-process.ts',
          'groupmq',
          this.namespace,
          cliOpts.jobType,
          i.toString(),
          cliOpts.db,
        ],
        {
          stdio: ['pipe', 'pipe', 'pipe'],
          cwd: process.cwd(),
        },
      );

      // Parse job completion messages from worker stdout
      workerProcess.stdout?.on('data', (data) => {
        const output = data.toString();
        const lines = output.split('\n');

        for (const line of lines) {
          if (line.startsWith('COMPLETED:')) {
            const parts = line.split(':');
            if (parts.length === 7) {
              // New format: COMPLETED:jobId:enqueuedAt:startTime:endTime:pickupMs:processingMs
              const [
                ,
                jobId,
                enqueuedAt,
                startTime,
                endTime,
                pickupMs,
                processingMs,
              ] = parts;

              this.completedJobs.push({
                id: jobId,
                enqueuedAt: parseFloat(enqueuedAt),
                startedAt: parseFloat(startTime),
                completedAt: parseFloat(endTime),
                pickupMs: parseFloat(pickupMs),
                processingMs: parseFloat(processingMs),
                totalMs: parseFloat(pickupMs) + parseFloat(processingMs),
              });
            } else {
              // Fallback to old format: COMPLETED:jobId:startTime:endTime
              const [, jobId, startTime, endTime] = parts;
              const start = parseFloat(startTime);
              const end = parseFloat(endTime);

              this.completedJobs.push({
                id: jobId,
                enqueuedAt: 0,
                startedAt: start,
                completedAt: end,
                pickupMs: 0,
                processingMs: end - start,
                totalMs: end - start,
              });
            }
          } else if (
            line.trim() &&
            !line.includes('Worker') &&
            !line.includes('ready')
          ) {
            console.log(`Worker ${i}:`, line.trim());
          }
        }
      });

      workerProcess.stderr?.on('data', (data) => {
        console.error(`Worker ${i} error:`, data.toString());
      });

      this.workerProcesses.push(workerProcess);
    }

    // Give workers time to start
    await new Promise((resolve) => setTimeout(resolve, 1000));
    console.log(`✅ Started ${count} worker processes`);
  }

  async waitForCompletion(timeoutMs = 60000): Promise<void> {
    const startTime = Date.now();
    let lastStatsCheck = 0;

    while (Date.now() - startTime < timeoutMs) {
      // Only check expensive stats every 500ms instead of every 100ms
      const now = Date.now();
      if (now - lastStatsCheck > 500) {
        const stats = await this.queue.getJobCounts();
        // const groups = await this.queue.getUniqueGroupsCount();

        if (stats.active === 0 && stats.waiting === 0 && stats.delayed === 0) {
          console.log('✅ All jobs completed');
          return;
        }

        if ((now - startTime) % 2000 < 1000) {
          console.log(
            `⏳ Time: ${new Date().toISOString()} Progress: ${this.completedJobs.length} completed, ${stats.active} active, ${stats.waiting} waiting, ${stats.delayed} delayed`,
          );
        }
        lastStatsCheck = now;
      }

      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    console.log('⚠️ Timeout waiting for completion');
  }

  async cleanup(): Promise<void> {
    console.log('🧹 Cleaning up GroupMQ...');

    // Close worker processes
    for (const workerProcess of this.workerProcesses) {
      try {
        workerProcess.kill('SIGTERM');
      } catch (err) {
        console.warn('Warning: Worker process kill error:', err);
      }
    }

    // Close in-process workers
    await Promise.all(
      this.workers.map(async (w, i) => {
        try {
          await w.close();
        } catch (err) {
          console.warn(`Warning: Worker ${i} close error:`, err);
        }
      }),
    );

    // Close Redis connection
    try {
      await this.redis.quit();
    } catch (err) {
      console.warn('Warning: Redis close error:', err);
    }

    // Force close any remaining connections
    this.redis.disconnect();
  }

  getCompletedJobs(): JobMetrics[] {
    return this.completedJobs;
  }

  getRedisInstance(): Redis {
    return this.redis;
  }
}

// Main benchmark function
async function runBenchmark(
  timestamp: number,
  opts: BenchmarkOptions,
): Promise<BenchmarkResult> {
  const adapter =
    opts.mq === 'bullmq' ? new BullMQAdapter(opts) : new GroupMQAdapter(opts);
  const monitor = new SystemMonitor();
  const jobHandler =
    opts.jobType === 'cpu'
      ? cpuIntensiveJob
      : opts.jobType === 'io'
        ? ioIntensiveJob
        : emptyJob;

  try {
    // Setup
    await adapter.setup();

    // Reset Redis stats before benchmark
    const redisInstance = adapter.getRedisInstance();
    await resetRedisStats(redisInstance);

    monitor.start();

    const benchmarkStart = performance.now();

    // Start workers
    await adapter.startWorkers(opts.workers, jobHandler, opts.multiProcess);

    // Enqueue jobs
    await adapter.enqueueJobs(opts.jobs);

    // Wait for completion
    await adapter.waitForCompletion(60_000 * 15);

    const benchmarkEnd = performance.now();
    const durationMs = benchmarkEnd - benchmarkStart;

    monitor.stop();

    // Collect Redis stats after benchmark
    const redisStats = await collectRedisStats(redisInstance);

    // Collect results
    const completedJobs = adapter.getCompletedJobs();
    const systemStats = monitor.getStats();

    console.log(
      `\n📈 Completed ${completedJobs.length}/${opts.jobs} jobs in ${durationMs.toFixed(0)}ms`,
    );

    if (completedJobs.length === 0) {
      throw new Error('No jobs were completed!');
    }

    const pickupTimes = completedJobs.map((j) => j.pickupMs);
    const processingTimes = completedJobs.map((j) => j.processingMs);
    const totalTimes = completedJobs.map((j) => j.totalMs);

    const result: BenchmarkResult = {
      timestamp,
      queueType: opts.mq,
      jobType: opts.jobType,
      totalJobs: opts.jobs,
      workersCount: opts.workers,
      completedJobs: completedJobs.length,
      durationMs: Math.round(durationMs),
      throughputJobsPerSec: parseFloat(
        (completedJobs.length / (durationMs / 1000)).toFixed(2),
      ),
      avgPickupMs: parseFloat(average(pickupTimes).toFixed(2)),
      avgProcessingMs: parseFloat(average(processingTimes).toFixed(2)),
      avgTotalMs: parseFloat(average(totalTimes).toFixed(2)),
      p95PickupMs: parseFloat(percentile(pickupTimes, 0.95).toFixed(2)),
      p95ProcessingMs: parseFloat(percentile(processingTimes, 0.95).toFixed(2)),
      p95TotalMs: parseFloat(percentile(totalTimes, 0.95).toFixed(2)),
      peakCpuPercent: parseFloat(systemStats.peakCpu.toFixed(1)),
      peakMemoryMB: parseFloat(systemStats.peakMemory.toFixed(1)),
      avgCpuPercent: parseFloat(systemStats.avgCpu.toFixed(1)),
      avgMemoryMB: parseFloat(systemStats.avgMemory.toFixed(1)),
      redisStats,
      settings: {
        mq: opts.mq,
        jobs: opts.jobs,
        workers: opts.workers,
        jobType: opts.jobType,
        multiProcess: Boolean(opts.multiProcess),
      },
    };

    await adapter.cleanup();

    return result;
  } catch (error) {
    monitor.stop();
    await adapter.cleanup().catch(() => {});
    throw error;
  }
}

// Output results
function displayResults(result: BenchmarkResult): void {
  console.log(`\n${'='.repeat(60)}`);
  console.log(`📊 ${result.queueType.toUpperCase()} BENCHMARK RESULTS`);
  console.log('='.repeat(60));

  console.log('\n🎯 COMPLETION:');
  console.log(
    `  Jobs Completed: ${result.completedJobs}/${result.totalJobs} (${((result.completedJobs / result.totalJobs) * 100).toFixed(1)}%)`,
  );
  console.log(`  Duration: ${result.durationMs}ms`);
  console.log(`  Throughput: ${result.throughputJobsPerSec} jobs/sec`);

  console.log('\n⚡ LATENCY (ms):');
  console.log(
    `  Pickup    - Avg: ${result.avgPickupMs.toString().padStart(6)}  P95: ${result.p95PickupMs.toString().padStart(6)}`,
  );
  console.log(
    `  Processing- Avg: ${result.avgProcessingMs.toString().padStart(6)}  P95: ${result.p95ProcessingMs.toString().padStart(6)}`,
  );
  console.log(
    `  Total     - Avg: ${result.avgTotalMs.toString().padStart(6)}  P95: ${result.p95TotalMs.toString().padStart(6)}`,
  );

  console.log('\n💻 SYSTEM USAGE:');
  console.log(
    `  CPU    - Peak: ${result.peakCpuPercent}%  Avg: ${result.avgCpuPercent}%`,
  );
  console.log(
    `  Memory - Peak: ${result.peakMemoryMB}MB  Avg: ${result.avgMemoryMB}MB`,
  );

  if (result.redisStats) {
    console.log('\n🔴 REDIS STATS:');
    console.log(`  Total Commands: ${result.redisStats.summary.totalCalls}`);
    console.log(
      `  Avg μs/call: ${result.redisStats.summary.avgUsecPerCall.toFixed(2)}`,
    );
    console.log(`  Command Types: ${result.redisStats.summary.commandCount}`);
    console.log('\n  Top Commands:');
    for (const cmd of result.redisStats.summary.topCommands.slice(0, 5)) {
      console.log(
        `    ${cmd.command?.padEnd(15)} - ${cmd.calls.toString().padStart(6)} calls (${cmd.usec_per_call.toFixed(2)} μs/call)`,
      );
    }
  }

  console.log(`\n${'='.repeat(60)}`);
}

//

function saveResults(opts: BenchmarkOptions, result: BenchmarkResult): void {
  let outputPath: string;
  if (opts.output) {
    outputPath = path.resolve(opts.output);
  } else {
    const filename = `${result.queueType}.json`;
    outputPath = path.resolve(process.cwd(), 'benchmark', 'results', filename);
  }

  const dir = path.dirname(outputPath);
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }

  let existing: any[] = [];
  if (fs.existsSync(outputPath)) {
    try {
      const content = fs.readFileSync(outputPath, 'utf8');
      const parsed = JSON.parse(content);
      if (Array.isArray(parsed)) {
        existing = parsed;
      } else if (parsed && typeof parsed === 'object') {
        existing = [parsed];
      }
    } catch (_err) {
      // If corrupt or unreadable, start fresh
      existing = [];
    }
  }

  existing.push(result);
  fs.writeFileSync(outputPath, JSON.stringify(existing, null, 2));
  console.log(`💾 Results appended to: ${outputPath}`);

  // Save Redis stats to separate file for easier analysis
  if (result.redisStats) {
    const redisStatsPath = outputPath.replace('.json', '_redis.json');
    fs.writeFileSync(
      redisStatsPath,
      JSON.stringify(result.redisStats, null, 2),
    );
    console.log(`💾 Redis stats saved to: ${redisStatsPath}`);
  }
}

// Run the benchmark
(async () => {
  try {
    const timestamp = Date.now();
    if (cliOpts.mq === 'both') {
      const results1 = await runBenchmark(timestamp, {
        ...cliOpts,
        mq: 'groupmq',
      });
      const results2 = await runBenchmark(timestamp, {
        ...cliOpts,
        mq: 'bullmq',
      });
      displayResults(results1);
      displayResults(results2);
      saveResults(cliOpts, results1);
      saveResults(cliOpts, results2);
    } else {
      const result = await runBenchmark(timestamp, cliOpts);
      displayResults(result);
      saveResults(cliOpts, result);
    }
    console.log('\n✅ Benchmark completed successfully');
    process.exit(0);
  } catch (error) {
    console.error('\n❌ Benchmark failed:', error);
    process.exit(1);
  }
})();
