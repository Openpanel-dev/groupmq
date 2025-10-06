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
  .option('--job-type <cpu|io>', 'Type of job workload', 'cpu')
  .option(
    '--multi-process',
    'Use separate processes for workers (better CPU parallelization)',
    false,
  )
  .option('--output <file>', 'Output file for results', '')
  .parse();

type BenchmarkOptions = {
  mq: 'bullmq' | 'groupmq' | 'both';
  jobs: number;
  workers: number;
  jobType: 'cpu' | 'io';
  multiProcess: boolean;
  output: string;
};
const cliOpts = program.opts() as BenchmarkOptions;

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
    this.queueName = `benchmark-${Date.now()}-${Math.random().toString(36).slice(2)}`;
    this.opts = opts;
  }

  async setup(): Promise<void> {
    this.redis = new Redis({
      host: 'localhost',
      port: 6379,
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
          `⏳ Progress: ${this.completedJobs.length} completed, ${active.length} active, ${waiting.length} waiting, ${delayed.length} delayed`,
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
    this.namespace = `benchmark-${Date.now()}-${Math.random().toString(36).slice(2)}`;
    this.opts = opts;
  }

  async setup(): Promise<void> {
    this.redis = new Redis({
      host: 'localhost',
      port: 6379,
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

    for (let i = 0; i < count; i++) {
      await this.queue.add({
        groupId: `group-${i % this.opts.workers}`,
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
        const groups = await this.queue.getUniqueGroupsCount();

        if (stats.active === 0 && stats.waiting === 0 && stats.delayed === 0) {
          console.log('✅ All jobs completed');
          return;
        }

        if ((now - startTime) % 2000 < 1000) {
          console.log(
            `⏳ Progress: ${this.completedJobs.length} completed, ${stats.active} active, ${stats.waiting} waiting, ${stats.delayed} delayed, ${groups}`,
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
}

// Main benchmark function
async function runBenchmark(
  timestamp: number,
  opts: BenchmarkOptions,
): Promise<BenchmarkResult> {
  const adapter =
    opts.mq === 'bullmq' ? new BullMQAdapter(opts) : new GroupMQAdapter(opts);
  const monitor = new SystemMonitor();
  const jobHandler = opts.jobType === 'cpu' ? cpuIntensiveJob : ioIntensiveJob;

  try {
    // Setup
    await adapter.setup();
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
