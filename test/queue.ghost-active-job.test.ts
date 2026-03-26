import Redis from 'ioredis';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { Queue } from '../src/queue';
import { Worker } from '../src/worker';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';

interface GhostReport {
  totalGhosts: number;
  totalProcessingOrphans: number;
  groups: {
    groupId: string;
    activeJobs: string[];
    details: {
      jobId: string;
      status: string | null;
      inProcessingSet: boolean;
      deadlineDaysAway: number | null;
    }[];
  }[];
}

async function getGhostReport(redis: Redis, ns: string): Promise<GhostReport> {
  const groups = await redis.smembers(`${ns}:groups`);
  const report: GhostReport = {
    totalGhosts: 0,
    totalProcessingOrphans: 0,
    groups: [],
  };

  for (const groupId of groups) {
    const activeKey = `${ns}:g:${groupId}:active`;
    const activeJobs = await redis.lrange(activeKey, 0, -1);
    if (activeJobs.length === 0) continue;

    const groupReport: GhostReport['groups'][number] = {
      groupId,
      activeJobs,
      details: [],
    };

    for (const jobId of activeJobs) {
      const status = await redis.hget(`${ns}:job:${jobId}`, 'status');
      const procScore = await redis.zscore(`${ns}:processing`, jobId);
      const inProcessingSet = procScore !== null;
      let deadlineDaysAway: number | null = null;

      if (procScore) {
        deadlineDaysAway = (Number(procScore) - Date.now()) / 1000 / 60 / 60 / 24;
      }

      groupReport.details.push({ jobId, status, inProcessingSet, deadlineDaysAway });
      report.totalGhosts++;
    }

    report.groups.push(groupReport);
  }

  const allProcessing = await redis.zrange(`${ns}:processing`, 0, -1);
  for (const jobId of allProcessing) {
    const status = await redis.hget(`${ns}:job:${jobId}`, 'status');
    if (status !== 'processing' && status !== 'completing') {
      report.totalProcessingOrphans++;
    }
  }

  return report;
}

function printGhostReport(label: string, report: GhostReport) {
  console.log(`\n--- ${label} ---`);
  console.log('╔══════════════════════════════════════════════════════╗');
  console.log('║            GHOST ACTIVE JOB REPORT                  ║');
  console.log('╠══════════════════════════════════════════════════════╣');
  console.log(`║  Total ghost active entries:  ${String(report.totalGhosts).padStart(4)}                  ║`);
  console.log(`║  Processing set orphans:      ${String(report.totalProcessingOrphans).padStart(4)}                  ║`);
  console.log('╠══════════════════════════════════════════════════════╣');

  for (const group of report.groups) {
    console.log(`║  Group: ${group.groupId.padEnd(43)} ║`);
    console.log(`║    Active list entries: ${group.activeJobs.length}                          ║`);
    for (const d of group.details) {
      const deadline = d.deadlineDaysAway !== null
        ? `${d.deadlineDaysAway.toFixed(1)}d away`
        : 'N/A';
      console.log(`║    - ${d.jobId.substring(0, 8)}...  status=${(d.status ?? 'null').padEnd(12)} proc=${d.inProcessingSet ? 'yes' : 'no '}  deadline=${deadline}`);
    }
  }

  if (report.groups.length === 0) {
    console.log('║  (no ghost entries found)                            ║');
  }

  console.log('╚══════════════════════════════════════════════════════╝\n');
}

describe('Ghost Active Job after Ctrl+C', () => {
  let redis: Redis;
  let namespace: string;

  beforeEach(async () => {
    namespace = `test-ghost-${Date.now()}-${Math.random().toString(36).slice(2)}`;
    redis = new Redis(REDIS_URL, { maxRetriesPerRequest: null });

    const keys = await redis.keys(`groupmq:${namespace}:*`);
    if (keys.length > 0) await redis.del(...keys);
  });

  afterEach(async () => {
    const keys = await redis.keys(`groupmq:${namespace}:*`);
    if (keys.length > 0) await redis.del(...keys);
    await redis.quit();
  });

  function simulateCtrlC(worker: Worker) {
    // @ts-ignore
    if (worker['blockingClient']) {
      worker['blockingClient'].disconnect();
      worker['blockingClient'] = null;
    }
    // @ts-ignore
    worker['stopping'] = true;
    // @ts-ignore
    worker['closed'] = true;
    // @ts-ignore
    if (worker['cleanupTimer']) clearInterval(worker['cleanupTimer']);
    // @ts-ignore
    if (worker['schedulerTimer']) clearInterval(worker['schedulerTimer']);
    // @ts-ignore
    if (worker['stalledCheckTimer']) clearInterval(worker['stalledCheckTimer']);
    // @ts-ignore
    worker['jobsInProgress'].clear();
  }

  async function setupGhost(redis: Redis, namespace: string) {
    const ns = `groupmq:${namespace}`;
    const queue = new Queue({
      redis: redis.duplicate(),
      namespace,
      jobTimeoutMs: 7 * 24 * 60 * 60 * 1000,
      orderingDelayMs: 1000,
    });

    const job1 = await queue.add({
      groupId: 'timeout',
      data: { msg: 'job-1' },
      orderMs: Date.now() + 100,
    });

    await new Promise((r) => setTimeout(r, 1500));
    await queue.runSchedulerOnce();

    await queue.add({
      groupId: 'timeout',
      data: { msg: 'job-2' },
      orderMs: Date.now() + 200,
    });

    await new Promise((r) => setTimeout(r, 1500));
    await queue.runSchedulerOnce();

    let pickedUpResolve: () => void;
    const pickedUp = new Promise<void>((r) => { pickedUpResolve = r; });

    const workerRef = new Worker({
      queue,
      handler: async (_job) => {
        pickedUpResolve();
        await new Promise(() => {});
      },
      concurrency: 1,
    });

    await pickedUp;
    simulateCtrlC(workerRef);

    return { queue, ns, job1 };
  }

  it('should detect ghost active entries after ungraceful shutdown', async () => {
    const { queue, ns, job1 } = await setupGhost(redis, namespace);

    const report = await getGhostReport(redis, ns);
    printGhostReport('AFTER Ctrl+C', report);

    // Prove the ghost exists
    expect(report.totalGhosts).toBe(1);
    expect(report.groups[0].groupId).toBe('timeout');
    expect(report.groups[0].details[0].jobId).toBe(job1.id);
    expect(report.groups[0].details[0].status).toBe('processing');
    expect(report.groups[0].details[0].inProcessingSet).toBe(true);
    expect(report.groups[0].details[0].deadlineDaysAway).toBeGreaterThan(6);

    await queue.close();
  }, 10000);

  it('should auto-recover ghosts when a new worker starts (no manual call needed)', async () => {
    const { queue, ns } = await setupGhost(redis, namespace);

    const reportBefore = await getGhostReport(redis, ns);
    printGhostReport('BEFORE new worker starts', reportBefore);
    expect(reportBefore.totalGhosts).toBe(1);

    // "Restart" — just create a new queue + worker, like a normal app startup.
    // The worker's run() auto-calls recoverActiveJobs() internally.
    const queue2 = new Queue({
      redis: redis.duplicate(),
      namespace,
      jobTimeoutMs: 7 * 24 * 60 * 60 * 1000,
      orderingDelayMs: 1000,
    });

    const processed: string[] = [];
    const worker2 = new Worker({
      queue: queue2,
      handler: async (job) => {
        processed.push(job.id);
      },
      concurrency: 1,
      blockingTimeoutSec: 1,
    });

    // Give the worker time to auto-recover and process jobs
    await new Promise((r) => setTimeout(r, 3000));
    await worker2.close(1000);

    const reportAfter = await getGhostReport(redis, ns);
    printGhostReport('AFTER new worker started', reportAfter);

    // Ghosts are gone, both jobs processed — all automatic
    expect(reportAfter.totalGhosts).toBe(0);
    expect(processed.length).toBe(2);
    console.log(`Auto-recovered and processed ${processed.length} jobs: ${processed.join(', ')}`);

    await queue.close();
    await queue2.close();
  }, 15000);
});
