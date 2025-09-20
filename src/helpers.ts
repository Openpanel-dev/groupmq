import type { Queue } from './queue';
import type { Worker } from './worker';

/**
 * Wait for a queue to become empty
 * @param queue The queue to monitor
 * @param timeoutMs Maximum time to wait (default: 60 seconds)
 * @returns Promise that resolves when queue is empty or timeout is reached
 */
export async function waitForQueueToEmpty(
  queue: Queue,
  timeoutMs = 60_000,
): Promise<boolean> {
  return queue.waitForEmpty(timeoutMs);
}

/**
 * Get status of all workers
 */
export function getWorkersStatus<T = any>(
  workers: Worker<T>[],
): {
  total: number;
  processing: number;
  idle: number;
  workers: Array<{
    index: number;
    isProcessing: boolean;
    currentJob?: {
      jobId: string;
      groupId: string;
      processingTimeMs: number;
    };
  }>;
} {
  const workersStatus = workers.map((worker, index) => {
    const currentJob = worker.getCurrentJob();
    return {
      index,
      isProcessing: worker.isProcessing(),
      currentJob: currentJob
        ? {
            jobId: currentJob.job.id,
            groupId: currentJob.job.groupId,
            processingTimeMs: currentJob.processingTimeMs,
          }
        : undefined,
    };
  });

  const processing = workersStatus.filter((w) => w.isProcessing).length;
  const idle = workersStatus.length - processing;

  return {
    total: workers.length,
    processing,
    idle,
    workers: workersStatus,
  };
}
