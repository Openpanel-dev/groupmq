import { BaseAdapter } from '@bull-board/api/dist/queueAdapters/base.js';
import type {
  JobCounts as BullBoardJobCounts,
  JobStatus as BullBoardJobStatus,
  QueueJob as BullBoardQueueJob,
  Status as BullBoardStatus,
  QueueType,
} from '@bull-board/api/typings/app';
import type { Job } from '../job';
import type { Queue } from '../queue';

export type GroupMQBullBoardAdapterOptions = {
  readOnlyMode?: boolean;
  prefix?: string;
  delimiter?: string;
  description?: string;
  displayName?: string;
};

export class BullBoardGroupMQAdapter<T = any> extends BaseAdapter {
  private queue: Queue<T>;
  private options: GroupMQBullBoardAdapterOptions;

  constructor(queue: Queue<T>, options: GroupMQBullBoardAdapterOptions = {}) {
    const libName = queue.namespace;
    super(libName as QueueType, options);
    this.queue = queue;
    this.options = options;
  }

  // ------------ Metadata ------------
  public getDescription(): string {
    return this.options.description || '';
  }

  public getDisplayName(): string {
    return this.options.displayName || '';
  }

  public getName(): string {
    const prefix = this.options.prefix || '';
    const delimiter = this.options.delimiter || '';
    return `${prefix}${delimiter}${this.queue.rawNamespace}`.replace(
      /(^[\s:]+)|([\s:]+$)/g,
      '',
    );
  }

  public async getRedisInfo(): Promise<string> {
    return this.queue.redis.info();
  }

  // ------------ Getters ------------
  public async getJob(
    id: string,
  ): Promise<BullBoardQueueJob | undefined | null> {
    return (await this.queue.getJob(id)) as unknown as BullBoardQueueJob;
  }

  public async getJobs(
    jobStatuses: BullBoardJobStatus[],
    start?: number,
    end?: number,
  ): Promise<BullBoardQueueJob[]> {
    const jobs = await this.queue.getJobsByStatus(
      jobStatuses as any,
      start,
      end,
    );
    // QueueJob's update and updateData methods mismatch Record<string, any> vs T
    return jobs as Omit<Job<T>, 'update' | 'updateData'>[];
  }

  public async getJobCounts(): Promise<BullBoardJobCounts> {
    const base = await this.queue.getJobCounts();
    const counts: BullBoardJobCounts = {
      latest: 0,
      active: base.active,
      waiting: base.waiting,
      'waiting-children': base['waiting-children'],
      prioritized: base.prioritized,
      completed: base.completed,
      failed: base.failed,
      delayed: base.delayed,
      paused: base.paused,
    } as BullBoardJobCounts;
    return counts;
  }

  public async getJobLogs(_id: string): Promise<string[]> {
    return [];
  }

  public getStatuses(): BullBoardStatus[] {
    return [
      'latest',
      'active',
      'waiting',
      'waiting-children',
      'prioritized',
      'completed',
      'failed',
      'delayed',
      'paused',
    ];
  }

  public getJobStatuses(): BullBoardJobStatus[] {
    return [
      'active',
      'waiting',
      'waiting-children',
      'prioritized',
      'completed',
      'failed',
      'delayed',
      'paused',
    ];
  }

  // ------------ Mutations (read-only stubs) ------------
  private assertWritable(): void {
    if (this.options.readOnlyMode) {
      throw new Error(
        'This adapter is in read-only mode. Mutations are disabled.',
      );
    }
  }

  public async clean(jobStatus: any, graceTimeMs: number): Promise<void> {
    this.assertWritable();
    // Align with BullMQ adapter: delegate to queue.clean
    if (
      jobStatus !== 'completed' &&
      jobStatus !== 'failed' &&
      jobStatus !== 'delayed'
    )
      return;
    await this.queue.clean(graceTimeMs, Number.MAX_SAFE_INTEGER, jobStatus);
  }

  public async addJob(
    _name: string,
    data: any,
    options: any,
  ): Promise<BullBoardQueueJob> {
    this.assertWritable();
    const job = await this.queue.add({
      groupId: options.groupId ?? Math.random().toString(36).substring(2, 15),
      data: data,
      ...options,
    });
    // QueueJob's update and updateData methods mismatch Record<string, any> vs T
    return job as Omit<Job<T>, 'update' | 'updateData'>;
  }

  public async isPaused(): Promise<boolean> {
    return this.queue.isPaused();
  }

  public async pause(): Promise<void> {
    this.assertWritable();
    await this.queue.pause();
  }

  public async resume(): Promise<void> {
    this.assertWritable();
    await this.queue.resume();
  }

  public async empty(): Promise<void> {
    this.assertWritable();
    throw new Error('Not implemented');
  }

  public async promoteAll(): Promise<void> {
    this.assertWritable();
    throw new Error('Not implemented');
  }
}
