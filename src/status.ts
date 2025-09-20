export declare const STATUS: {
  readonly latest: 'latest';
  readonly active: 'active';
  readonly waiting: 'waiting';
  readonly waitingChildren: 'waiting-children';
  readonly prioritized: 'prioritized';
  readonly completed: 'completed';
  readonly failed: 'failed';
  readonly delayed: 'delayed';
  readonly paused: 'paused';
};
export type Status = (typeof STATUS)[keyof typeof STATUS];
