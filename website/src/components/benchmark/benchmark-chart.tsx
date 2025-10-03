'use client';

import { useEffect, useMemo, useState } from 'react';
import {
  Bar,
  BarChart,
  CartesianGrid,
  Line,
  LineChart,
  XAxis,
  YAxis,
} from 'recharts';
import { withBase } from '@/lib/withBase';
import type { ChartConfig } from '../ui/chart';
import {
  ChartContainer,
  ChartLegend,
  ChartLegendContent,
  ChartTooltip,
  ChartTooltipContent,
} from '../ui/chart';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '../ui/select';

interface BenchmarkResult {
  timestamp: number;
  queueType: 'groupmq' | 'bullmq';
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
  settings: {
    mq: string;
    jobs: number;
    workers: number;
    jobType: string;
    multiProcess: boolean;
  };
}

interface FilterConfig {
  jobs: number | 'all';
  workers: number | 'all';
  jobType: string | 'all';
  multiProcess: boolean | 'all';
}

type MetricKey =
  | 'throughputJobsPerSec'
  | 'avgPickupMs'
  | 'avgProcessingMs'
  | 'avgTotalMs'
  | 'p95TotalMs'
  | 'avgMemoryMB'
  | 'avgCpuPercent';

const METRIC_LABELS: Record<MetricKey, { label: string; unit: string }> = {
  throughputJobsPerSec: { label: 'Throughput', unit: 'jobs/sec' },
  avgPickupMs: { label: 'Avg Pickup Time', unit: 'ms' },
  avgProcessingMs: { label: 'Avg Processing Time', unit: 'ms' },
  avgTotalMs: { label: 'Avg Total Time', unit: 'ms' },
  p95TotalMs: { label: 'P95 Total Time', unit: 'ms' },
  avgMemoryMB: { label: 'Avg Memory', unit: 'MB' },
  avgCpuPercent: { label: 'Avg CPU', unit: '%' },
};

const chartConfig = {
  groupmq: {
    label: 'GroupMQ',
    color: 'hsl(258, 90%, 66%)', // #8b5cf6
  },
  bullmq: {
    label: 'BullMQ',
    color: 'hsl(27, 96%, 54%)', // #f97316
  },
} satisfies ChartConfig;

export function BenchmarkChart() {
  const [groupmqData, setGroupmqData] = useState<BenchmarkResult[]>([]);
  const [bullmqData, setBullmqData] = useState<BenchmarkResult[]>([]);
  const [loading, setLoading] = useState(true);
  const [selectedMetric, setSelectedMetric] = useState<MetricKey>(
    'throughputJobsPerSec',
  );
  const [filter, setFilter] = useState<FilterConfig>({
    jobs: 5000,
    workers: 4,
    jobType: 'cpu',
    multiProcess: true,
  });

  // Load benchmark data
  useEffect(() => {
    Promise.all([
      fetch(`${withBase('groupmq.json')}`).then((r) => r.json()),
      fetch(`${withBase('bullmq.json')}`).then((r) => r.json()),
    ])
      .then(([gmq, bmq]) => {
        setGroupmqData(gmq);
        setBullmqData(bmq);
        setLoading(false);
      })
      .catch((err) => {
        console.error('Failed to load benchmark data:', err);
        setLoading(false);
      });
  }, []);

  // Get unique filter options
  const filterOptions = useMemo(() => {
    const allData = [...groupmqData, ...bullmqData];
    return {
      jobs: Array.from(new Set(allData.map((d) => d.settings.jobs))).sort(
        (a, b) => a - b,
      ),
      workers: Array.from(new Set(allData.map((d) => d.settings.workers))).sort(
        (a, b) => a - b,
      ),
      jobTypes: Array.from(new Set(allData.map((d) => d.settings.jobType))),
    };
  }, [groupmqData, bullmqData]);

  // Filter and format data for the chart
  const chartData = useMemo(() => {
    const filterFn = (result: BenchmarkResult) => {
      if (filter.jobs !== 'all' && result.settings.jobs !== filter.jobs)
        return false;
      if (
        filter.workers !== 'all' &&
        result.settings.workers !== filter.workers
      )
        return false;
      if (
        filter.jobType !== 'all' &&
        result.settings.jobType !== filter.jobType
      )
        return false;
      if (
        filter.multiProcess !== 'all' &&
        result.settings.multiProcess !== filter.multiProcess
      )
        return false;
      return true;
    };

    const filteredGroupmq = groupmqData
      .filter(filterFn)
      .sort((a, b) => a.timestamp - b.timestamp);
    const filteredBullmq = bullmqData
      .filter(filterFn)
      .sort((a, b) => a.timestamp - b.timestamp);

    // Merge datasets by timestamp
    const timestamps = [
      ...new Set([
        ...filteredGroupmq.map((d) => d.timestamp),
        ...filteredBullmq.map((d) => d.timestamp),
      ]),
    ].sort();

    return timestamps.map((ts) => {
      const gmq = filteredGroupmq.find((d) => d.timestamp === ts);
      const bmq = filteredBullmq.find((d) => d.timestamp === ts);
      const date = new Date(ts);

      return {
        timestamp: ts,
        date: date.toLocaleDateString('en-US', {
          month: 'short',
          day: 'numeric',
        }),
        groupmq: gmq?.[selectedMetric] ?? null,
        bullmq: bmq?.[selectedMetric] ?? null,
      };
    });
  }, [groupmqData, bullmqData, filter, selectedMetric]);

  // Calculate statistics
  const stats = useMemo(() => {
    const gmqValues = chartData
      .map((d) => d.groupmq)
      .filter((v): v is number => v !== null);
    const bmqValues = chartData
      .map((d) => d.bullmq)
      .filter((v): v is number => v !== null);

    const avg = (arr: number[]) =>
      arr.length > 0 ? arr.reduce((a, b) => a + b, 0) / arr.length : 0;
    const max = (arr: number[]) => (arr.length > 0 ? Math.max(...arr) : 0);
    const min = (arr: number[]) => (arr.length > 0 ? Math.min(...arr) : 0);

    return {
      groupmq: {
        avg: avg(gmqValues),
        max: max(gmqValues),
        min: min(gmqValues),
        count: gmqValues.length,
      },
      bullmq: {
        avg: avg(bmqValues),
        max: max(bmqValues),
        min: min(bmqValues),
        count: bmqValues.length,
      },
    };
  }, [chartData]);

  if (loading) {
    return (
      <div className="flex items-center justify-center p-20">
        <div className="text-center">
          <div className="inline-block h-10 w-10 animate-spin rounded-full border-4 border-solid border-primary/30 border-t-primary motion-reduce:animate-[spin_1.5s_linear_infinite]" />
          <p className="mt-6 text-sm font-medium text-muted-foreground">
            Loading benchmark data...
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-12">
      {/* Filters */}
      <div>
        <div className="mb-6">
          <h2 className="text-2xl font-bold">Filters</h2>
          <p className="text-muted-foreground mt-2">
            Customize your benchmark comparison
          </p>
        </div>
        <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-4">
          <div className="space-y-2">
            <label className="text-sm font-medium leading-none peer-disabled:cursor-not-allowed peer-disabled:opacity-70">
              Jobs
            </label>
            <Select
              value={String(filter.jobs)}
              onValueChange={(v) =>
                setFilter({
                  ...filter,
                  jobs: v === 'all' ? 'all' : Number(v),
                })
              }
            >
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All</SelectItem>
                {filterOptions.jobs.map((j) => (
                  <SelectItem key={j} value={String(j)}>
                    {j.toLocaleString()} jobs
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          <div className="space-y-2">
            <label className="text-sm font-medium leading-none peer-disabled:cursor-not-allowed peer-disabled:opacity-70">
              Workers
            </label>
            <Select
              value={String(filter.workers)}
              onValueChange={(v) =>
                setFilter({
                  ...filter,
                  workers: v === 'all' ? 'all' : Number(v),
                })
              }
            >
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All</SelectItem>
                {filterOptions.workers.map((w) => (
                  <SelectItem key={w} value={String(w)}>
                    {w} workers
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          <div className="space-y-2">
            <label className="text-sm font-medium leading-none peer-disabled:cursor-not-allowed peer-disabled:opacity-70">
              Job Type
            </label>
            <Select
              value={filter.jobType}
              onValueChange={(v) => setFilter({ ...filter, jobType: v })}
            >
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All</SelectItem>
                {filterOptions.jobTypes.map((jt) => (
                  <SelectItem key={jt} value={jt}>
                    {jt.toUpperCase()}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          <div className="space-y-2">
            <label className="text-sm font-medium leading-none peer-disabled:cursor-not-allowed peer-disabled:opacity-70">
              Multi-Process
            </label>
            <Select
              value={String(filter.multiProcess)}
              onValueChange={(v) =>
                setFilter({
                  ...filter,
                  multiProcess: v === 'all' ? 'all' : v === 'true',
                })
              }
            >
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All</SelectItem>
                <SelectItem value="true">Yes</SelectItem>
                <SelectItem value="false">No</SelectItem>
              </SelectContent>
            </Select>
          </div>
        </div>
      </div>

      {/* Metric Selector */}
      <div>
        <div className="mb-6">
          <h2 className="text-2xl font-bold">Metric</h2>
          <p className="text-muted-foreground mt-2">
            Select a performance metric to visualize
          </p>
        </div>
        <div className="flex flex-wrap gap-2">
          {(Object.keys(METRIC_LABELS) as MetricKey[]).map((metric) => (
            <button
              key={metric}
              type="button"
              onClick={() => setSelectedMetric(metric)}
              className={` px-4 py-2 text-sm font-medium transition-all ${
                selectedMetric === metric
                  ? 'bg-primary text-primary-foreground shadow-md'
                  : 'bg-muted text-muted-foreground hover:bg-muted/80 hover:text-foreground'
              }`}
            >
              {METRIC_LABELS[metric].label}
            </button>
          ))}
        </div>
      </div>

      {/* Performance Note */}
      {(selectedMetric === 'avgMemoryMB' ||
        selectedMetric === 'avgCpuPercent') && (
        <div className=" border border-amber-500/50 bg-amber-50/50 dark:bg-amber-950/20 p-6">
          <div className="flex items-start gap-3">
            <svg
              className="mt-0.5 h-5 w-5 flex-shrink-0 text-amber-600 dark:text-amber-500"
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2}
                d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"
              />
            </svg>
            <div className="flex-1">
              <p className="text-sm font-medium text-amber-900 dark:text-amber-200">
                Note about resource measurements
              </p>
              <p className="mt-1 text-sm text-amber-800 dark:text-amber-300">
                These benchmarks were not run on an isolated server. CPU and
                memory measurements may be affected by other system processes
                and should be taken with a grain of salt. They provide relative
                comparisons rather than absolute performance guarantees.
              </p>
            </div>
          </div>
        </div>
      )}

      {/* Statistics */}
      {chartData.length > 0 && (
        <div className="grid gap-6 md:grid-cols-2">
          <div className=" border border-l-4 border-l-[#8b5cf6] overflow-hidden">
            <div className="bg-[#8b5cf6]/5 p-6">
              <h3 className="text-lg font-bold text-[#8b5cf6]">
                GroupMQ Stats
              </h3>
              <p className="text-sm text-muted-foreground mt-1">
                Performance metrics for GroupMQ
              </p>
            </div>
            <div className="p-6">
              <dl className="space-y-3">
                <div className="flex items-center justify-between  bg-muted/50 p-3">
                  <dt className="text-sm font-medium text-muted-foreground">
                    Average
                  </dt>
                  <dd className="text-base font-bold text-[#8b5cf6]">
                    {stats.groupmq.avg.toFixed(2)}{' '}
                    <span className="text-xs font-normal text-muted-foreground">
                      {METRIC_LABELS[selectedMetric].unit}
                    </span>
                  </dd>
                </div>
                <div className="flex items-center justify-between  bg-muted/50 p-3">
                  <dt className="text-sm font-medium text-muted-foreground">
                    Maximum
                  </dt>
                  <dd className="text-base font-bold text-[#8b5cf6]">
                    {stats.groupmq.max.toFixed(2)}{' '}
                    <span className="text-xs font-normal text-muted-foreground">
                      {METRIC_LABELS[selectedMetric].unit}
                    </span>
                  </dd>
                </div>
                <div className="flex items-center justify-between  bg-muted/50 p-3">
                  <dt className="text-sm font-medium text-muted-foreground">
                    Minimum
                  </dt>
                  <dd className="text-base font-bold text-[#8b5cf6]">
                    {stats.groupmq.min.toFixed(2)}{' '}
                    <span className="text-xs font-normal text-muted-foreground">
                      {METRIC_LABELS[selectedMetric].unit}
                    </span>
                  </dd>
                </div>
                <div className="flex items-center justify-between  border bg-muted/30 p-3">
                  <dt className="text-sm font-medium text-muted-foreground">
                    Data Points
                  </dt>
                  <dd className="text-base font-bold">{stats.groupmq.count}</dd>
                </div>
              </dl>
            </div>
          </div>

          <div className=" border border-l-4 border-l-[#f97316] overflow-hidden">
            <div className="bg-[#f97316]/5 p-6">
              <h3 className="text-lg font-bold text-[#f97316]">BullMQ Stats</h3>
              <p className="text-sm text-muted-foreground mt-1">
                Performance metrics for BullMQ
              </p>
            </div>
            <div className="p-6">
              <dl className="space-y-3">
                <div className="flex items-center justify-between  bg-muted/50 p-3">
                  <dt className="text-sm font-medium text-muted-foreground">
                    Average
                  </dt>
                  <dd className="text-base font-bold text-[#f97316]">
                    {stats.bullmq.avg.toFixed(2)}{' '}
                    <span className="text-xs font-normal text-muted-foreground">
                      {METRIC_LABELS[selectedMetric].unit}
                    </span>
                  </dd>
                </div>
                <div className="flex items-center justify-between  bg-muted/50 p-3">
                  <dt className="text-sm font-medium text-muted-foreground">
                    Maximum
                  </dt>
                  <dd className="text-base font-bold text-[#f97316]">
                    {stats.bullmq.max.toFixed(2)}{' '}
                    <span className="text-xs font-normal text-muted-foreground">
                      {METRIC_LABELS[selectedMetric].unit}
                    </span>
                  </dd>
                </div>
                <div className="flex items-center justify-between  bg-muted/50 p-3">
                  <dt className="text-sm font-medium text-muted-foreground">
                    Minimum
                  </dt>
                  <dd className="text-base font-bold text-[#f97316]">
                    {stats.bullmq.min.toFixed(2)}{' '}
                    <span className="text-xs font-normal text-muted-foreground">
                      {METRIC_LABELS[selectedMetric].unit}
                    </span>
                  </dd>
                </div>
                <div className="flex items-center justify-between  border bg-muted/30 p-3">
                  <dt className="text-sm font-medium text-muted-foreground">
                    Data Points
                  </dt>
                  <dd className="text-base font-bold">{stats.bullmq.count}</dd>
                </div>
              </dl>
            </div>
          </div>
        </div>
      )}

      {/* Chart */}
      <div>
        <div className="mb-6">
          <h2 className="text-2xl font-bold">
            {METRIC_LABELS[selectedMetric].label} Over Time
          </h2>
          <p className="text-muted-foreground mt-2">
            Comparing performance trends between GroupMQ and BullMQ
          </p>
        </div>
        <div>
          {chartData.length === 0 ? (
            <div className="flex h-[350px] flex-col items-center justify-center text-muted-foreground">
              <svg
                className="mb-4 h-16 w-16 text-muted-foreground/30"
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={1.5}
                  d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"
                />
              </svg>
              <p className="text-base font-medium">No data available</p>
              <p className="mt-1 text-sm">Try adjusting your filters</p>
            </div>
          ) : (
            <ChartContainer config={chartConfig} className="h-[400px] w-full">
              <LineChart
                data={chartData}
                margin={{ left: 12, right: 12, top: 12, bottom: 12 }}
              >
                <CartesianGrid vertical={false} strokeDasharray="3 3" />
                <XAxis
                  dataKey="date"
                  tickLine={false}
                  axisLine={false}
                  tickMargin={8}
                  minTickGap={32}
                />
                <YAxis tickLine={false} axisLine={false} tickMargin={8} />
                <ChartTooltip
                  content={
                    <ChartTooltipContent
                      labelFormatter={(value) => `${value}`}
                      formatter={(value, name) => {
                        const label =
                          chartConfig[name as keyof typeof chartConfig]
                            ?.label || name;
                        return (
                          <>
                            <span className="text-muted-foreground">
                              {label}
                            </span>
                            <span className="font-mono font-medium tabular-nums text-foreground ml-auto">
                              {Number(value).toFixed(2)}{' '}
                              {METRIC_LABELS[selectedMetric].unit}
                            </span>
                          </>
                        );
                      }}
                    />
                  }
                />
                <ChartLegend content={<ChartLegendContent />} />
                <Line
                  type="monotone"
                  dataKey="groupmq"
                  stroke="var(--color-groupmq)"
                  strokeWidth={2.5}
                  dot={{ r: 4, strokeWidth: 2 }}
                  activeDot={{ r: 6 }}
                  connectNulls
                />
                <Line
                  type="monotone"
                  dataKey="bullmq"
                  stroke="var(--color-bullmq)"
                  strokeWidth={2.5}
                  dot={{ r: 4, strokeWidth: 2 }}
                  activeDot={{ r: 6 }}
                  connectNulls
                />
              </LineChart>
            </ChartContainer>
          )}
        </div>
      </div>

      {/* Comparison Bar Chart */}
      {stats.groupmq.count > 0 && stats.bullmq.count > 0 && (
        <div>
          <div className="mb-6">
            <h2 className="text-2xl font-bold">Direct Comparison</h2>
            <p className="text-muted-foreground mt-2">
              Side-by-side performance comparison
            </p>
          </div>
          <div>
            <ChartContainer config={chartConfig} className="h-[300px] w-full">
              <BarChart
                data={[
                  {
                    name: 'Average',
                    groupmq: stats.groupmq.avg,
                    bullmq: stats.bullmq.avg,
                  },
                  {
                    name: 'Maximum',
                    groupmq: stats.groupmq.max,
                    bullmq: stats.bullmq.max,
                  },
                  {
                    name: 'Minimum',
                    groupmq: stats.groupmq.min,
                    bullmq: stats.bullmq.min,
                  },
                ]}
                margin={{ left: 12, right: 12, top: 12, bottom: 12 }}
              >
                <CartesianGrid vertical={false} strokeDasharray="3 3" />
                <XAxis
                  dataKey="name"
                  tickLine={false}
                  axisLine={false}
                  tickMargin={8}
                />
                <YAxis tickLine={false} axisLine={false} tickMargin={8} />
                <ChartTooltip
                  content={
                    <ChartTooltipContent
                      formatter={(value, name) => {
                        const label =
                          chartConfig[name as keyof typeof chartConfig]
                            ?.label || name;
                        return (
                          <>
                            <span className="text-muted-foreground">
                              {label}
                            </span>
                            <span className="font-mono font-medium tabular-nums text-foreground ml-auto">
                              {Number(value).toFixed(2)}{' '}
                              {METRIC_LABELS[selectedMetric].unit}
                            </span>
                          </>
                        );
                      }}
                    />
                  }
                />
                <ChartLegend content={<ChartLegendContent />} />
                <Bar
                  dataKey="groupmq"
                  fill="var(--color-groupmq)"
                  radius={[4, 4, 0, 0]}
                />
                <Bar
                  dataKey="bullmq"
                  fill="var(--color-bullmq)"
                  radius={[4, 4, 0, 0]}
                />
              </BarChart>
            </ChartContainer>
          </div>
        </div>
      )}
    </div>
  );
}
