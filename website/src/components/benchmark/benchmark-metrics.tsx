import { ActivitySquare, Clock, Gauge, Timer } from 'lucide-react';

import { PlusSigns } from '@/components/icons/plus-signs';
import { cn } from '@/lib/utils';

type Metric = {
  icon: React.ElementType;
  title: string;
  description: string;
  subDescription: string;
};

const metrics: Metric[] = [
  {
    icon: Gauge,
    title: 'Throughput (jobs/sec)',
    description: 'More jobs processed per second.',
    subDescription:
      'Higher throughput means more jobs can be processed per second. GroupMQ is optimized for group-based processing while maintaining high throughput.',
  },
  {
    icon: Timer,
    title: 'Pickup Time',
    description: 'How quickly workers grab jobs.',
    subDescription:
      "This measures how quickly a worker picks up a job after it's enqueued. Lower pickup times indicate better responsiveness and less queuing overhead.",
  },
  {
    icon: ActivitySquare,
    title: 'Processing Time',
    description: 'Time spent executing job logic.',
    subDescription:
      'The actual time spent executing your job handler. This should be similar between both systems as it depends on your job logic.',
  },
  {
    icon: Clock,
    title: 'Total Time',
    description: 'End-to-end job latency.',
    subDescription:
      "End-to-end latency from when a job is added to the queue until it's completed. This includes pickup time + processing time + any queue overhead.",
  },
];

export function BenchmarkMetrics() {
  return (
    <div className="grid grid-cols-1 border border-t-0 md:grid-cols-2">
      {metrics.map((metric, index) => (
        <div
          key={metric.title}
          className={cn(
            'bordered-div-padding relative space-y-4 md:space-y-6',
            'shadow-[0_0_0_0.5px] shadow-border',
          )}
        >
          {index === 0 && (
            <PlusSigns className="absolute inset-0 -mt-0.25 hidden !h-[calc(100%+2px)] -translate-x-full border-y md:block" />
          )}
          <div className="space-y-4">
            <h3 className="text-muted-foreground flex items-center gap-2 text-sm leading-snug font-medium md:text-base">
              <metric.icon className="size-5" />
              {metric.title}
            </h3>
            <h4 className="text-foreground font-weight-display leading-snug md:text-xl">
              {metric.description}
            </h4>
          </div>
          <p className="text-muted-foreground text-sm leading-relaxed md:text-base">
            {metric.subDescription}
          </p>
        </div>
      ))}
    </div>
  );
}
