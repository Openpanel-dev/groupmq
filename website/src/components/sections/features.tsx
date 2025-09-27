import {
  AlarmClock,
  CalendarClock,
  Clock,
  GitCommit,
  Layers,
  RotateCcw,
} from 'lucide-react';

import { PlusSigns } from '@/components/icons/plus-signs';
import { cn } from '@/lib/utils';

type Feature = {
  icon: React.ElementType;
  title: string;
  description: string;
  subDescription: string;
  className?: string;
  images?: { src: string; width: number; height: number; alt: string }[];
};

const features: Feature[] = [
  {
    icon: Layers,
    title: 'Per‑Group FIFO',
    description: 'One in‑flight job per group, strict ordering guaranteed.',
    subDescription:
      'Jobs with the same groupId are processed sequentially. Different groups run in parallel for maximum throughput without conflicts.',
  },
  {
    icon: Clock,
    title: 'Order by Timestamp',
    description: 'Respects orderMs; oldest timestamps first.',
    subDescription:
      'If orderMs is provided, the queue delays processing to ensure events arrive in the correct order, minimizing race conditions from late producers.',
  },
  {
    icon: GitCommit,
    title: 'Idempotence',
    description: 'Safe to retry without duplicate side‑effects.',
    subDescription:
      'Deterministic job IDs and stable handlers make it straightforward to avoid double execution when retries occur.',
  },
  {
    icon: CalendarClock,
    title: 'Cron & Repeats',
    description: 'Simple repeatable jobs and cron patterns.',
    subDescription:
      'Use repeat.every for fixed intervals or repeat.pattern for cron syntax. Materialization occurs in the worker’s periodic cycle.',
  },
  {
    icon: AlarmClock,
    title: 'Delayed Jobs',
    description: 'Schedule work for the future.',
    subDescription:
      'Add jobs with a delay to control when they become eligible for processing, without blocking other groups.',
  },
  {
    icon: RotateCcw,
    title: 'Retries & Failures',
    description: 'Configurable backoff, attempts, and failure tracking.',
    subDescription:
      'Tune maxAttempts and backoff to your workload. Inspect counts and statuses to monitor and react to failures.',
  },
];

export function Features() {
  return (
    <section className="container">
      <div className="grid grid-cols-1 border border-t-0 md:grid-cols-2">
        {features.map((feature, index) => (
          <div
            key={feature.title}
            className={cn(
              feature.className,
              'bordered-div-padding relative space-y-8',
              'shadow-[0_0_0_0.5px] shadow-border',
            )}
          >
            {index === 0 && (
              // Height is 100% + 2px to account for parent border not being included in the calculation
              <PlusSigns className="absolute inset-0 -mt-0.25 hidden !h-[calc(100%+2px)] -translate-x-full border-y md:block" />
            )}
            <div className="space-y-4 md:space-y-6">
              <div className="space-y-4">
                <h2 className="text-muted-foreground flex items-center gap-2 text-sm leading-snug font-medium md:text-base">
                  <feature.icon className="size-5" />
                  {feature.title}
                </h2>
                <h3 className="text-foreground font-weight-display leading-snug md:text-xl">
                  {feature.description}
                </h3>
              </div>
              <p className="text-muted-foreground text-sm leading-relaxed md:text-base">
                {feature.subDescription}
              </p>
            </div>

            {feature.images && (
              <div className="flex flex-col gap-4 mask-b-from-30% mask-b-to-95%">
                {feature.images.map((image, index) => (
                  <img
                    key={image.src}
                    src={image.src}
                    alt={image.alt}
                    width={image.width}
                    height={image.height}
                  />
                ))}
              </div>
            )}
          </div>
        ))}
      </div>
    </section>
  );
}
