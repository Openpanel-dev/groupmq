'use client';
import { FaGithub } from 'react-icons/fa6';

export function GroupMQUsedBy() {
  return (
    <section className="container">
      <div className="grid grid-cols-1 border border-t-0 md:grid-cols-2">
        <div className="bordered-div-padding space-y-4 border-b md:border-e md:border-b-0">
          <h3 className="text-muted-foreground text-sm leading-snug font-medium md:text-base">
            Inspired by BullMQ
          </h3>
          <p className="text-muted-foreground text-sm leading-relaxed max-w-[560px]">
            GroupMQ is inspired by BullMQ, a fantastic library and one of the
            most popular Redis-based job queue libraries for Node.js. This is
            not a fork, we've built GroupMQ from scratch while taking
            inspiration from BullMQ's design and concepts. Some parts have been
            inspired by BullMQ, and we've tried to keep the API as similar as
            possible while adapting it for our specific use case.
          </p>
          <a
            href="https://github.com/taskforcesh/bullmq"
            target="_blank"
            rel="noreferrer"
            className="inline-flex w-fit items-center gap-2 rounded-sm border px-3 py-1.5 text-sm font-medium hover:bg-accent hover:text-accent-foreground"
          >
            Read more about BullMQ
          </a>
        </div>
        <div className="bordered-div-padding space-y-4">
          <h3 className="text-muted-foreground text-sm leading-snug font-medium md:text-base">
            Open Source
          </h3>
          <p className="text-muted-foreground text-sm leading-relaxed max-w-[560px]">
            MIT licensed. Contributions welcome.
          </p>
          <a
            className="inline-flex items-center gap-2 text-sm font-medium underline-offset-4 hover:underline"
            href="https://github.com/openpanel-dev/groupmq"
            target="_blank"
            rel="noreferrer"
          >
            <FaGithub className="size-4" />
            github.com/openpanel-dev/groupmq
          </a>
        </div>
      </div>
    </section>
  );
}
