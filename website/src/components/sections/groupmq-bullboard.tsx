'use client';
import { Code } from '@/components/code';
import { Card, CardContent } from '@/components/ui/card';

const SNIPPET = `import express from 'express';
import { createBullBoard } from '@bull-board/api';
import { ExpressAdapter } from '@bull-board/express';
import { BullBoardGroupMQAdapter } from 'groupmq/adapters/groupmq-bullboard-adapter';
import { queue } from './queue';

const app = express();
const serverAdapter = new ExpressAdapter();

createBullBoard({
  queues: [new BullBoardGroupMQAdapter(queue, { displayName: 'Orders' })],
  serverAdapter,
});

serverAdapter.setBasePath('/admin/queues');
app.use('/admin/queues', serverAdapter.getRouter());
app.listen(3000);`;

export function GroupMQBullboard() {
  return (
    <section className="container">
      <div className="bordered-div-padding border border-t-0">
        <div className="space-y-4">
          <h3 className="text-muted-foreground text-sm leading-snug font-medium md:text-base">
            BullBoard Adapter
          </h3>
          <h2 className="text-foreground font-weight-display leading-snug md:text-xl">
            Drop-in monitoring UI using your existing BullBoard setup
          </h2>
        </div>
        <div className="mt-6">
          <Card className="relative overflow-hidden">
            <CardContent className="!p-4">
              <Code>{SNIPPET}</Code>
            </CardContent>
          </Card>
        </div>
      </div>
    </section>
  );
}
