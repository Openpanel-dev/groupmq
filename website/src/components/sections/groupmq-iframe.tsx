'use client';

import Iframe from './iframe';

export function GroupMQIframe() {
  return (
    <section className="container">
      <div className="bordered-div-padding border border-t-0 overflow-hidden">
        <div className="space-y-4 mb-8">
          <h2 className="text-foreground font-weight-display leading-snug md:text-xl">
            GroupMQ powering OpenPanel.dev
          </h2>
          <p className="text-muted-foreground text-sm leading-relaxed max-w-[720px]">
            GroupMQ powers internal pipelines at OpenPanel.dev. We designed it
            to be small, reliable, and easy to operate.
          </p>
        </div>
        <div className="-mb-14">
          <Iframe />
        </div>
      </div>
    </section>
  );
}
