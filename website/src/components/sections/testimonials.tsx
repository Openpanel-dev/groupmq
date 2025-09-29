import { Verified } from 'lucide-react';

import { Marquee } from '@/components/magicui/marquee';
import { withBase } from '@/lib/withBase';

const companies = [
  {
    name: 'This could be you',
    logo: {
      src: withBase('/images/testimonials/placeholder.svg'),
    },
    href: 'https://github.com/openpanel-dev/groupmq',
  },
  {
    name: 'OpenPanel.dev',
    logo: { src: '/images/testimonials/op.svg' },
    href: 'https://openpanel.dev',
  },
  {
    name: 'This could be you',
    logo: {
      src: withBase('/images/testimonials/placeholder.svg'),
    },
    href: 'https://github.com/openpanel-dev/groupmq',
  },
];

export function Testimonials() {
  return (
    <div className="relative overflow-hidden">
      <section className="container">
        <div className="bordered-div-padding relative border border-t-0">
          {/* Trusted by text */}
          <h2 className="text-muted-foreground flex items-center gap-2 text-sm leading-snug font-medium md:text-base">
            <Verified className="size-5" />
            Trusted by
          </h2>

          {/* Company logos */}
          <Marquee className="items-center mt-6 [--gap:8rem] md:mt-8 lg:mt-10 xl:[&_div]:[animation-play-state:paused]">
            {companies.map((company) => (
              <a
                key={company.name}
                href={company.href}
                className="py-2.5 transition-opacity hover:opacity-80 flex items-center justify-center"
                target="_blank"
              >
                <img src={company.logo.src} alt={company.name} width={100} />
              </a>
            ))}
          </Marquee>
        </div>
        {/* Testimonial */}
        <blockquote className="bordered-div-padding flex flex-col justify-between gap-8 border border-t-0 md:flex-row">
          <p className="lg:text-4xxl font-weight-display flex-7 text-2xl leading-snug tracking-tighter md:text-3xl">
            GroupMQ reduced a lot of edge cases from simultaneous events for the
            same users. By controlling how many jobs per group run at once, we
            avoid race conditions and ensure events are processed in the right
            order.
          </p>

          <footer className="flex-6 self-end">
            <div className="flex items-center gap-4">
              <img
                src={withBase('/images/testimonials/lindesvard.jpg')}
                alt="Carl Lindesvärd"
                width={40}
                height={40}
                className="rounded-full"
              />
              <cite className="text-sm font-medium not-italic md:text-lg lg:text-xl">
                Carl Lindesvärd, Founder at OpenPanel.dev
              </cite>
            </div>
          </footer>
        </blockquote>
      </section>
    </div>
  );
}
