'use client';

import { CodeIcon, DollarSignIcon, Terminal } from 'lucide-react';
import { FaDiscord, FaGithub, FaLinkedin, FaXTwitter } from 'react-icons/fa6';

import Logo from '@/components/layout/logo';
import { Button } from '@/components/ui/button';
import { EXTERNAL_LINKS } from '@/constants/external-links';
import { cn } from '@/lib/utils';

const FEATURES = [
  {
    title: 'Open Source',
    description: '0$ / forever',
    features: [
      {
        name: 'Code is under MIT license',
        icon: <CodeIcon className="size-5" />,
      },
      {
        name: 'Always free',
        icon: <DollarSignIcon className="size-5" />,
      },
      {
        name: 'GitHub community support',
        icon: <FaGithub className="size-5" />,
      },
      {
        name: 'Ideal for developers and internal tools',
        icon: <Terminal className="size-5" />,
      },
    ],
    button: {
      text: 'View on GitHub',
      href: EXTERNAL_LINKS.GITHUB_REPO,
      className: 'bg-border hover:bg-border/80 text-foreground',
    },
  },
];

const Footer = () => {
  const themeClass = 'bg-foreground text-background [&_*]:border-border/30';

  return (
    <footer className={cn('overflow-hidden', themeClass)}>
      {/* Pricing Section */}
      <div className="container">
        <div className="bordered-div-padding border-x">
          <h2 className="flex flex-col lg:text-4xxl font-weight-display mt-6 text-xl md:mt-14 md:text-3xl lg:mt-40">
            <span>Start free.</span>
            <span>Always free.</span>
            <span>Scale confidently.</span>
          </h2>
        </div>

        <div className="grid divide-y border md:grid-cols-2 md:divide-x md:divide-y-0">
          {FEATURES.map((plan) => (
            <div
              key={plan.title}
              className={cn(
                'bordered-div-padding relative flex flex-col gap-6 md:gap-10',
              )}
            >
              <div>
                <h3 className="font-weight-display text-lg md:text-2xl lg:text-3xl">
                  {plan.title}
                </h3>
                <p className="font-weight-display mt-6 text-base md:text-xl">
                  {plan.description}
                </p>
              </div>

              <ul className="space-y-6">
                {plan.features.map((feature) => (
                  <li key={feature.name} className="flex items-start gap-2">
                    <span className="flex-shrink-0">{feature.icon}</span>
                    <span className="text-muted-foreground font-medium">
                      {feature.name}
                    </span>
                  </li>
                ))}
              </ul>

              <Button
                asChild
                className={cn('mt-auto mb-0 w-fit', plan.button.className)}
              >
                <a
                  href={plan.button.href}
                  target={
                    plan.button.href.startsWith('http') ? '_blank' : undefined
                  }
                  rel={
                    plan.button.href.startsWith('http')
                      ? 'noopener noreferrer'
                      : undefined
                  }
                >
                  {plan.button.text}
                </a>
              </Button>
            </div>
          ))}
        </div>

        {/* Social and Status Section */}
        <div className="flex flex-col justify-between border-x border-b md:flex-row">
          <div className="bordered-div-padding flex items-center space-x-3">
            <a
              href={EXTERNAL_LINKS.DISCORD}
              className="px-3 py-2.5 transition-opacity hover:opacity-80"
              target="_blank"
              rel="noopener noreferrer"
              aria-label="Discord"
            >
              <FaDiscord className="size-5" />
            </a>
            <a
              href={EXTERNAL_LINKS.GITHUB}
              className="px-3 py-2.5 transition-opacity hover:opacity-80"
              target="_blank"
              rel="noopener noreferrer"
              aria-label="GitHub"
            >
              <FaGithub className="size-5" />
            </a>
            <a
              href={EXTERNAL_LINKS.TWITTER}
              className="px-3 py-2.5 transition-opacity hover:opacity-80"
              target="_blank"
              rel="noopener noreferrer"
              aria-label="Twitter"
            >
              <FaXTwitter className="size-5" />
            </a>
            <a
              href={EXTERNAL_LINKS.LINKEDIN}
              className="px-3 py-2.5 transition-opacity hover:opacity-80"
              target="_blank"
              rel="noopener noreferrer"
              aria-label="LinkedIn"
            >
              <FaLinkedin className="size-5" />
            </a>
          </div>
          <div className="bordered-div-padding flex items-center border-t text-[#00A656] md:border-t-0">
            <span
              className={cn(
                'me-3 h-2 w-2 animate-pulse rounded-full bg-[#00A656]',
              )}
            ></span>
            <span className="font-medium">Waiting for jobs 🤙</span>
          </div>
        </div>

        {/* Large Logo */}
        <div className="flex justify-center border-x">
          <Logo className="justify-center" size="xxxxl" />
        </div>
      </div>
    </footer>
  );
};

export default Footer;
