// @ts-check

import mdx from '@astrojs/mdx';
import react from '@astrojs/react';
import sitemap from '@astrojs/sitemap';
import starlight from '@astrojs/starlight';
import tailwindcss from '@tailwindcss/vite';
import { defineConfig } from 'astro/config';
// import { EXTERNAL_LINKS } from 'src/constants/external-links';

// https://astro.build/config
export default defineConfig({
  site: 'https://openpanel-dev.github.io/groupmq/',
  integrations: [
    starlight({
      title: 'GroupMQ Docs',
      description: 'A Per-Group FIFO Queue for Node + Redis',
      favicon: '/favicon/favicon.svg',
      logo: {
        light: './public/layout/logo-black.svg',
        dark: './public/layout/logo-white.svg',
        replacesTitle: true,
        alt: 'GroupMQ Logo',
      },
      customCss: ['./src/styles/docs.css'],
      sidebar: [
        {
          label: 'Menu',
          items: [
            { label: 'Home', link: '/' },
            { label: 'Articles', link: '/blog' },
            { label: 'GitHub', link: 'https://git.new/groupmq' },
          ],
        },
        {
          label: 'Introduction',
          items: [
            { label: 'What is GroupMQ?', slug: 'docs' },
            { label: 'Key Features', slug: 'key-features' },
            { label: 'GroupMQ vs. BullMQ', slug: 'groupmq-vs-bullmq' },
          ],
        },
        {
          label: 'Getting Started',
          items: [
            { label: 'Installation', slug: 'installation' },
            { label: 'Quick Example', slug: 'quick-example' },
          ],
        },
        {
          label: 'Usage',
          items: [
            { label: 'Creating a Queue', slug: 'creating-a-queue' },
            { label: 'Adding Jobs', slug: 'adding-jobs' },
            { label: 'Job Types & Scheduling', slug: 'jobs' },
            { label: 'Processing Jobs', slug: 'processing-jobs' },
            { label: 'Configuration Options', slug: 'configuration-options' },
          ],
        },
        {
          label: 'Advanced',
          items: [
            {
              label: 'Error Handling & Retries',
              slug: 'error-handling-retries',
            },
            { label: 'Scaling Workers', slug: 'scaling-workers' },
            { label: 'Performance Tips', slug: 'performance-tips' },
          ],
        },
        {
          label: 'API Reference',
          items: [
            { label: 'Queue', slug: 'api-queue' },
            { label: 'Job', slug: 'api-job' },
            { label: 'Worker', slug: 'api-worker' },
          ],
        },
        {
          label: 'Contributing',
          items: [
            { label: 'How to Contribute', slug: 'contributing' },
            { label: 'Roadmap', slug: 'roadmap' },
            { label: 'License', slug: 'license' },
          ],
        },
      ],
      expressiveCode: {
        themes: ['github-light', 'github-dark'],
      },
    }),
    mdx({
      syntaxHighlight: {
        type: 'shiki',
      },
    }),
    sitemap(),
    react(),
  ],
  vite: {
    plugins: [tailwindcss()],
  },
});
