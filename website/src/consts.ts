// Place any global data in this file.
// You can import this data from anywhere in your site by using the `import` keyword.

import { withBase } from './lib/withBase';

export const SITE_TITLE = 'GroupMQ - Per-Group FIFO Queue for Node + Redis';
export const SITE_DESCRIPTION =
  'Guaranteed ordering within a group, parallel processing across groups. Simple API, fast Lua scripts, and compatible with BullBoard.';

export const SITE_METADATA = {
  title: {
    default: SITE_TITLE,
    template: '%s | GroupMQ',
  },
  description: SITE_DESCRIPTION,
  keywords: [
    'GroupMQ',
    'Redis',
    'Queue',
    'FIFO',
    'Worker',
    'Node',
    'TypeScript',
    'BullBoard',
    'BullMQ',
  ],
  authors: [{ name: 'GroupMQ Team' }],
  creator: 'GroupMQ',
  publisher: 'GroupMQ',
  robots: {
    index: true,
    follow: true,
  },
  icons: {
    icon: [
      { url: withBase('/favicon/favicon.ico'), sizes: '48x48' },
      { url: withBase('/favicon/favicon.svg'), type: 'image/svg+xml' },
      {
        url: withBase('/favicon/favicon-96x96.png'),
        sizes: '96x96',
        type: 'image/png',
      },
      { url: withBase('/favicon/favicon.svg'), type: 'image/svg+xml' },
      { url: withBase('/favicon/favicon.ico') },
    ],
    apple: [
      { url: withBase('/favicon/apple-touch-icon.png'), sizes: '180x180' },
    ],
    shortcut: [{ url: withBase('/favicon/favicon.ico') }],
  },
  openGraph: {
    title: SITE_TITLE,
    description: SITE_DESCRIPTION,
    siteName: 'GroupMQ',
    images: [
      {
        url: withBase('/og-image.png'),
        width: 1200,
        height: 630,
        alt: SITE_TITLE,
      },
    ],
  },
  twitter: {
    card: 'summary_large_image',
    title: SITE_TITLE,
    description: SITE_DESCRIPTION,
    images: [withBase('/og-image.png')],
    creator: '@groupmq',
  },
};
