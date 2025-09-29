// Place any global data in this file.
// You can import this data from anywhere in your site by using the `import` keyword.

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
      { url: '/favicon/favicon.ico', sizes: '48x48' },
      { url: '/favicon/favicon.svg', type: 'image/svg+xml' },
      { url: '/favicon/favicon-96x96.png', sizes: '96x96', type: 'image/png' },
      { url: '/favicon/favicon.svg', type: 'image/svg+xml' },
      { url: '/favicon/favicon.ico' },
    ],
    apple: [{ url: '/favicon/apple-touch-icon.png', sizes: '180x180' }],
    shortcut: [{ url: '/favicon/favicon.ico' }],
  },
  openGraph: {
    title: SITE_TITLE,
    description: SITE_DESCRIPTION,
    siteName: 'GroupMQ',
    images: [
      {
        url: '/og-image.jpg',
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
    images: ['/og-image.jpg'],
    creator: '@groupmq',
  },
};
