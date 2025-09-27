import { getCollection } from 'astro:content';
import rss from '@astrojs/rss';

import { SITE_DESCRIPTION, SITE_TITLE } from '@/consts';

export async function GET(context) {
  const posts = (await getCollection('blog')).sort(
    (a, b) => b.data.date.valueOf() - a.data.date.valueOf(),
  );
  return rss({
    title: SITE_TITLE,
    description: SITE_DESCRIPTION,
    site: context.site,
    items: posts.map((post) => ({
      title: post.data.title,
      description: post.data.description,
      pubDate: post.data.date,
      link: `/blog/${post.slug || post.id}/`,
    })),
  });
}
