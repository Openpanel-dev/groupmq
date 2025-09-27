import { docsLoader } from '@astrojs/starlight/loaders';
import { docsSchema } from '@astrojs/starlight/schema';
import { glob } from 'astro/loaders';
import { defineCollection, z } from 'astro:content';

const blog = defineCollection({
  // Load Markdown and MDX files in the `src/content/blog/` directory.
  loader: glob({ base: './src/content/blog', pattern: '**/*.{md,mdx}' }),
  // Type-check frontmatter using a schema
  schema: z.object({
    title: z.string(),
    description: z.string(),
    // Transform string to Date object
    date: z
      .string()
      .or(z.date())
      .transform((val) => new Date(val)),
    updatedDate: z
      .string()
      .or(z.date())
      .transform((val) => new Date(val))
      .optional(),
    authors: z
      .array(
        z.object({
          name: z.string(),
          image: z.string().optional(),
        }),
      )
      .optional(),
    tags: z.array(z.string()).optional(),
    coverImage: z.string().optional(),
  }),
});
const docs = defineCollection({
  loader: docsLoader(),
  schema: docsSchema(),
});
export const collections = { blog, docs };
