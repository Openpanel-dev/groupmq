// Types for blog posts that match Astro's content collection schema
export interface BlogPost {
  id: string;
  body?: string;
  collection: 'blog';
  data: {
    title: string;
    description: string;
    date: Date;
    updatedDate?: Date;
    authors?: Array<{
      name: string;
      image?: string;
    }>;
    tags?: string[];
    coverImage?: string;
  };
}

// Simplified type for component props
export interface BlogPostData {
  id: string;
  title: string;
  description: string;
  date: Date;
  slug: string;
  tags?: string[];
  authors?: Array<{
    name: string;
    image?: string;
  }>;
}

// Transform Astro content collection post to component-friendly format
export function transformBlogPost(post: BlogPost): BlogPostData {
  return {
    id: post.id,
    title: post.data.title,
    description: post.data.description,
    date: post.data.date,
    slug: post.id,
    tags: post.data.tags,
    authors: post.data.authors || [],
  };
}
