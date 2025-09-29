'use client';

import { Loader2 } from 'lucide-react';
import { useState } from 'react';

import { Button } from '@/components/ui/button';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs';
import type { BlogPostData } from '@/lib/blog';
import { cn } from '@/lib/utils';
import { withBase } from '@/lib/withBase';

type Category = 'All Posts';

const categories: Category[] = ['All Posts'];

const POSTS_PER_PAGE = 8;

export default function BlogPosts({
  blogPosts,
}: {
  blogPosts: BlogPostData[];
}) {
  const [activeCategory, setActiveCategory] = useState<Category>('All Posts');
  const [visiblePosts, setVisiblePosts] = useState(POSTS_PER_PAGE);
  const [isLoading, setIsLoading] = useState(false);

  // Filter posts by category
  const filteredPosts =
    activeCategory === 'All Posts'
      ? blogPosts
      : blogPosts.filter((post) => {
          const category = post.tags?.find((tag: string) =>
            categories.includes(tag as Category),
          );
          return category === activeCategory;
        });

  // Get currently visible posts
  const visiblePostsList = filteredPosts.slice(0, visiblePosts);

  // Determine if there are more posts to show
  const hasMorePosts = visiblePosts < filteredPosts.length;

  // Function to load more posts
  const loadMorePosts = () => {
    setIsLoading(true);

    // Simulate network delay for loading effect
    setTimeout(() => {
      setVisiblePosts((prev) =>
        Math.min(prev + POSTS_PER_PAGE, filteredPosts.length),
      );
      setIsLoading(false);
    }, 200);
  };

  // Reset visible posts when changing category
  const handleCategoryChange = (category: Category) => {
    setActiveCategory(category);
    setVisiblePosts(POSTS_PER_PAGE);
  };

  return (
    <section className="container">
      <div className="border-x border-b">
        <div className="hidden p-7.5 md:block" />

        <div className="bordered-div-padding border-b">
          <h1 className="font-weight-display text-2xl leading-snug tracking-tighter md:text-3xl lg:text-5xl">
            Articles
          </h1>
          <div className="mt-6 block md:hidden">
            <Select
              value={activeCategory}
              onValueChange={(value) => handleCategoryChange(value as Category)}
            >
              <SelectTrigger className="w-full">
                <SelectValue>{activeCategory}</SelectValue>
              </SelectTrigger>
              <SelectContent>
                {categories.map((category) => (
                  <SelectItem key={`category-${category}`} value={category}>
                    {category}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        </div>

        <div className="bordered-div-padding hidden border-b md:block">
          <Tabs
            value={activeCategory}
            onValueChange={(value) => handleCategoryChange(value as Category)}
          >
            <TabsList className="flex gap-3">
              {categories.map((category) => (
                <TabsTrigger key={`tab-${category}`} value={category}>
                  {category}
                </TabsTrigger>
              ))}
            </TabsList>
          </Tabs>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2">
          {visiblePostsList.map((post, index) => {
            // Determine if this is in the last row
            const isLastRow =
              index >=
              visiblePostsList.length -
                (visiblePostsList.length % 2 === 0 ? 2 : 1);
            // Is it the last item?
            const isLastItem = index === visiblePostsList.length - 1;
            // Is it odd and in an odd position? (left column)
            const isLeftColumn = index % 2 === 0;
            // Is it the last item in an odd-length list?
            const isLastSingleItem =
              isLastItem && visiblePostsList.length % 2 !== 0;

            return (
              <BlogPostItem
                key={post.slug}
                post={post}
                className={cn({
                  // No bottom border for last row items
                  'border-b-0': isLastRow,
                  // No right border for items in the right column
                  'md:border-r-0': !isLeftColumn,
                  // Full width and no borders for last item in odd-length list
                  'md:col-span-2': isLastSingleItem,
                  // Add right border only to left column items
                  'md:border-r': isLeftColumn && !isLastSingleItem,
                })}
              />
            );
          })}
        </div>

        {filteredPosts.length > 0 && (
          <div className="bordered-div-padding flex flex-col items-center justify-center gap-4 border-t text-center">
            {hasMorePosts && (
              <Button
                variant="outline"
                className="rounded-full"
                size="lg"
                onClick={loadMorePosts}
                disabled={isLoading}
              >
                {isLoading ? (
                  <>
                    <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                    Loading...
                  </>
                ) : (
                  'Show More Posts'
                )}
              </Button>
            )}
            <p className="text-muted-foreground text-sm">
              Showing {Math.min(visiblePosts, filteredPosts.length)} of{' '}
              {filteredPosts.length} posts
            </p>
          </div>
        )}

        {filteredPosts.length === 0 && (
          <div className="bordered-div-padding text-center">
            <p className="text-muted-foreground">
              No posts found for this category.
            </p>
          </div>
        )}
      </div>
    </section>
  );
}

function BlogPostItem({
  post,
  className,
}: {
  post: BlogPostData;
  className?: string;
}) {
  // Extract category from tags
  const category =
    (post.tags?.find((tag: string) =>
      categories.includes(tag as Category),
    ) as Category) || 'Product';

  // Format date: Apr 11, 2025
  const formattedDate = new Date(post.date).toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
  });
  return (
    <a
      href={withBase(`/blog/${post.slug}`)}
      className={cn(
        'bordered-div-padding hover:bg-muted/30 dark:hover:bg-muted block border-b',
        className,
      )}
    >
      <div className="">
        <div className="flex items-center justify-between gap-2">
          <span className="text-secondary text-sm font-medium md:text-base">
            {category}
          </span>
          <span className="text-muted-foreground text-sm">{formattedDate}</span>
        </div>
        <h2 className="font-weight-display mt-4 text-base leading-snug tracking-tighter md:text-xl">
          {post.title}
        </h2>
        <p className="text-muted-foreground mt-6 text-sm leading-relaxed md:text-base">
          {post.description}
        </p>
      </div>

      <div className="mt-6 flex items-center justify-between md:mt-8 lg:mt-10">
        {post.authors && post.authors.length > 0 && (
          <div className="flex items-center gap-2">
            <div className="flex -space-x-3">
              {post.authors.map(
                (author: { name: string; image?: string }) =>
                  author.image && (
                    <img
                      key={author.name}
                      src={author.image}
                      alt={author.name}
                      width={32}
                      height={32}
                      className="border-background h-8 w-8 rounded-full border-2"
                    />
                  ),
              )}
            </div>
            <span className="text-muted-foreground text-sm font-medium">
              {post.authors
                .map((author: { name: string }) => author.name)
                .join(' and ')}
            </span>
          </div>
        )}
      </div>
    </a>
  );
}
