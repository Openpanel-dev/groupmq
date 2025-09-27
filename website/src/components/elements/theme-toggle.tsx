'use client';

import { useEffect, useState, useRef } from 'react';

import { motion as m } from 'motion/react';

import { Button } from '@/components/ui/button';
import { cn } from '@/lib/utils';

interface ThemeToggleProps {
  className?: string;
}

export function ThemeToggle({ className }: ThemeToggleProps = {}) {
  const [theme, setTheme] = useState<'light' | 'dark'>('light');
  const buttonRef = useRef<HTMLButtonElement>(null);

  useEffect(() => {
    // Get initial theme from localStorage, default to 'light' if none exists
    const savedTheme = localStorage.getItem('starlight-theme') as
      | 'light'
      | 'dark'
      | null;
    const initialTheme = savedTheme || 'light';
    setTheme(initialTheme);
    document.documentElement.classList.toggle('dark', initialTheme === 'dark');

    // Listen for theme changes
    const handleStorageChange = () => {
      const newTheme = localStorage.getItem('starlight-theme') as
        | 'light'
        | 'dark'
        | null;
      if (newTheme) {
        setTheme(newTheme);
      }
    };

    window.addEventListener('storage', handleStorageChange);

    // Listen for direct DOM class changes (for immediate updates)
    const observer = new MutationObserver(() => {
      const isDark = document.documentElement.classList.contains('dark');
      setTheme(isDark ? 'dark' : 'light');
    });

    observer.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ['class'],
    });

    return () => {
      window.removeEventListener('storage', handleStorageChange);
      observer.disconnect();
    };
  }, []);

  const toggleTheme = () => {
    if (document.startViewTransition) {
      // Remove page-transition class to avoid conflicts
      document.documentElement.classList.remove('page-transition');
      // Add theme-transition class
      document.documentElement.classList.add('theme-transition');

      document
        .startViewTransition(() => {
          const newTheme = theme === 'dark' ? 'light' : 'dark';
          setTheme(newTheme);
          document.documentElement.classList.toggle('dark');
          localStorage.setItem('starlight-theme', newTheme);
        })
        .finished.then(() => {
          // Clean up theme-transition class after all animations complete
          setTimeout(() => {
            document.documentElement.classList.remove('theme-transition');
          }, 50);
        })
        .catch(() => {
          // Fallback cleanup in case of error
          document.documentElement.classList.remove('theme-transition');
        });
    } else {
      const newTheme = theme === 'dark' ? 'light' : 'dark';
      setTheme(newTheme);
      document.documentElement.classList.toggle('dark');
      localStorage.setItem('starlight-theme', newTheme);
    }
  };

  return (
    <Button
      aria-label="Toggle theme"
      variant="ghost"
      onClick={toggleTheme}
      data-theme-toggle
      ref={buttonRef}
      className={cn('flex rounded-md px-4 py-0 lg:px-2', className)}
      size="sm"
    >
      <div className="relative size-4">
        {/* Sun Icon */}
        <m.svg
          width="20"
          height="20"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
          className="absolute inset-0"
          initial={{ opacity: 0, scale: 0.8, rotate: -90 }}
          animate={{
            opacity: theme === 'light' ? 1 : 0,
            scale: theme === 'light' ? 1 : 0.8,
            rotate: theme === 'light' ? 0 : -90,
          }}
          transition={{ duration: 0.4 }}
        >
          <circle cx="12" cy="12" r="4" />
          <m.g
            initial={{ opacity: 0, scale: 0.8 }}
            animate={{
              opacity: theme === 'light' ? 1 : 0,
              scale: theme === 'light' ? 1 : 0.8,
            }}
            transition={{ duration: 0.3, delay: 0.1 }}
          >
            <m.path d="M12 2v2" />
            <m.path d="M12 20v2" />
            <m.path d="M4.93 4.93l1.41 1.41" />
            <m.path d="M17.66 17.66l1.41 1.41" />
            <m.path d="M2 12h2" />
            <m.path d="M20 12h2" />
            <m.path d="M6.34 17.66l-1.41 1.41" />
            <m.path d="M19.07 4.93l-1.41 1.41" />
          </m.g>
        </m.svg>

        {/* Moon Icon */}
        <m.svg
          width="20"
          height="20"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
          className="absolute inset-0"
          initial={{ opacity: 0, scale: 0.8, rotate: -90 }}
          animate={{
            opacity: theme === 'dark' ? 1 : 0,
            scale: theme === 'dark' ? 1 : 0.8,
            rotate: theme === 'dark' ? 0 : -90,
          }}
          transition={{ duration: 0.4 }}
        >
          <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z" />
        </m.svg>
      </div>
    </Button>
  );
}
