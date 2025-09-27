import { useCallback, useEffect, useState } from 'react';

// Tailwind default breakpoints in pixels
const SCREEN_SIZES = {
  sm: 640,
  md: 768,
  lg: 1024,
  xl: 1280,
  '2xl': 1536,
} as const;

type ScreenSize = keyof typeof SCREEN_SIZES;

// Debounce utility function
const debounce = <T extends (...args: unknown[]) => void>(
  func: T,
  wait: number,
) => {
  let timeout: NodeJS.Timeout;

  return (...args: Parameters<T>) => {
    clearTimeout(timeout);
    timeout = setTimeout(() => func(...args), wait);
  };
};

export const useMediaQuery = (debounceMs = 100) => {
  const [screenSize, setScreenSize] = useState<ScreenSize>('sm');

  const updateScreenSize = useCallback(() => {
    // Use document.documentElement.clientWidth for more reliable responsive breakpoints
    // This matches what CSS media queries use and excludes scrollbars
    const effectiveWidth =
      document.documentElement.clientWidth || window.innerWidth;

    let newSize: ScreenSize;
    if (effectiveWidth >= SCREEN_SIZES['2xl']) {
      newSize = '2xl';
    } else if (effectiveWidth >= SCREEN_SIZES.xl) {
      newSize = 'xl';
    } else if (effectiveWidth >= SCREEN_SIZES.lg) {
      newSize = 'lg';
    } else if (effectiveWidth >= SCREEN_SIZES.md) {
      newSize = 'md';
    } else {
      newSize = 'sm';
    }

    setScreenSize(newSize);
  }, []);

  useEffect(() => {
    // Create debounced handler
    const debouncedHandler = debounce(updateScreenSize, debounceMs);

    // Initial check without debounce
    updateScreenSize();

    // Add event listener with debounced handler
    window.addEventListener('resize', debouncedHandler);

    // Cleanup
    return () => window.removeEventListener('resize', debouncedHandler);
  }, [debounceMs, updateScreenSize]);

  return {
    screenSize,
    isSm: screenSize === 'sm',
    isMd: screenSize === 'md',
    isLg: screenSize === 'lg',
    isXl: screenSize === 'xl',
    is2Xl: screenSize === '2xl',
    // Helper methods for comparisons
    isAtLeast: (size: ScreenSize) => {
      const breakpoints: ScreenSize[] = ['sm', 'md', 'lg', 'xl', '2xl'];
      const currentIndex = breakpoints.indexOf(screenSize);
      const targetIndex = breakpoints.indexOf(size);
      return currentIndex >= targetIndex;
    },
    isAtMost: (size: ScreenSize) => {
      const breakpoints: ScreenSize[] = ['sm', 'md', 'lg', 'xl', '2xl'];
      const currentIndex = breakpoints.indexOf(screenSize);
      const targetIndex = breakpoints.indexOf(size);
      return currentIndex <= targetIndex;
    },
  };
};
