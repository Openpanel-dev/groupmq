'use client';

import { useEffect, useRef } from 'react';

import { useMediaQuery } from '@/hooks/use-media-query';
import { cn } from '@/lib/utils';

interface PlusSignsProps {
  className?: string;
  plusSize?: number;
  rotationAngle?: number;
  spacing?: number;
}

export const PlusSigns = ({
  className,
  plusSize = 6,
  spacing = 16,
}: PlusSignsProps) => {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const animationRef = useRef<number>(null);
  const strokeWidth = 4;
  const { screenSize } = useMediaQuery();

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;

    const ctx = canvas.getContext('2d');
    if (!ctx) return;

    // Animation configuration
    const BREATH_CYCLE_DURATION = 8000; // 8 seconds per breathing cycle
    const STAGGER_DELAY = 150; // 150ms stagger between each plus sign
    const MAX_OPACITY = 0.07; // Maximum opacity for the plus signs
    const MIN_OPACITY_THRESHOLD = 0.001; // Minimum opacity to consider drawing

    // Create a cache of random base opacities that remain consistent
    const opacityCache = new Map();
    const getBaseOpacity = (x: number, y: number) => {
      const key = `${x},${y}`;
      if (!opacityCache.has(key)) {
        // 70% chance of having a visible plus sign, 30% chance of being nearly invisible
        opacityCache.set(
          key,
          Math.random() < 0.7 ? Math.random() * MAX_OPACITY : 0,
        );
      }
      return opacityCache.get(key);
    };

    // Animation timing function (cubic ease-in-out)
    const easeInOutCubic = (t: number) => {
      return t < 0.5 ? 4 * t * t * t : 1 - Math.pow(-2 * t + 2, 3) / 2;
    };

    // Get staggered animation progress based on position
    const getStaggeredProgress = (x: number, y: number, time: number) => {
      // Create a consistent stagger based on position
      const staggerOffset = (x * 7919 + y * 104729) % 1000; // Use prime numbers to avoid patterns
      const staggeredTime =
        (time + staggerOffset * STAGGER_DELAY) % BREATH_CYCLE_DURATION;
      return staggeredTime / BREATH_CYCLE_DURATION;
    };

    // Calculate breathing effect multiplier (0-1 range, sine wave)
    const getBreathingMultiplier = (progress: number) => {
      // Sine wave that goes from 0.6 to 1.0 for a gentle breathing effect
      return 0.6 + 0.4 * Math.sin(progress * Math.PI * 2);
    };

    // Calculate fade-in effect (0-1 range)
    const getFadeInMultiplier = (time: number) => {
      const fadeInDuration = 2000; // 2 second fade-in
      const fadeInProgress = Math.min(time / fadeInDuration, 1);
      return easeInOutCubic(fadeInProgress);
    };

    let startTime: number | null = null;

    const drawPattern = (timestamp: number) => {
      if (!startTime) startTime = timestamp;
      const elapsedTime = timestamp - startTime;
      const fadeInMultiplier = getFadeInMultiplier(elapsedTime);

      // Get parent container dimensions
      const rect = canvas.getBoundingClientRect();
      const width = window.innerWidth;
      const height = rect.height;

      // Ensure we have valid dimensions
      if (width <= 0 || height <= 0) return;

      // Set canvas dimensions with device pixel ratio for crisp rendering
      const dpr = window.devicePixelRatio || 1;
      canvas.width = width * dpr;
      canvas.height = height * dpr;
      canvas.style.width = width + 'px';
      canvas.style.height = height + 'px';

      // Reset transform and clear canvas before scaling
      ctx.setTransform(1, 0, 0, 1, 0, 0);
      ctx.clearRect(0, 0, canvas.width, canvas.height);

      // Apply scaling after clearing
      ctx.scale(dpr, dpr);

      // Use CSS custom property for color
      const computedStyle = getComputedStyle(canvas);
      const color = computedStyle.getPropertyValue('--muted-foreground').trim();

      // Fallback color if CSS variable is not available
      if (!color || color === '') {
        ctx.fillStyle = 'rgba(156, 163, 175, 0.1)'; // gray-400 with low opacity
      } else {
        ctx.fillStyle = color;
      }

      // Calculate grid dimensions
      const cols = Math.ceil(width / spacing);
      const rows = Math.ceil(height / spacing);

      // Draw plus signs in a grid pattern
      for (let row = 0; row < rows; row++) {
        for (let col = 0; col < cols; col++) {
          const x = col * spacing + spacing / 2;
          const y = row * spacing + spacing / 2;

          // Skip if outside canvas bounds
          if (x > width || y > height) continue;

          // Get the base opacity for this position
          const baseOpacity = getBaseOpacity(x, y);

          // Skip drawing if the base opacity is too low
          if (baseOpacity < MIN_OPACITY_THRESHOLD) continue;

          // Get the animation progress with staggered timing
          const progress = getStaggeredProgress(x, y, elapsedTime);

          // Apply breathing effect
          const breathingMultiplier = getBreathingMultiplier(progress);

          // Combine all effects for final opacity
          const finalOpacity =
            baseOpacity * breathingMultiplier * fadeInMultiplier;

          ctx.save();
          ctx.translate(x, y);
          ctx.globalAlpha = finalOpacity;

          // Draw plus sign with configurable stroke
          const halfStroke = strokeWidth / 2;
          ctx.fillRect(-plusSize, -halfStroke, plusSize * 2, strokeWidth);
          ctx.fillRect(-halfStroke, -plusSize, strokeWidth, plusSize * 2);

          ctx.restore();
        }
      }

      // Continue animation loop
      animationRef.current = requestAnimationFrame(drawPattern);
    };

    // Start the animation
    animationRef.current = requestAnimationFrame(drawPattern);

    // Cleanup function
    return () => {
      if (animationRef.current) {
        cancelAnimationFrame(animationRef.current);
      }
    };
  }, [plusSize, strokeWidth, spacing, screenSize]);

  return (
    <canvas ref={canvasRef} className={cn('pointer-events-none', className)} />
  );
};
