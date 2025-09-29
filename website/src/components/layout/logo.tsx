import { cn } from '@/lib/utils';
import { withBase } from '@/lib/withBase';

interface LogoProps {
  className?: string;
  href?: string;
  size?: 'default' | 'xxxxl';
}

export default function Logo({
  className,
  href = '/',
  size = 'default',
}: LogoProps) {
  return (
    <a
      href={withBase(href)}
      className={cn(
        'group flex items-baseline font-mono font-extrabold w-24',
        size === 'xxxxl' && 'w-[1300px]',
        className,
      )}
    >
      <span className={cn('text-2xl', size === 'xxxxl' && 'text-[300px]')}>
        G
      </span>
      <div
        className={cn(
          'w-0 overflow-hidden group-hover:w-12 transition-all duration-300',
          size === 'xxxxl' && 'group-hover:w-[1300px]',
        )}
      >
        <span
          className={cn(
            'text-xl text-muted-foreground whitespace-pre',
            size === 'xxxxl' && 'text-[300px]',
          )}
        >
          roup
        </span>
      </div>
      <span className={cn('text-2xl', size === 'xxxxl' && 'text-[300px]')}>
        MQ
      </span>
    </a>
  );
}
