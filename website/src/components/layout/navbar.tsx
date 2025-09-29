'use client';

import { ChevronDown } from 'lucide-react';
import { AnimatePresence, motion } from 'motion/react';
import * as React from 'react';
import { FaGithub } from 'react-icons/fa6';

import { ThemeToggle } from '@/components/elements/theme-toggle';
import Logo from '@/components/layout/logo';
import { Button } from '@/components/ui/button';
import {
  NavigationMenu,
  NavigationMenuContent,
  NavigationMenuItem,
  NavigationMenuLink,
  NavigationMenuList,
  NavigationMenuTrigger,
  navigationMenuTriggerStyle,
} from '@/components/ui/navigation-menu';
import { EXTERNAL_LINKS } from '@/constants/external-links';
import { useMediaQuery } from '@/hooks/use-media-query';
import { cn } from '@/lib/utils';
import { withBase } from '@/lib/withBase';

type NavItem = {
  title: string;
  href?: string;
  subitems?: Array<{
    title: string;
    items: Array<{
      title: string;
      href: string;
      description?: string;
      icon?: React.ComponentType<{ className?: string }>;
      isHighlighted?: boolean;
    }>;
  }>;
};

const navigationItems: NavItem[] = [
  { title: 'Home', href: withBase('/') },
  { title: 'Docs', href: withBase('/docs') },
  { title: 'Articles', href: withBase('/blog') },
];

interface NavbarProps {
  currentPage?: string;
}

function Navbar({ currentPage }: NavbarProps) {
  const [isMenuOpen, setIsMenuOpen] = React.useState(false);
  const { isAtMost } = useMediaQuery();
  const isMobile = isAtMost('md');
  const [theme, setTheme] = React.useState<'light' | 'dark'>('light');

  const isMenuColorInverted = isMenuOpen && isMobile;

  React.useEffect(() => {
    // Get initial theme from localStorage, default to 'light' if none exists
    const savedTheme = localStorage.getItem('theme') as 'light' | 'dark' | null;
    setTheme(savedTheme || 'light');

    // Listen for theme changes
    const handleStorageChange = () => {
      const newTheme = localStorage.getItem('theme') as 'light' | 'dark' | null;
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

  React.useEffect(() => {
    if (isMenuOpen && isMobile) {
      document.documentElement.style.overflow = 'hidden';
      document.body.style.overflow = 'hidden';
    } else {
      document.body.style.overflow = 'unset';
      document.documentElement.style.overflow = 'unset';
    }

    return () => {
      document.body.style.overflow = 'unset';
      document.documentElement.style.overflow = 'unset';
    };
  }, [isMenuOpen, isMobile]);

  return (
    <header
      className={cn(
        'border-b transition-all duration-300',
        isMenuColorInverted
          ? theme === 'dark'
            ? 'light bg-foreground text-background [&_*]:border-border/30'
            : 'dark bg-background text-foreground'
          : '',
      )}
    >
      <div className="container px-4">
        <div
          className={cn(
            'flex items-center border-x py-4 lg:border-none lg:py-6',
          )}
        >
          <Logo
            className={cn(
              'ps-6 transition-all duration-300 lg:ps-0',
              isMenuColorInverted
                ? theme === 'dark'
                  ? '[&>img]:invert-0'
                  : '[&>img]:invert'
                : 'dark:[&>img]:invert',
            )}
          />

          {/* Hamburger Menu Button (Mobile Only) */}
          <div className="me-6 ml-auto flex flex-1 items-center justify-end lg:me-0 lg:hidden">
            <ThemeToggle />

            <Button
              variant="outline"
              size="icon"
              className={cn('relative flex !bg-transparent')}
              onClick={() => setIsMenuOpen(!isMenuOpen)}
            >
              <span className="sr-only">Open main menu</span>
              <div className="absolute top-1/2 left-1/2 block w-[18px] -translate-x-1/2 -translate-y-1/2">
                <span
                  aria-hidden="true"
                  className={cn(
                    'absolute block h-0.5 w-full rounded-full bg-current transition-transform duration-500 ease-in-out',
                    isMenuOpen ? 'rotate-45' : '-translate-y-1.5',
                  )}
                ></span>
                <span
                  aria-hidden="true"
                  className={cn(
                    'absolute block h-0.5 w-full rounded-full bg-current transition-transform duration-500 ease-in-out',
                    isMenuOpen ? 'opacity-0' : '',
                  )}
                ></span>
                <span
                  aria-hidden="true"
                  className={cn(
                    'absolute block h-0.5 w-full rounded-full bg-current transition-transform duration-500 ease-in-out',
                    isMenuOpen ? '-rotate-45' : 'translate-y-1.5',
                  )}
                ></span>
              </div>
            </Button>
          </div>
          {/* Desktop Navigation */}
          <div className="ms-8 hidden flex-1 items-center justify-between lg:flex">
            <NavigationMenu>
              <NavigationMenuList className="gap-2">
                {navigationItems.map((item) => (
                  <DesktopNavItem
                    key={item.title}
                    item={item}
                    currentPage={currentPage}
                  />
                ))}
              </NavigationMenuList>
            </NavigationMenu>

            <NavBarAction />
          </div>

          {/* Mobile Navigation */}
          <AnimatePresence>
            {isMenuOpen && (
              <motion.div
                initial={{ opacity: 0, y: -20 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -20 }}
                transition={{
                  duration: 0.4,
                  ease: [0.25, 0.46, 0.45, 0.94], // Custom cubic-bezier for smooth feel
                }}
                className={cn(
                  'fixed inset-0 top-16 z-50 container flex flex-col overflow-hidden text-sm font-medium lg:hidden',
                  isMenuColorInverted
                    ? theme === 'dark'
                      ? 'light bg-foreground text-background'
                      : 'dark bg-background text-foreground'
                    : '',
                )}
              >
                <motion.div
                  initial={{ opacity: 0, y: -10 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ delay: 0.1, duration: 0.3 }}
                >
                  <NavBarAction />
                </motion.div>

                <motion.div
                  className="bordered-div-padding flex flex-1 flex-col space-y-3 overflow-y-auto border-x"
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 1 }}
                  transition={{ delay: 0.15, duration: 0.3 }}
                >
                  {navigationItems.map((item, index) => (
                    <motion.div
                      key={item.title}
                      initial={{ opacity: 0, y: -20 }}
                      animate={{ opacity: 1, y: 0 }}
                      transition={{
                        delay: 0.2 + index * 0.05,
                        duration: 0.3,
                        ease: 'easeOut',
                      }}
                    >
                      <MobileNavItem
                        item={item}
                        setIsMenuOpen={setIsMenuOpen}
                      />
                    </motion.div>
                  ))}
                </motion.div>

                <motion.div
                  className="border border-b-0 p-8"
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 1 }}
                  transition={{ delay: 0.3, duration: 0.2 }}
                />
              </motion.div>
            )}
          </AnimatePresence>
        </div>
      </div>
    </header>
  );
}

const NavBarAction = () => {
  return (
    <div className="bordered-div-padding flex items-center justify-between border lg:border-none lg:!p-0 gap-4">
      <ThemeToggle />
      <a href={EXTERNAL_LINKS.GITHUB_REPO} className="flex items-center">
        <Button
          variant="ghost"
          className="gap-2 font-medium lg:text-base"
          size="sm"
        >
          <FaGithub className="size-5" />
          <span className="hidden">1k</span>
        </Button>
      </a>
    </div>
  );
};

function MobileNavItem({
  item,
  setIsMenuOpen,
}: {
  item: NavItem;
  setIsMenuOpen: (isMenuOpen: boolean) => void;
}) {
  const [isOpen, setIsOpen] = React.useState(false);
  const sectionId = React.useId();

  if (!item.subitems) {
    return (
      <a
        href={item.href!}
        className="block"
        onClick={() => setIsMenuOpen(false)}
      >
        <Button variant="ghost" size="sm">
          {item.title}
        </Button>
      </a>
    );
  }

  return (
    <div>
      <div className="flex w-full items-center justify-between">
        <Button
          variant="ghost"
          size="sm"
          onClick={() => setIsOpen(!isOpen)}
          aria-expanded={isOpen}
          aria-controls={sectionId}
        >
          {item.title}
        </Button>
        <ChevronDown
          className={`h-5 w-5 transition-transform duration-200 ${
            isOpen ? 'rotate-180' : ''
          }`}
        />
      </div>

      <AnimatePresence>
        {isOpen && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{
              height: 'auto',
              opacity: 1,
            }}
            exit={{
              height: 0,
              opacity: 0,
            }}
            transition={{
              duration: 0.3,
              ease: [0.25, 0.46, 0.45, 0.94],
              opacity: { duration: 0.2 },
            }}
            style={{ overflow: 'hidden' }}
            id={sectionId}
          >
            <motion.div
              className="mt-3"
              initial={{ y: -10 }}
              animate={{ y: 0 }}
              transition={{ delay: 0.1, duration: 0.2 }}
            >
              {item.subitems.flatMap((section, sectionIndex) =>
                section.items.map((subitem, itemIndex) => (
                  <motion.div
                    key={subitem.title}
                    initial={{ opacity: 0, x: -15 }}
                    animate={{ opacity: 1, x: 0 }}
                    transition={{
                      delay:
                        0.15 +
                        (sectionIndex * section.items.length + itemIndex) *
                          0.03,
                      duration: 0.25,
                      ease: 'easeOut',
                    }}
                  >
                    <a
                      href={withBase(subitem.href)}
                      className="text-muted-foreground hover:text-foreground flex items-center gap-3 p-3 transition-colors duration-200"
                    >
                      {subitem.icon && <subitem.icon className="size-4.5" />}
                      <span className="">{subitem.title}</span>
                    </a>
                  </motion.div>
                )),
              )}
            </motion.div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}

function DesktopNavItem({
  item,
  currentPage,
}: {
  item: NavItem;
  currentPage?: string;
}) {
  if (!item.subitems) {
    return (
      <NavigationMenuItem className="">
        <a
          href={item.href!}
          className={cn(
            navigationMenuTriggerStyle(),
            'text-base font-medium',
            currentPage === item.href && 'text-base',
          )}
        >
          {item.title}
        </a>
      </NavigationMenuItem>
    );
  }

  return (
    <NavigationMenuItem>
      <NavigationMenuTrigger className="text-base font-medium">
        {item.title}
      </NavigationMenuTrigger>
      <NavigationMenuContent className="p-3">
        <div className="grid w-[min(1173px,70vw)] grid-cols-3 gap-6">
          {item.subitems.map((section) => (
            <div key={section.title} className="">
              <div className="text-muted-foreground p-3 text-base font-medium">
                {section.title}
              </div>
              <ul className="">
                {section.items.map((subitem) => (
                  <ListItem
                    key={subitem.title}
                    title={subitem.title}
                    href={withBase(subitem.href)}
                    icon={subitem.icon}
                    isHighlighted={subitem.isHighlighted}
                  >
                    {subitem.description}
                  </ListItem>
                ))}
              </ul>
            </div>
          ))}
        </div>
      </NavigationMenuContent>
    </NavigationMenuItem>
  );
}

const ListItem = React.forwardRef<
  React.ElementRef<'a'>,
  React.ComponentPropsWithoutRef<'a'> & {
    title: string;
    icon?: React.ComponentType<{ className?: string }>;
    isHighlighted?: boolean;
  }
>(
  (
    { className, title, children, icon: Icon, isHighlighted, ...props },
    ref,
  ) => {
    return (
      <li>
        <NavigationMenuLink asChild>
          <a
            ref={ref}
            className={cn(
              'hover:bg-accent group hover:text-accent-foreground focus:bg-accent focus:text-accent-foreground block space-y-1 rounded-md border-none p-3 leading-none no-underline transition-colors select-none',
              className,
            )}
            {...props}
          >
            <div className="flex items-center gap-3">
              <div
                className={cn(
                  'flex size-8 shrink-0 items-center justify-center rounded-md',
                  isHighlighted &&
                    'bg-secondary [&>svg]:!text-secondary-foreground',
                )}
              >
                {Icon && <Icon className="text-foreground size-4" />}
              </div>
              <div>
                <div className="leading-none font-medium">{title}</div>
                <p className="text-muted-foreground group-hover:text-foreground mt-1 text-sm transition-colors">
                  {children}
                </p>
              </div>
            </div>
          </a>
        </NavigationMenuLink>
      </li>
    );
  },
);
ListItem.displayName = 'ListItem';

export default Navbar;
