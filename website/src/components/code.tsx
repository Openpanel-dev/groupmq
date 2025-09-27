import { highlight } from 'sugar-high';

export function Code({ children }: { children: string }) {
  return (
    <pre
      className="text-sm font-mono leading-relaxed whitespace-pre-wrap"
      dangerouslySetInnerHTML={{
        __html: highlight(children),
      }}
    />
  );
}
