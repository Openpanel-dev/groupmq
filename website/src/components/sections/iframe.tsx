export default function Iframe() {
  return (
    <div className="overflow-hidden rounded-lg border border-border bg-background shadow-lg">
      <div className="flex items-center gap-2 px-4 py-2 border-b border-border bg-muted/50 h-12">
        <div className="flex gap-1.5">
          <div className="w-3 h-3 rounded-full bg-red-500"></div>
          <div className="w-3 h-3 rounded-full bg-yellow-500"></div>
          <div className="w-3 h-3 rounded-full bg-green-500"></div>
        </div>
        <a
          target="_blank"
          rel="noreferrer noopener nofollow"
          href="https://demo.openpanel.dev/demo/shoey"
          className="group flex-1 mx-4 px-3 py-1 text-sm bg-background rounded-md border border-border flex items-center gap-2"
        >
          <span className="text-muted-foreground flex-1">
            https://demo.openpanel.dev
          </span>
          <svg
            xmlns="http://www.w3.org/2000/svg"
            width="24"
            height="24"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
            className="lucide lucide-arrow-right size-4 opacity-0 group-hover:opacity-100 transition-opacity"
            aria-hidden="true"
          >
            <path d="M5 12h14"></path>
            <path d="m12 5 7 7-7 7"></path>
          </svg>
        </a>
      </div>
      <iframe
        src="https://demo.openpanel.dev/demo/shoey?range=lastHour"
        className="w-full h-full aspect-video"
        title="Live preview"
        scrolling="no"
      ></iframe>
    </div>
  );
}
