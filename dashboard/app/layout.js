export default function RootLayout({ children }) {
  return (
    <html lang="en">
      <head>
        <meta charSet="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>SyncForge</title>
        <style>{`
          body { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; background: #0f172a; color: #e2e8f0; margin: 0; padding: 24px; }
          h1 { font-size: 20px; }
          .card { background: #1e293b; border: 1px solid #334155; border-radius: 8px; padding: 16px; margin: 12px 0; }
          .ok { color: #4ade80; }
          .err { color: #f87171; }
          .row { display: flex; gap: 24px; flex-wrap: wrap; }
          .metric { min-width: 140px; }
          .metric .v { font-size: 22px; }
          table { border-collapse: collapse; width: 100%; }
          th, td { text-align: left; padding: 6px 10px; border-bottom: 1px solid #334155; }
          .dot { display:inline-block; width:10px; height:10px; border-radius:50%; margin-right:6px; }
        `}</style>
      </head>
      <body>{children}</body>
    </html>
  );
}
