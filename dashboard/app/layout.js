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
          .card-head { display: flex; align-items: center; justify-content: space-between; margin-bottom: 8px; }
          .card-head h2 { font-size: 15px; margin: 0; text-transform: uppercase; letter-spacing: .04em; color: #94a3b8; }
          .count { background: #334155; border-radius: 999px; padding: 2px 10px; font-size: 13px; color: #cbd5e1; }
          .ok { color: #4ade80; }
          .warn { color: #f87171; }
          .err { color: #f87171; }
          .row { display: flex; gap: 24px; flex-wrap: wrap; }
          .metric { min-width: 140px; }
          .metric .v { font-size: 22px; }
          .empty { color: #64748b; padding: 12px 0; }
          table { border-collapse: collapse; width: 100%; }
          th, td { text-align: left; padding: 6px 10px; border-bottom: 1px solid #334155; }
          th { color: #94a3b8; font-size: 12px; text-transform: uppercase; letter-spacing: .03em; }
          .badge { display: inline-block; border-radius: 4px; padding: 1px 8px; font-size: 12px; color: #0f172a; font-weight: 600; }
          .dot { display:inline-block; width:10px; height:10px; border-radius:50%; margin-right:6px; }
        `}</style>
      </head>
      <body>{children}</body>
    </html>
  );
}
