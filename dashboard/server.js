// Tiny static server for the SyncForge dashboard. Serves the Next.js static
// export and proxies /api/* to the SyncForge API service.
const http = require("http");
const fs = require("fs");
const path = require("path");

const PORT = process.env.PORT || 3001;
const API_BASE = process.env.API_BASE || "http://localhost:8080";
const OUT = path.join(__dirname, "out");

const MIME = {
  ".html": "text/html",
  ".js": "text/javascript",
  ".css": "text/css",
  ".json": "application/json",
  ".png": "image/png",
  ".svg": "image/svg+xml",
  ".ico": "image/x-icon",
};

const server = http.createServer((req, res) => {
  if (req.url.startsWith("/api/") || req.url === "/health" || req.url.startsWith("/health?")) {
    proxy(req, res);
    return;
  }
  serveStatic(req, res);
});

function serveStatic(req, res) {
  let p = decodeURIComponent(req.url.split("?")[0]);
  if (p === "/") p = "/index.html";
  const file = path.join(OUT, p);
  fs.readFile(file, (err, data) => {
    if (err) {
      res.writeHead(404, { "Content-Type": "text/html" });
      res.end("Not found");
      return;
    }
    res.writeHead(200, { "Content-Type": MIME[path.extname(file)] || "application/octet-stream" });
    res.end(data);
  });
}

function proxy(req, res) {
  const upstream = http.request(
    API_BASE + req.url,
    { method: req.method, headers: req.headers },
    (ures) => {
      res.writeHead(ures.statusCode, ures.headers);
      ures.pipe(res);
    }
  );
  upstream.on("error", () => {
    res.writeHead(502, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ error: "upstream unavailable" }));
  });
  req.pipe(upstream);
}

server.listen(PORT, () => {
  console.log(`dashboard listening on :${PORT}, API_BASE=${API_BASE}`);
});
