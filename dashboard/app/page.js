"use client";

import { useEffect, useState } from "react";

const API_KEY = "sfk_acme_dev";

async function getJSON(path) {
  const res = await fetch(path, {
    headers: { "X-API-Key": API_KEY },
    cache: "no-store",
  });
  if (!res.ok) throw new Error(`${path} -> ${res.status}`);
  return res.json();
}

export default function Page() {
  const [health, setHealth] = useState(null);
  const [connections, setConnections] = useState([]);
  const [error, setError] = useState(null);

  async function refresh() {
    try {
      const [h, conns] = await Promise.all([
        getJSON("/health"),
        getJSON("/api/v1/connections"),
      ]);
      setHealth(h);
      setConnections(conns.connections || []);
      setError(null);
    } catch (e) {
      setError(String(e.message || e));
    }
  }

  useEffect(() => {
    refresh();
    const id = setInterval(refresh, 5000);
    return () => clearInterval(id);
  }, []);

  return (
    <div>
      <h1>SyncForge — Operational Dashboard</h1>
      {error && <div className="card err">Cannot reach API: {error}</div>}

      <div className="card">
        <div className="row">
          <div className="metric">
            <div className="v ok">{health ? health.status : "…"}</div>
            <div>API status</div>
          </div>
          <div className="metric">
            <div className="v">{health && health.checks ? health.checks.database : "…"}</div>
            <div>Database</div>
          </div>
          <div className="metric">
            <div className="v">{health && health.checks ? health.checks.redis : "…"}</div>
            <div>Redis</div>
          </div>
          <div className="metric">
            <div className="v">{connections.length}</div>
            <div>Connections</div>
          </div>
        </div>
      </div>

      <div className="card">
        <h1 style={{ fontSize: 16 }}>Connections</h1>
        <table>
          <thead>
            <tr>
              <th>Name</th>
              <th>Provider</th>
              <th>Status</th>
              <th>Base URL</th>
            </tr>
          </thead>
          <tbody>
            {connections.map((c) => (
              <tr key={c.id}>
                <td>{c.name}</td>
                <td>{c.provider}</td>
                <td>
                  <span className="dot" style={{ background: c.status === "healthy" ? "#4ade80" : "#facc15" }} />
                  {c.status}
                </td>
                <td>{c.base_url}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
