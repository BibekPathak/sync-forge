"use client";

import { useEffect, useState } from "react";

const DEMO_EMAIL = "admin@acme.dev";
const DEMO_PASSWORD = "syncforge-demo";
const TENANT_SLUG = "acme";

// A signed session token replaces the hardcoded API key: the dashboard logs in
// as the seeded demo user and authenticates every request with the Bearer
// token, so no static credential ships in the client bundle.
async function login() {
  const res = await fetch("/api/v1/auth/login", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      tenant_slug: TENANT_SLUG,
      email: DEMO_EMAIL,
      password: DEMO_PASSWORD,
    }),
  });
  if (!res.ok) throw new Error(`login -> ${res.status}`);
  const data = await res.json();
  return data.token;
}

let _tokenPromise = null;
async function getJSON(path, retry = true) {
  if (!_tokenPromise) {
    _tokenPromise = login().catch((e) => {
      _tokenPromise = null;
      throw e;
    });
  }
  const token = await _tokenPromise;
  const res = await fetch(path, {
    headers: { Authorization: `Bearer ${token}` },
    cache: "no-store",
  });
  // The session token expires after 12h; on a 401 re-login once and retry so
  // the dashboard self-heals instead of erroring until a page reload.
  if (res.status === 401 && retry) {
    _tokenPromise = null;
    return getJSON(path, false);
  }
  if (!res.ok) throw new Error(`${path} -> ${res.status}`);
  return res.json();
}

function fmtTime(v) {
  if (!v) return "—";
  const d = new Date(v);
  return isNaN(d.getTime()) ? String(v) : d.toLocaleString();
}

function statusColor(s) {
  if (!s) return "#9ca3af";
  const t = s.toLowerCase();
  if (t.includes("pending") || t.includes("running") || t.includes("failed") || t.includes("dlq")) return "#f87171";
  if (t.includes("resolved") || t.includes("completed") || t.includes("healthy") || t.includes("applied")) return "#4ade80";
  if (t.includes("auto") || t.includes("retry") || t.includes("validated") || t.includes("received")) return "#facc15";
  return "#9ca3af";
}

function Table({ cols, rows, empty = "No data" }) {
  if (!rows || rows.length === 0) return <div className="empty">{empty}</div>;
  return (
    <table>
      <thead>
        <tr>{cols.map((c) => <th key={c.key}>{c.label}</th>)}</tr>
      </thead>
      <tbody>
        {rows.map((r, i) => (
          <tr key={r.id ?? i}>
            {cols.map((c) => (
              <td key={c.key}>
                {c.render ? c.render(r) : r[c.key] != null ? String(r[c.key]) : "—"}
              </td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function StatusBadge({ value }) {
  return (
    <span className="badge" style={{ background: statusColor(value) }}>{value || "—"}</span>
  );
}

function Card({ title, count, children }) {
  return (
    <div className="card">
      <div className="card-head">
        <h2>{title}</h2>
        {count != null && <span className="count">{count}</span>}
      </div>
      {children}
    </div>
  );
}

export default function Page() {
  const [health, setHealth] = useState(null);
  const [connections, setConnections] = useState([]);
  const [conflicts, setConflicts] = useState([]);
  const [runs, setRuns] = useState([]);
  const [findings, setFindings] = useState([]);
  const [dlq, setDlq] = useState([]);
  const [events, setEvents] = useState([]);
  const [audit, setAudit] = useState([]);
  const [ops, setOps] = useState([]);
  const [jobs, setJobs] = useState([]);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(true);

  async function refresh() {
    try {
      const [h, conns, conf, rns, dq, evs, au, op, jb] = await Promise.all([
        getJSON("/health"),
        getJSON("/api/v1/connections"),
        getJSON("/api/v1/conflicts?status=CONFLICT_PENDING&limit=20"),
        getJSON("/api/v1/reconciliations?limit=5"),
        getJSON("/api/v1/dlq?limit=20"),
        getJSON("/api/v1/sync-events?limit=10"),
        getJSON("/api/v1/audit?limit=15"),
        getJSON("/api/v1/operations?limit=15"),
        getJSON("/api/v1/sync-jobs?limit=10"),
      ]);
      setHealth(h);
      setConnections(conns.connections || []);
      setConflicts(conf.items || []);
      setRuns(rns.items || []);
      setDlq(dq.items || []);
      setEvents(evs.events || []);
      setAudit(au.items || []);
      setOps(op.items || []);
      setJobs(jb.jobs || []);
      // Findings from the most recent reconciliation run.
      const latest = (rns.items || [])[0];
      if (latest && latest.id) {
        const f = await getJSON(`/api/v1/reconciliations/${latest.id}/findings?limit=20`);
        setFindings(f.items || []);
      } else {
        setFindings([]);
      }
      setError(null);
    } catch (e) {
      setError(String(e.message || e));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    refresh();
    const id = setInterval(refresh, 5000);
    return () => clearInterval(id);
  }, []);

  const pendingConflicts = conflicts.filter((c) => c.status === "CONFLICT_PENDING").length;

  return (
    <div>
      <h1>SyncForge — Operational Dashboard</h1>
      {error && <div className="card err">Cannot reach API: {error}</div>}
      {loading && <div className="card"><div className="empty">Loading…</div></div>}

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
          <div className="metric">
            <div className="v warn">{pendingConflicts}</div>
            <div>Pending conflicts</div>
          </div>
          <div className="metric">
            <div className="v">{dlq.length}</div>
            <div>DLQ entries</div>
          </div>
        </div>
      </div>

      <Card title="Connections" count={connections.length}>
        <Table
          cols={[
            { key: "name", label: "Name" },
            { key: "provider", label: "Provider" },
            { key: "status", label: "Status", render: (r) => <StatusBadge value={r.status} /> },
            { key: "base_url", label: "Base URL" },
          ]}
          rows={connections}
        />
      </Card>

      <Card title="Pending conflicts" count={pendingConflicts}>
        <Table
          cols={[
            { key: "entity_id", label: "Entity" },
            { key: "source_a", label: "Source A" },
            { key: "source_b", label: "Source B" },
            { key: "resolution_strategy", label: "Strategy" },
            { key: "detected_at", label: "Detected", render: (r) => fmtTime(r.detected_at) },
          ]}
          rows={conflicts}
        />
      </Card>

      <Card title="Reconciliation runs" count={runs.length}>
        <Table
          cols={[
            { key: "source", label: "Source" },
            { key: "mode", label: "Mode" },
            { key: "status", label: "Status", render: (r) => <StatusBadge value={r.status} /> },
            { key: "total", label: "Total" },
            { key: "drift", label: "Drift" },
            { key: "missed", label: "Missed" },
            { key: "deleted", label: "Deleted" },
            { key: "finished_at", label: "Finished", render: (r) => fmtTime(r.finished_at) },
          ]}
          rows={runs}
        />
      </Card>

      <Card title="Reconciliation findings (latest run)" count={findings.length}>
        <Table
          cols={[
            { key: "kind", label: "Kind" },
            { key: "provider_id", label: "Provider ID" },
            { key: "direction", label: "Direction" },
            { key: "status", label: "Status", render: (r) => <StatusBadge value={r.status} /> },
            { key: "error", label: "Error" },
            { key: "created_at", label: "Created", render: (r) => fmtTime(r.created_at) },
          ]}
          rows={findings}
        />
      </Card>

      <Card title="Dead-letter queue" count={dlq.length}>
        <Table
          cols={[
            { key: "event_id", label: "Event ID" },
            { key: "error_class", label: "Error class" },
            { key: "reason", label: "Reason" },
            { key: "status", label: "Status", render: (r) => <StatusBadge value={r.status} /> },
            { key: "created_at", label: "Created", render: (r) => fmtTime(r.created_at) },
          ]}
          rows={dlq}
        />
      </Card>

      <Card title="Recent sync events" count={events.length}>
        <Table
          cols={[
            { key: "event_id", label: "Event ID" },
            { key: "source", label: "Source" },
            { key: "event_type", label: "Type" },
            { key: "status", label: "Status", render: (r) => <StatusBadge value={r.status} /> },
            { key: "received_at", label: "Received", render: (r) => fmtTime(r.received_at) },
          ]}
          rows={events}
        />
      </Card>

      <Card title="Sync jobs" count={jobs.length}>
        <Table
          cols={[
            { key: "id", label: "ID" },
            { key: "type", label: "Type" },
            { key: "status", label: "Status", render: (r) => <StatusBadge value={r.status} /> },
            { key: "processed", label: "Processed" },
            { key: "failed", label: "Failed" },
            { key: "created_at", label: "Created", render: (r) => fmtTime(r.created_at) },
          ]}
          rows={jobs}
        />
      </Card>

      <Card title="Applied writes ledger" count={ops.length}>
        <Table
          cols={[
            { key: "entity_id", label: "Entity" },
            { key: "target_source", label: "Target" },
            { key: "applied_version", label: "Version" },
            { key: "fingerprint", label: "Fingerprint" },
            { key: "created_at", label: "Written", render: (r) => fmtTime(r.created_at) },
          ]}
          rows={ops}
        />
      </Card>

      <Card title="Audit log" count={audit.length}>
        <Table
          cols={[
            { key: "actor", label: "Actor" },
            { key: "action", label: "Action" },
            { key: "resource", label: "Resource" },
            { key: "resource_id", label: "Resource ID" },
            { key: "created_at", label: "At", render: (r) => fmtTime(r.created_at) },
          ]}
          rows={audit}
        />
      </Card>
    </div>
  );
}
