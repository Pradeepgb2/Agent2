import { useEffect, useMemo, useState } from "react";
import { apiGet, buildQuery } from "./api";
import "./App.css";

const NAV_ITEMS = [
  { key: "dashboard", label: "Dashboard", icon: "\u25A4" },
  { key: "signals", label: "Signals", icon: "\u2261" },
  { key: "export-history", label: "Export History", icon: "\u25A2" },
  { key: "settings", label: "Settings", icon: "\u2699" },
];

function ComingSoon({ label }) {
  return (
    <div className="placeholder">
      <h2>{label}</h2>
      <p>This page is coming soon.</p>
    </div>
  );
}

export default function App() {
  const [page, setPage] = useState("dashboard");

  const [weeks, setWeeks] = useState([]);
  const [week, setWeek] = useState("");
  const [summary, setSummary] = useState(null);

  const [loadingWeeks, setLoadingWeeks] = useState(false);
  const [loadingSummary, setLoadingSummary] = useState(false);
  const [loadingSignals, setLoadingSignals] = useState(false);
  const [error, setError] = useState("");

  const [signals, setSignals] = useState([]);
  const [total, setTotal] = useState(0);

  const [q, setQ] = useState("");
  const [status, setStatus] = useState("");
  const [company, setCompany] = useState("");
  const [city, setCity] = useState("");

  const [applied, setApplied] = useState({
    q: "",
    status: "",
    company: "",
    city: "",
  });

  const [limit] = useState(50);
  const [offset, setOffset] = useState(0);

  const [copiedMsg, setCopiedMsg] = useState("");

  const queryString = useMemo(() => {
    return buildQuery({
      week,
      q: applied.q,
      status: applied.status,
      company_name: applied.company,
      city: applied.city,
      limit,
      offset,
    });
  }, [week, applied, limit, offset]);

  async function loadWeeks() {
    setError("");
    setLoadingWeeks(true);
    try {
      const data = await apiGet("/weeks");
      const list = data.weeks || [];
      setWeeks(list);
      if (!week && list.length) setWeek(list[0]);
    } catch (e) {
      setError(String(e.message || e));
    } finally {
      setLoadingWeeks(false);
    }
  }

  async function loadSummary(selectedWeek) {
    if (!selectedWeek) return;
    setError("");
    setLoadingSummary(true);
    try {
      const data = await apiGet(`/runs/summary?${buildQuery({ week: selectedWeek })}`);
      setSummary(data);
    } catch (e) {
      setSummary(null);
      setError(String(e.message || e));
    } finally {
      setLoadingSummary(false);
    }
  }

  async function loadSignals() {
    if (!week) return;
    setError("");
    setLoadingSignals(true);
    try {
      const data = await apiGet(`/signals?${queryString}`);
      setSignals(data.items || []);
      setTotal(data.total || 0);
    } catch (e) {
      setSignals([]);
      setTotal(0);
      setError(String(e.message || e));
    } finally {
      setLoadingSignals(false);
    }
  }

  async function refreshAll() {
    await loadSummary(week);
    await loadSignals();
  }

  function applyFilters() {
    setApplied({
      q: q.trim(),
      status,
      company: company.trim(),
      city: city.trim(),
    });
    setOffset(0);
  }

  function clearFilters() {
    setQ("");
    setStatus("");
    setCompany("");
    setCity("");
    setApplied({ q: "", status: "", company: "", city: "" });
    setOffset(0);
  }

  function onExport() {
    const exportQS = buildQuery({
      week,
      q: applied.q,
      status: applied.status,
      company_name: applied.company,
      city: applied.city,
    });
    const base = import.meta.env.VITE_API_BASE_URL;
    window.open(`${base}/signals/export?${exportQS}`, "_blank");
  }

  useEffect(() => {
    loadWeeks().catch(console.error);
  }, []);

  useEffect(() => {
    if (!week) return;
    loadSummary(week).catch(console.error);
    setOffset(0);
  }, [week]);

  useEffect(() => {
    loadSignals().catch(console.error);
  }, [queryString]);

  const isApplyDirty =
    q.trim() !== applied.q ||
    status !== applied.status ||
    company.trim() !== applied.company ||
    city.trim() !== applied.city;

  const hasAnyFilter =
    !!applied.q || !!applied.status || !!applied.company || !!applied.city;

  const pageLabel = NAV_ITEMS.find((n) => n.key === page)?.label || "Dashboard";

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="sidebar-brand">
          <h1>Agent2</h1>
          <p>Recruiting Intelligence Platform</p>
        </div>
        <ul className="nav-list">
          {NAV_ITEMS.map((item) => (
            <li
              key={item.key}
              className={`nav-item ${page === item.key ? "active" : ""}`}
              onClick={() => setPage(item.key)}
            >
              <span className="nav-icon">{item.icon}</span>
              <span>{item.label}</span>
            </li>
          ))}
        </ul>
      </aside>

      <main className="main-content">
        <div className="topbar">
          <h2 className="topbar-title">{pageLabel}</h2>
          <div className="avatar">R</div>
        </div>

        {error && <div className="banner banner-error"><b>Error:</b> {error}</div>}
        {copiedMsg && <div className="banner banner-success">{copiedMsg}</div>}

        {page === "dashboard" && (
          <>
            <div className="stat-row">
              <div className="stat-card">
                <div className="stat-label">Total Rows</div>
                <div className="stat-value blue">
                  {loadingSummary ? "\u2026" : summary?.total_rows_fetched ?? "\u2013"}
                </div>
              </div>
              <div className="stat-card">
                <div className="stat-label">Valid Rows</div>
                <div className="stat-value green">
                  {loadingSummary ? "\u2026" : summary?.valid_rows_processed ?? "\u2013"}
                </div>
              </div>
              <div className="stat-card">
                <div className="stat-label">Skipped Rows</div>
                <div className="stat-value orange">
                  {loadingSummary ? "\u2026" : summary?.invalid_rows_skipped ?? "\u2013"}
                </div>
              </div>
              <div className="stat-card">
                <div className="stat-label">Signals Found</div>
                <div className="stat-value purple">
                  {loadingSummary ? "\u2026" : summary?.signals_count ?? "\u2013"}
                </div>
              </div>
              <div className="stat-card">
                <div className="stat-label">Run Status</div>
                <div className="stat-status">
                  <span className="status-dot" />
                  {loadingSummary ? "\u2026" : (summary?.run_status || summary?.status || "success")}
                </div>
              </div>
            </div>

            <div className="meta-row">
              <span>
                <b>Week:</b>{" "}
                <select value={week} onChange={(e) => setWeek(e.target.value)} disabled={loadingWeeks}>
                  {weeks.map((w) => (
                    <option key={w} value={w}>{w}</option>
                  ))}
                </select>
              </span>
              {loadingWeeks && <span>Loading weeks...</span>}
              <button className="btn btn-outline" onClick={refreshAll} disabled={!week || loadingSummary || loadingSignals}>
                Refresh
              </button>
            </div>

            <div className="panel">
              <div className="panel-header">
                <h3 className="panel-title">Company Change Signals</h3>
                <div className="panel-controls">
                  <input
                    className="input-sm"
                    placeholder="Search (name/company/city/url)"
                    value={q}
                    onChange={(e) => setQ(e.target.value)}
                  />
                  <input
                    className="input-sm"
                    placeholder="Company"
                    value={company}
                    onChange={(e) => setCompany(e.target.value)}
                    style={{ minWidth: 120 }}
                  />
                  <input
                    className="input-sm"
                    placeholder="City"
                    value={city}
                    onChange={(e) => setCity(e.target.value)}
                    style={{ minWidth: 100 }}
                  />
                  <select value={status} onChange={(e) => setStatus(e.target.value)}>
                    <option value="">All Status</option>
                    <option value="company changed">company changed</option>
                  </select>
                  <button className="btn btn-primary" onClick={applyFilters} disabled={!isApplyDirty || loadingSignals}>
                    Apply
                  </button>
                  <button className="btn btn-outline" onClick={clearFilters} disabled={!hasAnyFilter && !isApplyDirty}>
                    Clear
                  </button>
                  <button className="btn btn-dark" onClick={onExport} disabled={loadingSignals}>
                    Export CSV
                  </button>
                </div>
              </div>

              <div style={{ padding: "10px 18px 0", fontSize: 12.5, color: "#6b7280" }}>
                <b style={{ color: "#1a2233" }}>Total:</b> {total}
                {hasAnyFilter && <span style={{ marginLeft: 10 }}>Filters applied</span>}
              </div>

              <div className="table-wrap">
                <table className="data-table">
                  <thead>
                    <tr>
                      <th>Name</th>
                      <th>Past Company URL</th>
                      <th>Past Company</th>
                      <th>New Company</th>
                      <th>Company (City)</th>
                      <th>Status</th>
                    </tr>
                  </thead>
                  <tbody>
                    {loadingSignals ? (
                      <tr><td colSpan="6">Loading signals...</td></tr>
                    ) : !signals.length ? (
                      <tr><td colSpan="6">No signals found for the selected filters.</td></tr>
                    ) : (
                      signals.map((row, idx) => (
                        <tr key={idx}>
                          <td className="cell-name">{row["Name"]}</td>
                          <td>
                            {row["Past Company URL"] ? (
                              <a
                                className="cell-link"
                                href={row["Past Company URL"]}
                                target="_blank"
                                rel="noreferrer"
                              >
                                {row["Past Company URL"]}
                              </a>
                            ) : (
                              ""
                            )}
                          </td>
                          <td className="cell-prev">{row["Past Company"]}</td>
                          <td className="cell-new">{row["New Company"]}</td>
                          <td>{row["Company (City)"]}</td>
                          <td><span className="badge">{row["Status"]}</span></td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            </div>

            <div className="pagination-row">
              <button
                className="btn btn-outline"
                disabled={offset === 0 || loadingSignals}
                onClick={() => setOffset(Math.max(0, offset - limit))}
              >
                Prev
              </button>
              <button
                className="btn btn-outline"
                disabled={offset + limit >= total || loadingSignals}
                onClick={() => setOffset(offset + limit)}
              >
                Next
              </button>
              <span className="pagination-info">
                Showing {total === 0 ? 0 : offset + 1}–{Math.min(offset + limit, total)} of {total}
              </span>
            </div>
          </>
        )}

        {page === "signals" && <ComingSoon label="Signals" />}
        {page === "export-history" && <ComingSoon label="Export History" />}
        {page === "settings" && <ComingSoon label="Settings" />}
      </main>
    </div>
  );
}
