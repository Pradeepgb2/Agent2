import { useEffect, useMemo, useState } from "react";
import { apiGet, buildQuery } from "./api";

export default function App() {
  const [weeks, setWeeks] = useState([]);
  const [week, setWeek] = useState("");
  const [summary, setSummary] = useState(null);

  const [loadingWeeks, setLoadingWeeks] = useState(false);
  const [loadingSummary, setLoadingSummary] = useState(false);
  const [loadingSignals, setLoadingSignals] = useState(false);
  const [error, setError] = useState("");

  const [signals, setSignals] = useState([]);
  const [total, setTotal] = useState(0);

  // Filters (editable inputs)
  const [q, setQ] = useState("");
  const [status, setStatus] = useState("");
  const [role, setRole] = useState("");
  const [company, setCompany] = useState("");
  const [location, setLocation] = useState("");

  // Applied filters snapshot (what API uses)
  const [applied, setApplied] = useState({
    q: "",
    status: "",
    role: "",
    company: "",
    location: "",
  });

  // Pagination
  const [limit] = useState(50);
  const [offset, setOffset] = useState(0);

  // Small UX: copy toast
  const [copiedMsg, setCopiedMsg] = useState("");

  const queryString = useMemo(() => {
    return buildQuery({
      week,
      q: applied.q,
      status: applied.status,
      role: applied.role,
      company_name: applied.company,
      company_location: applied.location,
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
      if (!week && list.length) setWeek(list[0]); // latest
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
    // snapshot current inputs to applied filters
    setApplied({
      q: q.trim(),
      status,
      role: role.trim(),
      company: company.trim(),
      location: location.trim(),
    });
    setOffset(0);
  }

  function clearFilters() {
    setQ("");
    setStatus("");
    setRole("");
    setCompany("");
    setLocation("");
    setApplied({ q: "", status: "", role: "", company: "", location: "" });
    setOffset(0);
  }

  function onExport() {
    const exportQS = buildQuery({
      week,
      q: applied.q,
      status: applied.status,
      role: applied.role,
      company_name: applied.company,
      company_location: applied.location,
    });
    const base = import.meta.env.VITE_API_BASE_URL;
    window.open(`${base}/signals/export?${exportQS}`, "_blank");
  }

  async function copyEmail(email) {
    try {
      await navigator.clipboard.writeText(email);
      setCopiedMsg(`Copied: ${email}`);
      setTimeout(() => setCopiedMsg(""), 1200);
    } catch {
      setCopiedMsg("Copy failed");
      setTimeout(() => setCopiedMsg(""), 1200);
    }
  }

  // initial
  useEffect(() => {
    loadWeeks().catch(console.error);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // when week changes
  useEffect(() => {
    if (!week) return;
    loadSummary(week).catch(console.error);
    setOffset(0);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [week]);

  // load signals when queryString changes (week/applied/offset)
  useEffect(() => {
    loadSignals().catch(console.error);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [queryString]);

  const isApplyDirty =
    (q.trim() !== applied.q) ||
    (status !== applied.status) ||
    (role.trim() !== applied.role) ||
    (company.trim() !== applied.company) ||
    (location.trim() !== applied.location);

  const hasAnyFilter =
    !!applied.q || !!applied.status || !!applied.role || !!applied.company || !!applied.location;

  return (
    <div style={{ padding: 16, fontFamily: "Arial", maxWidth: 1200, margin: "0 auto" }}>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 12 }}>
        <h2 style={{ margin: 0 }}>Agent2 Dashboard</h2>
        <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
          <button onClick={refreshAll} disabled={!week || loadingSummary || loadingSignals}>
            Refresh
          </button>
        </div>
      </div>

      {error && (
        <div style={{ margin: "12px 0", padding: 10, border: "1px solid #f99", borderRadius: 8 }}>
          <b>Error:</b> {error}
        </div>
      )}

      {copiedMsg && (
        <div style={{ margin: "12px 0", padding: 10, border: "1px solid #9f9", borderRadius: 8 }}>
          {copiedMsg}
        </div>
      )}

      {/* Week Selector */}
      <div style={{ margin: "12px 0", display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
        <label><b>Week:</b></label>
        <select value={week} onChange={(e) => setWeek(e.target.value)} disabled={loadingWeeks}>
          {weeks.map((w) => (
            <option key={w} value={w}>{w}</option>
          ))}
        </select>
        {loadingWeeks && <span>Loading weeks...</span>}
      </div>

      {/* Weekly Summary Card */}
      <div style={{ border: "1px solid #ddd", padding: 12, borderRadius: 10, marginBottom: 16 }}>
        <h3 style={{ marginTop: 0 }}>Weekly Summary</h3>
        {loadingSummary ? (
          <div>Loading summary...</div>
        ) : !summary ? (
          <div>No summary available for this week.</div>
        ) : (
          <div style={{ display: "flex", gap: 24, flexWrap: "wrap" }}>
            <div><b>Week:</b> {summary.week_present}</div>
            <div><b>Total Rows:</b> {summary.total_rows_fetched}</div>
            <div><b>Valid:</b> {summary.valid_rows_processed}</div>
            <div><b>Skipped:</b> {summary.invalid_rows_skipped}</div>
            <div><b>Signals:</b> {summary.signals_count}</div>
            <div><b>Status:</b> {summary.run_status}</div>
          </div>
        )}
      </div>

      {/* Filters */}
      <div style={{ border: "1px solid #eee", padding: 12, borderRadius: 10, marginBottom: 12 }}>
        <div style={{ display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center" }}>
          <input style={{ minWidth: 220 }} placeholder="Search (q)" value={q} onChange={(e) => setQ(e.target.value)} />
          <input placeholder="Role" value={role} onChange={(e) => setRole(e.target.value)} />
          <input placeholder="Company" value={company} onChange={(e) => setCompany(e.target.value)} />
          <input placeholder="Location" value={location} onChange={(e) => setLocation(e.target.value)} />
          <select value={status} onChange={(e) => setStatus(e.target.value)}>
            <option value="">All Status</option>
            <option value="company changed">company changed</option>
            <option value="role changed">role changed</option>
            <option value="role and company changed">role and company changed</option>
          </select>

          <button onClick={applyFilters} disabled={!isApplyDirty || loadingSignals}>
            Apply
          </button>

          <button onClick={clearFilters} disabled={!hasAnyFilter && !isApplyDirty}>
            Clear
          </button>

          <button onClick={onExport} disabled={loadingSignals}>
            Export CSV
          </button>

          {hasAnyFilter && (
            <span style={{ fontSize: 12, opacity: 0.8 }}>
              Filters applied
            </span>
          )}
        </div>
        <div style={{ marginTop: 8, fontSize: 12, opacity: 0.8 }}>
          Tip: Type your filters, then click <b>Apply</b> (prevents API calls on every keypress).
        </div>
      </div>

      {/* Table Header */}
      <div style={{ marginBottom: 8, display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <div><b>Total:</b> {total}</div>
        <div style={{ fontSize: 12, opacity: 0.8 }}>
          Click an email to copy
        </div>
      </div>

      {/* Table */}
      <div style={{ border: "1px solid #ddd", borderRadius: 10, overflow: "auto", maxHeight: 520 }}>
        <table style={{ width: "100%", borderCollapse: "collapse" }}>
          <thead style={{ position: "sticky", top: 0, background: "#fff", zIndex: 1 }}>
            <tr>
              <th style={thStyle}>Email</th>
              <th style={thStyle}>Company (Location)</th>
              <th style={thStyle}>Position</th>
              <th style={thStyle}>Status</th>
            </tr>
          </thead>
          <tbody>
            {loadingSignals ? (
              <tr><td style={tdStyle} colSpan="4">Loading signals...</td></tr>
            ) : !signals.length ? (
              <tr><td style={tdStyle} colSpan="4">No signals found for the selected filters.</td></tr>
            ) : (
              signals.map((row, idx) => (
                <tr
                  key={idx}
                  style={rowStyle}
                  onMouseEnter={(e) => (e.currentTarget.style.background = "#fafafa")}
                  onMouseLeave={(e) => (e.currentTarget.style.background = "transparent")}
                >
                  <td
                    style={{ ...tdStyle, color: "#0b57d0", cursor: "pointer" }}
                    title="Click to copy"
                    onClick={() => copyEmail(row["Email"])}
                  >
                    {row["Email"]}
                  </td>
                  <td style={tdStyle}>{row["Company (Location)"]}</td>
                  <td style={tdStyle}>{row["Position"]}</td>
                  <td style={tdStyle}>{row["Status"]}</td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      {/* Pagination */}
      <div style={{ marginTop: 12, display: "flex", gap: 8, alignItems: "center" }}>
        <button
          disabled={offset === 0 || loadingSignals}
          onClick={() => setOffset(Math.max(0, offset - limit))}
        >
          Prev
        </button>
        <button
          disabled={offset + limit >= total || loadingSignals}
          onClick={() => setOffset(offset + limit)}
        >
          Next
        </button>
        <span style={{ fontSize: 12, opacity: 0.8 }}>
          Showing {total === 0 ? 0 : offset + 1}–{Math.min(offset + limit, total)} of {total}
        </span>
      </div>
    </div>
  );
}

const thStyle = {
  textAlign: "left",
  padding: 10,
  borderBottom: "1px solid #ddd",
  fontSize: 13,
};

const tdStyle = {
  padding: 10,
  borderBottom: "1px solid #eee",
  fontSize: 13,
  verticalAlign: "top",
};

const rowStyle = {
  transition: "background 120ms ease",
};