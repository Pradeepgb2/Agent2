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


  // Filters
  const [q, setQ] = useState("");
  const [status, setStatus] = useState("");
  const [role, setRole] = useState("");
  const [company, setCompany] = useState("");
  const [location, setLocation] = useState("");

  // Pagination
  const [limit] = useState(50);
  const [offset, setOffset] = useState(0);

  const queryString = useMemo(() => {
    return buildQuery({
      week,
      q,
      status,
      role,
      company_name: company,
      company_location: location,
      limit,
      offset,
    });
  }, [week, q, status, role, company, location, limit, offset]);

  async function loadWeeks() {
  setError("");
  setLoadingWeeks(true);
  try {
    const data = await apiGet("/weeks");
    setWeeks(data.weeks || []);
    if (!week && data.weeks?.length) setWeek(data.weeks[0]);
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
  useEffect(() => {
    loadWeeks().catch(console.error);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    if (!week) return;
    loadSummary(week).catch(console.error);
    //setOffset(0);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [week]);

  useEffect(() => {
    loadSignals().catch(console.error);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [queryString]);
  useEffect(() => {
    setOffset(0);
  }, [q, status, role, company, location, week]);

  function onExport() {
    const exportQS = buildQuery({
      week,
      q,
      status,
      role,
      company_name: company,
      company_location: location,
    });
    const base = import.meta.env.VITE_API_BASE_URL;
    window.open(`${base}/signals/export?${exportQS}`, "_blank");
  }

  return (
    <div style={{ padding: 16, fontFamily: "Arial" }}>
      <h2>Agent2 Dashboard</h2>
      {error && (
        <div style={{ margin: "12px 0", padding: 10, border: "1px solid #f99", borderRadius: 8 }}>
          <b>Error:</b> {error}
        </div>
      )}

      {/* Week Selector */}
      <div style={{ marginBottom: 12 }}>
        <label>Week: </label>
        <select value={week} onChange={(e) => setWeek(e.target.value)}>
          {weeks.map((w) => (
            <option key={w} value={w}>{w}</option>
          ))}
        </select>
        <button onClick={refreshAll} disabled={!week || loadingSummary || loadingSignals}>Refresh</button>
      </div>

      {/* Weekly Summary Card */}
      <div style={{ border: "1px solid #ddd", padding: 12, borderRadius: 8, marginBottom: 16 }}>
        <h3>Weekly Summary</h3>
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
      <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginBottom: 12 }}>
        <input placeholder="Search (q)" value={q} onChange={(e) => setQ(e.target.value)} />
        <input placeholder="Role" value={role} onChange={(e) => setRole(e.target.value)} />
        <input placeholder="Company" value={company} onChange={(e) => setCompany(e.target.value)} />
        <input placeholder="Location" value={location} onChange={(e) => setLocation(e.target.value)} />
        <select value={status} onChange={(e) => setStatus(e.target.value)}>
          <option value="">All Status</option>
          <option value="company changed">company changed</option>
          <option value="role changed">role changed</option>
          <option value="role and company changed">role and company changed</option>
        </select>
        <button onClick={onExport}>Export CSV</button>
      </div>

      {/* Table */}
      <div style={{ marginBottom: 8 }}>
        <b>Total:</b> {total}
      </div>

      <table border="1" cellPadding="6" style={{ borderCollapse: "collapse", width: "100%" }}>
        <thead>
          <tr>
            <th>Email</th>
            <th>Company (Location)</th>
            <th>Position</th>
            <th>Status</th>
          </tr>
        </thead>
        <tbody>
          {loadingSignals ? (
            <tr>
              <td colSpan="4">Loading signals...</td>
            </tr>
          ) : !signals.length ? (
            <tr>
              <td colSpan="4">No signals found for the selected filters.</td>
            </tr>
          ) : (
          signals.map((row, idx) => (
            <tr key={idx}>
              <td>{row["Email"]}</td>
              <td>{row["Company (Location)"]}</td>
              <td>{row["Position"]}</td>
              <td>{row["Status"]}</td>
            </tr>
          ))
        )}
        </tbody>
      </table>

      {/* Pagination */}
      <div style={{ marginTop: 12, display: "flex", gap: 8 }}>
        <button disabled={offset === 0} onClick={() => setOffset(Math.max(0, offset - limit))}>
          Prev
        </button>
        <button disabled={offset + limit >= total} onClick={() => setOffset(offset + limit)}>
          Next
        </button>
      </div>
    </div>
  );
}