const { useEffect, useRef, useState } = React;

const API = "/api";

// Polling intervals in milliseconds.
const DATA_REFRESH_MS = 5000;
const SYMBOL_REFRESH_MS = 10000;

// Fetch JSON from the Javalin API and turn HTTP failures into UI errors.
async function getJson(path) {
  const response = await fetch(`${API}${path}`);
  if (!response.ok) {
    throw new Error(`Request failed (${response.status})`);
  }
  return response.json();
}

function formatNumber(value) {
  return value == null ? "-" : Number(value).toFixed(2);
}

function formatTime(value) {
  return value ? new Date(value).toLocaleString() : "-";
}

function hasActualValue(row) {
  return [row.lastPrice, row.ema38, row.ema100].some((value) => value != null && Number(value) !== 0);
}

function arraysEqual(firstValues, secondValues) {
  return (
    firstValues.length === secondValues.length && firstValues.every((value, index) => value === secondValues[index])
  );
}

function rowsEqual(firstRows, secondRows) {
  if (firstRows.length !== secondRows.length) {
    return false;
  }

  return firstRows.every((firstRow, index) => {
    const secondRow = secondRows[index];
    return ["symbol", "windowEnd", "lastPrice", "ema38", "ema100", "receivedAt"].every(
      (field) => firstRow[field] === secondRow[field],
    );
  });
}

function latestValuesEqual(firstValues, secondValues, symbols) {
  return symbols.every((value) =>
    rowsEqual(firstValues[value] ? [firstValues[value]] : [], secondValues[value] ? [secondValues[value]] : []),
  );
}

function historiesEqual(firstHistories, secondHistories, symbols) {
  return symbols.every((value) => rowsEqual(firstHistories[value] || [], secondHistories[value] || []));
}

function TrendChart({ histories, symbols }) {
  const canvasRef = useRef(null);

  // Recreate the chart only when the data rows actually change.
  useEffect(() => {
    if (!canvasRef.current) {
      return undefined;
    }

    const allRows = symbols.flatMap((value) => histories[value] || []);
    const labels = [...new Set(allRows.map((row) => row.windowEnd))].sort();
    const colors = [
      "#1f6feb",
      "#d97706",
      "#188038",
      "#a23b72",
      "#6b4fbb",
      "#0891b2",
      "#dc2626",
      "#65a30d",
      "#c026d3",
      "#7c3aed",
      "#0f766e",
      "#ea580c",
    ];
    const datasets = [];

    symbols.forEach((symbol, symbolIndex) => {
      const rows = histories[symbol] || [];
      const rowsByTime = new Map(rows.map((row) => [row.windowEnd, row]));
      const color = colors[symbolIndex % colors.length];

      [
        ["EMA 38", "ema38", undefined],
        ["EMA 100", "ema100", [2, 3]],
      ].forEach(([label, field, borderDash]) => {
        datasets.push({
          label: `${symbol} ${label}`,
          data: labels.map((labelValue) => rowsByTime.get(labelValue)?.[field] ?? null),
          borderColor: color,
          backgroundColor: color,
          borderDash,
          spanGaps: true,
        });
      });
    });

    const chart = new Chart(canvasRef.current, {
      type: "line",
      data: {
        labels: labels.map(formatTime),
        datasets,
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { mode: "index", intersect: false },
        scales: {
          y: { beginAtZero: false },
        },
      },
    });

    return () => chart.destroy();
  }, [histories, symbols]);

  return (
    <div className="chart">
      <canvas ref={canvasRef}></canvas>
    </div>
  );
}

function App() {
  const [symbols, setSymbols] = useState([]);
  const [search, setSearch] = useState("");
  const [selectedSymbols, setSelectedSymbols] = useState([]);
  const [latestBySymbol, setLatestBySymbol] = useState({});
  const [histories, setHistories] = useState({});
  const [signalsBySymbol, setSignalsBySymbol] = useState({});
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    let timeoutId;

    // Refresh the selector separately so new symbols appear without changing
    // the currently selected symbol when it is still available.
    async function loadSymbols() {
      try {
        const values = await getJson("/symbols");
        if (!cancelled) {
          setSymbols((current) => (arraysEqual(current, values) ? current : values));
          setSelectedSymbols((current) => {
            const available = current.filter((value) => values.includes(value));
            const next = available.length > 0 ? available : values.slice(0, 1);
            return arraysEqual(current, next) ? current : next;
          });
        }
      } catch (requestError) {
        if (!cancelled) {
          setError(requestError.message);
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
          timeoutId = setTimeout(loadSymbols, SYMBOL_REFRESH_MS);
        }
      }
    }

    loadSymbols();
    return () => {
      cancelled = true;
      clearTimeout(timeoutId);
    };
  }, []);

  useEffect(() => {
    if (selectedSymbols.length === 0) {
      return undefined;
    }

    let cancelled = false;
    let timeoutId;
    let loaded = false;

    // Poll only the selected symbol. The next poll starts after this request
    // finishes, preventing overlapping requests on a slow database response.
    async function loadData() {
      if (!loaded) {
        setLoading(true);
      }
      setError("");
      try {
        const results = await Promise.all(
          selectedSymbols.map(async (symbol) => {
            const [trendRows, signalRows] = await Promise.all([
              getJson(`/trends/${encodeURIComponent(symbol)}?limit=1000`),
              getJson(`/signals/${encodeURIComponent(symbol)}?limit=20`),
            ]);
            return [symbol, trendRows.filter(hasActualValue), signalRows];
          }),
        );
        if (!cancelled) {
          const nextHistories = Object.fromEntries(results.map(([s, h]) => [s, h]));
          const nextSignals = Object.fromEntries(results.map(([s, _, sig]) => [s, sig]));
          const nextLatest = Object.fromEntries(results.map(([value, rows]) => [value, rows[rows.length - 1] || null]));

          // Keep existing references when values are unchanged so Chart.js
          // does not redraw on every polling request.
          setHistories((current) =>
            historiesEqual(current, nextHistories, selectedSymbols) ? current : nextHistories,
          );
          setLatestBySymbol((current) =>
            latestValuesEqual(current, nextLatest, selectedSymbols) ? current : nextLatest,
          );
          setSignalsBySymbol(nextSignals);
          loaded = true;
        }
      } catch (requestError) {
        if (!cancelled) {
          setError(requestError.message);
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
          timeoutId = setTimeout(loadData, DATA_REFRESH_MS);
        }
      }
    }

    loadData();
    return () => {
      // Stop an in-flight update from changing state after the symbol changes
      // or the page is closed.
      cancelled = true;
      clearTimeout(timeoutId);
    };
  }, [selectedSymbols]);

  const observationCount = selectedSymbols.reduce((total, value) => total + (histories[value]?.length || 0), 0);
  const visibleSymbols = symbols.filter((value) => value.toLowerCase().includes(search.trim().toLowerCase()));

  return (
    <main>
      <header>
        <h1>Trading Trends</h1>
        <p className="muted">EMA history and BUY/SELL signals.</p>
      </header>

      <div className="dashboard-layout">
        <aside className="toolbar">
          <label>
            Search stocks
            <input
              type="search"
              value={search}
              placeholder="Type a symbol"
              onChange={(event) => setSearch(event.target.value)}
            />
          </label>
          <label>
            Stocks
            <select
              multiple
              size={Math.min(Math.max(visibleSymbols.length, 3), 8)}
              value={selectedSymbols}
              onChange={(event) => {
                const selectedVisible = Array.from(event.target.selectedOptions, (option) => option.value);
                const hiddenSelected = selectedSymbols.filter((value) => !visibleSymbols.includes(value));
                setSelectedSymbols([...hiddenSelected, ...selectedVisible]);
              }}
            >
              {visibleSymbols.length === 0 && (
                <option value="" disabled>
                  No matching symbols
                </option>
              )}
              {visibleSymbols.map((value) => (
                <option key={value} value={value}>
                  {value}
                </option>
              ))}
            </select>
          </label>
          <span className="status">
            {loading ? "Loading..." : `${selectedSymbols.length} stocks, ${observationCount} observations`}
          </span>
        </aside>

        <div className="dashboard-content">
          {error && <p className="error">{error}</p>}
          <section className="latest-values">
            <h2>Latest Values</h2>
            <div className="cards">
              {selectedSymbols.map((value) => {
                const latest = latestBySymbol[value];
                const latestSignal = (signalsBySymbol[value] || [])[0];
                return (
                  <div className="panel metric" key={value}>
                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                      <h3>{value}</h3>
                      {latestSignal && (
                        <span className={`badge badge-${latestSignal.type.toLowerCase()}`}>
                          {latestSignal.type}
                        </span>
                      )}
                    </div>
                    <span>Price</span>
                    <strong>{formatNumber(latest?.lastPrice)}</strong>
                    <span>EMA 38: {formatNumber(latest?.ema38)}</span>
                    <span>EMA 100: {formatNumber(latest?.ema100)}</span>
                  </div>
                );
              })}
            </div>
          </section>

          <section className="panel" style={{ marginBottom: "16px" }}>
            <h2>Trading Signals (Query 2 Breakouts)</h2>
            {selectedSymbols.some((s) => (signalsBySymbol[s] || []).length > 0) ? (
              <div className="signals-list">
                {selectedSymbols
                  .flatMap((s) => (signalsBySymbol[s] || []).map((sig) => ({ ...sig, symbol: s })))
                  .sort((a, b) => (b.windowEnd || "").localeCompare(a.windowEnd || ""))
                  .slice(0, 15)
                  .map((sig, idx) => (
                    <div className="signal-item" key={`${sig.symbol}-${sig.windowEnd}-${idx}`}>
                      <div>
                        <strong>{sig.symbol}</strong>
                        <span className="muted" style={{ marginLeft: "8px" }}>
                          Window: {formatTime(sig.windowEnd)}
                        </span>
                      </div>
                      <div style={{ display: "flex", alignItems: "center", gap: "12px" }}>
                        <span>EMA 38: {formatNumber(sig.ema38)}</span>
                        <span>EMA 100: {formatNumber(sig.ema100)}</span>
                        <span className={`badge badge-${sig.type.toLowerCase()}`}>
                          {sig.type}
                        </span>
                      </div>
                    </div>
                  ))}
              </div>
            ) : (
              <p className="muted">No crossover breakout signals detected yet for selected stocks.</p>
            )}
          </section>

          <section className="panel">
            <h2>Price history</h2>
            {observationCount > 0 ? (
              <TrendChart histories={histories} symbols={selectedSymbols} />
            ) : (
              <p className="muted">No history available.</p>
            )}
          </section>
        </div>
      </div>
    </main>
  );
}

ReactDOM.createRoot(document.getElementById("root")).render(<App />);
