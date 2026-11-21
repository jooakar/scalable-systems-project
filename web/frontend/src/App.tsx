import { useCallback, useEffect, useMemo } from "react";
import { ChartPanel, RANGES } from "./components/ChartPanel";
import { Overview } from "./components/Overview";
import { SignalFeed } from "./components/SignalFeed";
import { TopBar } from "./components/TopBar";
import { Watchlist } from "./components/Watchlist";
import { formatAge, formatCount } from "./format";
import { useConnection, useNow, usePoll, useStoredState } from "./hooks";
import { rangeStats, toQuote } from "./model";
import type { Signal, Summary, TrendRow, WatchEntry } from "./types";

const FAST_MS = 5_000;
const SLOW_MS = 30_000;
const SPARK_POINTS = 48;
const EMPTY_ROWS: TrendRow[] = [];
const EMPTY_SIGNALS: Signal[] = [];

export function App() {
  const [watchlist, setWatchlist] = useStoredState<string[]>("terminal.watchlist", []);
  const [seeded, setSeeded] = useStoredState("terminal.seeded", false);
  const [selected, setSelected] = useStoredState<string | null>("terminal.symbol", null);
  const [rangeHours, setRangeHours] = useStoredState("terminal.range", 24);
  const connection = useConnection();
  const now = useNow(1_000);

  const symbols = usePoll<string[]>("/api/symbols", SLOW_MS);
  const summary = usePoll<Summary>("/api/summary", FAST_MS);
  const feed = usePoll<Signal[]>("/api/signals/latest?limit=80", FAST_MS);

  // First visit: start the watchlist with the most actively traded symbols.
  const starter = usePoll<TrendRow[]>(seeded ? null : "/api/trends/latest?limit=12&sort=active", FAST_MS);
  useEffect(() => {
    if (!seeded && starter.data && starter.data.length > 0) {
      setWatchlist((current) => (current.length > 0 ? current : starter.data!.map((row) => row.symbol)));
      setSeeded(true);
    }
  }, [seeded, starter.data, setWatchlist, setSeeded]);

  const loadTopActive = useCallback(async () => {
    try {
      const response = await fetch("/api/trends/latest?limit=12&sort=active");
      if (response.ok) {
        const data: TrendRow[] = await response.json();
        const topSymbols = data.map((row) => row.symbol);
        setWatchlist((current) => {
          const merged = new Set([...current, ...topSymbols]);
          return Array.from(merged);
        });
        setSeeded(true);
        if (topSymbols.length > 0 && (selected == null || !watchlist.includes(selected))) {
          setSelected(topSymbols[0]);
        }
      }
    } catch {
      // ignore
    }
  }, [setWatchlist, setSeeded, selected, watchlist, setSelected]);

  useEffect(() => {
    if (selected == null && watchlist.length > 0) {
      setSelected(watchlist[0]);
    }
  }, [selected, watchlist, setSelected]);

  const watchUrl =
    watchlist.length > 0
      ? `/api/watchlist?points=${SPARK_POINTS}&symbols=${watchlist.map(encodeURIComponent).join(",")}`
      : null;
  const watch = usePoll<WatchEntry[]>(watchUrl, FAST_MS, { keepPrevious: true });
  const quotes = useMemo(() => {
    const bySymbol = new Map((watch.data ?? []).map((entry) => [entry.symbol, toQuote(entry)]));
    return watchlist.flatMap((symbol) => bySymbol.get(symbol) ?? []);
  }, [watch.data, watchlist]);

  const path = selected ? encodeURIComponent(selected) : null;
  const history = usePoll<TrendRow[]>(path && `/api/trends/${path}?hours=${rangeHours}&limit=5000`, FAST_MS);
  const signals = usePoll<Signal[]>(path && `/api/signals/${path}?hours=${rangeHours}&limit=5000`, FAST_MS);
  const rows = useMemo(
    () => (history.data ?? EMPTY_ROWS).filter((row) => row.lastPrice != null && row.lastPrice !== 0),
    [history.data],
  );
  const symbolSignals = signals.data ?? EMPTY_SIGNALS;
  const stats = useMemo(() => rangeStats(rows, symbolSignals), [rows, symbolSignals]);

  const toggleWatch = useCallback(
    (symbol: string) => {
      setSeeded(true);
      setWatchlist((current) =>
        current.includes(symbol) ? current.filter((value) => value !== symbol) : [...current, symbol],
      );
    },
    [setWatchlist, setSeeded],
  );

  const rangeLabel = RANGES.find((range) => range.hours === rangeHours)?.label ?? `${rangeHours}h`;
  const host = window.location.hostname;
  const noData = symbols.data !== undefined && symbols.data.length === 0;

  return (
    <div className="app">
      <TopBar
        symbols={symbols.data ?? []}
        watchlist={watchlist}
        quotes={quotes}
        latestWindowEnd={summary.data?.latestWindowEnd}
        connection={connection}
        onSelect={setSelected}
        onToggleWatch={toggleWatch}
      />

      {noData && (
        <div className="banner" role="status">
          <strong>Waiting for data.</strong> The database has no EMA results yet. Start the producer to replay tick
          data; results appear here as the Flink job closes each 5-minute window.
        </div>
      )}
      {connection.failing && (
        <div className="banner banner-error" role="alert">
          <strong>Connection lost.</strong> {connection.error}. Showing the last values received; retrying every few
          seconds.
        </div>
      )}

      <main className="workspace">
        <Overview
          symbol={selected}
          stats={stats}
          signals={symbolSignals}
          rangeLabel={rangeLabel}
          watched={selected != null && watchlist.includes(selected)}
          onToggleWatch={() => selected && toggleWatch(selected)}
        />
        <ChartPanel
          symbol={selected}
          rows={rows}
          signals={symbolSignals}
          loading={history.loading}
          rangeHours={rangeHours}
          onRange={setRangeHours}
        />
        <SignalFeed
          signals={feed.data ?? EMPTY_SIGNALS}
          summary={summary.data}
          selected={selected}
          loading={feed.data === undefined}
          onSelect={setSelected}
        />
        <Watchlist
          quotes={quotes}
          selected={selected}
          loading={watchlist.length > 0 ? watch.data === undefined : !seeded && !noData}
          onSelect={setSelected}
          onRemove={toggleWatch}
          onAddTopActive={loadTopActive}
        />
      </main>

      <footer className="statusbar">
        <span>{formatCount(symbols.data?.length)} symbols with data</span>
        <span>5-minute tumbling windows · EMA 38 / EMA 100</span>
        <span>
          {connection.lastSuccess ? `Updated ${formatAge(now - connection.lastSuccess)}` : "Not connected"}
          {connection.latencyMs != null && ` · ${Math.round(connection.latencyMs)} ms`}
        </span>
        <nav aria-label="Operations">
          <a href={`http://${host}:8081`} target="_blank" rel="noreferrer">
            Flink
          </a>
          <a href={`http://${host}:3000`} target="_blank" rel="noreferrer">
            Grafana
          </a>
          <a href={`http://${host}:9090`} target="_blank" rel="noreferrer">
            Prometheus
          </a>
        </nav>
      </footer>
    </div>
  );
}
