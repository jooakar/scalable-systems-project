import { useMemo, useState } from "react";
import { dayKey, formatCount, formatDay, formatPrice, formatTime, pricePrecision, splitSymbol } from "../format";
import { usePoll } from "../hooks";
import type { Signal, SignalType, Summary } from "../types";
import { Empty, Panel, SignalBadge } from "./bits";

interface Props {
  summary: Summary | undefined;
  selected: string | null;
  onSelect: (symbol: string) => void;
}

const PAGE_SIZE = 50;

export function SignalFeed({ summary, selected, onSelect }: Props) {
  const [filter, setFilter] = useState<SignalType | "ALL">("ALL");
  const [scope, setScope] = useState<"ALL" | "SELECTED">("ALL");
  const [page, setPage] = useState(1);

  const activeScope = scope === "SELECTED" && selected ? "SELECTED" : "ALL";

  const typeParam = filter !== "ALL" ? `&type=${filter}` : "";
  const symbolParam = activeScope === "SELECTED" && selected ? `&symbol=${encodeURIComponent(selected)}` : "";
  const offset = (page - 1) * PAGE_SIZE;

  const url = `/api/signals/latest?limit=${PAGE_SIZE}&offset=${offset}${typeParam}${symbolParam}`;
  const feed = usePoll<Signal[]>(url, 5000, { keepPrevious: true });
  const signals = feed.data ?? [];
  const loading = feed.data === undefined;

  const totalCount = useMemo(() => {
    if (activeScope === "SELECTED") {
      return null;
    }
    if (filter === "BUY") return summary?.buySignals ?? 0;
    if (filter === "SELL") return summary?.sellSignals ?? 0;
    return (summary?.buySignals ?? 0) + (summary?.sellSignals ?? 0);
  }, [activeScope, filter, summary]);

  const totalPages =
    totalCount != null
      ? Math.max(1, Math.ceil(totalCount / PAGE_SIZE))
      : signals.length === PAGE_SIZE
        ? page + 1
        : page;

  let lastDay = "";

  return (
    <Panel
      title="Signals"
      className="signals"
      aside={
        <div style={{ display: "inline-flex", alignItems: "center", gap: "6px" }}>
          {selected && (
            <div className="segmented" role="group" aria-label="Signal scope">
              <button
                type="button"
                className={activeScope === "ALL" ? "active" : ""}
                onClick={() => {
                  setScope("ALL");
                  setPage(1);
                }}
              >
                All
              </button>
              <button
                type="button"
                className={activeScope === "SELECTED" ? "active" : ""}
                onClick={() => {
                  setScope("SELECTED");
                  setPage(1);
                }}
                title={`Filter signals for ${selected}`}
              >
                {splitSymbol(selected).ticker}
              </button>
            </div>
          )}
          <div className="segmented" role="group" aria-label="Signal type">
            {(["ALL", "BUY", "SELL"] as const).map((value) => (
              <button
                key={value}
                type="button"
                className={filter === value ? "active" : ""}
                aria-pressed={filter === value}
                onClick={() => {
                  setFilter(value);
                  setPage(1);
                }}
              >
                {value === "ALL" ? "All" : value === "BUY" ? "Buy" : "Sell"}
              </button>
            ))}
          </div>
        </div>
      }
    >
      <div className="signal-totals">
        <div>
          <span className="label">Buy advisories</span>
          <strong className="num up">{formatCount(summary?.buySignals)}</strong>
        </div>
        <div>
          <span className="label">Sell advisories</span>
          <strong className="num down">{formatCount(summary?.sellSignals)}</strong>
        </div>
      </div>
      {signals.length === 0 ? (
        <Empty title={loading ? "Loading…" : "No crossovers found"}>
          {loading ? undefined : "No advisories match the current filter or page."}
        </Empty>
      ) : (
        <ol className="feed">
          {signals.map((signal) => {
            const { ticker, exchange } = splitSymbol(signal.symbol);
            const day = dayKey(signal.windowEnd);
            const showDay = day !== lastDay;
            lastDay = day;
            const precision = pricePrecision(signal.ema38);
            return (
              <li key={`${signal.symbol}|${signal.windowEnd}`}>
                {showDay && <div className="feed-day">{formatDay(signal.windowEnd)}</div>}
                <button
                  type="button"
                  className={`feed-row ${signal.symbol === selected ? "selected" : ""}`}
                  onClick={() => onSelect(signal.symbol)}
                >
                  <span className="num muted">{formatTime(signal.windowEnd)}</span>
                  <span className="feed-symbol">
                    {ticker}
                    <span className="muted">{exchange}</span>
                  </span>
                  <SignalBadge type={signal.type} />
                  <span className="feed-values num muted">
                    {formatPrice(signal.ema38, precision)} / {formatPrice(signal.ema100, precision)}
                  </span>
                </button>
              </li>
            );
          })}
        </ol>
      )}
      <footer className="panel-foot feed-pagination">
        <button
          type="button"
          disabled={page <= 1}
          onClick={() => setPage((p) => Math.max(1, p - 1))}
          title="Newer signals"
        >
          ◀ Newer
        </button>
        <span className="num muted">
          {totalCount != null
            ? `Page ${page} of ${totalPages} · ${totalCount.toLocaleString("en-US")} total`
            : `Page ${page} · ${signals.length} on page`}
        </span>
        <button
          type="button"
          disabled={page >= totalPages || (totalCount == null && signals.length < PAGE_SIZE)}
          onClick={() => setPage((p) => p + 1)}
          title="Older signals"
        >
          Older ▶
        </button>
      </footer>
    </Panel>
  );
}
