import { useState } from "react";
import { dayKey, formatCount, formatDay, formatPrice, formatTime, pricePrecision, splitSymbol } from "../format";
import type { Signal, SignalType, Summary } from "../types";
import { Empty, Panel, SignalBadge } from "./bits";

interface Props {
  signals: Signal[];
  summary: Summary | undefined;
  selected: string | null;
  loading: boolean;
  onSelect: (symbol: string) => void;
}

export function SignalFeed({ signals, summary, selected, loading, onSelect }: Props) {
  const [filter, setFilter] = useState<SignalType | "ALL">("ALL");
  const visible = filter === "ALL" ? signals : signals.filter((signal) => signal.type === filter);
  let lastDay = "";

  return (
    <Panel
      title="Signals"
      className="signals"
      aside={
        <div className="segmented" role="group" aria-label="Signal type">
          {(["ALL", "BUY", "SELL"] as const).map((value) => (
            <button
              key={value}
              type="button"
              className={filter === value ? "active" : ""}
              aria-pressed={filter === value}
              onClick={() => setFilter(value)}
            >
              {value === "ALL" ? "All" : value === "BUY" ? "Buy" : "Sell"}
            </button>
          ))}
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
      {visible.length === 0 ? (
        <Empty title={loading ? "Loading…" : "No crossovers yet"}>
          {loading ? undefined : "Advisories appear here when EMA 38 crosses EMA 100."}
        </Empty>
      ) : (
        <ol className="feed">
          {visible.map((signal) => {
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
      <footer className="panel-foot muted">Latest {signals.length}, all symbols · EMA 38 / EMA 100</footer>
    </Panel>
  );
}
