import {
  direction,
  formatDateTime,
  formatPercent,
  formatPrice,
  formatSigned,
  pricePrecision,
  splitSymbol,
  windowsBetween,
} from "../format";
import type { RangeStats } from "../model";
import type { Signal } from "../types";
import { Empty, SignalBadge, TrendTag } from "./bits";

interface Props {
  symbol: string | null;
  stats: RangeStats;
  signals: Signal[];
  rangeLabel: string;
  watched: boolean;
  onToggleWatch: () => void;
}

export function Overview({ symbol, stats, signals, rangeLabel, watched, onToggleWatch }: Props) {
  if (symbol == null) {
    return (
      <section className="panel overview">
        <Empty title="No symbol selected">Search above to open a symbol.</Empty>
      </section>
    );
  }

  const { ticker, exchange, exchangeName } = splitSymbol(symbol);
  const latest = stats.latest;
  const precision = pricePrecision(latest?.lastPrice);
  const move = direction(stats.change);
  const warming = stats.warmup != null && Math.abs(1 - stats.warmup) > 0.05;
  const sinceSignal = stats.lastSignal && latest ? windowsBetween(stats.lastSignal.windowEnd, latest.windowEnd) : null;

  return (
    <section className="panel overview">
      <div className="overview-grid">
        <header className="overview-head">
          <div>
            <h1>
              {ticker}
              {exchange && <span className="exchange">{exchange}</span>}
            </h1>
            <p className="muted">{exchangeName || "Unknown exchange"}</p>
          </div>
          <button
            type="button"
            className={`watch-toggle ${watched ? "active" : ""}`}
            aria-pressed={watched}
            onClick={onToggleWatch}
          >
            <span aria-hidden="true">{watched ? "★" : "☆"}</span>
            {watched ? "Watching" : "Watch"}
          </button>
        </header>

        <div className="quote">
          <span className="quote-price num">{formatPrice(latest?.lastPrice, precision)}</span>
          <span className={`quote-change num ${move}`}>
            <span aria-hidden="true">{move === "up" ? "▲" : move === "down" ? "▼" : ""}</span>
            {formatSigned(stats.change, precision)} ({formatPercent(stats.changePct)})
          </span>
          <span className="muted quote-note">vs previous window · {formatDateTime(latest?.windowEnd)}</span>
        </div>

        <div className="verdict">
          <div>
            <span className="label">Trend</span>
            <TrendTag trend={stats.trend} />
          </div>
          <div>
            <span className="label">Last signal</span>
            {stats.lastSignal ? (
              <SignalBadge type={stats.lastSignal.type} />
            ) : (
              <span className="dim">None in range</span>
            )}
            {sinceSignal != null && (
              <span className="muted num" title={formatDateTime(stats.lastSignal?.windowEnd)}>
                {sinceSignal === 0 ? "this window" : `${sinceSignal} windows ago`}
              </span>
            )}
          </div>
        </div>

        <dl className="keydata">
          <div className="kv">
            <dt>
              <i className="key key-ema38" />
              EMA 38
            </dt>
            <dd className="num">{formatPrice(latest?.ema38, precision)}</dd>
          </div>
          <div className="kv">
            <dt>
              <i className="key key-ema100" />
              EMA 100
            </dt>
            <dd className="num">{formatPrice(latest?.ema100, precision)}</dd>
          </div>
          <div className="kv">
            <dt>Spread (38 − 100)</dt>
            <dd className={`num ${direction(stats.spread)}`}>
              {formatSigned(stats.spread, precision)} <span className="muted">{formatPercent(stats.spreadPct)}</span>
            </dd>
          </div>
          <div className="kv">
            <dt>Signals ({rangeLabel})</dt>
            <dd className="num">
              {stats.buys} buy <span className="muted">·</span> {stats.sells} sell
            </dd>
          </div>
          <div className="kv">
            <dt>Change ({rangeLabel})</dt>
            <dd className={`num ${direction(stats.rangeChangePct)}`}>{formatPercent(stats.rangeChangePct)}</dd>
          </div>
          <div className="kv">
            <dt>High ({rangeLabel})</dt>
            <dd className="num">{formatPrice(stats.high, precision)}</dd>
          </div>
          <div className="kv">
            <dt>Low ({rangeLabel})</dt>
            <dd className="num">{formatPrice(stats.low, precision)}</dd>
          </div>
          <div className="kv">
            <dt>Windows ({rangeLabel})</dt>
            <dd className="num">{stats.windows}</dd>
          </div>
        </dl>

        <div className="history">
          <span className="label">Crossovers ({rangeLabel})</span>
          {signals.length === 0 ? (
            <p className="dim">No crossovers in this range.</p>
          ) : (
            <ol>
              {signals
                .slice(-8)
                .reverse()
                .map((signal) => (
                  <li key={signal.windowEnd}>
                    <span className="num muted">{formatDateTime(signal.windowEnd)}</span>
                    <SignalBadge type={signal.type} />
                    <span className="num">{formatPrice(signal.ema38, precision)}</span>
                  </li>
                ))}
            </ol>
          )}
        </div>

        {warming && (
          <p className="notice">
            <strong>EMAs warming up.</strong> Both averages start from zero, so EMA 100 is at{" "}
            {Math.round((stats.warmup ?? 0) * 100)}% of the price. Early crossovers reflect that ramp, not the market.
          </p>
        )}
      </div>
    </section>
  );
}
