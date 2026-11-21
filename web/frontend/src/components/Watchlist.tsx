import { useMemo, useState } from "react";
import { formatPrice, formatSigned, formatPercent, formatTime, pricePrecision, splitSymbol } from "../format";
import type { Quote } from "../model";
import { Change, Empty, Panel, SignalBadge, Sparkline, SpreadBar, TrendTag } from "./bits";

type SortKey = "symbol" | "last" | "changePct" | "spreadPct";

const SORTABLE: Record<SortKey, (quote: Quote) => number | string | null> = {
  symbol: (quote) => quote.symbol,
  last: (quote) => quote.last,
  changePct: (quote) => quote.changePct,
  spreadPct: (quote) => quote.spreadPct,
};

interface Props {
  quotes: Quote[];
  selected: string | null;
  loading: boolean;
  onSelect: (symbol: string) => void;
  onRemove: (symbol: string) => void;
}

export function Watchlist({ quotes, selected, loading, onSelect, onRemove }: Props) {
  const [sort, setSort] = useState<{ key: SortKey; descending: boolean } | null>(null);

  const ordered = useMemo(() => {
    if (!sort) return quotes;
    const read = SORTABLE[sort.key];
    return [...quotes].sort((first, second) => {
      const a = read(first);
      const b = read(second);
      if (a == null) return 1;
      if (b == null) return -1;
      const order = typeof a === "string" ? a.localeCompare(b as string) : a - (b as number);
      return sort.descending ? -order : order;
    });
  }, [quotes, sort]);

  const spreadScale = Math.max(0, ...quotes.map((quote) => Math.abs(quote.spreadPct ?? 0)));

  function header(key: SortKey, label: string, numeric = true) {
    const active = sort?.key === key;
    return (
      <th className={numeric ? "r" : ""} aria-sort={active ? (sort.descending ? "descending" : "ascending") : "none"}>
        <button
          type="button"
          className="sort"
          onClick={() =>
            setSort(active ? (sort.descending ? null : { key, descending: true }) : { key, descending: false })
          }
        >
          {label}
          <span aria-hidden="true">{active ? (sort.descending ? " ↓" : " ↑") : ""}</span>
        </button>
      </th>
    );
  }

  return (
    <Panel
      title="Watchlist"
      className="watchlist"
      aside={<span className="muted">{quotes.length} symbols · change vs previous 5-min window</span>}
    >
      {quotes.length === 0 ? (
        <Empty title={loading ? "Loading…" : "Your watchlist is empty"}>
          {loading ? undefined : "Search for a symbol and press Watch to follow it here."}
        </Empty>
      ) : (
        <div className="table-scroll">
          <table className="grid">
            <thead>
              <tr>
                {header("symbol", "Symbol", false)}
                <th>Exch</th>
                {header("last", "Last")}
                <th className="r">+/−</th>
                {header("changePct", "+/− %")}
                <th title="Price over the last 48 windows">Recent</th>
                <th className="r">EMA 38</th>
                <th className="r">EMA 100</th>
                {header("spreadPct", "Spread %")}
                <th aria-label="Spread bar" />
                <th>Trend</th>
                <th>Last signal</th>
                <th className="r">Time</th>
                <th aria-label="Remove" />
              </tr>
            </thead>
            <tbody>
              {ordered.map((quote) => {
                const { ticker, exchange } = splitSymbol(quote.symbol);
                const precision = pricePrecision(quote.last);
                return (
                  <tr
                    key={quote.symbol}
                    className={quote.symbol === selected ? "selected" : ""}
                    onClick={() => onSelect(quote.symbol)}
                  >
                    <td>
                      <button type="button" className="rowlink" onClick={() => onSelect(quote.symbol)}>
                        {ticker}
                      </button>
                    </td>
                    <td className="muted">{exchange}</td>
                    <td className="r num strong">{formatPrice(quote.last, precision)}</td>
                    <td className="r">
                      <Change value={quote.change}>{formatSigned(quote.change, precision)}</Change>
                    </td>
                    <td className="r">
                      <Change value={quote.changePct} />
                    </td>
                    <td>
                      <Sparkline values={quote.series} />
                    </td>
                    <td className="r num">{formatPrice(quote.ema38, precision)}</td>
                    <td className="r num">{formatPrice(quote.ema100, precision)}</td>
                    <td className="r">
                      <Change value={quote.spreadPct}>{formatPercent(quote.spreadPct)}</Change>
                    </td>
                    <td>
                      <SpreadBar value={quote.spreadPct} scale={spreadScale} />
                    </td>
                    <td>
                      <TrendTag trend={quote.trend} />
                    </td>
                    <td>
                      {quote.lastSignal ? (
                        <span className="signal-cell">
                          <SignalBadge type={quote.lastSignal.type} />
                          <span className="muted num">{formatTime(quote.lastSignal.windowEnd)}</span>
                        </span>
                      ) : (
                        <span className="dim">–</span>
                      )}
                    </td>
                    <td className="r num muted">{formatTime(quote.windowEnd)}</td>
                    <td className="r">
                      <button
                        type="button"
                        className="icon"
                        aria-label={`Remove ${quote.symbol} from watchlist`}
                        title="Remove from watchlist"
                        onClick={(event) => {
                          event.stopPropagation();
                          onRemove(quote.symbol);
                        }}
                      >
                        ×
                      </button>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </Panel>
  );
}
