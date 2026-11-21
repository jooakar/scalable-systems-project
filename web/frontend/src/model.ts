import type { Signal, TrendRow, WatchEntry } from "./types";

/** A watchlist entry reduced to the numbers the tables and ticker show. */
export interface Quote {
  symbol: string;
  windowEnd: string | null;
  last: number | null;
  change: number | null;
  changePct: number | null;
  ema38: number | null;
  ema100: number | null;
  spread: number | null;
  spreadPct: number | null;
  trend: "bullish" | "bearish" | "neutral" | null;
  series: number[];
  lastSignal: Signal | null;
}

export function trendOf(ema38: number | null, ema100: number | null, windowCount?: number): Quote["trend"] {
  if (ema38 == null || ema100 == null || (windowCount !== undefined && windowCount < 2)) return null;
  if (ema38 > ema100) return "bullish";
  if (ema38 < ema100) return "bearish";
  return "neutral";
}

export function toQuote(entry: WatchEntry): Quote {
  const rows = entry.rows.filter((row) => row.lastPrice != null && row.lastPrice !== 0);
  const latest = rows[rows.length - 1];
  const previous = rows[rows.length - 2];
  const last = latest?.lastPrice ?? null;
  const change = last != null && previous?.lastPrice != null ? last - previous.lastPrice : null;
  const ema38 = latest?.ema38 ?? null;
  const ema100 = latest?.ema100 ?? null;
  const spread = ema38 != null && ema100 != null ? ema38 - ema100 : null;

  return {
    symbol: entry.symbol,
    windowEnd: latest?.windowEnd ?? null,
    last,
    change,
    changePct: change != null && previous?.lastPrice ? (change / previous.lastPrice) * 100 : null,
    ema38,
    ema100,
    spread,
    spreadPct: spread != null && ema100 ? (spread / ema100) * 100 : null,
    trend: trendOf(ema38, ema100, rows.length),
    series: rows.map((row) => row.lastPrice as number),
    lastSignal: entry.lastSignal,
  };
}

/** Summary of one symbol over the loaded chart range. */
export interface RangeStats {
  latest: TrendRow | null;
  change: number | null;
  changePct: number | null;
  rangeChangePct: number | null;
  high: number | null;
  low: number | null;
  windows: number;
  spread: number | null;
  spreadPct: number | null;
  trend: Quote["trend"];
  /** How far EMA 100 has converged on the price; it starts from zero. */
  warmup: number | null;
  buys: number;
  sells: number;
  lastSignal: Signal | null;
}

export function rangeStats(rows: TrendRow[], signals: Signal[]): RangeStats {
  const latest = rows[rows.length - 1] ?? null;
  const previous = rows[rows.length - 2] ?? null;
  const first = rows[0] ?? null;
  const prices = rows.map((row) => row.lastPrice as number);
  const last = latest?.lastPrice ?? null;
  const change = last != null && previous?.lastPrice != null ? last - previous.lastPrice : null;
  const ema38 = latest?.ema38 ?? null;
  const ema100 = latest?.ema100 ?? null;
  const spread = ema38 != null && ema100 != null ? ema38 - ema100 : null;

  return {
    latest,
    change,
    changePct: change != null && previous?.lastPrice ? (change / previous.lastPrice) * 100 : null,
    rangeChangePct:
      last != null && first?.lastPrice && rows.length > 1 ? ((last - first.lastPrice) / first.lastPrice) * 100 : null,
    high: prices.length ? Math.max(...prices) : null,
    low: prices.length ? Math.min(...prices) : null,
    windows: rows.length,
    spread,
    spreadPct: spread != null && ema100 ? (spread / ema100) * 100 : null,
    trend: trendOf(ema38, ema100, rows.length),
    warmup: last && ema100 != null ? ema100 / last : null,
    buys: signals.filter((signal) => signal.type === "BUY").length,
    sells: signals.filter((signal) => signal.type === "SELL").length,
    lastSignal: signals[signals.length - 1] ?? null,
  };
}
