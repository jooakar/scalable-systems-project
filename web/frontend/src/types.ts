export type SignalType = "BUY" | "SELL";

export interface TrendRow {
  symbol: string;
  windowEnd: string;
  lastPrice: number | null;
  ema38: number | null;
  ema100: number | null;
  receivedAt: string | null;
}

export interface Signal {
  symbol: string;
  windowEnd: string;
  type: SignalType;
  ema38: number | null;
  ema100: number | null;
  receivedAt: string | null;
}

export interface WatchPoint {
  windowEnd: string;
  lastPrice: number | null;
  ema38: number | null;
  ema100: number | null;
}

export interface WatchEntry {
  symbol: string;
  rows: WatchPoint[];
  lastSignal: Signal | null;
}

export interface Summary {
  latestWindowEnd: string | null;
  buySignals: number;
  sellSignals: number;
}
