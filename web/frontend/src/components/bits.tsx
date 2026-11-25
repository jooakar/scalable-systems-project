import type { ReactNode } from "react";
import { direction, formatPercent } from "../format";
import type { Quote } from "../model";
import type { SignalType } from "../types";

/** BUY/SELL tag. The arrow and the word carry the meaning, not the color alone. */
export function SignalBadge({ type }: { type: SignalType }) {
  return (
    <span className={`badge badge-${type.toLowerCase()}`}>
      <span aria-hidden="true">{type === "BUY" ? "▲" : "▼"}</span>
      {type}
    </span>
  );
}

export function TrendTag({ trend }: { trend: Quote["trend"] }) {
  if (trend == null) return <span className="dim">–</span>;
  const glyph = trend === "bullish" ? "▲" : trend === "bearish" ? "▼" : "■";
  return (
    <span className={`trend trend-${trend}`}>
      <span aria-hidden="true">{glyph}</span>
      {trend === "bullish" ? "Bullish" : trend === "bearish" ? "Bearish" : "Neutral"}
    </span>
  );
}

export function Change({ value, children }: { value: number | null; children?: ReactNode }) {
  return <span className={`num ${direction(value)}`}>{children ?? formatPercent(value)}</span>;
}

export function Sparkline({ values, width = 96, height = 22 }: { values: number[]; width?: number; height?: number }) {
  if (values.length < 2) return <span className="dim">–</span>;
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = max - min || 1;
  const step = (width - 2) / (values.length - 1);
  const points = values
    .map((value, index) => `${(1 + index * step).toFixed(1)},${(1 + (height - 2) * (1 - (value - min) / span)).toFixed(1)}`)
    .join(" ");
  const trend = direction(values[values.length - 1] - values[0]);
  return (
    <svg className={`spark ${trend}`} width={width} height={height} viewBox={`0 0 ${width} ${height}`} aria-hidden="true">
      <polyline points={points} fill="none" strokeWidth="1.5" strokeLinejoin="round" strokeLinecap="round" />
    </svg>
  );
}

/** Diverging bar: EMA 38 above EMA 100 grows right, below grows left. */
export function SpreadBar({ value, scale }: { value: number | null; scale: number }) {
  if (value == null) return <span className="dim">–</span>;
  const share = scale > 0 ? Math.min(1, Math.abs(value) / scale) * 50 : 0;
  const side = direction(value);
  return (
    <span className="spreadbar" aria-hidden="true">
      <span
        className={`spreadbar-fill ${side}`}
        style={side === "down" ? { right: "50%", width: `${share}%` } : { left: "50%", width: `${share}%` }}
      />
    </span>
  );
}

export function Panel({
  title,
  aside,
  className = "",
  children,
}: {
  title: ReactNode;
  aside?: ReactNode;
  className?: string;
  children: ReactNode;
}) {
  return (
    <section className={`panel ${className}`}>
      <header className="panel-head">
        <h2>{title}</h2>
        {aside && <div className="panel-aside">{aside}</div>}
      </header>
      {children}
    </section>
  );
}

export function Empty({ title, children }: { title: string; children?: ReactNode }) {
  return (
    <div className="empty">
      <strong>{title}</strong>
      {children && <span>{children}</span>}
    </div>
  );
}
