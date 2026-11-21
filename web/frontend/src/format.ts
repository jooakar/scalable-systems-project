/** The data set is recorded in exchange-local time, so the UI shows that zone. */
export const MARKET_ZONE = "Europe/Paris";

const EXCHANGES: Record<string, { code: string; name: string }> = {
  FR: { code: "PAR", name: "Euronext Paris" },
  NL: { code: "AMS", name: "Euronext Amsterdam" },
  ETR: { code: "XETRA", name: "Xetra Frankfurt" },
};

export function splitSymbol(symbol: string): { ticker: string; exchange: string; exchangeName: string } {
  const dot = symbol.lastIndexOf(".");
  if (dot <= 0) {
    return { ticker: symbol, exchange: "", exchangeName: "" };
  }
  const suffix = symbol.slice(dot + 1);
  const known = EXCHANGES[suffix];
  return {
    ticker: symbol.slice(0, dot),
    exchange: known?.code ?? suffix,
    exchangeName: known?.name ?? suffix,
  };
}

/** Decimal places that keep small prices readable without padding large ones. */
export function pricePrecision(value: number | null | undefined): number {
  const magnitude = Math.abs(value ?? 0);
  if (magnitude === 0 || magnitude >= 100) return 2;
  if (magnitude >= 1) return 3;
  return 4;
}

export function formatPrice(value: number | null | undefined, precision?: number): string {
  if (value == null || !Number.isFinite(value)) return "–";
  const digits = precision ?? pricePrecision(value);
  return value.toLocaleString("en-US", { minimumFractionDigits: digits, maximumFractionDigits: digits });
}

export function formatSigned(value: number | null | undefined, precision?: number): string {
  if (value == null || !Number.isFinite(value)) return "–";
  const digits = precision ?? pricePrecision(value);
  const rounded = Number(value.toFixed(digits));
  const text = formatPrice(Math.abs(rounded), digits);
  if (rounded > 0) return `+${text}`;
  if (rounded < 0) return `−${text}`;
  return text;
}

export function formatPercent(value: number | null | undefined, digits = 2): string {
  if (value == null || !Number.isFinite(value)) return "–";
  // Sign follows the rounded value, so a tiny move never prints as "−0.00%".
  const rounded = Number(value.toFixed(digits));
  const text = `${Math.abs(rounded).toFixed(digits)}%`;
  if (rounded > 0) return `+${text}`;
  if (rounded < 0) return `−${text}`;
  return text;
}

export function formatCount(value: number | null | undefined): string {
  return value == null ? "–" : value.toLocaleString("en-US");
}

export function direction(value: number | null | undefined): "up" | "down" | "flat" {
  if (value == null || !Number.isFinite(value) || Math.abs(value) < 5e-5) return "flat";
  return value > 0 ? "up" : "down";
}

const timeFormat = new Intl.DateTimeFormat("en-GB", {
  timeZone: MARKET_ZONE,
  hour: "2-digit",
  minute: "2-digit",
});
const dayFormat = new Intl.DateTimeFormat("en-GB", {
  timeZone: MARKET_ZONE,
  day: "2-digit",
  month: "short",
});
const dayKeyFormat = new Intl.DateTimeFormat("en-CA", { timeZone: MARKET_ZONE });

export function formatTime(iso: string | null | undefined): string {
  return iso ? timeFormat.format(new Date(iso)) : "–";
}

export function formatDay(iso: string | null | undefined): string {
  return iso ? dayFormat.format(new Date(iso)) : "–";
}

export function formatDateTime(iso: string | null | undefined): string {
  return iso ? `${formatDay(iso)} ${formatTime(iso)}` : "–";
}

/** Calendar day of an instant in the market zone, used to group rows. */
export function dayKey(iso: string): string {
  return dayKeyFormat.format(new Date(iso));
}

const offsetFormat = new Intl.DateTimeFormat("en-US", {
  timeZone: MARKET_ZONE,
  hourCycle: "h23",
  year: "numeric",
  month: "numeric",
  day: "numeric",
  hour: "numeric",
  minute: "numeric",
  second: "numeric",
});
const offsetCache = new Map<number, number>();

/** Offset of the market zone from UTC, in seconds, at the given instant. */
function zoneOffsetSeconds(epochMs: number): number {
  const hour = Math.floor(epochMs / 3_600_000);
  const cached = offsetCache.get(hour);
  if (cached !== undefined) return cached;

  const parts: Record<string, number> = {};
  for (const part of offsetFormat.formatToParts(new Date(hour * 3_600_000))) {
    if (part.type !== "literal") parts[part.type] = Number(part.value);
  }
  const wallClock = Date.UTC(parts.year, parts.month - 1, parts.day, parts.hour, parts.minute, parts.second);
  const offset = Math.round((wallClock - hour * 3_600_000) / 1000);
  offsetCache.set(hour, offset);
  return offset;
}

/**
 * The chart library draws timestamps as UTC. Shifting each instant by the
 * market zone's offset makes its axis read in exchange-local time.
 */
export function chartTime(iso: string): number {
  const epochMs = new Date(iso).getTime();
  return Math.floor(epochMs / 1000) + zoneOffsetSeconds(epochMs);
}

/** "3 windows ago" style distance between two window ends (5-minute windows). */
export function windowsBetween(fromIso: string, toIso: string): number {
  return Math.max(0, Math.round((new Date(toIso).getTime() - new Date(fromIso).getTime()) / 300_000));
}

export function formatAge(ms: number): string {
  const seconds = Math.max(0, Math.round(ms / 1000));
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.round(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  return `${Math.round(minutes / 60)}h ago`;
}
