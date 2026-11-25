import { useEffect, useMemo, useRef, useState } from "react";
import { direction, formatDateTime, formatPercent, formatPrice, splitSymbol } from "../format";
import type { Connection } from "../hooks";
import type { Quote } from "../model";

interface Props {
  symbols: string[];
  watchlist: string[];
  quotes: Quote[];
  latestWindowEnd: string | null | undefined;
  connection: Connection;
  onSelect: (symbol: string) => void;
  onToggleWatch: (symbol: string) => void;
}

const MAX_RESULTS = 40;

export function TopBar({ symbols, watchlist, quotes, latestWindowEnd, connection, onSelect, onToggleWatch }: Props) {
  const [query, setQuery] = useState("");
  const [open, setOpen] = useState(false);
  const [active, setActive] = useState(0);
  const input = useRef<HTMLInputElement>(null);
  const box = useRef<HTMLDivElement>(null);
  const watched = useMemo(() => new Set(watchlist), [watchlist]);

  const results = useMemo(() => {
    const needle = query.trim().toUpperCase();
    if (!needle) return symbols.slice(0, MAX_RESULTS);
    const starts: string[] = [];
    const contains: string[] = [];
    for (const symbol of symbols) {
      const upper = symbol.toUpperCase();
      if (upper.startsWith(needle)) starts.push(symbol);
      else if (upper.includes(needle)) contains.push(symbol);
      if (starts.length >= MAX_RESULTS) break;
    }
    return [...starts, ...contains].slice(0, MAX_RESULTS);
  }, [symbols, query]);

  useEffect(() => {
    function onKey(event: KeyboardEvent) {
      const target = event.target as HTMLElement | null;
      if (event.key === "/" && target?.tagName !== "INPUT") {
        event.preventDefault();
        input.current?.focus();
      }
    }
    function onPointer(event: PointerEvent) {
      if (!box.current?.contains(event.target as Node)) setOpen(false);
    }
    window.addEventListener("keydown", onKey);
    window.addEventListener("pointerdown", onPointer);
    return () => {
      window.removeEventListener("keydown", onKey);
      window.removeEventListener("pointerdown", onPointer);
    };
  }, []);

  function choose(symbol: string) {
    onSelect(symbol);
    setQuery("");
    setOpen(false);
    input.current?.blur();
  }

  const status = connection.failing ? "down" : connection.lastSuccess ? "live" : "idle";

  return (
    <header className="topbar">
      <div className="brand" aria-label="Crossover">
        cross<span aria-hidden="true">/</span>over
      </div>

      <div className="search" ref={box}>
        <input
          ref={input}
          type="search"
          role="combobox"
          aria-expanded={open}
          aria-controls="symbol-results"
          aria-label="Search symbols"
          placeholder={`Search ${symbols.length ? symbols.length.toLocaleString("en-US") + " " : ""}symbols`}
          value={query}
          onFocus={() => setOpen(true)}
          onChange={(event) => {
            setQuery(event.target.value);
            setActive(0);
            setOpen(true);
          }}
          onKeyDown={(event) => {
            if (event.key === "ArrowDown") {
              event.preventDefault();
              setOpen(true);
              setActive((index) => Math.min(index + 1, results.length - 1));
            } else if (event.key === "ArrowUp") {
              event.preventDefault();
              setActive((index) => Math.max(index - 1, 0));
            } else if (event.key === "Enter" && results[active]) {
              choose(results[active]);
            } else if (event.key === "Escape") {
              setOpen(false);
              input.current?.blur();
            }
          }}
        />
        <kbd aria-hidden="true">/</kbd>
        {open && (
          <ul id="symbol-results" className="results" role="listbox">
            {!query.trim() && results.length > 0 && (
              <li
                className="results-empty muted"
                style={{
                  borderBottom: "1px solid var(--line-soft)",
                  padding: "4px 10px",
                  fontSize: "11px",
                  fontWeight: 600,
                  letterSpacing: "0.06em",
                  textTransform: "uppercase",
                }}
              >
                Most Traded Symbols
              </li>
            )}
            {results.length === 0 && <li className="results-empty muted">No symbol matches “{query}”.</li>}
            {results.map((symbol, index) => {
              const { ticker, exchangeName } = splitSymbol(symbol);
              return (
                <li
                  key={symbol}
                  role="option"
                  aria-selected={index === active}
                  className={index === active ? "active" : ""}
                  onPointerEnter={() => setActive(index)}
                  onClick={() => choose(symbol)}
                >
                  <span className="results-ticker">{ticker}</span>
                  <span className="muted">{exchangeName}</span>
                  <button
                    type="button"
                    className={`icon star ${watched.has(symbol) ? "active" : ""}`}
                    aria-label={watched.has(symbol) ? `Stop watching ${symbol}` : `Watch ${symbol}`}
                    onClick={(event) => {
                      event.stopPropagation();
                      onToggleWatch(symbol);
                    }}
                  >
                    {watched.has(symbol) ? "★" : "☆"}
                  </button>
                </li>
              );
            })}
          </ul>
        )}
      </div>

      <div className="ticker" aria-label="Watchlist prices">
        {quotes.map((quote) => {
          const move = direction(quote.changePct);
          return (
            <button type="button" key={quote.symbol} className="tick" onClick={() => onSelect(quote.symbol)}>
              <span className="tick-name">
                <span className={move} aria-hidden="true">
                  {move === "up" ? "▲" : move === "down" ? "▼" : "•"}
                </span>
                {splitSymbol(quote.symbol).ticker}
              </span>
              <span className="tick-values num">
                <span className={move}>{formatPercent(quote.changePct)}</span>
                <span>{formatPrice(quote.last)}</span>
              </span>
            </button>
          );
        })}
      </div>

      <div className="clock">
        <span className="label">Market time · Paris</span>
        <strong className="num">{latestWindowEnd ? formatDateTime(latestWindowEnd) : "–"}</strong>
      </div>
      <div className={`status status-${status}`} title={connection.error ?? undefined}>
        <i aria-hidden="true" />
        {status === "live" ? "Live" : status === "down" ? "Offline" : "Connecting"}
      </div>
    </header>
  );
}
