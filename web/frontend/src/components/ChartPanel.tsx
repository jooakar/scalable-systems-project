import {
  AreaSeries,
  ColorType,
  CrosshairMode,
  HistogramSeries,
  LineSeries,
  LineStyle,
  createChart,
  createSeriesMarkers,
  type IChartApi,
  type ISeriesApi,
  type ISeriesMarkersPluginApi,
  type SeriesMarker,
  type Time,
  type UTCTimestamp,
} from "lightweight-charts";
import { useEffect, useMemo, useRef, useState } from "react";
import { chartTime, formatDateTime, formatPrice, formatSigned, pricePrecision } from "../format";
import { theme } from "../theme";
import type { Signal, TrendRow } from "../types";
import { Empty } from "./bits";

export const RANGES = [
  { label: "6H", hours: 6 },
  { label: "1D", hours: 24 },
  { label: "3D", hours: 72 },
  { label: "1W", hours: 168 },
];

type Fit = "price" | "all";

interface Handles {
  chart: IChartApi;
  price: ISeriesApi<"Area">;
  ema38: ISeriesApi<"Line">;
  ema100: ISeriesApi<"Line">;
  spread: ISeriesApi<"Histogram">;
  markers: ISeriesMarkersPluginApi<Time>;
}

interface Props {
  symbol: string | null;
  rows: TrendRow[];
  signals: Signal[];
  loading: boolean;
  rangeHours: number;
  onRange: (hours: number) => void;
}

export function ChartPanel({ symbol, rows, signals, loading, rangeHours, onRange }: Props) {
  const container = useRef<HTMLDivElement>(null);
  const handles = useRef<Handles | null>(null);
  const fitted = useRef<string | null>(null);
  const [fit, setFit] = useState<Fit>("price");
  const [hovered, setHovered] = useState<number | null>(null);

  // Chart times must be strictly increasing; rows arrive ordered by window end.
  const points = useMemo(() => {
    const result: { time: UTCTimestamp; row: TrendRow }[] = [];
    let previous = -Infinity;
    for (const row of rows) {
      const time = chartTime(row.windowEnd);
      if (time > previous) {
        result.push({ time: time as UTCTimestamp, row });
        previous = time;
      }
    }
    return result;
  }, [rows]);
  const byTime = useMemo(() => new Map(points.map((point) => [point.time as number, point.row])), [points]);

  useEffect(() => {
    if (!container.current) return undefined;

    const chart = createChart(container.current, {
      autoSize: true,
      layout: {
        background: { type: ColorType.Solid, color: theme.panel },
        textColor: theme.muted,
        fontFamily: theme.font,
        fontSize: 11,
        attributionLogo: false,
        panes: { separatorColor: theme.line, separatorHoverColor: theme.line, enableResize: false },
      },
      grid: { vertLines: { visible: false }, horzLines: { color: theme.grid, style: LineStyle.Dotted } },
      rightPriceScale: { borderVisible: false },
      timeScale: { borderVisible: false, timeVisible: true, secondsVisible: false, rightOffset: 3 },
      crosshair: {
        mode: CrosshairMode.Normal,
        vertLine: { color: theme.muted, width: 1, style: LineStyle.Dashed, labelBackgroundColor: theme.line },
        horzLine: { color: theme.muted, width: 1, style: LineStyle.Dashed, labelBackgroundColor: theme.line },
      },
    });

    const price = chart.addSeries(AreaSeries, {
      lineColor: theme.price,
      lineWidth: 2,
      topColor: theme.priceWash,
      bottomColor: theme.priceFade,
      priceLineVisible: true,
      priceLineColor: theme.price,
      crosshairMarkerRadius: 4,
    });
    const ema38 = chart.addSeries(LineSeries, {
      color: theme.ema38,
      lineWidth: 2,
      priceLineVisible: false,
      lastValueVisible: true,
      crosshairMarkerRadius: 3,
    });
    const ema100 = chart.addSeries(LineSeries, {
      color: theme.ema100,
      lineWidth: 2,
      priceLineVisible: false,
      lastValueVisible: true,
      crosshairMarkerRadius: 3,
    });
    const spread = chart.addSeries(
      HistogramSeries,
      { priceLineVisible: false, lastValueVisible: false, base: 0 },
      1,
    );
    // Leave headroom for the pane label.
    spread.priceScale().applyOptions({ scaleMargins: { top: 0.28, bottom: 0.08 } });
    spread.createPriceLine({ price: 0, color: theme.muted, lineWidth: 1, lineStyle: LineStyle.Solid, axisLabelVisible: false });
    chart.panes()[1]?.setHeight(110);

    const markers = createSeriesMarkers(price, []);
    chart.subscribeCrosshairMove((param) => {
      setHovered(typeof param.time === "number" && param.point ? param.time : null);
    });

    handles.current = { chart, price, ema38, ema100, spread, markers };
    return () => {
      handles.current = null;
      chart.remove();
    };
  }, []);

  useEffect(() => {
    const current = handles.current;
    if (!current) return;

    const precision = pricePrecision(points[points.length - 1]?.row.lastPrice);
    const priceFormat = { type: "price" as const, precision, minMove: 1 / 10 ** precision };
    current.price.applyOptions({ priceFormat });
    current.ema38.applyOptions({ priceFormat });
    current.ema100.applyOptions({ priceFormat });
    current.spread.applyOptions({ priceFormat });
    current.chart.applyOptions({
      localization: { priceFormatter: (value: number) => formatPrice(value, precision) },
    });

    current.price.setData(points.map(({ time, row }) => ({ time, value: row.lastPrice as number })));
    current.ema38.setData(points.map(({ time, row }) => ({ time, value: row.ema38 ?? 0 })));
    current.ema100.setData(points.map(({ time, row }) => ({ time, value: row.ema100 ?? 0 })));
    current.spread.setData(
      points.map(({ time, row }) => {
        const value = (row.ema38 ?? 0) - (row.ema100 ?? 0);
        return { time, value, color: value >= 0 ? theme.buyBar : theme.sellBar };
      }),
    );

    // Reframe only when the view changes, so live updates keep the user's zoom.
    const view = `${symbol}|${rangeHours}`;
    if (points.length > 0 && fitted.current !== view) {
      current.chart.timeScale().fitContent();
      fitted.current = view;
    }
  }, [points, symbol, rangeHours]);

  useEffect(() => {
    const current = handles.current;
    if (!current) return;

    const visible: SeriesMarker<Time>[] = [];
    for (const signal of signals) {
      const time = chartTime(signal.windowEnd);
      if (!byTime.has(time)) continue;
      const buy = signal.type === "BUY";
      visible.push({
        time: time as UTCTimestamp,
        position: buy ? "belowBar" : "aboveBar",
        shape: buy ? "arrowUp" : "arrowDown",
        color: buy ? theme.buy : theme.sell,
        text: signal.type,
        size: 1.4,
      });
    }
    current.markers.setMarkers(visible);
  }, [signals, byTime]);

  useEffect(() => {
    const current = handles.current;
    if (!current) return;
    // While the averages are still far from the price they would flatten the
    // price line, so "Price" scaling leaves them out of the autoscale.
    const provider = fit === "price" ? () => null : undefined;
    current.ema38.applyOptions({ autoscaleInfoProvider: provider });
    current.ema100.applyOptions({ autoscaleInfoProvider: provider });
  }, [fit]);

  const shown = (hovered != null ? byTime.get(hovered) : undefined) ?? points[points.length - 1]?.row;
  const precision = pricePrecision(shown?.lastPrice);
  const spreadValue = shown?.ema38 != null && shown.ema100 != null ? shown.ema38 - shown.ema100 : null;

  return (
    <section className="panel chart-panel">
      <header className="panel-head chart-head">
        <div className="legend" aria-live="off">
          <span className="legend-item">
            <i className="key key-price" />
            Price <b className="num">{formatPrice(shown?.lastPrice, precision)}</b>
          </span>
          <span className="legend-item">
            <i className="key key-ema38" />
            EMA 38 <b className="num">{formatPrice(shown?.ema38, precision)}</b>
          </span>
          <span className="legend-item">
            <i className="key key-ema100" />
            EMA 100 <b className="num">{formatPrice(shown?.ema100, precision)}</b>
          </span>
          <span className="legend-item">
            Spread <b className="num">{formatSigned(spreadValue, precision)}</b>
          </span>
          <span className="legend-time num">{shown ? formatDateTime(shown.windowEnd) : ""}</span>
        </div>
        <div className="chart-controls">
          <div className="segmented" role="group" aria-label="Price scale">
            {(["price", "all"] as const).map((mode) => (
              <button
                key={mode}
                type="button"
                className={fit === mode ? "active" : ""}
                aria-pressed={fit === mode}
                title={mode === "price" ? "Scale the axis to the price only" : "Scale the axis to price and both EMAs"}
                onClick={() => setFit(mode)}
              >
                {mode === "price" ? "Fit price" : "Fit all"}
              </button>
            ))}
          </div>
          <div className="segmented" role="group" aria-label="Time range">
            {RANGES.map((range) => (
              <button
                key={range.hours}
                type="button"
                className={rangeHours === range.hours ? "active" : ""}
                aria-pressed={rangeHours === range.hours}
                onClick={() => onRange(range.hours)}
              >
                {range.label}
              </button>
            ))}
          </div>
        </div>
      </header>
      <div className="chart-body">
        <div ref={container} className={`chart-canvas ${loading ? "stale" : ""}`} />
        <span className="pane-label">EMA 38 − EMA 100</span>
        {points.length === 0 && (
          <div className="chart-empty">
            {symbol == null ? (
              <Empty title="No symbol selected">Search for a symbol or pick one from the watchlist.</Empty>
            ) : loading ? (
              <Empty title="Loading…" />
            ) : (
              <Empty title="No windows in this range">No 5-minute windows are stored for {symbol} yet.</Empty>
            )}
          </div>
        )}
      </div>
    </section>
  );
}
