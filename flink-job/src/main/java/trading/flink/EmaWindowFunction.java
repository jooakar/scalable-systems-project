package trading.flink;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import trading.common.EmaResult;
import trading.common.TickEvent;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * For each 5-minute tumbling window per symbol, finds the last price event
 * (Close) and computes EMA-38 and EMA-100 using the previous window's EMA
 * stored in keyed state.
 *
 * <p>EMA formula:
 * <pre>
 *   EMA_j(w_i) = Close(w_i) * (2 / (1 + j))  +  EMA_j(w_{i-1}) * (1 - 2 / (1 + j))
 * </pre>
 * where Close(w_i) is the last price in window w_i, and EMA_j(w_0) = 0.
 */
public class EmaWindowFunction
        extends ProcessWindowFunction<TickEvent, EmaResult, String, TimeWindow> {

    /** Smoothing multiplier for j=38: 2/(1+38) ≈ 0.05128 */
    private static final double MULTIPLIER_38  = 2.0 / (1 + 38);
    /** Smoothing multiplier for j=100: 2/(1+100) ≈ 0.01980 */
    private static final double MULTIPLIER_100 = 2.0 / (1 + 100);

    private static final DateTimeFormatter FMT =
            DateTimeFormatter.ISO_OFFSET_DATE_TIME
                    .withZone(ZoneId.of("Europe/Paris"));

    // Keyed state: previous EMA values survive across windows and job restarts
    private transient ValueState<Double> prevEma38;
    private transient ValueState<Double> prevEma100;

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        prevEma38 = getRuntimeContext().getState(
                new ValueStateDescriptor<>("ema38", Types.DOUBLE));
        prevEma100 = getRuntimeContext().getState(
                new ValueStateDescriptor<>("ema100", Types.DOUBLE));
    }

    @Override
    public void process(
            String symbol,
            Context context,
            Iterable<TickEvent> events,
            Collector<EmaResult> out) throws Exception {

        // Find the closing price: the last price event observed in the window,
        // selected by the latest trading time string (falling back to update time).
        Double closePrice = null;
        String latestTime = null;
        for (TickEvent e : events) {
            if (e.last() != null) {
                String eventTime = e.tradingTime() != null ? e.tradingTime() : e.time();
                if (latestTime == null || (eventTime != null && eventTime.compareTo(latestTime) >= 0)) {
                    closePrice = e.last();
                    latestTime = eventTime;
                }
            }
        }

        if (closePrice == null) {
            return; // no price events in this window for this symbol
        }

        // Retrieve previous EMA values (default to 0.0 for the very first window)
        double prev38  = prevEma38.value()  != null ? prevEma38.value()  : 0.0;
        double prev100 = prevEma100.value() != null ? prevEma100.value() : 0.0;

        // EMA formula: EMA_j(w_i) = Close * (2/(1+j)) + prevEMA * (1 - 2/(1+j))
        double ema38  = closePrice * MULTIPLIER_38  + prev38  * (1 - MULTIPLIER_38);
        double ema100 = closePrice * MULTIPLIER_100 + prev100 * (1 - MULTIPLIER_100);

        // Persist for the next window
        prevEma38.update(ema38);
        prevEma100.update(ema100);

        // Emit result
        String windowEnd = FMT.format(Instant.ofEpochMilli(context.window().getEnd()));
        out.collect(new EmaResult(symbol, windowEnd, closePrice, ema38, ema100));
    }
}
