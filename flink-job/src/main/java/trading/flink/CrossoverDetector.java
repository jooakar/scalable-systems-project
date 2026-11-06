package trading.flink;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import trading.common.CrossoverEvent;
import trading.common.EmaResult;

/**
 * Detects bullish and bearish EMA crossovers (Query 2).
 *
 * <p>For each symbol the previous window's EMA-38 and EMA-100 values are kept
 * in Flink keyed state. When a new {@link EmaResult} arrives the detector
 * compares the current and previous relationship between the two EMAs:
 *
 * <ul>
 *   <li><b>Bullish breakout (BUY)</b>: EMA38(w_i) &gt; EMA100(w_i)
 *       AND EMA38(w_{i-1}) &le; EMA100(w_{i-1})</li>
 *   <li><b>Bearish breakout (SELL)</b>: EMA38(w_i) &lt; EMA100(w_i)
 *       AND EMA38(w_{i-1}) &ge; EMA100(w_{i-1})</li>
 * </ul>
 *
 * <p>No event is emitted for the very first window of a symbol (no previous
 * state to compare against).
 */
public class CrossoverDetector extends KeyedProcessFunction<String, EmaResult, CrossoverEvent> {

    private transient ValueState<Double> prevEma38;
    private transient ValueState<Double> prevEma100;

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        prevEma38 = getRuntimeContext().getState(
                new ValueStateDescriptor<>("crossover-prev-ema38", Types.DOUBLE));
        prevEma100 = getRuntimeContext().getState(
                new ValueStateDescriptor<>("crossover-prev-ema100", Types.DOUBLE));
    }

    @Override
    public void processElement(EmaResult ema, Context ctx, Collector<CrossoverEvent> out)
            throws Exception {
        Double prev38 = prevEma38.value();
        Double prev100 = prevEma100.value();

        if (prev38 != null && prev100 != null) {
            boolean prevBelowOrEqual = prev38 <= prev100;
            boolean currAbove = ema.ema38() > ema.ema100();

            boolean prevAboveOrEqual = prev38 >= prev100;
            boolean currBelow = ema.ema38() < ema.ema100();

            if (prevBelowOrEqual && currAbove) {
                // EMA-38 crossed above EMA-100 — bullish breakout
                out.collect(new CrossoverEvent(
                        ema.symbol(), ema.windowEnd(), CrossoverEvent.BUY,
                        ema.ema38(), ema.ema100()));
            } else if (prevAboveOrEqual && currBelow) {
                // EMA-38 crossed below EMA-100 — bearish breakout
                out.collect(new CrossoverEvent(
                        ema.symbol(), ema.windowEnd(), CrossoverEvent.SELL,
                        ema.ema38(), ema.ema100()));
            }
        }

        // Always persist current values for the next window
        prevEma38.update(ema.ema38());
        prevEma100.update(ema.ema100());
    }
}
