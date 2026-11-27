package trading.flink;

import org.junit.jupiter.api.Test;

import trading.common.TickEvent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Verifies the EMA formula used in EmaWindowFunction.
 *
 * <p>Formula:
 * <pre>
 *   EMA_j(w_i) = Close(w_i) * (2/(1+j))  +  EMA_j(w_{i-1}) * (1 - 2/(1+j))
 * </pre>
 * Initial value: EMA_j(w_0) = 0
 */
class EmaWindowFunctionTest {

    private static final double M38 = 2.0 / (1 + 38); // ≈ 0.05128
    private static final double M100 = 2.0 / (1 + 100); // ≈ 0.01980
    private static final double DELTA = 1e-9;

    @Test
    void firstWindowStartsFromZero() {
        // Window 0: prevEMA = 0 (per spec: EMA_j(w_0) = 0)
        double close = 100.0;
        double ema38 = close * M38 + 0.0 * (1 - M38);
        double ema100 = close * M100 + 0.0 * (1 - M100);

        assertEquals(close * M38, ema38, DELTA);
        assertEquals(close * M100, ema100, DELTA);
    }

    @Test
    void secondWindowUsesFirstWindowEma() {
        double close0 = 100.0;
        double ema38_0 = close0 * M38; // prevEMA = 0 for first window
        double ema100_0 = close0 * M100;

        double close1 = 105.0;
        double ema38_1 = close1 * M38 + ema38_0 * (1 - M38);
        double ema100_1 = close1 * M100 + ema100_0 * (1 - M100);

        double expected38 = 105.0 * M38 + (100.0 * M38) * (1 - M38);
        double expected100 = 105.0 * M100 + (100.0 * M100) * (1 - M100);

        assertEquals(expected38, ema38_1, DELTA);
        assertEquals(expected100, ema100_1, DELTA);
    }

    @Test
    void emaConvergesToConstantPriceOverTime() {
        // If price stays constant, EMA should converge towards that price.
        double constantPrice = 50.0;
        double ema38 = 0.0;
        double ema100 = 0.0;
        for (int i = 0; i < 1000; i++) {
            ema38 = constantPrice * M38 + ema38 * (1 - M38);
            ema100 = constantPrice * M100 + ema100 * (1 - M100);
        }
        assertEquals(constantPrice, ema38, 1e-6);
        assertEquals(constantPrice, ema100, 1e-6);
    }

    @Test
    void laterPicksLatestTradingTimeAndBreaksTiesByArrival() {
        TickEvent early = new TickEvent("ADP.FR", "E", "08-11-2021", "09:00:00.000", 10.0, "09:00:01.000", null);
        TickEvent late = new TickEvent("ADP.FR", "E", "08-11-2021", "09:00:00.000", 11.0, "09:00:02.000", null);
        TickEvent tie = new TickEvent("ADP.FR", "E", "08-11-2021", "09:00:00.000", 12.0, "09:00:02.000", null);
        TickEvent noTradingTime = new TickEvent("ADP.FR", "E", "08-11-2021", "09:00:03.000", 13.0, null, null);

        assertSame(late, EmaWindowFunction.later(early, late));
        assertSame(late, EmaWindowFunction.later(late, early));
        assertSame(tie, EmaWindowFunction.later(late, tie));
        assertSame(noTradingTime, EmaWindowFunction.later(late, noTradingTime));
    }
}
