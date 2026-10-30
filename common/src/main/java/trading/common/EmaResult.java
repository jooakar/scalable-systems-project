package trading.common;

/**
 * The result of computing EMAs for a symbol over one 5-minute window.
 *
 * @param symbol     symbol ID (e.g. "RDSA.NL")
 * @param windowEnd  ISO-8601 timestamp of the window's end
 * @param lastPrice  the closing price (last price seen in the window)
 * @param ema38      EMA with smoothing factor j=38
 * @param ema100     EMA with smoothing factor j=100
 */
public record EmaResult(
        String symbol,
        String windowEnd,
        double lastPrice,
        double ema38,
        double ema100
) {}
