package trading.flink;

import org.junit.jupiter.api.Test;
import trading.common.TickEvent;

import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TrendJobTest {

    @Test
    void rejectsEventsWithoutPriceOrTimestamps() {
        // Housekeeping event: no price, no time
        TickEvent housekeeping = new TickEvent("ADP.FR", "E", "08-11-2021", null, null, null, null);
        assertFalse(TrendJob.isUsablePriceEvent(housekeeping));

        // Price event without any timestamp
        TickEvent priceWithoutTime = new TickEvent("ADP.FR", "E", "08-11-2021", "", 10.5, "", null);
        assertFalse(TrendJob.isUsablePriceEvent(priceWithoutTime));

        // Midnight housekeeping / non-positive price events
        TickEvent zeroPrice = new TickEvent("723813.ETR", "I", "08-11-2021", "00:00:00.000", 0.0, "00:00:00.000", null);
        assertFalse(TrendJob.isUsablePriceEvent(zeroPrice));

        TickEvent negativePrice = new TickEvent("723813.ETR", "I", "08-11-2021", "00:00:00.000", -5.0, "00:00:00.000", null);
        assertFalse(TrendJob.isUsablePriceEvent(negativePrice));

        // Valid price event
        TickEvent validPrice = new TickEvent("RDSA.NL", "E", "08-11-2021", "09:00:00.000", 42.0, "09:00:00.123", null);
        assertTrue(TrendJob.isUsablePriceEvent(validPrice));
    }

    @Test
    void extractsEpochMillisFromDateAndTradingTime() {
        TickEvent event = new TickEvent(
                "RDSA.NL", "E", "08-11-2021", "09:00:00.000",
                42.0, "09:00:00.123", null);

        long millis = TrendJob.extractEpochMillis(event);

        ZonedDateTime expected = ZonedDateTime.of(
                2021, 11, 8, 9, 0, 0, 123_000_000,
                ZoneId.of("Europe/Paris"));
        assertEquals(expected.toInstant().toEpochMilli(), millis);
    }

    @Test
    void fallsBackToTimeWhenTradingTimeIsNull() {
        TickEvent event = new TickEvent(
                "ALO.FR", "E", "08-11-2021", "12:37:13.000",
                null, null, null);

        long millis = TrendJob.extractEpochMillis(event);

        ZonedDateTime expected = ZonedDateTime.of(
                2021, 11, 8, 12, 37, 13, 0,
                ZoneId.of("Europe/Paris"));
        assertEquals(expected.toInstant().toEpochMilli(), millis);
    }

    @Test
    void parsesSubMillisFractionalDigits() {
        // tradingTime with 4 fractional digits: "12:37:13.3600"
        TickEvent event = new TickEvent(
                "A1EX3A.ETR", "I", "08-11-2021", "12:37:13.000",
                2143.69, "12:37:13.3600", null);

        long millis = TrendJob.extractEpochMillis(event);

        ZonedDateTime expected = ZonedDateTime.of(
                2021, 11, 8, 12, 37, 13, 360_000_000,
                ZoneId.of("Europe/Paris"));
        assertEquals(expected.toInstant().toEpochMilli(), millis);
    }
}
