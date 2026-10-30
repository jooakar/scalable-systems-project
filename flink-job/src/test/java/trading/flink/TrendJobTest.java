package trading.flink;

import org.junit.jupiter.api.Test;
import trading.common.TickEvent;

import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TrendJobTest {

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
