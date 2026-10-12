package trading.common;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class TickEventTest {

    @Test
    void roundTripsThroughJson() {
        TickEvent event = new TickEvent("A1EX3A.ETR", "I", "08-11-2021", "12:37:13.000", 2143.69, "12:37:13.360", null);
        assertEquals(event, TickEvent.fromJson(event.toJson()));
    }

    @Test
    void omitsNullFields() {
        TickEvent event = new TickEvent("ALO.FR", "E", "08-11-2021", "12:37:13.000", null, "12:37:13.373", null);
        String json = new String(event.toJson(), StandardCharsets.UTF_8);
        assertFalse(json.contains("last") || json.contains("tradingDate"), json);
    }
}
