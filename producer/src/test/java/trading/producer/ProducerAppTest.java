package trading.producer;

import trading.common.TickEvent;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ProducerAppTest {

    @Test
    void parsesPriceEvent() {
        String line = "A1EX3A.ETR,I,08-11-2021,12:37:13.000,,,,,,,,,,,,,,,,,,2143.690000,,12:37:13.360,,,,,,,,,,12:37:13.000,,,,,,";
        assertEquals(new TickEvent("A1EX3A.ETR", "I", "08-11-2021", "12:37:13.000", 2143.69, "12:37:13.360", null),
                ProducerApp.parse(line));
    }

    @Test
    void parsesNonPriceEvent() {
        String line = "ALO.FR,E,08-11-2021,12:37:13.000,31.560000,445,,,,,,,,,,,,,,,,,,12:37:13.373,,,,,,,,,,,,,,,,";
        assertEquals(new TickEvent("ALO.FR", "E", "08-11-2021", "12:37:13.000", null, "12:37:13.373", null),
                ProducerApp.parse(line));
    }
}
