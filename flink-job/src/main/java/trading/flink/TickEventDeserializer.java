package trading.flink;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import trading.common.TickEvent;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Deserializes Kafka record values (JSON bytes) into TickEvent objects.
 * Delegates to TickEvent.fromJson() which uses Jackson 3 (tools.jackson).
 *
 * <p>We use a custom schema instead of Flink's built-in JsonDeserializationSchema
 * because the latter relies on Flink's shaded Jackson 2, which is incompatible
 * with the Jackson 3 types used by our TickEvent record.</p>
 *
 * <p>Only usable price events are emitted; returning null makes the Kafka source skip the
 * record, so the timestamp assigner and windows never see the other events.</p>
 */
public class TickEventDeserializer implements DeserializationSchema<TickEvent> {

    // The producer omits null fields, so events without this key are not price events
    // and can be skipped without parsing them.
    private static final byte[] LAST_KEY = "\"last\":".getBytes(StandardCharsets.UTF_8);

    @Override
    public TickEvent deserialize(byte[] message) throws IOException {
        if (!contains(message, LAST_KEY)) {
            return null;
        }
        TickEvent event = TickEvent.fromJson(message);
        return TrendJob.isUsablePriceEvent(event) ? event : null;
    }

    private static boolean contains(byte[] haystack, byte[] needle) {
        outer:
        for (int i = 0; i <= haystack.length - needle.length; i++) {
            for (int j = 0; j < needle.length; j++) {
                if (haystack[i + j] != needle[j]) {
                    continue outer;
                }
            }
            return true;
        }
        return false;
    }

    @Override
    public boolean isEndOfStream(TickEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<TickEvent> getProducedType() {
        return TypeInformation.of(TickEvent.class);
    }
}
