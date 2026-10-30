package trading.flink;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import trading.common.TickEvent;

import java.io.IOException;

/**
 * Deserializes Kafka record values (JSON bytes) into TickEvent objects.
 * Delegates to TickEvent.fromJson() which uses Jackson 3 (tools.jackson).
 *
 * <p>We use a custom schema instead of Flink's built-in JsonDeserializationSchema
 * because the latter relies on Flink's shaded Jackson 2, which is incompatible
 * with the Jackson 3 types used by our TickEvent record.</p>
 */
public class TickEventDeserializer implements DeserializationSchema<TickEvent> {

    @Override
    public TickEvent deserialize(byte[] message) throws IOException {
        return TickEvent.fromJson(message);
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
