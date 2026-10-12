package trading.common;

import com.fasterxml.jackson.annotation.JsonInclude;
import tools.jackson.databind.json.JsonMapper;

/** A tick event with only the fields the project needs. Empty CSV fields are null; a non-null `last` means a price event. */
public record TickEvent(String id, String secType, String date, String time, Double last, String tradingTime, String tradingDate) {

    public static final String TOPIC = "ticks";

    private static final JsonMapper JSON = JsonMapper.builder()
            .changeDefaultPropertyInclusion(inclusion -> inclusion.withValueInclusion(JsonInclude.Include.NON_NULL))
            .build();

    public byte[] toJson() {
        return JSON.writeValueAsBytes(this);
    }

    public static TickEvent fromJson(byte[] json) {
        return JSON.readValue(json, TickEvent.class);
    }
}
