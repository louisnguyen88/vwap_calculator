package com.example.util;

import com.example.dto.VWAP;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

public class VWAPDesserializer  implements Deserializer<VWAP> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public VWAP deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data, VWAP.class);
        } catch (Exception e) {
            throw new RuntimeException("Error deserializing JSON to " + e);
        }
    }
}
