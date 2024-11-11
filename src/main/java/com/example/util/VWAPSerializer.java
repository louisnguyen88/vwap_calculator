package com.example.util;

import com.example.dto.VWAP;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

public class VWAPSerializer implements Serializer<VWAP> {
    private static ObjectMapper objectMapper = new ObjectMapper();
    @Override
    public byte[] serialize(String topic, VWAP data) {
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new RuntimeException("Error serializing AggregateResult", e);
        }
    }
}