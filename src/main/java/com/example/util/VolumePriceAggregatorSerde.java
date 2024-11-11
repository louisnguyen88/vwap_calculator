package com.example.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

public class VolumePriceAggregatorSerde extends Serdes.WrapperSerde<VolumePriceAggregator> {
    public static ObjectMapper objectMapper = new ObjectMapper();
    public VolumePriceAggregatorSerde() {
        super(new VolumePriceAggregatorSerializer(), new VolumePriceAggregatorDeserializer());
    }

    public static class VolumePriceAggregatorSerializer implements Serializer<VolumePriceAggregator> {

        @Override
        public byte[] serialize(String topic, VolumePriceAggregator data) {
            // Serialize the VelocityPriceAggregator object
            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new RuntimeException("Error serializing VolumePriceAggregator", e);
            }
        }
    }

    public static class VolumePriceAggregatorDeserializer implements Deserializer<VolumePriceAggregator> {

        @Override
        public VolumePriceAggregator deserialize(String topic, byte[] data) {
            try {
                return objectMapper.readValue(data, VolumePriceAggregator.class);
            } catch (Exception e) {
                throw new RuntimeException("Error deserializing VolumePriceAggregator", e);
            }
        }
    }
}