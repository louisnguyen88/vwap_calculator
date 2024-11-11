package com.example.service;


import com.example.util.VWAPProccessor;
import com.example.util.VWAPSerializer;
import com.example.util.VolumePriceAggregatorSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.Stores;
import org.springframework.stereotype.Service;
import java.util.Properties;

@Slf4j
@RequiredArgsConstructor
@Service
public class KafkaStreamsService {
    private KafkaStreams streams;

    @PostConstruct
    public void start() {
        // Load configuration properties
        Properties config = getStreamsConfig();

        // Build the topology
        Topology topology = buildTopology();

        // Initialize and start the Kafka Streams instance
        streams = new KafkaStreams(topology, config);
        streams.start();

        log.info("Kafka Streams service started.");
    }

    @PreDestroy
    public void stop() {
        if (streams != null) {
            streams.close();
        }
        log.info("Kafka Streams service stopped.");
    }

    private Properties getStreamsConfig() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "vwap-calculation-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put("schema.registry.url", "http://localhost:8081");
        props.put("allow.auto.create.topics", true);
        return props;
    }

    public Topology buildTopology() {
        Topology topologyBldr = new Topology();

        // Define the source of the stream
        topologyBldr.addSource("Source", "inputTopic");

        // Add a custom processor for stream processing
        topologyBldr.addProcessor("Process", VWAPProccessor::new, "Source")
                .addStateStore(
                        Stores.keyValueStoreBuilder(
                                Stores.persistentKeyValueStore("vwap-store"),
                                Serdes.String(),
                                new VolumePriceAggregatorSerde()
                        ), "Process")
                .addSink("Sink", "outputTopic", Serdes.String().serializer(), new VWAPSerializer(), "Process");

        return topologyBldr;
    }

}
