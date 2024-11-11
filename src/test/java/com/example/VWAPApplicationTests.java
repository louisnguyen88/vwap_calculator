package com.example;

import com.example.dto.CurrencyInfo;
import com.example.dto.VWAP;
import com.example.util.VWAPDesserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
class VWAPApplicationTests {

	@Test
	void testVWAP() {
		Properties props = new Properties();
		props.put("bootstrap.servers","localhost:19092" );
		props.put("key.serializer", StringSerializer.class.getName());
		props.put("value.serializer", KafkaAvroSerializer.class.getName());
		props.put("schema.registry.url", "http://localhost:8081");

		Producer<String, CurrencyInfo> producer = new KafkaProducer<>(props);
		CurrencyInfo currencyInfo = createCurrencyInfo("AUD/USD");

		// Send the CurrencyInfo record
		int i =0;
		while (i<3){
			ProducerRecord<String, CurrencyInfo> record = new ProducerRecord<>("inputTopic", currencyInfo.getCurrencyPair(), currencyInfo);
			producer.send(record);
			i+=1;
		}
		i=0;
		currencyInfo = createCurrencyInfo("USD/JPY");

		while (i<3){
			ProducerRecord<String, CurrencyInfo> record = new ProducerRecord<>("inputTopic", currencyInfo.getCurrencyPair(), currencyInfo);
			producer.send(record);
			i+=1;
		}
		i=0;
		currencyInfo = createCurrencyInfo("NZD/GBP");
		while (i<3){
			ProducerRecord<String, CurrencyInfo> record = new ProducerRecord<>("inputTopic", currencyInfo.getCurrencyPair(), currencyInfo);
			producer.send(record);
			i+=1;
		}
		consumeOutputMessage();



	}

	public void consumeOutputMessage()  {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "group-id");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, VWAPDesserializer.class.getName());
		props.put("schema.registry.url", "http://localhost:8081");
		props.put("specific.avro.reader", "true");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		KafkaConsumer<String, VWAP> consumer = new KafkaConsumer<>(props);


		try{
			Thread.sleep(2 * 40 * 1000);
			consumer.subscribe(Collections.singletonList("outputTopic"));
			ConsumerRecords<String, VWAP> records = consumer.poll(Duration.ofSeconds(10)); // Adjust duration if needed

			assertEquals(3, records.count(), "Expected 3 records");
			for (ConsumerRecord<String, VWAP> record : records) {
				if (record.key().equals("USD/JPY")) {
					assertEquals(2.0, record.value().getVwap(), "Expected result for USD/JPY");
				} else if (record.key().equals("AUD/USD")) {
					assertEquals(2.0, record.value().getVwap(), "Expected result for AUD/USD");
				} else {
					assertEquals(2.0, record.value().getVwap(), "Expected result for NZD/GBP");
				}
			}

			// Commit offsets manually if needed
			consumer.commitSync();
		} catch (InterruptedException e) {

		}

	}


	private static CurrencyInfo createCurrencyInfo(String currencyPair) {
		long timestamp = System.currentTimeMillis();

		double price = 2;
		double volume = 2;

		return CurrencyInfo.newBuilder()
				.setTimestamp(timestamp)
				.setCurrencyPair(currencyPair)
				.setPrice(price)
				.setVolume(volume)
				.build();
	}

}
