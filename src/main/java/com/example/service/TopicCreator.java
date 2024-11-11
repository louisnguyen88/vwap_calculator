package com.example.service;


import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Properties;

@Service
@Slf4j
public class TopicCreator {

    @PostConstruct
    public void start() {
        createTopic("inputTopic");
        createTopic("outputTopic");
    }


    public void createTopic(String topicName) {
        // Kafka broker configuration
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");

        // Create AdminClient instance
        AdminClient adminClient = AdminClient.create(properties);

        // Create the NewTopic object with default partitions and replication
        NewTopic newTopic = new NewTopic(topicName, 1, (short) 1); // Default 1 partition, 1 replica

        try {
            // Create the topic
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
            log.info("Topic '" + topicName + "' created successfully.");
        } catch (TopicExistsException e) {
            log.info("Topic '" + topicName + "' already exists.");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            adminClient.close();
        }
    }
}
