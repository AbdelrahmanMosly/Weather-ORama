package org.example.services;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.models.WeatherStatus;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;


public class KafkaChannel {
    private final String topic;
    private KafkaConsumer<String, String> consumer;
    private final Gson gson;

    public KafkaChannel(String kafkaEndpoint, String topic, String groupId) {
        this.topic = topic;
        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaEndpoint);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        // Additional properties (optional)
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Specify where to start consuming messages
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"); // Enable auto-commit
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000"); // Auto-commit interval in milliseconds

        consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singleton(topic));

        this.gson = new GsonBuilder()
                .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
                .create();
    }

    public ArrayList<WeatherStatus> consume() {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
        ArrayList<WeatherStatus> result = new ArrayList<>();
        records.forEach(record -> {
            result.add(this.gson.fromJson(record.value(), WeatherStatus.class));
            // System.out.println("Received message: " + record.key() + " : " + record.value());
        });
        return result;
    }

    public void cleanUp() {
        consumer.close();
    }
}
