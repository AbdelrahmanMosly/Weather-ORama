package org.example;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.DTO.WeatherStatusDTO;
import org.example.entity.WeatherStatus;


public class CentralStation {
    //WeatherStatusDTO weatherStatusDTO = new WeatherStatusDTO("src/main/resources/weather-status.avsc");
    WeatherStatusDTO weatherStatusDTO = new WeatherStatusDTO();

    private void process(byte[] message){
        System.out.println("Received message: " + new String(message));
        try {
            WeatherStatus weatherStatus = weatherStatusDTO.deserialize(message);
            //bitcask
            //elasticSearch
            //parquet
            System.out.println(weatherStatus.toString());
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private void consume(){
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        // Additional properties (optional)
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Specify where to start consuming messages
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"); // Enable auto-commit
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000"); // Auto-commit interval in milliseconds

        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("my-topic"));
        try{
            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(500));
                records.forEach(record -> process(record.value()));
            }
        }finally {
            consumer.close();
        }
    }


    public static void main(String[] args) {
        System.out.println("Hello world!");
        CentralStation centralStation = new CentralStation();
        centralStation.consume();
    }
}
