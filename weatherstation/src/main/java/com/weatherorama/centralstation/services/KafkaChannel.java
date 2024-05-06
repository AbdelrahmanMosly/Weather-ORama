package com.weatherorama.centralstation.services;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.weatherorama.centralstation.interfaces.CentralStation;

/**
 * KafkaChannel
 */
/**
 * A class that represents a Kafka channel for sending data to a central station.
 *
 * @param <K> The type of the key used for sending data.
 * @param <V> The type of the value used for sending data.
 */
public class KafkaChannel<K, V> implements CentralStation<K, V>{
    private final Logger logger = LoggerFactory.getLogger(KafkaChannel.class);
    private final String topic;
    private KafkaProducer<String, String> producer;
    private Gson gson;

    /**
     * Constructs a new KafkaChannel with the specified Kafka endpoint and topic.
     *
     * @param kafkaEndpoint the Kafka endpoint to connect to
     * @param topic the topic to produce messages to
     */
    public KafkaChannel(String kafkaEndpoint, String topic){
        this.topic = topic;
        Properties properties = new Properties();
        
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaEndpoint);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                                StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                                StringSerializer.class.getName());

        this.producer = new KafkaProducer<String, String>(properties);
        this.gson = new GsonBuilder()
                        .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
                        .create();
    }
    /**
     * Notifies the Kafka channel with the specified key and value.
     * 
     * @param id The key to be sent to the Kafka channel.
     * @param data The value to be sent to the Kafka channel.
     */
    @Override
    public void notify(K id, V data) {
        this.producer.send(new ProducerRecord<String, String>(topic, id.toString(), this.gson.toJson(data)),
               new Callback() {
                   public void onCompletion(RecordMetadata metadata, Exception e) {
                       if(e != null) {
                          logger.error(e.toString());
                       } else {
                          logger.info("The offset of the record we just sent is: " + metadata.offset());
                       }
                   }
               });
    }
}