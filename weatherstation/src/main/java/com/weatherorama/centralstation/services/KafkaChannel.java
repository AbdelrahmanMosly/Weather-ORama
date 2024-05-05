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

import com.google.gson.Gson;
import com.weatherorama.centralstation.interfaces.CentralStation;

/**
 * KafkaChannel
 */
public class KafkaChannel<K, V> implements CentralStation<K, V>{
    private final Logger logger = LoggerFactory.getLogger(KafkaChannel.class);
    private final String topic;
    private KafkaProducer<String, String> producer;

    public KafkaChannel(String kafkaEndpoint, String topic){
        this.topic = topic;
        Properties properties = new Properties();
        
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaEndpoint);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                                StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                                StringSerializer.class.getName());

        this.producer = new KafkaProducer<String, String>(properties);
    }
    @Override
    public void notify(K id, V data) {
        this.producer.send(new ProducerRecord<String, String>(topic, id.toString(), new Gson().toJson(data)),
               new Callback() {
                   public void onCompletion(RecordMetadata metadata, Exception e) {
                       if(e != null) {
                          logger.error(e.getMessage());
                       } else {
                          logger.info("The offset of the record we just sent is: " + metadata.offset());
                       }
                   }
               });
    }
}