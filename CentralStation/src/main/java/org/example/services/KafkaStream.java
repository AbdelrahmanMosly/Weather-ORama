package org.example.services;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import org.example.models.WeatherStatus;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import lombok.extern.log4j.Log4j;

@Log4j
public class KafkaStream {
    private Properties properties;
    private Gson gson;
    public KafkaStream(String kafkaEndpoint, String groupId) {
        log.info("Rain pls");
        properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "RainProcessor");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaEndpoint);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        this.gson = new GsonBuilder()
        .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
        .create();
    }

    public void buildStream(String weatherTopic, String rainTopic, int humidityThreshold){
        System.out.println("Building rain Stream");
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> rainStream = builder.stream("weather")
                                                    .map((key, value) -> {
                                                        log.info("Key: " + key + " Value: " + value);
                                                        WeatherStatus weatherStatus = this.gson.fromJson(value.toString(), WeatherStatus.class);
                                                        return new KeyValue<String, WeatherStatus>(key.toString(), weatherStatus);
                                                    })
                                                    .filter((key, value) -> value.getWeather().getHumidity() > humidityThreshold)
                                                    .map((key, value) -> {
                                                        log.info("Key: " + key + " Value: " + value);
                                                        return new KeyValue<String, String>(key, "It's raining Tacos.");
                                                    });

        rainStream.to("rain");

        Topology topology = builder.build();
        log.info(topology.describe().toString());
        // topology.addSource("source", weatherTopic);
        // topology.addProcessor("rain-processor",
        //                      () -> new RainProcessor("raining-sink", humidityThreshold),
        //                       "source");

        // topology.addSink("raining-sink", rainTopic, "rain-processor");

        KafkaStreams streams = new KafkaStreams(topology, this.properties);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
