package org.example.services;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.example.models.RainingMessage;
import org.example.models.WeatherStatus;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


public class RainWatcher {
    
    private Properties properties;
    private Gson gson;
    public RainWatcher(String kafkaEndpoint, String groupId) {

        properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "RainProcessor");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaEndpoint);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());


        this.gson = new GsonBuilder()
        .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
        .create();
    }

    public void buildWatcher(String weatherTopic, String rainTopic, int humidityThreshold){
        System.out.println("Building rain Stream");
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> rainStream = builder.stream(weatherTopic)
                                                    .map((key, value) -> {
                                                        return new KeyValue<String, RainingMessage>(key.toString(), strToRainMsg(value.toString()));
                                                    })
                                                    .filter((key, value) -> value.getHumidity() > humidityThreshold)
                                                    .map((key, value) -> {
                                                        return new KeyValue<String, String>(key, "It's raining Tacos at Station " + value.getStationId() + ".");
                                                    });

        rainStream.to(rainTopic);

        Topology topology = builder.build();

        KafkaStreams streams = new KafkaStreams(topology, this.properties);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private RainingMessage strToRainMsg(String value){
        WeatherStatus weatherStatus = this.gson.fromJson(value, WeatherStatus.class);
        return RainingMessage.builder()
                            .stationId(weatherStatus.getStationId())
                            .humidity(weatherStatus.getWeather().getHumidity())
                            .build();
    }
}
