package org.example;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import org.example.bitcask.Bitcask;
import org.example.bitcask.DataFileSegment;
import org.example.bitcask.RecoveryManager;
import org.example.models.WeatherStatus;
import org.example.services.KafkaChannel;


public class CentralStation {
    private static Bitcask bitcask;

    private static Properties loadProperties() {
        String rootPath = Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResource("")).getPath();
        String appConfigPath = rootPath + "app.properties";
        Properties appProps = new Properties();
        try (FileInputStream fp = new FileInputStream(appConfigPath)) {
            appProps.load(fp);
        } catch (Exception e) {
            System.err.println("app.properties is not found. Will be using default values if applicable.");
        }
        return appProps;
    }

    private static void process(WeatherStatus weatherStatus) {
        System.out.println(weatherStatus.getStatusTimestamp());
        bitcask.put(weatherStatus);
        //elasticSearch
        //parquet

    }

    private static void consume() {
        Properties appProps = loadProperties();
        String kafkaTopic = appProps.getProperty("kafkaTopic", "test");
        String kafkaBroker = appProps.getProperty("kafkaBroker", "localhost:9094");
        String groupId = appProps.getProperty("groupId", "test");
        KafkaChannel channel = new KafkaChannel(kafkaBroker, kafkaTopic, groupId);

        try {
            while (true) {
                List<WeatherStatus> records = channel.consume();
                records.forEach(CentralStation::process);
            }
        } catch (Exception e) {
            System.err.println("Error consuming messages" + e.getMessage());
        } finally {
            channel.cleanUp();
        }
    }


    public static void main(String[] args) {
        try {
            if(DataFileSegment.readLastSegmentNumber() > 0){
                bitcask = RecoveryManager.recover();
            }else{
                bitcask = new Bitcask();
            }
        } catch (IOException e) {
            System.err.println(e.getMessage());
            bitcask = new Bitcask();
        }

        consume();
    }
}
