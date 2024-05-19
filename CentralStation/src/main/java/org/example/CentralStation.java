package org.example;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;

import org.example.bitcask.Bitcask;
import org.example.bitcask.DataFileSegment;
import org.example.bitcask.RecoveryManager;

import org.example.models.WeatherStatus;
import org.example.services.KafkaChannel;
import org.example.services.RainWatcher;
import org.example.archiver.WeatherStatusArchiver;



public class CentralStation {
    private static Bitcask bitcask;
    private static WeatherStatusArchiver archiver;
    private static Properties appProps;

    private static void loadProperties(){
        appProps = new Properties();
        try{
            String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
            String appConfigPath = rootPath + "app.properties";
            try (FileInputStream fp = new FileInputStream(appConfigPath)) {
                appProps.load(fp);
            }
        } catch (Exception e) {
            System.err.println("app.properties is not found. Will use Environmental Variables.");
            appProps.putAll(System.getenv());
            
        }
    }

    private static void process(WeatherStatus weatherStatus) {
        System.out.println(weatherStatus.getStatusTimestamp());
        bitcask.put(weatherStatus);
        //elasticSearch
        //parquet
        try {
            archiver.archiveWeatherStatus(weatherStatus);
        } catch (IOException e) {
            System.err.println("Error archiving weather status: " + e.getMessage());
        }

    }

    private static void consume() {
        String weatherTopic = appProps.getProperty("WEATHER_TOPIC", "test");
        String rainTopic = appProps.getProperty("RAIN_TOPIC", "rain-test");

        
        String kafkaBroker = appProps.getProperty("KAFKA_BROKER", "localhost:9094");
        String groupId = appProps.getProperty("GROUP_ID", "test");
        int humidityThreshold = Integer.parseInt(appProps.getProperty("HUMIDITY_THRESHOLD", "70"));

        RainWatcher rainWatcher = new RainWatcher(kafkaBroker, groupId);
        rainWatcher.buildWatcher(weatherTopic, rainTopic, humidityThreshold);


        KafkaChannel channel = new KafkaChannel(kafkaBroker, weatherTopic, groupId);

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

        {
            loadProperties();
            try {
                Files.createDirectories(Paths.get(appProps.getProperty("BITCASK_DIR", "bitcask")));
                Files.createDirectories(Paths.get(appProps.getProperty("PARQUET_DIR", "parquet")));

            } catch (IOException e) {
                System.err.println("Error creating directories: " + e.getMessage());
            }

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
            try {
                String parquetFilesDirectory = appProps.getProperty("PARQUET_DIR");
                int batchSize = Integer.parseInt(appProps.getProperty("BATCH_SIZE", "100"));
                archiver = new WeatherStatusArchiver(parquetFilesDirectory, batchSize);
                consume();
            } catch (IOException e) {
                System.err.println("Error initializing WeatherStatusArchiver: " + e.getMessage());
            }
        
        }
    }
}
