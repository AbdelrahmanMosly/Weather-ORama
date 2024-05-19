package org.example;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.example.models.WeatherStatus;
import org.example.services.KafkaChannel;
import org.example.services.RainWatcher;
import org.example.archiver.WeatherStatusArchiver;



public class CentralStation {
    private static WeatherStatusArchiver archiver;

    private static Properties loadProperties(){
        Properties appProps = new Properties();
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
        return appProps;
    }

    private static void process(WeatherStatus weatherStatus) {
        // System.out.println(weatherStatus.getStatusTimestamp());
        //bitcask
        //elasticSearch
        //parquet
        try {
            archiver.archiveWeatherStatus(weatherStatus);
        } catch (IOException e) {
            System.err.println("Error archiving weather status: " + e.getMessage());
        }

    }

    private static void consume() {
        Properties appProps = loadProperties();
        String weatherTopic = appProps.getProperty("WEATHER_TOPIC", "test");
        String rainTopic = appProps.getProperty("RAIN_TOPIC", "rain-test");

        
        String kafkaBroker = appProps.getProperty("KAFKA_BROKER", "localhost:9094");
        String groupId = appProps.getProperty("GROUP_ID", "test");
        int humidityThreshold = Integer.parseInt(appProps.getProperty("HUMIDITY_THRESHOLD", "70"));

        RainWatcher stream = new RainWatcher(kafkaBroker, groupId);
        stream.buildStream(weatherTopic, rainTopic, humidityThreshold);


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
            try {
                String homeDirectory = System.getProperty("user.home");
                String parquetFilesDirectory = homeDirectory + "/parquet-files";
                archiver = new WeatherStatusArchiver(parquetFilesDirectory);
                consume();
            } catch (IOException e) {
                System.err.println("Error initializing WeatherStatusArchiver: " + e.getMessage());
            }
        }
    }
}
