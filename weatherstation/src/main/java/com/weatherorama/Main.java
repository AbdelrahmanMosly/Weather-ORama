package com.weatherorama;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.weatherorama.centralstation.interfaces.CentralStation;
import com.weatherorama.centralstation.services.KafkaChannel;
import com.weatherorama.centralstation.services.MsgDropChannel;
import com.weatherorama.weatherstation.models.StationStatus;
import com.weatherorama.weatherstation.services.WeatherSensorFactory;
import com.weatherorama.weatherstation.services.WeatherStation;
import com.weatherorama.weatherstation.services.WeatherStationBuilder;



public class Main {
    public static void main(String[] args) throws InterruptedException, FileNotFoundException, IOException {
    	Logger logger = LoggerFactory.getLogger(Main.class);

        Properties appProps = loadProperties(logger);

        String kafkaTopic = appProps.getProperty("KAFKA_TOPIC", "test");
        String kafkaBroker = appProps.getProperty("KAFKA_BROKER", "localhost:9094");
        int dropRate = Integer.parseInt(appProps.getProperty("DROP_RATE", "10"));

        CentralStation<Long, StationStatus> channel = new KafkaChannel<>(kafkaBroker, kafkaTopic);
        channel = new MsgDropChannel<>(channel, dropRate);


        long stationID = Long.parseLong(appProps.getProperty("STATION_ID", "0"));
        double longitude = Double.parseDouble(appProps.getProperty("STATION_LONGITUDE", "47.1915"));
        double latitude = Double.parseDouble(appProps.getProperty("STATION_LATITUDE", "-52.8371"));
        String weatherAPI = appProps.getProperty("WEATHER_API");

        if(weatherAPI == null){
            logger.error("No weather API was given. The station will shutdown");
            System.exit(1);
        }

        WeatherStation weatherStation = new WeatherStationBuilder()
                                                .stationId(stationID)
                                                .centralStation(channel)
                                                .weatherSensor(WeatherSensorFactory.getWeatherSensor(weatherAPI, longitude, latitude))
                                                .build();
        
        long pollEvery = Long.parseLong(appProps.getProperty("POLL_EVERY", "1000"));
        
        while(true){
            weatherStation.invoke();
            Thread.sleep(pollEvery);
        }
    }


    private static Properties loadProperties(Logger logger){
        Properties appProps = new Properties();
        try{
            String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
            String appConfigPath = rootPath + "app.properties";
            try (FileInputStream fp = new FileInputStream(appConfigPath)) {
                appProps.load(fp);
            }
        } catch (Exception e) {
            logger.warn("app.properties is not found. Will use Environmental Variables.");
            appProps.putAll(System.getenv());
            
        }
        return appProps;
    }
}