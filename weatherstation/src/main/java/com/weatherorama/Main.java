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
import com.weatherorama.weatherstation.mocks.MockWeatherSensor;
import com.weatherorama.weatherstation.models.StationStatus;
import com.weatherorama.weatherstation.services.WeatherStation;



public class Main {
    public static void main(String[] args) throws InterruptedException, FileNotFoundException, IOException {
    	Logger logger = LoggerFactory.getLogger(Main.class);

        Properties appProps = loadProperties(logger);

        long pollEvery = Long.parseLong(appProps.getProperty("pollEvery", "1000"));
        long stationID = Long.parseLong(appProps.getProperty("stationID", "0"));
        String kafkaTopic = appProps.getProperty("kafkaTopic", "test");
        String kafkaBroker = appProps.getProperty("kafkaBroker", "localhost:9094");
        int dropRate = Integer.parseInt(appProps.getProperty("dropRate", "10"));
        CentralStation<Long, StationStatus> channel = new MsgDropChannel<>(new KafkaChannel<>(kafkaBroker, kafkaTopic), dropRate);
        WeatherStation weatherStation = new WeatherStation(stationID, new MockWeatherSensor(), channel);
        
        while(true){
            weatherStation.invoke();
            Thread.sleep(pollEvery);
        }
    }


    private static Properties loadProperties(Logger logger){
        String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
        String appConfigPath = rootPath + "app.properties";
        Properties appProps = new Properties();
        try (FileInputStream fp = new FileInputStream(appConfigPath)) {
            appProps.load(fp);
        } catch (Exception e) {
            logger.warn("app.properties is not found. Will be using default values if applicable.");
        }
        return appProps;
    }
}