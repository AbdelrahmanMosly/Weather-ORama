package com.weatherorama;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.weatherorama.WeatherStation.Mocks.MockCentralStation;
import com.weatherorama.WeatherStation.Mocks.MockWeatherSensor;
import com.weatherorama.WeatherStation.Services.WeatherStation;



public class Main {
    public static void main(String[] args) throws InterruptedException, FileNotFoundException, IOException {
    	Logger logger = LoggerFactory.getLogger(Main.class);

        Properties appProps = loadProperties(logger);

        long pollEvery = Long.parseLong(appProps.getProperty("pollEvery", "1000"));

        WeatherStation weatherStation = new WeatherStation(new MockWeatherSensor(), new MockCentralStation());
        
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