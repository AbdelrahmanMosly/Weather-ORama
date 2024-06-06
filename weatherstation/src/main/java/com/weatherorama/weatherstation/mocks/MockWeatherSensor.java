package com.weatherorama.weatherstation.mocks;

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.weatherorama.weatherstation.interfaces.WeatherSensor;
import com.weatherorama.weatherstation.models.SensorReadings;
import com.weatherorama.weatherstation.models.Weather;

public class MockWeatherSensor implements WeatherSensor{
    private final Logger logger = LoggerFactory.getLogger(MockWeatherSensor.class);
    private final Random rng = new Random();

    @Override
    public SensorReadings getReadings() {
        logger.info("Reading Data...");
        SensorReadings sensorReadings = new SensorReadings();
        try {
            Thread.sleep(500);
            sensorReadings.setWeather(new Weather(rng.nextInt(100), rng.nextInt(100), rng.nextInt(100)));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // logger.info(sensorReadings.toString());
        return sensorReadings;
    }
    
}
