package com.weatherorama.weatherstation.mocks;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.weatherorama.weatherstation.interfaces.WeatherSensor;
import com.weatherorama.weatherstation.models.SensorReadings;

public class MockWeatherSensor implements WeatherSensor{
    private final Logger logger = LoggerFactory.getLogger(MockWeatherSensor.class);

    @Override
    public SensorReadings getReadings() {
        logger.info("Reading Data...");
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        SensorReadings sensorReadings = new SensorReadings();
        logger.info(sensorReadings.toString());
        return sensorReadings;
    }
    
}
