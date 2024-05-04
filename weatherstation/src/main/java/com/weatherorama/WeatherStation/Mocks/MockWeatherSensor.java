package com.weatherorama.WeatherStation.Mocks;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.weatherorama.WeatherStation.Interfaces.WeatherSensor;
import com.weatherorama.WeatherStation.Models.SensorReadings;

public class MockWeatherSensor implements WeatherSensor{
    private final Logger logger = LoggerFactory.getLogger(MockWeatherSensor.class);

    @Override
    public SensorReadings getReadings() {
        logger.info("Reading Data...");
        return new SensorReadings();
    }
    
}
