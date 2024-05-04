package com.weatherorama.WeatherStation.Mocks;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.weatherorama.WeatherStation.Interfaces.CentralStation;
import com.weatherorama.WeatherStation.Models.WeatherData;

public class MockCentralStation implements CentralStation{
    private final Logger logger = LoggerFactory.getLogger(MockCentralStation.class);

    @Override
    public void notify(WeatherData data) {
        logger.info("Data Recieved.");
    }
    
}
