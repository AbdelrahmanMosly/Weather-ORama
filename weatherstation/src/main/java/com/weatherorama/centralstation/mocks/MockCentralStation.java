package com.weatherorama.centralstation.mocks;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.weatherorama.centralstation.interfaces.CentralStation;
import com.weatherorama.weatherstation.models.WeatherData;

public class MockCentralStation implements CentralStation{
    private final Logger logger = LoggerFactory.getLogger(MockCentralStation.class);

    @Override
    public void notify(WeatherData data) {
        logger.info("Data Recieved.");
    }
    
}
