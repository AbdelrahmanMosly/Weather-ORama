package com.weatherorama.centralstation.mocks;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.weatherorama.centralstation.interfaces.CentralStation;
import com.weatherorama.weatherstation.models.StationStatus;

public class MockCentralStation implements CentralStation{
    private final Logger logger = LoggerFactory.getLogger(MockCentralStation.class);

    @Override
    public void notify(StationStatus data) {
        logger.info("Data Recieved.");
        logger.info(data.toString());
    }
    
}
