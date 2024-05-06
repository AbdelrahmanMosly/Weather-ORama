package com.weatherorama.centralstation.mocks;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.weatherorama.centralstation.interfaces.CentralStation;

public class MockCentralStation<K, V> implements CentralStation<K, V>{
    private final Logger logger = LoggerFactory.getLogger(MockCentralStation.class);

    @Override
    public void notify(K id, V data) {
        logger.info("Data Recieved From " + id.toString() + ".");
        logger.info(data.toString());
    }
    
}
