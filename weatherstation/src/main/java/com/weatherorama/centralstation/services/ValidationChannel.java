package com.weatherorama.centralstation.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.weatherorama.centralstation.interfaces.CentralStation;
import com.weatherorama.weatherstation.interfaces.Valid;

public class ValidationChannel<K, V extends Valid> implements CentralStation<K, V>{
    private final Logger logger = LoggerFactory.getLogger(ValidationChannel.class);

    private CentralStation<K, V> centralStation;    

    public ValidationChannel(CentralStation<K, V> centralStation) {
        this.centralStation = centralStation;
    }



    @Override
    public void notify(K id, V data) {
        if(data.isValid()){
            this.centralStation.notify(id, data);
        }else{
            logger.error("Got an Invalid Message");
        }
    }
    
}
