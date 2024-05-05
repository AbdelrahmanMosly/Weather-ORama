package com.weatherorama.centralstation.services;

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.weatherorama.centralstation.interfaces.CentralStation;

/**
 * ErroneousChannel
 */
public class MsgDropChannel<K, V> implements CentralStation<K, V>{
    private final Logger logger = LoggerFactory.getLogger(MsgDropChannel.class); 

    private Random rng;
    private CentralStation<K, V> centralStation;
    private int dropRate;

    public MsgDropChannel(CentralStation<K, V> centralStation, int dropRate){
        this.centralStation = centralStation;
        this.dropRate = dropRate;
        this.rng = new Random(System.currentTimeMillis());
    }

    @Override
    public void notify(K id, V data) {
        if(rng.nextInt(100) < dropRate){
            logger.info("Message From " + id.toString() + " has been Dropped.");
        }else{
            this.centralStation.notify(id, data);
        }

    }

    
}