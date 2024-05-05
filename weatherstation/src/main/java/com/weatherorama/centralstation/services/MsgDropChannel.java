package com.weatherorama.centralstation.services;

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.weatherorama.centralstation.interfaces.CentralStation;

/**
 * MsgDropChannel
 */
/**
 * A message drop channel implementation of the CentralStation interface.
 * This class drops messages based on a given drop rate.
 *
 * @param <K> the type of the message ID
 * @param <V> the type of the message data
 */
public class MsgDropChannel<K, V> implements CentralStation<K, V> {
    private final Logger logger = LoggerFactory.getLogger(MsgDropChannel.class);

    private Random rng;
    private CentralStation<K, V> centralStation;
    private int dropRate;

    /**
     * Constructs a new MsgDropChannel with the specified central station and drop rate.
     *
     * @param centralStation the central station to forward non-dropped messages to
     * @param dropRate the drop rate in percentage (0-100) for dropping messages
     */
    public MsgDropChannel(CentralStation<K, V> centralStation, int dropRate) {
        this.centralStation = centralStation;
        this.dropRate = dropRate;
        this.rng = new Random(System.currentTimeMillis());
    }

    /**
     * Notifies the central station about a message.
     * If the drop rate is higher than a randomly generated number, the message is dropped.
     * Otherwise, the message is forwarded to the central station.
     *
     * @param id the ID of the message
     * @param data the data of the message
     */
    @Override
    public void notify(K id, V data) {
        if (rng.nextInt(100) < dropRate) {
            logger.info("Message From " + id.toString() + " has been Dropped.");
        } else {
            this.centralStation.notify(id, data);
        }
    }
}