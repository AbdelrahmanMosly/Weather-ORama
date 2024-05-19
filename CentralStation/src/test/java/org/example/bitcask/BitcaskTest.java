package org.example.bitcask;


import org.example.models.WeatherStatus;
import org.junit.Test;

import java.io.IOException;

public class BitcaskTest {


    @Test
    public void testGet() throws IOException, ClassNotFoundException {
        Bitcask bitcask = RecoveryManager.recover();
        WeatherStatus ws = bitcask.get(1);
        System.out.println(ws);
        assert(ws.getStationId() == 1);

    }
}
