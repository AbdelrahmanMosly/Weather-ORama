package org.example.bitcask;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

public class SnapshotService {
    public static final long SNAPSHOT_INTERVAL = 10 * 60 * 1000; // 10 minutes
    private final Bitcask bitcask;

    public SnapshotService(Bitcask bitcask){
        this.bitcask = bitcask;
        scheduleSnapshot();
    }

    private void scheduleSnapshot() {
        Timer timer = new Timer(true);
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                takeSnapshot();
            }
        }, SNAPSHOT_INTERVAL, SNAPSHOT_INTERVAL); // Schedule every 10 minutes, starting now
    }

    private void takeSnapshot() {
        String snapshotFileName = String.format("snapshot_%d.txt", bitcask.getCurrentSegmentNumber());
        try (PrintWriter writer = new PrintWriter(snapshotFileName)) {
            for (Map.Entry<Long, Map.Entry<String, Long>> entry : bitcask.getHashIndex().entrySet()) {
                writer.println(entry.getKey() + "," + entry.getValue().getKey() + "," + entry.getValue().getValue());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
