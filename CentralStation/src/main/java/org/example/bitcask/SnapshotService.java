package org.example.bitcask;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

public class SnapshotService {
    public static final long SNAPSHOT_INTERVAL = 10 * 60 * 1000; // 10 minutes
    private final Bitcask bitcask;
    public static final String SNAPSHOT_DIRECTORY = "snapshot";
    public static final String SNAPSHOT_PREFIX = "snapshot_";


    public SnapshotService(Bitcask bitcask) {
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
        String snapshotFileName = SNAPSHOT_DIRECTORY + "/" + SNAPSHOT_PREFIX + (bitcask.getCurrentSegmentNumber()-1) + ".txt";
        try (PrintWriter writer = new PrintWriter(snapshotFileName)) {
            for (Map.Entry<Long, Map.Entry<String, Long>> entry : bitcask.getHashIndex().entrySet()) {
                writer.println(entry.getKey() + "," + entry.getValue().getKey() + "," + entry.getValue().getValue());
            }
        } catch (IOException e) {
            System.err.println("Error writing snapshot file: " + snapshotFileName + " - " + e.getMessage());
        }
    }
}
