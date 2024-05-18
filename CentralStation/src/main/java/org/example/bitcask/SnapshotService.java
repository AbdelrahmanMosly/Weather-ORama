package org.example.bitcask;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

public class SnapshotService {
    public static final long SNAPSHOT_INTERVAL = 5 * 60 * 1000; // 5 minutes
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
        File directory = new File(SNAPSHOT_DIRECTORY);
        if (!directory.exists()) {
            if (!directory.mkdirs()) {
                throw new RuntimeException("Error creating directory: " + SNAPSHOT_DIRECTORY);
            }
        }

        String snapshotFileName = SNAPSHOT_PREFIX + bitcask.getCurrentSegment().getSegmentNumber() + ".txt";
        try (PrintWriter writer = new PrintWriter(new File(SNAPSHOT_DIRECTORY, snapshotFileName))) {
            for (Map.Entry<Long, Map.Entry<String, Long>> entry : bitcask.getHashIndex().entrySet()) {
                writer.println(entry.getKey() + "," + entry.getValue().getKey() + "," + entry.getValue().getValue());
            }
        } catch (IOException e) {
            System.err.println("Error writing snapshot file: " + snapshotFileName + " - " + e.getMessage());
        }
    }

    public static int loadLatestSnapshot(Map<Long, Map.Entry<String, Long>> hashIndex) throws IOException {
        int lastSnapshotSegmentNum = 0;
        File directory = new File(SNAPSHOT_DIRECTORY);
        // check if the directory exists
        if (!directory.exists()) {
            return lastSnapshotSegmentNum;
        }
        File[] snapshotFiles = directory.listFiles();
        if (snapshotFiles != null) {
            for (File file : snapshotFiles) {
                int segmentNum = extractSegmentNumber(file.getName());
                if (segmentNum > lastSnapshotSegmentNum) {
                    lastSnapshotSegmentNum = segmentNum;
                }
            }
            if (lastSnapshotSegmentNum > 0) {
                hashIndex.putAll(getSnapshot(lastSnapshotSegmentNum));
            }
        }
        return lastSnapshotSegmentNum;
    }

    private static int extractSegmentNumber(String fileName) {
        return Integer.parseInt(fileName.substring(fileName.indexOf('_') + 1, fileName.indexOf('.')));
    }

    private static Map<Long, Map.Entry<String, Long>> getSnapshot(int snapshotSegmentNum) throws IOException {
        File directory = new File(SNAPSHOT_DIRECTORY);
        if (!directory.exists()) {
            throw new FileNotFoundException("Snapshot directory not found: " + SNAPSHOT_DIRECTORY);
        }

        String snapshotFileName = SNAPSHOT_PREFIX + snapshotSegmentNum + ".txt";
        BufferedReader reader = new BufferedReader(new FileReader(new File(SNAPSHOT_DIRECTORY, snapshotFileName)));
        Map<Long, Map.Entry<String, Long>> hashIndex = new HashMap<>();
        String line;
        while ((line = reader.readLine()) != null) {
            String[] parts = line.split(",");
            long stationId = Long.parseLong(parts[0]);
            String segmentFileName = parts[1];
            long offset = Long.parseLong(parts[2]);
            hashIndex.put(stationId, Map.entry(segmentFileName, offset));
        }
        return hashIndex;
    }
}
