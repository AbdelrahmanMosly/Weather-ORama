package org.example.bitcask;

import org.example.models.WeatherStatus;

import java.io.*;
import java.util.*;

public class Bitcask {
    private static final String SEGMENT_PREFIX = "segment_";
    private static final long COMPACT_INTERVAL = 60 * 1000; // 1 minute

    private final Map<Long, Map.Entry<String, Long>> hashIndex; // StationId to (SegmentFileName, Offset) mapping
    private int objectsWrittenToCurrentSegment;
    private DataFileSegment currentSegment;
    private int currentSegmentNumber;
    private int lastCompactedSegment;

    public Bitcask() {
        hashIndex = new HashMap<>();
        objectsWrittenToCurrentSegment = 0;
        createNewSegment(getNextSegmentFileName());
        lastCompactedSegment = 0;
        scheduleCompaction();

    }

    private void createNewSegment(String segmentFileName) {
        try {
            currentSegment = new DataFileSegment(segmentFileName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String getNextSegmentFileName() {
        int segmentNumber = getCurrentSegmentNumber() + 1;
        currentSegmentNumber = segmentNumber;
        return SEGMENT_PREFIX + segmentNumber + ".dat";
    }

    private int getCurrentSegmentNumber() {
        File[] files = new File(".").listFiles();
        if (files == null) {
            return 0;
        }
        int maxSegmentNumber = 0;
        for (File file : files) {
            if (file.getName().startsWith(SEGMENT_PREFIX)) {
                int segmentNumber = Integer.parseInt(file.getName().substring(SEGMENT_PREFIX.length(), file.getName().indexOf(".")));
                if (segmentNumber > maxSegmentNumber) {
                    maxSegmentNumber = segmentNumber;
                }
            }
        }
        return maxSegmentNumber;
    }

    public void put(WeatherStatus weatherStatus) {
        long stationId = weatherStatus.getStationId();
        try {
            long offset = currentSegment.addObject(weatherStatus);
            hashIndex.put(stationId, Map.entry(currentSegment.getSegmentFileName(), offset));

            if (++objectsWrittenToCurrentSegment >= DataFileSegment.MAX_OBJECTS_PER_SEGMENT) {
                objectsWrittenToCurrentSegment = 0;
                currentSegmentNumber++;
                createNewSegment(SEGMENT_PREFIX + currentSegmentNumber + ".dat");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public WeatherStatus get(long stationId) {
        Map.Entry<String, Long> entry = hashIndex.get(stationId);
        if (entry != null) {
            String segmentFileName = entry.getKey();
            long offset = entry.getValue();
            try (RandomAccessFile file = new RandomAccessFile(segmentFileName, "r")) {
                file.seek(offset);
                ObjectInputStream inputStream = new ObjectInputStream(new FileInputStream(file.getFD()));
                return (WeatherStatus) inputStream.readObject();
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    private void scheduleCompaction() {
        Timer timer = new Timer(true);
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                compactSegments();
            }
        }, COMPACT_INTERVAL, COMPACT_INTERVAL);
    }

    private void compactSegments() {
        int startSegment = lastCompactedSegment + 1;
        int endSegment = currentSegmentNumber - 1;
        if (startSegment >= endSegment) {
            return;
        }

        compactSegments(startSegment, endSegment);
        lastCompactedSegment = endSegment;
    }

    private void compactSegments(int startSegment, int endSegment) {

        String compactedFileName = String.format("compacted_%d_%d.dat", startSegment, endSegment);
        // read all segments and create map of stationId to latest weather status and create the hashIndex for the compacted file
        Map<Long, WeatherStatus> stationIdToLatestWeatherStatus = new HashMap<>();
        Map<Long, Map.Entry<String, Long>> compactedHashIndex = new HashMap<>();
        for (int i = startSegment; i <= endSegment; i++) {
            String segmentFileName = SEGMENT_PREFIX + i + ".dat";
            try (RandomAccessFile file = new RandomAccessFile(segmentFileName, "r")) {
                while (file.getFilePointer() < file.length()) {
                    long currentPosition = file.getFilePointer();
                    WeatherStatus weatherStatus = (WeatherStatus) new ObjectInputStream(new FileInputStream(file.getFD())).readObject();
                    long stationId = weatherStatus.getStationId();
                    stationIdToLatestWeatherStatus.put(stationId, weatherStatus);
                    compactedHashIndex.put(stationId, Map.entry(compactedFileName, currentPosition));
                }
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
        // write the latest weather status to the compacted file
        try (FileOutputStream outputStream = new FileOutputStream(compactedFileName);
             BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(outputStream);
             ObjectOutputStream objectOutputStream = new ObjectOutputStream(bufferedOutputStream)) {
            for (WeatherStatus weatherStatus : stationIdToLatestWeatherStatus.values()) {
                objectOutputStream.writeObject(weatherStatus);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        generateHintFile(startSegment, endSegment, compactedHashIndex);
    }

    private void generateHintFile(int startSegment, int endSegment, Map<Long, Map.Entry<String, Long>> compactedHashIndex) {
        String hintFileName = String.format("hint_%d_%d.txt", startSegment, endSegment);
        try (PrintWriter writer = new PrintWriter(hintFileName)) {
            for (Map.Entry<Long, Map.Entry<String, Long>> entry : compactedHashIndex.entrySet()) {
                writer.println(entry.getKey() + "," + entry.getValue().getKey() + "," + entry.getValue().getValue());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
