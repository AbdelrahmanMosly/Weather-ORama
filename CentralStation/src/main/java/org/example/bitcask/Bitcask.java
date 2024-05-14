package org.example.bitcask;

import lombok.Getter;
import org.example.models.WeatherStatus;

import java.io.*;
import java.util.*;

public class Bitcask {
    public static final String SEGMENT_PREFIX = "segment_";

    @Getter
    private final Map<Long, Map.Entry<String, Long>> hashIndex; // StationId to (SegmentFileName, Offset) mapping
    @Getter
    private int currentSegmentNumber;

    private int objectsWrittenToCurrentSegment;
    private DataFileSegment currentSegment;
    private CompactService compactService;
    private SnapshotService snapshotService;

    public Bitcask() {
        hashIndex = new HashMap<>();
        objectsWrittenToCurrentSegment = 0;
        createNewSegment(getNextSegmentFileName());
        compactService = new CompactService(this, 0);
        snapshotService = new SnapshotService(this);

    }

    private void createNewSegment(String segmentFileName) {
        try {
            currentSegment = new DataFileSegment(segmentFileName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String getNextSegmentFileName() {
        int segmentNumber = getLastSegmentNumber() + 1;
        currentSegmentNumber = segmentNumber;
        return SEGMENT_PREFIX + segmentNumber + ".dat";
    }

    private int getLastSegmentNumber() {
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








}
