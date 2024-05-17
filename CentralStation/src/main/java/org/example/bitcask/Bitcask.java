package org.example.bitcask;

import lombok.Getter;
import org.example.models.WeatherStatus;

import java.io.*;
import java.util.*;

public class Bitcask {

    @Getter
    private final Map<Long, Map.Entry<String, Long>> hashIndex; // StationId to (SegmentFileName, Offset) mapping
    @Getter
    private DataFileSegment currentSegment;

    private final CompactService compactService;
    private final SnapshotService snapshotService;

    public Bitcask() {
        hashIndex = new HashMap<>();
        createNewSegment(1);
        compactService = new CompactService(this, 0);
        snapshotService = new SnapshotService(this);
    }

    public Bitcask(Map<Long, Map.Entry<String, Long>> hashIndex, DataFileSegment currentSegment, int lastCompactedSegment) {
        this.hashIndex = hashIndex;
        this.currentSegment = currentSegment;
        compactService = new CompactService(this, lastCompactedSegment);
        snapshotService = new SnapshotService(this);
    }

    private void createNewSegment(int segmentNumber) {
        try {
            currentSegment = new DataFileSegment(segmentNumber);
        } catch (IOException e) {
            System.err.println("Error creating new segment: " + segmentNumber + " - " + e.getMessage());
        }
    }


    public void put(WeatherStatus weatherStatus) {
        long stationId = weatherStatus.getStationId();
        try {
            long offset = currentSegment.addObject(weatherStatus);
            hashIndex.put(stationId, Map.entry(currentSegment.getSegmentFileName(), offset));

            if (currentSegment.getObjectsWritten() >= DataFileSegment.MAX_OBJECTS_PER_SEGMENT) {
                createNewSegment(currentSegment.getSegmentNumber());
            }
        } catch (IOException e) {
            System.err.println("Error writing to segment: " + currentSegment.getSegmentFileName() + " - " + e.getMessage());
        }
    }

    public WeatherStatus get(long stationId) throws IOException, ClassNotFoundException {
        Map.Entry<String, Long> entry = hashIndex.get(stationId);
        if (entry == null) {
            return null;
        }
        String segmentFileName = entry.getKey();
        long offset = entry.getValue();
        try (RandomAccessFile file = new RandomAccessFile(new File(DataFileSegment.SEGMENT_DIRECTORY, segmentFileName), "r")) {
            file.seek(offset);
            ObjectInputStream inputStream = new ObjectInputStream(new FileInputStream(file.getFD()));
            return (WeatherStatus) inputStream.readObject();
        } catch (IOException | ClassNotFoundException e) {
            System.err.println("Error reading from segment: " + segmentFileName + " - " + e.getMessage());
            throw e;
        }

    }
}
