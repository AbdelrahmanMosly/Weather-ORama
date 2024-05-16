package org.example.bitcask;

import lombok.Getter;
import org.example.models.WeatherStatus;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

@Getter
public class DataFileSegment {
    public static final int MAX_OBJECTS_PER_SEGMENT = 10;

    private final String segmentFileName;
    public static final String SEGMENT_DIRECTORY = "segments";

    private final Map<Long, Long> hashIndex; // StationId to offset mapping
    private int objectsWritten;
    private RandomAccessFile file;


    public DataFileSegment(String segmentFileName) throws IOException {
        this.segmentFileName = segmentFileName;
        this.hashIndex = new HashMap<>();
        this.objectsWritten = 0;
        createFile();
    }

    private void createFile() throws IOException {
        file = new RandomAccessFile(SEGMENT_DIRECTORY + "/" + segmentFileName, "rw");
    }

    public long addObject(WeatherStatus weatherStatus) throws IOException {
        long currentPosition = file.length();
        file.seek(currentPosition);
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(new FileOutputStream(file.getFD()));
        objectOutputStream.writeObject(weatherStatus);
        objectOutputStream.close();
        hashIndex.put(weatherStatus.getStationId(), currentPosition);
        objectsWritten++;

        if (objectsWritten >= MAX_OBJECTS_PER_SEGMENT) {
            close();
        }
        return currentPosition;
    }

    public void close() throws IOException {
        file.close();
    }

    public int getSegmentNumber(){
        return extractSegmentNumber(segmentFileName);
    }

    private static DataFileSegment open(String segmentFileName) throws IOException, ClassNotFoundException {
        Map<Long, Long> hashIndex = new HashMap<>();
        try (RandomAccessFile file = new RandomAccessFile(SEGMENT_DIRECTORY + "/" + segmentFileName, "r")) {
            while (file.getFilePointer() < file.length()) {
                long currentPosition = file.getFilePointer();
                WeatherStatus weatherStatus = (WeatherStatus) new ObjectInputStream(new FileInputStream(file.getFD())).readObject();
                hashIndex.put(weatherStatus.getStationId(), currentPosition);
            }
        }

        DataFileSegment segment = new DataFileSegment(segmentFileName);
        segment.hashIndex.putAll(hashIndex);
        segment.objectsWritten = hashIndex.size();
        return segment;
    }

    public static DataFileSegment recoverFromSegment(Map<Long, Map.Entry<String, Long>> hashIndex, int startSegmentNum) {
        DataFileSegment currentSegment = null;
        int lastSegmentNum = startSegmentNum - 1;

        for (int i = startSegmentNum; ; i++) {
            String segmentFileName = Bitcask.SEGMENT_PREFIX + i + ".dat";
            try {
                currentSegment = open(segmentFileName);
                lastSegmentNum = i;
                for (Map.Entry<Long, Long> entry : currentSegment.getHashIndex().entrySet()) {
                    long stationId = entry.getKey();
                    long offset = entry.getValue();
                    // Update hashIndex with the latest offset for each stationId
                    hashIndex.put(stationId, Map.entry(segmentFileName, offset));
                }
            } catch (IOException | ClassNotFoundException e) {
                System.err.println("Error recovering from segment: " + segmentFileName + " - " + e.getMessage());
                break; // Stop recovery on error
            }
        }
        if (currentSegment != null && currentSegment.getObjectsWritten() >= MAX_OBJECTS_PER_SEGMENT) {
            lastSegmentNum++;
            String newSegmentFileName = Bitcask.SEGMENT_PREFIX + lastSegmentNum + ".dat";
            try {
                currentSegment = new DataFileSegment(newSegmentFileName);
            } catch (IOException e) {
                System.err.println("Error creating new segment: " + newSegmentFileName + " - " + e.getMessage());
            }
        }

        return currentSegment;
    }

    private int extractSegmentNumber(String fileName) {
        return Integer.parseInt(fileName.substring(fileName.indexOf('_') + 1, fileName.indexOf('.')));
    }
}
