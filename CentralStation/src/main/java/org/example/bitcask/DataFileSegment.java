package org.example.bitcask;

import lombok.Getter;
import org.example.models.WeatherStatus;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class DataFileSegment {
    public static final int MAX_OBJECTS_PER_SEGMENT = 10;

    @Getter
    private final String segmentFileName;
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
        file = new RandomAccessFile(segmentFileName, "rw");
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

    public static DataFileSegment open(String segmentFileName) throws IOException, ClassNotFoundException {
        Map<Long, Long> hashIndex = new HashMap<>();
        try (RandomAccessFile file = new RandomAccessFile(segmentFileName, "r")) {
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
}
