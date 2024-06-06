package org.example.bitcask;

import lombok.Getter;
import org.example.models.WeatherStatus;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

@Getter
public class DataFileSegment {
    public static final int MAX_OBJECTS_PER_SEGMENT = System.getenv("MAX_OBJECTS_PER_SEGMENT") == null ?
                                                         128 : Integer.parseInt(System.getenv("MAX_OBJECTS_PER_SEGMENT"));
    public static final String SEGMENT_DIRECTORY = System.getenv("BITCASK_DIR") + "/segments";
    public static final String SEGMENT_PREFIX = "segment_";
    private static final String SEGMENT_NUMBER_FILE = System.getenv("BITCASK_DIR") + "/last_segment_number.txt";

    private final String segmentFileName;
    private final int segmentNumber;
    private final Map<Long, Long> hashIndex; // StationId to offset mapping
    private int objectsWritten;
    private RandomAccessFile file;


    public DataFileSegment(int segmentNumber) throws IOException {
        this.segmentNumber = segmentNumber;
        this.segmentFileName = SEGMENT_PREFIX + segmentNumber + ".dat";
        this.hashIndex = new HashMap<>();
        this.objectsWritten = 0;
        createFile();
    }

    private void createFile() throws IOException {
        File directory = new File(SEGMENT_DIRECTORY);
        if (!directory.exists()) {
            if (!directory.mkdirs()) {
                throw new RuntimeException("Error creating directory: " + SEGMENT_DIRECTORY);
            }
        }

        file = new RandomAccessFile(new File(SEGMENT_DIRECTORY, segmentFileName), "rw");
        if (readLastSegmentNumber() < segmentNumber)
            writeLastSegmentNumber(segmentNumber);
    }

    public static void writeLastSegmentNumber(int segmentNumber) throws IOException {
        try (PrintWriter writer = new PrintWriter(new FileWriter(SEGMENT_NUMBER_FILE))) {
            writer.println(segmentNumber);
        }
    }

    public static int readLastSegmentNumber() throws IOException {
        File file = new File(SEGMENT_NUMBER_FILE);
        if (!file.exists()) {
            return 0; // Default value if the file does not exist
        }
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line = reader.readLine();
            return Integer.parseInt(line);
        } catch (NumberFormatException e) {
            throw new IOException("Invalid segment number in file: " + SEGMENT_NUMBER_FILE, e);
        }
    }

    public long addObject(WeatherStatus weatherStatus) throws IOException {
        long currentPosition = file.length();
        file.seek(currentPosition);
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(new FileOutputStream(file.getFD()));
        objectOutputStream.writeObject(weatherStatus);
        hashIndex.put(weatherStatus.getStationId(), currentPosition);
        objectsWritten++;

        if (objectsWritten >= MAX_OBJECTS_PER_SEGMENT) {
            objectOutputStream.close();
            close();
        }
        return currentPosition;
    }

    public void close() throws IOException {
        file.close();
    }

    private static DataFileSegment open(int segmentNumber) throws IOException, ClassNotFoundException {
        Map<Long, Long> hashIndex = new HashMap<>();
        String segmentFileName = SEGMENT_PREFIX + segmentNumber + ".dat";
        int counter = 0;
        try (RandomAccessFile file = new RandomAccessFile(new File(SEGMENT_DIRECTORY, segmentFileName), "r")) {
            while (file.getFilePointer() < file.length()) {
                long currentPosition = file.getFilePointer();
                WeatherStatus weatherStatus = (WeatherStatus) new ObjectInputStream(new FileInputStream(file.getFD())).readObject();
                hashIndex.put(weatherStatus.getStationId(), currentPosition);
                counter++;
            }
        }

        DataFileSegment segment = new DataFileSegment(segmentNumber);
        segment.hashIndex.putAll(hashIndex);
        segment.objectsWritten = counter;
        return segment;
    }

    public static void compactSegment(int segmentNumber, Map<Long, WeatherStatus> stationIdToLatestWeatherStatus,
                                      Map<Long, Map.Entry<String, Long>> compactedHashIndex) throws IOException, ClassNotFoundException {

        String segmentFileName = SEGMENT_PREFIX + segmentNumber + ".dat";
        try (RandomAccessFile file = new RandomAccessFile(new File(SEGMENT_DIRECTORY, segmentFileName), "r")) {
            while (file.getFilePointer() < file.length()) {
                long currentPosition = file.getFilePointer();
                WeatherStatus weatherStatus = (WeatherStatus) new ObjectInputStream(new FileInputStream(file.getFD())).readObject();
                long stationId = weatherStatus.getStationId();
                stationIdToLatestWeatherStatus.put(stationId, weatherStatus);
                compactedHashIndex.put(stationId, Map.entry(segmentFileName, currentPosition));
            }
        }

    }

    public static DataFileSegment recoverStartingFromSegment(Map<Long, Map.Entry<String, Long>> hashIndex, int startSegmentNum) {
        DataFileSegment currentSegment = null;
        int lastSegmentNum = startSegmentNum - 1;

        int lastSegmentNumber;
        try {
            lastSegmentNumber = readLastSegmentNumber();
        } catch (IOException e) {
            System.err.println(e.getMessage());
            lastSegmentNumber = -1;
        }

        for (int i = startSegmentNum; i <= lastSegmentNumber || lastSegmentNumber == -1; i++) {
            String segmentFileName = SEGMENT_PREFIX + i + ".dat";
            try {
                currentSegment = open(i);
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
            try {
                currentSegment = new DataFileSegment(lastSegmentNum);
            } catch (IOException e) {
                System.err.println("Error creating new segment: " + SEGMENT_PREFIX + lastSegmentNum + ".dat - " + e.getMessage());
            }
        }
        System.err.println("Recovering: Loaded from segment:" + startSegmentNum + " to segment: " + lastSegmentNum);

        return currentSegment;
    }
}
