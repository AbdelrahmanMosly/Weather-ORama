package org.example.bitcask;

import org.example.models.WeatherStatus;

import java.io.*;
import java.util.*;

public class CompactService {
    private static final long COMPACT_INTERVAL = 60 * 1000; // 1 minute
    private int lastCompactedSegment;
    private static final String COMPACT_PREFIX = "compacted_";
    private static final String Hint_PREFIX = "hint_";
    private static final String COMPACTED_DIRECTORY = "compacted";
    private static final String HINT_DIRECTORY = "hints";
    private final Bitcask bitcask;

    public CompactService(Bitcask bitcask, int lastCompactedSegment) {
        this.bitcask = bitcask;
        this.lastCompactedSegment = lastCompactedSegment;
        scheduleCompaction();
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
        int endSegment = bitcask.getCurrentSegmentNumber() - 1;
        if (startSegment >= endSegment) {
            return;
        }

        compactSegments(startSegment, endSegment);
        lastCompactedSegment = endSegment;
    }

    private void compactSegments(int startSegment, int endSegment) {
        String compactedFileName = COMPACTED_DIRECTORY + "/" + COMPACT_PREFIX + String.format("%d_%d.dat", startSegment, endSegment);
        // read all segments and create map of stationId to latest weather status and create the hashIndex for the compacted file
        Map<Long, WeatherStatus> stationIdToLatestWeatherStatus = new HashMap<>();
        Map<Long, Map.Entry<String, Long>> compactedHashIndex = new HashMap<>();
        for (int i = startSegment; i <= endSegment; i++) {
            String segmentFileName = Bitcask.SEGMENT_PREFIX + i + ".dat";
            try (RandomAccessFile file = new RandomAccessFile(Bitcask.SEGMENT_DIRECTORY + "/" + segmentFileName, "r")) {
                while (file.getFilePointer() < file.length()) {
                    long currentPosition = file.getFilePointer();
                    WeatherStatus weatherStatus = (WeatherStatus) new ObjectInputStream(new FileInputStream(file.getFD())).readObject();
                    long stationId = weatherStatus.getStationId();
                    stationIdToLatestWeatherStatus.put(stationId, weatherStatus);
                    compactedHashIndex.put(stationId, Map.entry(segmentFileName, currentPosition));
                }
            } catch (IOException | ClassNotFoundException e) {
                System.err.println("Error reading segment file: " + segmentFileName + " - " + e.getMessage());
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
            System.err.println("Error writing to compacted file: " + compactedFileName + " - " + e.getMessage());
        }
        generateHintFile(startSegment, endSegment, compactedHashIndex);
    }

    private void generateHintFile(int startSegment, int endSegment, Map<Long, Map.Entry<String, Long>> compactedHashIndex) {
        String hintFileName = HINT_DIRECTORY + "/" + Hint_PREFIX + String.format("%d_%d.txt", startSegment, endSegment);
        try (PrintWriter writer = new PrintWriter(hintFileName)) {
            for (Map.Entry<Long, Map.Entry<String, Long>> entry : compactedHashIndex.entrySet()) {
                writer.println(entry.getKey() + "," + entry.getValue().getKey() + "," + entry.getValue().getValue());
            }
        } catch (IOException e) {
            System.err.println("Error writing hint file: " + hintFileName + " - " + e.getMessage());
        }
    }

    public static int updateWithHintFiles(Map<Long, Map.Entry<String, Long>> hashIndex, int lastSnapshotSegmentNum) {
        int lastHintFileSegmentNum = 0;
        File directory = new File(HINT_DIRECTORY);
        if (!directory.exists()) {
            return lastHintFileSegmentNum;
        }
        File[] hintFiles = directory.listFiles();
        if (hintFiles != null) {
            Arrays.sort(hintFiles, new HintFileComparator());

            for (File file : hintFiles) {
                int firstSegmentNum = extractFirstSegmentNumber(file.getName());
                int lastSegmentNum = extractLastSegmentNumber(file.getName());
                if (lastSnapshotSegmentNum < firstSegmentNum || lastSnapshotSegmentNum <= lastSegmentNum) {
                    try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                        String line;
                        while ((line = reader.readLine()) != null) {
                            String[] parts = line.split(",");
                            if (parts.length == 3) {
                                long stationId = Long.parseLong(parts[0]);
                                String segmentFileName = parts[1];
                                long offset = Long.parseLong(parts[2]);
                                hashIndex.put(stationId, Map.entry(segmentFileName, offset));
                            }
                        }
                        lastHintFileSegmentNum = lastSegmentNum;
                    } catch (IOException e) {
                        System.err.println("Error reading hint file: " + file.getName() + " - " + e.getMessage());
                    }
                }
            }
        }
        return lastHintFileSegmentNum;
    }

    private static int extractFirstSegmentNumber(String fileName) {
        return Integer.parseInt(fileName.substring(fileName.indexOf('_') + 1, fileName.lastIndexOf('_')));
    }

    private static int extractLastSegmentNumber(String fileName) {
        return Integer.parseInt(fileName.substring(fileName.lastIndexOf('_') + 1, fileName.indexOf('.')));
    }

    private static class HintFileComparator implements Comparator<File> {
        @Override
        public int compare(File file1, File file2) {
            int firstSegmentNum1 = extractFirstSegmentNumber(file1.getName());
            int firstSegmentNum2 = extractFirstSegmentNumber(file2.getName());
            return Integer.compare(firstSegmentNum1, firstSegmentNum2);
        }
    }
}
