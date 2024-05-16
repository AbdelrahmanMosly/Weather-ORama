package org.example.bitcask;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class RecoveryManager {

    public static Bitcask recover() {
            Map<Long, Map.Entry<String, Long>> hashIndex = new HashMap<>();
            //Load the latest snapshot
            int lastSnapshotSegmentNum = 0;

            try {
                lastSnapshotSegmentNum = SnapshotService.loadLatestSnapshot(hashIndex);
            } catch (IOException e) {
                System.err.println("Error loading latest snapshot: " + e.getMessage());
            }
            //Update with hint files taken after the snapshot
            int lastHintFileSegmentNum = CompactService.updateWithHintFiles(hashIndex, lastSnapshotSegmentNum);

            int startSegmentNum;
            if (lastSnapshotSegmentNum > lastHintFileSegmentNum) {
                startSegmentNum = lastSnapshotSegmentNum;
            } else {
                startSegmentNum = lastHintFileSegmentNum + 1;
            }
            // Step 3: Update with segment files
            DataFileSegment currentDataFileSegment = DataFileSegment.recoverFromSegment(hashIndex, startSegmentNum);

        return new Bitcask(hashIndex, currentDataFileSegment, lastSnapshotSegmentNum);
    }
}
