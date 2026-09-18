package io.github.neoionet.netty.microbenchmark.data;

import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingFile;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

public class ECommercePattern {

    private static final int[] SIZE_PATTERN ;
    public static final int[] FLATTENED_SIZE_ARRAY;

    static {
        try {
            // Make sure the e-commerce.jfr file exists in the `netty-allocator` directory.
            SIZE_PATTERN = buildPattern("e-commerce.jfr");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        int[] sizePattern = SIZE_PATTERN;
        // Flat the size pattern array.
        ArrayList<Integer> sizeList = new ArrayList<>();
        for (int i = 0; i < sizePattern.length; i += 2) {
            int size = sizePattern[i];
            int frequency = sizePattern[i + 1];
            for (int j = 0; j < frequency; j++) {
                sizeList.add(size);
            }
        }
        FLATTENED_SIZE_ARRAY = sizeList.stream().mapToInt(Integer::intValue).toArray();
    }

    private static int[] buildPattern(String jfrFile) throws IOException {
        Path path = toAbsolutePath(jfrFile);
        TreeMap<Integer, Integer> summation = new TreeMap<>();
        try (RecordingFile eventReader = new RecordingFile(path)) {
            while (eventReader.hasMoreEvents()) {
                RecordedEvent event = eventReader.readEvent();
                String name = event.getEventType().getName();
                if (("AllocateBufferEvent".equals(name) || "io.netty.AllocateBuffer".equals(name)) &&
                        event.hasField("size")) {
                    int size = event.getInt("size");
                    summation.compute(size, (k, v) -> v == null ? 1 : v + 1);
                }
            }
        }
        if (summation.isEmpty()) {
            throw new IllegalStateException("No 'AllocateBufferEvent' records found in JFR file: " + jfrFile);
        }
        int[] pattern = new int[summation.size() * 2];
        int index = 0;
        for (Map.Entry<Integer, Integer> entry : summation.entrySet()) {
            pattern[index++] = entry.getKey();
            pattern[index++] = entry.getValue();
        }
        return pattern;
    }

    @SuppressWarnings("JvmTaintAnalysis")
    private static Path toAbsolutePath(String jfrFile) {
        return Paths.get(jfrFile).toAbsolutePath();
    }

}
