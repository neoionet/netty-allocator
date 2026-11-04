package io.github.neoionet.netty.microbenchmark.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

public final class MemoryUtil {

    private MemoryUtil() {}

    public static long getSelfRssKb() {
        return readField("VmRSS:");
    }

    public static long getSelfPeakRssKb() {
        return readField("VmHWM:");
    }

    private static long readField(String prefix) {
        Path statusPath = Paths.get("/proc/self/status");
        try (Stream<String> lines = Files.lines(statusPath)) {
            return lines
                    .filter(line -> line.startsWith(prefix))
                    .findFirst()
                    .map(MemoryUtil::parseKbLine)
                    .orElseThrow(() -> new IllegalStateException(prefix + " not found in /proc/self/status"));
        } catch (IOException e) {
            System.err.println("Failed to read /proc/self/status");
        }
        return -1;
    }

    private static long parseKbLine(String line) {
        String[] parts = line.trim().split("\\s+");
        return Long.parseLong(parts[1]);
    }
}
