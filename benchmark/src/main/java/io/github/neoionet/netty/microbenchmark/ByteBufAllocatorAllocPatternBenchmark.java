/*
 * Copyright 2025 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.github.neoionet.netty.microbenchmark;

import io.github.neoionet.netty.microbenchmark.data.ECommercePattern;
import io.github.neoionet.netty.microbenchmark.data.WebSocketProxyPattern;
import io.github.neoionet.netty.microbenchmark.data.ApiGatewayPattern;
import io.netty.buffer.AdaptiveByteBufAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.internal.MathUtil;
import io.github.neoionet.netty.mimalloc.MiByteBufAllocator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.CompilerControl;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * This is a modified portion of `io.netty.microbench.buffer.ByteBufAllocatorAllocPatternBenchmark`
 * from the <a href="https://github.com/netty/netty">netty</a> project.
 */
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@Fork(value = 1, jvmArgs = {
        "-server",
        "-dsa", "-da",
        "-XX:InitialRAMPercentage=40.0",
        "-XX:MaxRAMPercentage=40.0",
        "-Dio.netty.leakDetection.level=disabled",
        "-Djmh.executor=CUSTOM",
        "-Djmh.executor.class=io.github.neoionet.netty.microbenchmark.executor.FastThreadLocalThreadHarnessExecutor"
})
public class ByteBufAllocatorAllocPatternBenchmark {

    static {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.DISABLED);
    }

    public enum SizePattern {
        API_GATEWAY(() -> ApiGatewayPattern.FLATTENED_SIZE_ARRAY),
        SOCKET_PROXY(() -> WebSocketProxyPattern.FLATTENED_SIZE_ARRAY),
        E_COMMERCE(() -> ECommercePattern.FLATTENED_SIZE_ARRAY);
        private final Supplier<int[]> factory;
        SizePattern(Supplier<int[]> factory) {
            this.factory = factory;
        }
        private int[] create() {
            return factory.get();
        }
    }

    @Param({
            "SOCKET_PROXY",
            "API_GATEWAY",
            "E_COMMERCE",
    })
    public SizePattern sizePattern;
    private int[] sizesArray;

    public enum AllocatorType {
        ADAPTIVE(AdaptiveByteBufAllocator::new),
        MIMALLOC(MiByteBufAllocator::new),
        POOLED(() -> PooledByteBufAllocator.DEFAULT);
        private final Supplier<ByteBufAllocator> factory;
        AllocatorType(Supplier<ByteBufAllocator> factory) {
            this.factory = factory;
        }
        private ByteBufAllocator create() {
            return factory.get();
        }
    }
    @Param({
            "POOLED",
            "ADAPTIVE",
            "MIMALLOC",
    })
    public AllocatorType allocatorType;
    private ByteBufAllocator allocator;

    @Param({
            "false",
            "true"
    })
    public boolean enableReadWrite;

    // Must be power of 2.
    @Param({
           "128",  // 128 buffers per thread
           "1024", // 1K buffers per thread
           "4096", // 4K buffers per thread
           "8192", // 8K buffers per thread
           "16384", // 16K buffers per thread
           "32768", // 32K buffers per thread
           "65536", // 64K buffers per thread
    })
    public int MAX_LIVE_BUFFERS;

    public static Set<BenchmarkParams> benchParamsSet = ConcurrentHashMap.newKeySet();

    @State(Scope.Thread)
    public static class AllocationPatternState {
        private int[] releaseIndexes;
        private int[] sizes;
        private int nextReleaseIndex;
        private int nextSizeIndex;
        private ByteBuf[] buffers;
        private ByteBufAllocator allocator;
        private boolean enableReadWrite;
        @Setup
        public void setup(ByteBufAllocatorAllocPatternBenchmark benchmark, BenchmarkParams params) {
            this.allocator = benchmark.allocator;
            this.enableReadWrite = benchmark.enableReadWrite;
            releaseIndexes = new int[benchmark.MAX_LIVE_BUFFERS];
            sizes = new int[MathUtil.findNextPositivePowerOfTwo(benchmark.sizesArray.length)];
            SplittableRandom rand = new SplittableRandom(42L);
            // Pre-generate the to be released index.
            for (int i = 0; i < releaseIndexes.length; i++) {
                releaseIndexes[i] = i;
            }
            for (int i = releaseIndexes.length - 1; i > 0; i--) {
                int index = rand.nextInt(i + 1);
                int temp = releaseIndexes[index];
                releaseIndexes[index] = releaseIndexes[i];
                releaseIndexes[i] = temp;
            }
            // Shuffle the `flattenedSizeArray` to `sizes`.
            for (int i = 0; i < sizes.length; i++) {
                int sizeIndex = rand.nextInt(benchmark.sizesArray.length);
                sizes[i] = benchmark.sizesArray[sizeIndex];
            }
            nextReleaseIndex = 0;
            nextSizeIndex = 0;
            buffers = new ByteBuf[benchmark.MAX_LIVE_BUFFERS];
            // Fill the live set here, in slot order. Filled lazily by the benchmark method, the n-th operation creates
            // the n-th buffer object and stores it in slot releaseIndexes[n], so every later pass releases the buffer
            // objects in the order they were created: a sequential walk through memory that the hardware prefetcher
            // hides, until the first young GC copies them in slot order and the walk becomes random. The score then
            // depends on when that GC happens, i.e. on how much garbage the allocator under test produces.
            boolean direct = params.getBenchmark().endsWith("directAllocation");
            for (int i = 0; i < buffers.length; i++) {
                int size = sizes[getNextSizeIndex()];
                ByteBuf buf = direct ? allocateDirect(allocator, size) : allocateHeap(allocator, size);
                if (enableReadWrite) {
                    buf.writeByte(size);
                }
                buffers[i] = buf;
            }
            nextSizeIndex = 0;
        }

        private int getNextReleaseIndex() {
            int index = nextReleaseIndex;
            nextReleaseIndex = (nextReleaseIndex + 1) & (releaseIndexes.length - 1);
            return releaseIndexes[index];
        }

        private int getNextSizeIndex() {
            int index = nextSizeIndex;
            nextSizeIndex = (nextSizeIndex + 1) & (sizes.length - 1);
            return index;
        }

        @CompilerControl(CompilerControl.Mode.DONT_INLINE)
        private static ByteBuf allocateHeap(ByteBufAllocator allocator, int size) {
            return allocator.heapBuffer(size);
        }

        @CompilerControl(CompilerControl.Mode.DONT_INLINE)
        private static ByteBuf allocateDirect(ByteBufAllocator allocator, int size) {
            return allocator.directBuffer(size);
        }

        @CompilerControl(CompilerControl.Mode.DONT_INLINE)
        public ByteBuf performDirectAllocation() {
            int releaseIndex = getNextReleaseIndex();
            this.readAndRelease(releaseIndex);
            int size = sizes[getNextSizeIndex()];
            ByteBuf newBuf = allocateDirect(allocator, size);
            if (enableReadWrite) {
                newBuf.writeByte(size);
            }
            buffers[releaseIndex] = newBuf;
            return newBuf;
        }

        @CompilerControl(CompilerControl.Mode.DONT_INLINE)
        public ByteBuf performHeapAllocation() {
            int releaseIndex = getNextReleaseIndex();
            this.readAndRelease(releaseIndex);
            int size = sizes[getNextSizeIndex()];
            ByteBuf newBuf = allocateHeap(allocator, size);
            if (enableReadWrite) {
                newBuf.writeByte(size);
            }
            buffers[releaseIndex] = newBuf;
            return newBuf;
        }

        @CompilerControl(CompilerControl.Mode.DONT_INLINE)
        private ByteBuf readAndRelease(int releaseIndex) {
            ByteBuf oldBuf = this.buffers[releaseIndex];
            if (oldBuf != null) {
                if (enableReadWrite) {
                    oldBuf.readByte();
                }
                oldBuf.release();
            }
            return oldBuf;
        }

        private static void releaseBufferArray(ByteBuf[] buffers) {
            if (buffers == null) {
                return;
            }
            for (int i = 0; i < buffers.length; i++) {
                if (buffers[i] != null && buffers[i].refCnt() > 0) {
                    buffers[i].release();
                    buffers[i] = null;
                }
            }
        }

        private static void printProcRSS() throws Exception {
            System.out.println("cRSS-pRSS:[" + ProcessMemoryUtil.getSelfRssKb() / 1024 + ", " +
                    ProcessMemoryUtil.getSelfPeakRssKb() / 1024 + "]");
        }

        private void printUsedMemory(BenchmarkParams benchmarkParams) throws Exception {
            long usedMem = 0;
            if (allocator instanceof PooledByteBufAllocator) {
                if (benchmarkParams.getBenchmark().contains("directAllocation")) {
                    usedMem = ((PooledByteBufAllocator) allocator).metric().usedDirectMemory();
                } else {
                    usedMem = ((PooledByteBufAllocator) allocator).metric().usedHeapMemory();
                }
            }
            if (allocator instanceof AdaptiveByteBufAllocator) {
                if (benchmarkParams.getBenchmark().contains("directAllocation")) {
                    usedMem = ((AdaptiveByteBufAllocator) allocator).usedDirectMemory();
                } else {
                    usedMem = ((AdaptiveByteBufAllocator) allocator).usedHeapMemory();
                }
            }
            if (allocator instanceof MiByteBufAllocator) {
                if (benchmarkParams.getBenchmark().contains("directAllocation")) {
                    usedMem = ((MiByteBufAllocator) allocator).usedDirectMemory();
                } else {
                    usedMem = ((MiByteBufAllocator) allocator).usedHeapMemory();
                }

            }
            System.out.println(";used-memory:[" + usedMem / 1024 / 1024 + "]");
        }

        @TearDown
        public void tearDown(BenchmarkParams benchmarkParams) throws Exception {
            if (benchParamsSet.add(benchmarkParams)) {
                printProcRSS();
                printUsedMemory(benchmarkParams);
            }
            releaseBufferArray(buffers);
        }
    }

    @Setup
    public void setupAllocator() {
        allocator = allocatorType.create();
        sizesArray = sizePattern.create();
        benchParamsSet.clear();
    }

    @Benchmark
    public void directAllocation(AllocationPatternState state) {
        state.performDirectAllocation();
    }

    @Benchmark
    public void heapAllocation(AllocationPatternState state) {
        state.performHeapAllocation();
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ByteBufAllocatorAllocPatternBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    private static final class ProcessMemoryUtil {

        private ProcessMemoryUtil() {}

        static long getSelfRssKb() {
            return readField("VmRSS:");
        }

        static long getSelfPeakRssKb() {
            return readField("VmHWM:");
        }

        private static long readField(String prefix) {
            Path statusPath = Paths.get("/proc/self/status");
            try (Stream<String> lines = Files.lines(statusPath)) {
                return lines
                        .filter(line -> line.startsWith(prefix))
                        .findFirst()
                        .map(ProcessMemoryUtil::parseKbLine)
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
}
