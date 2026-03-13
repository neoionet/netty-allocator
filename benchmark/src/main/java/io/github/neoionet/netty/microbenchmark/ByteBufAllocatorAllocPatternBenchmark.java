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

import io.github.neoionet.netty.microbenchmark.data.LogNormalDistributionPattern;
import io.github.neoionet.netty.microbenchmark.data.WebSocketProxyPattern;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * This is a modified portion of `io.netty.microbench.buffer.ByteBufAllocatorAllocPatternBenchmark`
 * from the <a href="https://github.com/netty/netty">netty</a> project.
 */
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 10, time = 1)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@Fork(value = 1, jvmArgs = {
        "-server",
        "-dsa", "-da",
        "-Dio.netty.leakDetection.level=disabled",
        "-Djmh.executor=CUSTOM",
        "-Djmh.executor.class=io.github.neoionet.netty.microbenchmark.executor.FastThreadLocalThreadHarnessExecutor"})
public class ByteBufAllocatorAllocPatternBenchmark {

    static {
        System.setProperty("io.netty.allocator.chunkReuseQueueCapacity", "1024");
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.DISABLED);
    }

    public enum SizePattern {
        LOG_NORMAL_PATTERN(() -> LogNormalDistributionPattern.FLATTENED_SIZE_ARRAY),
        SOCKET_PROXY_PATTERN(() -> WebSocketProxyPattern.FLATTENED_SIZE_ARRAY);
        private final Supplier<int[]> factory;
        SizePattern(Supplier<int[]> factory) {
            this.factory = factory;
        }
        private int[] create() {
            return factory.get();
        }
    }

    @Param({
            "LOG_NORMAL_PATTERN",
            "SOCKET_PROXY_PATTERN"
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
            "ADAPTIVE",
            "POOLED",
            "MIMALLOC"
    })
    public AllocatorType allocatorType;
    private ByteBufAllocator allocator;

    @Param({
            "true",
            "false"
    })
    public boolean enableReadWrite;

    // Must be power of 2.
    @Param({
            "128",
            "256",
            "512",
            "1024",
            "2048",
            "4096",
            "8192",
    })
    public int MAX_LIVE_BUFFERS;

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
        public void setup(ByteBufAllocatorAllocPatternBenchmark benchmark) {
            this.allocator = benchmark.allocator;
            this.enableReadWrite = benchmark.enableReadWrite;
            releaseIndexes = new int[benchmark.MAX_LIVE_BUFFERS];
            sizes = new int[MathUtil.findNextPositivePowerOfTwo(benchmark.sizesArray.length)];
            SplittableRandom rand = new SplittableRandom(Thread.currentThread().getId());
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
        private static void release(ByteBuf buf) {
            buf.release();
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
        public void performDirectAllocation(Blackhole blackhole) {
            int size = sizes[getNextSizeIndex()];
            int releaseIndex = getNextReleaseIndex();
            ByteBuf[] buffers = this.buffers;
            ByteBuf oldBuf = buffers[releaseIndex];
            if (oldBuf != null) {
                if (enableReadWrite) {
                    oldBuf.readByte();
                    blackhole.consume(oldBuf);
                }
                release(oldBuf);
            }
            ByteBuf newBuf = allocateDirect(allocator, size);
            if (enableReadWrite) {
                newBuf.writeByte(size);
            }
            buffers[releaseIndex] = newBuf;
            blackhole.consume(newBuf);
        }

        @CompilerControl(CompilerControl.Mode.DONT_INLINE)
        public void performHeapAllocation(Blackhole blackhole) {
            int size = sizes[getNextSizeIndex()];
            int releaseIndex = getNextReleaseIndex();
            ByteBuf[] buffers = this.buffers;
            ByteBuf oldBuf = buffers[releaseIndex];
            if (oldBuf != null) {
                if (enableReadWrite) {
                    oldBuf.readByte();
                    blackhole.consume(oldBuf);
                }
                release(oldBuf);
            }
            ByteBuf newBuf = allocateHeap(allocator, size);
            if (enableReadWrite) {
                newBuf.writeByte(size);
            }
            buffers[releaseIndex] = newBuf;
            blackhole.consume(newBuf);
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

        @TearDown
        public void tearDown() {
            releaseBufferArray(buffers);
        }
    }

    @Setup
    public void setupAllocator() {
        allocator = allocatorType.create();
        sizesArray = sizePattern.create();
    }

    @Threads(16)
    @Benchmark
    public void directAllocation(AllocationPatternState state, Blackhole blackhole) {
        state.performDirectAllocation(blackhole);
    }

    @Threads(16)
    @Benchmark
    public void heapAllocation(AllocationPatternState state, Blackhole blackhole) {
        state.performHeapAllocation(blackhole);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ByteBufAllocatorAllocPatternBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }
}
