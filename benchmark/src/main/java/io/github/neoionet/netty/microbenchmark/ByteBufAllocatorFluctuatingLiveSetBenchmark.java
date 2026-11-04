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

import io.github.neoionet.netty.microbenchmark.data.*;
import io.github.neoionet.netty.microbenchmark.util.MemoryUtil;
import io.github.neoionet.netty.microbenchmark.util.PaddingBefore;
import io.github.neoionet.netty.mimalloc.MiByteBufAllocator;
import io.netty.buffer.*;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.internal.MathUtil;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.infra.ThreadParams;
import org.openjdk.jmh.runner.IterationType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Like {@link ByteBufAllocatorAllocPatternBenchmark}, but the live set is not fixed.
 * <p>
 * In {@link ByteBufAllocatorAllocPatternBenchmark} every thread keeps exactly {@code MAX_LIVE_BUFFERS} buffers
 * and replaces them in a fixed order, so every buffer lives exactly {@code MAX_LIVE_BUFFERS} operations and the
 * allocator settles into a state that repeats.
 * That measures the fast path well, but not what happens when the load changes. Here, per thread:
 * <ul>
 *     <li>The number of short-lived buffers, the target, follows a triangle wave between {@code N * (1 - a)} and
 *     {@code N * (1 + a)}, with {@code N = MAX_LIVE_BUFFERS} and {@code a = amplitudePercent / 100}: it starts at
 *     the low point and moves by one every {@code stepsPerChange} steps, up to the high point and back down.
 *     When the live count is below the target, a step only allocates; when it is above, a step only frees;
 *     otherwise a step frees one buffer and allocates one. So {@code 1 / stepsPerChange} of the steps only
 *     allocate or only free, and the live count changes by about {@code N / stepsPerChange} buffers during the
 *     mean lifetime of a buffer: the smaller {@code stepsPerChange}, the faster the load rises and falls.
 *     A period (low, high and back to low) takes {@code 2 * (high - low) * stepsPerChange} steps.</li>
 *     <li>The buffer to free is picked at random among the live ones, so lifetimes vary (roughly exponential, with
 *     a mean of about {@code N} steps) instead of being all the same.</li>
 *     <li>{@code longLivedPercent} percent of {@code N} extra buffers live
 *     {@value #LONG_LIVED_LIFETIME_MULTIPLIER} {@code N} steps each; they are replaced in turn, one every so many
 *     steps. They keep pages in use while the short-lived buffers around them come and go.</li>
 * </ul>
 * Sizes are generated in the setup; the buffer to free is picked with a SplitMix64 generator per thread (the
 * algorithm of {@link java.util.SplittableRandom}), whose state is a field of the thread's state object.
 * <p>
 * Each measurement iteration prints the used memory of the allocator (direct or heap, depending on the
 * benchmark) at the end of the iteration, the highest value seen by a sampler thread during it if
 * {@code sampleMemory} is true, and the RSS of the process. The used memory of each allocator is counted
 * differently, so the RSS is the better figure to compare them by.
 */
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 5, time = 1)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@Fork(value = 1, jvmArgs = {
        "-server",
        "-dsa", "-da",
        "-Dio.netty.leakDetection.level=disabled",
        "-Djmh.executor=CUSTOM",
        "-Djmh.executor.class=io.github.neoionet.netty.microbenchmark.executor.FastThreadLocalThreadHarnessExecutor"
})
@Threads(8)
public class ByteBufAllocatorFluctuatingLiveSetBenchmark {

    static {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.DISABLED);
    }

    // How long a long-lived buffer lives, in multiples of MAX_LIVE_BUFFERS steps (a short-lived one lives about
    // MAX_LIVE_BUFFERS steps).
    static final int LONG_LIVED_LIFETIME_MULTIPLIER = 256;
    private static final long MEMORY_SAMPLE_INTERVAL_MS = 5;
    private static final long MiB = 1024 * 1024;

    public enum SizePattern {
        API_GATEWAY(() -> ApiGatewayPattern.FLATTENED_SIZE_ARRAY),
        SOCKET_PROXY(() -> WebSocketProxyPattern.FLATTENED_SIZE_ARRAY),
        E_COMMERCE(() -> ECommercePattern.FLATTENED_SIZE_ARRAY),
        UNIFORM(() -> UniformPattern.FLATTENED_SIZE_ARRAY),
        LOG_UNIFORM(() -> LogUniformSizePattern.FLATTENED_SIZE_ARRAY);
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
            "LOG_UNIFORM",
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
//            "false",
            "true"
    })
    public boolean enableReadWrite;

    // The middle of the wave, per thread. With a small value the swing is too small to fill or empty many pages,
    // so the wave has little effect.
    @Param({
            "128",
            "1024",
            "8192",
            "32768",
    })
    public int MAX_LIVE_BUFFERS;

    // Half the swing of the wave, in percent of MAX_LIVE_BUFFERS: [0, 100).
    @Param({"90"})
    public int amplitudePercent;

    // The wave moves by one buffer every this many steps: >= 1. 1 / stepsPerChange of the steps only allocate or
    // only free; the smaller, the faster the load rises and falls.
    @Param({"2"})
    public int stepsPerChange;

    // Long-lived buffers, in percent of MAX_LIVE_BUFFERS, on top of the short-lived ones.
    @Param({"0"})
    public int longLivedPercent;

    // Whether a thread samples the used memory during each iteration to find its peak.
    // With false, only the used memory at the end of each measurement iteration is printed.
    @Param({"false"})
    public boolean sampleMemory;

    private volatile boolean sampling;
    private volatile long peakUsedMemory;
    private Thread sampler;
    private boolean isDirect;

    @Setup
    public void setupAllocator(BenchmarkParams params) {
        allocator = allocatorType.create();
        sizesArray = sizePattern.create();
        isDirect = params.getBenchmark().endsWith("directAllocation");
    }

    @Setup(Level.Iteration)
    public void startMemorySampler() {
        peakUsedMemory = 0;
        if (!sampleMemory) {
            return;
        }
        sampling = true;
        sampler = new Thread(() -> {
            while (sampling) {
                long used = usedMemory();
                if (used > peakUsedMemory) {
                    peakUsedMemory = used;
                }
                try {
                    Thread.sleep(MEMORY_SAMPLE_INTERVAL_MS);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }, "used-memory-sampler");
        sampler.setDaemon(true);
        sampler.start();
    }

    @TearDown(Level.Iteration)
    public void stopMemorySampler(IterationParams iterationParams) throws InterruptedException {
        if (sampler != null) {
            sampling = false;
            sampler.interrupt();
            sampler.join();
            sampler = null;
        }
        if (iterationParams.getType() == IterationType.MEASUREMENT) {
            // All threads have finished the iteration, and their buffers are still live.
            long end = usedMemory();
            String peak = sampleMemory ? String.valueOf(Math.max(peakUsedMemory, end) / MiB) : "-";
            System.out.println("used-memory(MiB):[end=" + end / MiB + ", peak=" + peak + "]"
                    + ";rss(MiB):[" + MemoryUtil.getSelfRssKb() / 1024 + "]");
        }
    }

    private long usedMemory() {
        if (allocator instanceof MiByteBufAllocator) {
            MiByteBufAllocator a = (MiByteBufAllocator) allocator;
            return isDirect ? a.usedDirectMemory() : a.usedHeapMemory();
        }
        if (allocator instanceof AdaptiveByteBufAllocator) {
            AdaptiveByteBufAllocator a = (AdaptiveByteBufAllocator) allocator;
            return isDirect ? a.usedDirectMemory() : a.usedHeapMemory();
        }
        if (allocator instanceof PooledByteBufAllocator) {
            PooledByteBufAllocatorMetric m = ((PooledByteBufAllocator) allocator).metric();
            return isDirect ? m.usedDirectMemory() : m.usedHeapMemory();
        }
        return -1;
    }

    @State(Scope.Thread)
    public static class FluctuatingState extends PaddingBefore {
        private ByteBufAllocator allocator;
        private boolean enableReadWrite;

        private int[] sizes;
        private int nextSize;
        // The state of the SplitMix64 generator that generates the sizes in the setup, then picks the buffers to
        // free. It is a field here rather than a separate SplittableRandom: the GC may place the small objects of
        // the threads next to each other, and the generator is written at every step, so the threads would share
        // cache lines (false sharing).
        private long seed;

        // Short-lived buffers, packed in [0, liveCount).
        private ByteBuf[] live;
        private int liveCount;

        // The wave: target moves by direction every stepsPerChange steps, and turns around at low and high.
        private int low;
        private int high;
        private int target;
        private int direction; // +1 going up, -1 going down, 0 if there is no swing.
        private int stepsPerChange;
        private int changeCountdown;

        // Long-lived buffers, null if there are none; one is replaced every longLivedInterval steps.
        private ByteBuf[] longLived;
        private int nextLongLived;
        private int longLivedInterval;
        private int longLivedCountdown;

        @Setup(Level.Trial)
        public void setup(ByteBufAllocatorFluctuatingLiveSetBenchmark benchmark, ThreadParams threadParams) {
            int n = benchmark.MAX_LIVE_BUFFERS;
            if (n <= 0) {
                throw new IllegalArgumentException("MAX_LIVE_BUFFERS must be > 0: " + n);
            }
            if (benchmark.amplitudePercent < 0 || benchmark.amplitudePercent >= 100) {
                throw new IllegalArgumentException("amplitudePercent must be in [0, 100): "
                        + benchmark.amplitudePercent);
            }
            if (benchmark.stepsPerChange < 1) {
                throw new IllegalArgumentException("stepsPerChange must be >= 1: " + benchmark.stepsPerChange);
            }
            if (benchmark.longLivedPercent < 0) {
                throw new IllegalArgumentException("longLivedPercent must be >= 0: " + benchmark.longLivedPercent);
            }
            int delta = (int) ((long) n * benchmark.amplitudePercent / 100);
            if (benchmark.amplitudePercent > 0 && delta == 0) {
                throw new IllegalArgumentException("amplitudePercent " + benchmark.amplitudePercent
                        + " gives no swing with MAX_LIVE_BUFFERS " + n);
            }
            this.allocator = benchmark.allocator;
            this.enableReadWrite = benchmark.enableReadWrite;

            // A different seed per thread, so the threads do not run in lockstep; the same seeds in every fork.
            seed = 42L + threadParams.getThreadIndex();
            int[] pattern = benchmark.sizesArray;
            sizes = new int[MathUtil.findNextPositivePowerOfTwo(pattern.length)];
            for (int i = 0; i < sizes.length; i++) {
                sizes[i] = pattern[nextRandomInt(pattern.length)];
            }
            nextSize = 0;

            low = n - delta; // >= 1, since amplitudePercent < 100
            high = n + delta;
            // The wave starts at its low point, going up; it does not move if there is no swing.
            target = low;
            direction = high > low ? 1 : 0;
            stepsPerChange = benchmark.stepsPerChange;
            changeCountdown = stepsPerChange;

            boolean isDirect = benchmark.isDirect;
            live = new ByteBuf[high];
            liveCount = 0;
            for (int i = 0; i < low; i++) {
                live[liveCount++] = isDirect ? newDirectBuffer() : newHeapBuffer();
            }

            int longLivedCount = (int) ((long) n * benchmark.longLivedPercent / 100);
            if (longLivedCount > 0) {
                longLived = new ByteBuf[longLivedCount];
                for (int i = 0; i < longLivedCount; i++) {
                    longLived[i] = isDirect ? newDirectBuffer() : newHeapBuffer();
                }
                // Replace them in turn, one every `interval` steps, so each lives
                // LONG_LIVED_LIFETIME_MULTIPLIER * N steps (the first ones less, as they are all allocated here).
                long interval = (long) n * LONG_LIVED_LIFETIME_MULTIPLIER / longLivedCount;
                longLivedInterval = (int) Math.max(1L, Math.min(interval, Integer.MAX_VALUE));
                longLivedCountdown = longLivedInterval;
            } else {
                longLived = null;
                longLivedInterval = 0;
                longLivedCountdown = 0;
            }
            nextLongLived = 0;

            if (threadParams.getThreadIndex() == 0) {
                long period = 2L * (high - low) * stepsPerChange;
                System.out.println("wave(per thread):[low=" + low + ", high=" + high
                        + ", period=" + (period == 0 ? "-" : period + " steps (" + period / n + " N)")
                        + ", longLived=" + longLivedCount + "]");
            }
        }

        // The target number of short-lived buffers for this step: a triangle wave in [low, high].
        private int nextTarget() {
            if (--changeCountdown == 0) {
                changeCountdown = stepsPerChange;
                int t = target + direction;
                if (t == high || t == low) {
                    direction = -direction; // Turn around at the high and the low point.
                }
                target = t;
            }
            return target;
        }

        @CompilerControl(CompilerControl.Mode.DONT_INLINE)
        public void performDirectStep() {
            if (longLived != null && --longLivedCountdown == 0) {
                longLivedCountdown = longLivedInterval;
                replaceLongLived(true);
            }
            int target = nextTarget();
            int n = liveCount;
            if (n > target) {
                releaseRandom();
            } else if (n < target) {
                live[liveCount++] = newDirectBuffer();
            } else {
                releaseRandom();
                live[liveCount++] = newDirectBuffer();
            }
        }

        @CompilerControl(CompilerControl.Mode.DONT_INLINE)
        public void performHeapStep() {
            if (longLived != null && --longLivedCountdown == 0) {
                longLivedCountdown = longLivedInterval;
                replaceLongLived(false);
            }
            int target = nextTarget();
            int n = liveCount;
            if (n > target) {
                releaseRandom();
            } else if (n < target) {
                live[liveCount++] = newHeapBuffer();
            } else {
                releaseRandom();
                live[liveCount++] = newHeapBuffer();
            }
        }

        private ByteBuf newDirectBuffer() {
            int size = sizes[nextSize];
            nextSize = (nextSize + 1) & (sizes.length - 1);
            ByteBuf buf = allocateDirect(allocator, size);
            if (enableReadWrite) {
                buf.writeByte(size);
            }
            return buf;
        }

        private ByteBuf newHeapBuffer() {
            int size = sizes[nextSize];
            nextSize = (nextSize + 1) & (sizes.length - 1);
            ByteBuf buf = allocateHeap(allocator, size);
            if (enableReadWrite) {
                buf.writeByte(size);
            }
            return buf;
        }

        private void releaseRandom() {
            int n = liveCount;
            int i = nextRandomInt(n);
            ByteBuf buf = live[i];
            live[i] = live[n - 1];
            live[n - 1] = null;
            liveCount = n - 1;
            release(buf);
        }

        // SplitMix64, the algorithm of SplittableRandom.nextInt().
        private int nextRandomInt() {
            long z = (seed += 0x9E3779B97F4A7C15L);
            z = (z ^ (z >>> 33)) * 0x62A9D9ED799705F5L;
            return (int) (((z ^ (z >>> 28)) * 0xCB24D0A5C88C35B3L) >>> 32);
        }

        // A random int in [0, bound), mapped with a multiply instead of a division.
        private int nextRandomInt(int bound) {
            return (int) (((nextRandomInt() & 0xFFFFFFFFL) * bound) >>> 32);
        }

        private void replaceLongLived(boolean isDirect) {
            int i = nextLongLived;
            ByteBuf old = longLived[i];
            longLived[i] = isDirect ? newDirectBuffer() : newHeapBuffer();
            release(old);
            nextLongLived = i + 1 == longLived.length ? 0 : i + 1;
        }

        private void release(ByteBuf buf) {
            if (enableReadWrite) {
                buf.readByte();
            }
            buf.release();
        }

        @CompilerControl(CompilerControl.Mode.DONT_INLINE)
        private static ByteBuf allocateDirect(ByteBufAllocator allocator, int size) {
            return allocator.directBuffer(size);
        }

        @CompilerControl(CompilerControl.Mode.DONT_INLINE)
        private static ByteBuf allocateHeap(ByteBufAllocator allocator, int size) {
            return allocator.heapBuffer(size);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            for (int i = 0; i < liveCount; i++) {
                live[i].release();
                live[i] = null;
            }
            liveCount = 0;
            if (longLived != null) {
                for (int i = 0; i < longLived.length; i++) {
                    longLived[i].release();
                    longLived[i] = null;
                }
            }
        }
    }

    @Benchmark
    @Fork(jvmArgsAppend = {"-Xms4g", "-Xmx4g", "-XX:MaxDirectMemorySize=16g", "-XX:+AlwaysPreTouch"})
    public void directAllocation(FluctuatingState state) {
        state.performDirectStep();
    }

    @Benchmark
    @Fork(jvmArgsAppend = {"-Xms16g", "-Xmx16g", "-XX:MaxDirectMemorySize=1g", "-XX:+ExitOnOutOfMemoryError"})
    public void heapAllocation(FluctuatingState state) {
        state.performHeapStep();
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ByteBufAllocatorFluctuatingLiveSetBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }
}
