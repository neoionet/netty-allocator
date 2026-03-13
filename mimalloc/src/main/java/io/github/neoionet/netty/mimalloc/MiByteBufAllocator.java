package io.github.neoionet.netty.mimalloc;

import io.netty.buffer.AbstractByteBufAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufAllocatorMetric;
import io.netty.buffer.ByteBufAllocatorMetricProvider;
import io.netty.buffer.UnpooledDirectByteBuf;
import io.netty.buffer.UnpooledHeapByteBuf;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.UnstableApi;

/**
 * A Free-List {@link ByteBufAllocator} derived from `mimalloc`:
 * <a href="https://www.microsoft.com/en-us/research/wp-content/uploads/2019/06/mimalloc-tr-v1.pdf">mimalloc-paper</a>.
 * <a href="https://github.com/microsoft/mimalloc">mimalloc-code</a>
 * <p>
 *
 * <strong>Note:</strong> This allocator is <strong>experimental</strong>.
 * It is recommended to roll out usage slowly, and to carefully monitor application performance in the process.
 * <p>
 *
 * See the {@link MiMallocByteBufAllocator} class documentation for implementation details.
 */
@UnstableApi
public final class MiByteBufAllocator extends AbstractByteBufAllocator
        implements ByteBufAllocatorMetricProvider, ByteBufAllocatorMetric {

    private final MiMallocByteBufAllocator direct;
    private final MiMallocByteBufAllocator heap;

    /**
     * Creates a new instance of {@code MiByteBufAllocator} with the default settings.
     */
    public MiByteBufAllocator() {
        this(!PlatformDependent.isExplicitNoPreferDirect());
    }

    /**
     * Creates a new instance of {@code MiByteBufAllocator} with the specified preference for direct buffers.
     */
    public MiByteBufAllocator(boolean preferDirect) {
        super(preferDirect);
        direct = new MiMallocByteBufAllocator(new DirectChunkAllocator(this));
        heap = new MiMallocByteBufAllocator(new HeapChunkAllocator(this));
    }

    @Override
    protected ByteBuf newHeapBuffer(int initialCapacity, int maxCapacity) {
        return toLeakAwareBuffer(heap.allocate(initialCapacity, maxCapacity));
    }

    @Override
    protected ByteBuf newDirectBuffer(int initialCapacity, int maxCapacity) {
        return toLeakAwareBuffer(direct.allocate(initialCapacity, maxCapacity));
    }

    @Override
    public boolean isDirectBufferPooled() {
        return true;
    }

    @Override
    public long usedHeapMemory() {
        return heap.usedMemory();
    }

    @Override
    public long usedDirectMemory() {
        return direct.usedMemory();
    }

    @Override
    public ByteBufAllocatorMetric metric() {
        return this;
    }

    private static final class HeapChunkAllocator implements MiMallocByteBufAllocator.ChunkAllocator {
        private final ByteBufAllocator allocator;

        private HeapChunkAllocator(ByteBufAllocator allocator) {
            this.allocator = allocator;
        }

        @Override
        public UnpooledHeapByteBuf allocate(int initialCapacity, int maxCapacity) {
            return PlatformDependent.hasUnsafe() ?
                    new MiUnpooledUnsafeHeapByteBuf(allocator, initialCapacity, maxCapacity) :
                    new MiUnpooledHeapByteBuf(allocator, initialCapacity, maxCapacity);
        }
    }

    private static final class DirectChunkAllocator implements MiMallocByteBufAllocator.ChunkAllocator {
        private final ByteBufAllocator allocator;

        private DirectChunkAllocator(ByteBufAllocator allocator) {
            this.allocator = allocator;
        }

        @Override
        public UnpooledDirectByteBuf allocate(int initialCapacity, int maxCapacity) {
            return PlatformDependent.hasUnsafe() ?
                    new MiUnpooledUnsafeNoCleanerDirectByteBuf(allocator, initialCapacity, maxCapacity) :
                    new MiUnpooledDirectByteBuf(allocator, initialCapacity, maxCapacity);
        }
    }
}
