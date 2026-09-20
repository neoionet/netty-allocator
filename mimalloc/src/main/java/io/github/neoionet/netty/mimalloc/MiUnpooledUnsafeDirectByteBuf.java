package io.github.neoionet.netty.mimalloc;

import io.netty.buffer.ByteBufAllocator;
import io.netty.util.internal.CleanableDirectBuffer;
import io.netty.util.internal.PlatformDependent;
import io.netty.buffer.UnpooledUnsafeDirectByteBuf;

final class MiUnpooledUnsafeDirectByteBuf extends UnpooledUnsafeDirectByteBuf implements MiByteBufAdapter {

    MiUnpooledUnsafeDirectByteBuf(ByteBufAllocator alloc, int initialCapacity, int maxCapacity) {
        // Netty's constructor that permits expensive clean is package-private, so start with an empty buffer
        // and grow it via `capacity(int)`, which allocates through the overridden `allocateDirectBuffer(int)`.
        super(alloc, 0, maxCapacity);
        if (initialCapacity > 0) {
            capacity(initialCapacity);
        }
    }

    @Override
    protected CleanableDirectBuffer allocateDirectBuffer(int capacity) {
        // Chunks are long-lived and released explicitly by the allocator, so an expensive clean is acceptable,
        // the same as Netty's own pooling allocators (`PoolArena`, `AdaptiveByteBufAllocator`).
        return PlatformDependent.allocateDirect(capacity, true);
    }

    @Override
    public byte _getByte(int index) {
        return super._getByte(index);
    }

    @Override
    public short _getShort(int index) {
        return super._getShort(index);
    }

    @Override
    public short _getShortLE(int index) {
        return super._getShortLE(index);
    }

    @Override
    public int _getUnsignedMedium(int index) {
        return super._getUnsignedMedium(index);
    }

    @Override
    public int _getUnsignedMediumLE(int index) {
        return super._getUnsignedMediumLE(index);
    }

    @Override
    public int _getInt(int index) {
        return super._getInt(index);
    }

    @Override
    public int _getIntLE(int index) {
        return super._getIntLE(index);
    }

    @Override
    public long _getLong(int index) {
        return super._getLong(index);
    }

    @Override
    public long _getLongLE(int index) {
        return super._getLongLE(index);
    }

    @Override
    public void _setByte(int index, int value) {
        super._setByte(index, value);
    }

    @Override
    public void _setShort(int index, int value) {
        super._setShort(index, value);
    }

    @Override
    public void _setShortLE(int index, int value) {
        super._setShortLE(index, value);
    }

    @Override
    public void _setMedium(int index, int value) {
        super._setMedium(index, value);
    }

    @Override
    public void _setMediumLE(int index, int value) {
        super._setMediumLE(index, value);
    }

    @Override
    public void _setInt(int index, int value) {
        super._setInt(index, value);
    }

    @Override
    public void _setIntLE(int index, int value) {
        super._setIntLE(index, value);
    }

    @Override
    public void _setLong(int index, long value) {
        super._setLong(index, value);
    }

    @Override
    public void _setLongLE(int index, long value) {
        super._setLongLE(index, value);
    }
}
