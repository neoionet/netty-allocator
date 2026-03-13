package io.github.neoionet.netty.mimalloc;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledUnsafeHeapByteBuf;

class MiUnpooledUnsafeHeapByteBuf extends UnpooledUnsafeHeapByteBuf implements MiByteBufAdapter {

    MiUnpooledUnsafeHeapByteBuf(ByteBufAllocator alloc, int initialCapacity, int maxCapacity) {
        super(alloc, initialCapacity, maxCapacity);
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
