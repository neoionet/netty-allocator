package io.github.neoionet.netty.mimalloc;

/**
 * Since these methods have protected access in `io.netty.buffer.AbstractByteBuf`,
 * so they are placed within this class to allow us to access them.
 */
interface MiByteBufAdapter {
    byte _getByte(int index);
    short _getShort(int index);
    short _getShortLE(int index);
    int _getUnsignedMedium(int index);
    int _getUnsignedMediumLE(int index);
    int _getInt(int index);
    int _getIntLE(int index);
    long _getLong(int index);
    long _getLongLE(int index);

    void _setByte(int index, int value);
    void _setShort(int index, int value);
    void _setShortLE(int index, int value);
    void _setMedium(int index, int value);
    void _setMediumLE(int index, int value);
    void _setInt(int index, int value);
    void _setIntLE(int index, int value);
    void _setLong(int index, long value);
    void _setLongLE(int index, long value);
}
