/*
 * Copyright 2012 The Netty Project
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
package io.github.neoionet.netty.mimalloc;

import io.netty.buffer.AbstractByteBuf;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.AsciiString;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.concurrent.FastThreadLocalThread;
import io.netty.util.internal.PlatformDependent;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import static io.netty.util.internal.StringUtil.isSurrogate;

/**
 * This is a modified portion of class `io.netty.buffer.ByteBufUtil`
 * from the <a href="https://github.com/netty/netty">netty</a> project.
 */
final class MiByteBufUtil {

    private static final int MAX_TL_ARRAY_LEN = 1024;
    private static final FastThreadLocal<byte[]> BYTE_ARRAYS = new FastThreadLocal<byte[]>() {
        @Override
        protected byte[] initialValue() throws Exception {
            return PlatformDependent.allocateUninitializedArray(MAX_TL_ARRAY_LEN);
        }
    };
    private static final byte WRITE_UTF_UNKNOWN = (byte) '?';
    private static final int WRITE_CHUNK_SIZE = 8192;

    static byte[] threadLocalTempArray(int minLength) {
        // Only make use of ThreadLocal if we use a FastThreadLocalThread to make the implementation
        // Virtual Thread friendly.
        // See https://github.com/netty/netty/issues/14609
        if (minLength <= MAX_TL_ARRAY_LEN && FastThreadLocalThread.currentThreadHasFastThreadLocal()) {
            return BYTE_ARRAYS.get();
        }
        return PlatformDependent.allocateUninitializedArray(minLength);
    }

    static int writeUtf8(AbstractByteBuf buffer, int writerIndex, int reservedBytes, CharSequence seq, int len) {
        return writeUtf8(buffer, writerIndex, reservedBytes, seq, 0, len);
    }

    // Fast-Path implementation
    static int writeUtf8(AbstractByteBuf buffer, int writerIndex, int reservedBytes,
                         CharSequence seq, int start, int end) {
        if (seq instanceof AsciiString) {
            writeAsciiString(buffer, writerIndex, (AsciiString) seq, start, end);
            return end - start;
        }
        if (PlatformDependent.hasUnsafe()) {
            if (buffer.hasArray()) {
                return unsafeWriteUtf8(buffer.array(), PlatformDependent.byteArrayBaseOffset(),
                                       buffer.arrayOffset() + writerIndex, seq, start, end);
            }
            if (buffer.hasMemoryAddress()) {
                return unsafeWriteUtf8(null, buffer.memoryAddress(), writerIndex, seq, start, end);
            }
        } else {
            if (buffer.hasArray()) {
                return safeArrayWriteUtf8(buffer.array(), buffer.arrayOffset() + writerIndex, seq, start, end);
            }
            if (buffer.isDirect() && buffer.nioBufferCount() == 1) {
                final ByteBuffer internalDirectBuffer = buffer.internalNioBuffer(writerIndex, reservedBytes);
                final int bufferPosition = internalDirectBuffer.position();
                return safeDirectWriteUtf8(internalDirectBuffer, bufferPosition, seq, start, end);
            }
        }
        return safeWriteUtf8(buffer, writerIndex, seq, start, end);
    }

    // AsciiString Fast-Path implementation - no explicit bound-checks
    static void writeAsciiString(AbstractByteBuf buffer, int writerIndex, AsciiString seq, int start, int end) {
        final int begin = seq.arrayOffset() + start;
        final int length = end - start;
        if (PlatformDependent.hasUnsafe()) {
            if (buffer.hasArray()) {
                PlatformDependent.copyMemory(seq.array(), begin,
                                             buffer.array(), buffer.arrayOffset() + writerIndex, length);
                return;
            }
            if (buffer.hasMemoryAddress()) {
                PlatformDependent.copyMemory(seq.array(), begin, buffer.memoryAddress() + writerIndex, length);
                return;
            }
        }
        if (buffer.hasArray()) {
            System.arraycopy(seq.array(), begin, buffer.array(), buffer.arrayOffset() + writerIndex, length);
            return;
        }
        buffer.setBytes(writerIndex, seq.array(), begin, length);
    }

    // unsafe Fast-Path implementation
    private static int unsafeWriteUtf8(byte[] buffer, long memoryOffset, int writerIndex,
                                       CharSequence seq, int start, int end) {
        assert !(seq instanceof AsciiString);
        long writerOffset = memoryOffset + writerIndex;
        final long oldWriterOffset = writerOffset;
        for (int i = start; i < end; i++) {
            char c = seq.charAt(i);
            if (c < 0x80) {
                PlatformDependent.putByte(buffer, writerOffset++, (byte) c);
            } else if (c < 0x800) {
                PlatformDependent.putByte(buffer, writerOffset++, (byte) (0xc0 | (c >> 6)));
                PlatformDependent.putByte(buffer, writerOffset++, (byte) (0x80 | (c & 0x3f)));
            } else if (isSurrogate(c)) {
                if (!Character.isHighSurrogate(c)) {
                    PlatformDependent.putByte(buffer, writerOffset++, WRITE_UTF_UNKNOWN);
                    continue;
                }
                // Surrogate Pair consumes 2 characters.
                if (++i == end) {
                    PlatformDependent.putByte(buffer, writerOffset++, WRITE_UTF_UNKNOWN);
                    break;
                }
                char c2 = seq.charAt(i);
                // Extra method is copied here to NOT allow inlining of writeUtf8
                // and increase the chance to inline CharSequence::charAt instead
                if (!Character.isLowSurrogate(c2)) {
                    PlatformDependent.putByte(buffer, writerOffset++, WRITE_UTF_UNKNOWN);
                    PlatformDependent.putByte(buffer, writerOffset++,
                                             (byte) (Character.isHighSurrogate(c2)? WRITE_UTF_UNKNOWN : c2));
                } else {
                    int codePoint = Character.toCodePoint(c, c2);
                    // See https://www.unicode.org/versions/Unicode7.0.0/ch03.pdf#G2630.
                    PlatformDependent.putByte(buffer, writerOffset++, (byte) (0xf0 | (codePoint >> 18)));
                    PlatformDependent.putByte(buffer, writerOffset++, (byte) (0x80 | ((codePoint >> 12) & 0x3f)));
                    PlatformDependent.putByte(buffer, writerOffset++, (byte) (0x80 | ((codePoint >> 6) & 0x3f)));
                    PlatformDependent.putByte(buffer, writerOffset++, (byte) (0x80 | (codePoint & 0x3f)));
                }
            } else {
                PlatformDependent.putByte(buffer, writerOffset++, (byte) (0xe0 | (c >> 12)));
                PlatformDependent.putByte(buffer, writerOffset++, (byte) (0x80 | ((c >> 6) & 0x3f)));
                PlatformDependent.putByte(buffer, writerOffset++, (byte) (0x80 | (c & 0x3f)));
            }
        }
        return (int) (writerOffset - oldWriterOffset);
    }

    // safe byte[] Fast-Path implementation
    private static int safeArrayWriteUtf8(byte[] buffer, int writerIndex, CharSequence seq, int start, int end) {
        int oldWriterIndex = writerIndex;
        for (int i = start; i < end; i++) {
            char c = seq.charAt(i);
            if (c < 0x80) {
                buffer[writerIndex++] = (byte) c;
            } else if (c < 0x800) {
                buffer[writerIndex++] = (byte) (0xc0 | (c >> 6));
                buffer[writerIndex++] = (byte) (0x80 | (c & 0x3f));
            } else if (isSurrogate(c)) {
                if (!Character.isHighSurrogate(c)) {
                    buffer[writerIndex++] = WRITE_UTF_UNKNOWN;
                    continue;
                }
                // Surrogate Pair consumes 2 characters.
                if (++i == end) {
                    buffer[writerIndex++] = WRITE_UTF_UNKNOWN;
                    break;
                }
                char c2 = seq.charAt(i);
                // Extra method is copied here to NOT allow inlining of writeUtf8
                // and increase the chance to inline CharSequence::charAt instead
                if (!Character.isLowSurrogate(c2)) {
                    buffer[writerIndex++] = WRITE_UTF_UNKNOWN;
                    buffer[writerIndex++] = (byte) (Character.isHighSurrogate(c2)? WRITE_UTF_UNKNOWN : c2);
                } else {
                    int codePoint = Character.toCodePoint(c, c2);
                    // See https://www.unicode.org/versions/Unicode7.0.0/ch03.pdf#G2630.
                    buffer[writerIndex++] = (byte) (0xf0 | (codePoint >> 18));
                    buffer[writerIndex++] = (byte) (0x80 | ((codePoint >> 12) & 0x3f));
                    buffer[writerIndex++] = (byte) (0x80 | ((codePoint >> 6) & 0x3f));
                    buffer[writerIndex++] = (byte) (0x80 | (codePoint & 0x3f));
                }
            } else {
                buffer[writerIndex++] = (byte) (0xe0 | (c >> 12));
                buffer[writerIndex++] = (byte) (0x80 | ((c >> 6) & 0x3f));
                buffer[writerIndex++] = (byte) (0x80 | (c & 0x3f));
            }
        }
        return writerIndex - oldWriterIndex;
    }

    // Safe off-heap Fast-Path implementation
    private static int safeDirectWriteUtf8(ByteBuffer buffer, int writerIndex, CharSequence seq, int start, int end) {
        assert !(seq instanceof AsciiString);
        int oldWriterIndex = writerIndex;

        // We can use the _set methods as these not need to do any index checks and reference checks.
        // This is possible as we called ensureWritable(...) before.
        for (int i = start; i < end; i++) {
            char c = seq.charAt(i);
            if (c < 0x80) {
                buffer.put(writerIndex++, (byte) c);
            } else if (c < 0x800) {
                buffer.put(writerIndex++, (byte) (0xc0 | (c >> 6)));
                buffer.put(writerIndex++, (byte) (0x80 | (c & 0x3f)));
            } else if (isSurrogate(c)) {
                if (!Character.isHighSurrogate(c)) {
                    buffer.put(writerIndex++, WRITE_UTF_UNKNOWN);
                    continue;
                }
                // Surrogate Pair consumes 2 characters.
                if (++i == end) {
                    buffer.put(writerIndex++, WRITE_UTF_UNKNOWN);
                    break;
                }
                // Extra method is copied here to NOT allow inlining of writeUtf8
                // and increase the chance to inline CharSequence::charAt instead
                char c2 = seq.charAt(i);
                if (!Character.isLowSurrogate(c2)) {
                    buffer.put(writerIndex++, WRITE_UTF_UNKNOWN);
                    buffer.put(writerIndex++, Character.isHighSurrogate(c2)? WRITE_UTF_UNKNOWN : (byte) c2);
                } else {
                    int codePoint = Character.toCodePoint(c, c2);
                    // See https://www.unicode.org/versions/Unicode7.0.0/ch03.pdf#G2630.
                    buffer.put(writerIndex++, (byte) (0xf0 | (codePoint >> 18)));
                    buffer.put(writerIndex++, (byte) (0x80 | ((codePoint >> 12) & 0x3f)));
                    buffer.put(writerIndex++, (byte) (0x80 | ((codePoint >> 6) & 0x3f)));
                    buffer.put(writerIndex++, (byte) (0x80 | (codePoint & 0x3f)));
                }
            } else {
                buffer.put(writerIndex++, (byte) (0xe0 | (c >> 12)));
                buffer.put(writerIndex++, (byte) (0x80 | ((c >> 6) & 0x3f)));
                buffer.put(writerIndex++, (byte) (0x80 | (c & 0x3f)));
            }
        }
        return writerIndex - oldWriterIndex;
    }

    // Safe off-heap Fast-Path implementation
    private static int safeWriteUtf8(AbstractByteBuf buf, int writerIndex, CharSequence seq, int start, int end) {
        assert !(seq instanceof AsciiString);
        int oldWriterIndex = writerIndex;

        // We can use the _set methods as these not need to do any index checks and reference checks.
        // This is possible as we called ensureWritable(...) before.
        MiByteBufAdapter buffer = (MiByteBufAdapter) buf;
        for (int i = start; i < end; i++) {
            char c = seq.charAt(i);
            if (c < 0x80) {
                buffer._setByte(writerIndex++, (byte) c);
            } else if (c < 0x800) {
                buffer._setByte(writerIndex++, (byte) (0xc0 | (c >> 6)));
                buffer._setByte(writerIndex++, (byte) (0x80 | (c & 0x3f)));
            } else if (isSurrogate(c)) {
                if (!Character.isHighSurrogate(c)) {
                    buffer._setByte(writerIndex++, WRITE_UTF_UNKNOWN);
                    continue;
                }
                // Surrogate Pair consumes 2 characters.
                if (++i == end) {
                    buffer._setByte(writerIndex++, WRITE_UTF_UNKNOWN);
                    break;
                }
                // Extra method is copied here to NOT allow inlining of writeUtf8
                // and increase the chance to inline CharSequence::charAt instead
                char c2 = seq.charAt(i);
                if (!Character.isLowSurrogate(c2)) {
                    buffer._setByte(writerIndex++, WRITE_UTF_UNKNOWN);
                    buffer._setByte(writerIndex++, Character.isHighSurrogate(c2)? WRITE_UTF_UNKNOWN : c2);
                } else {
                    int codePoint = Character.toCodePoint(c, c2);
                    // See https://www.unicode.org/versions/Unicode7.0.0/ch03.pdf#G2630.
                    buffer._setByte(writerIndex++, (byte) (0xf0 | (codePoint >> 18)));
                    buffer._setByte(writerIndex++, (byte) (0x80 | ((codePoint >> 12) & 0x3f)));
                    buffer._setByte(writerIndex++, (byte) (0x80 | ((codePoint >> 6) & 0x3f)));
                    buffer._setByte(writerIndex++, (byte) (0x80 | (codePoint & 0x3f)));
                }
            } else {
                buffer._setByte(writerIndex++, (byte) (0xe0 | (c >> 12)));
                buffer._setByte(writerIndex++, (byte) (0x80 | ((c >> 6) & 0x3f)));
                buffer._setByte(writerIndex++, (byte) (0x80 | (c & 0x3f)));
            }
        }
        return writerIndex - oldWriterIndex;
    }

    static int writeAscii(AbstractByteBuf buffer, int writerIndex, CharSequence seq, int len) {
        if (seq instanceof AsciiString) {
            writeAsciiString(buffer, writerIndex, (AsciiString) seq, 0, len);
        } else {
            writeAsciiCharSequence(buffer, writerIndex, seq, len);
        }
        return len;
    }

    private static int writeAsciiCharSequence(AbstractByteBuf buf, int writerIndex, CharSequence seq, int len) {
        // We can use the _set methods as these not need to do any index checks and reference checks.
        // This is possible as we called ensureWritable(...) before.
        MiByteBufAdapter buffer = (MiByteBufAdapter) buf;
        for (int i = 0; i < len; i++) {
            buffer._setByte(writerIndex++, AsciiString.c2b(seq.charAt(i)));
        }
        return len;
    }

    /**
     * Read bytes from the given {@link ByteBuffer} into the given {@link OutputStream} using the {@code position} and
     * {@code length}. The position and limit of the given {@link ByteBuffer} may be adjusted.
     */
    static void readBytes(ByteBufAllocator allocator, ByteBuffer buffer, int position, int length, OutputStream out)
            throws IOException {
        if (buffer.hasArray()) {
            out.write(buffer.array(), position + buffer.arrayOffset(), length);
        } else {
            int chunkLen = Math.min(length, WRITE_CHUNK_SIZE);
            buffer.clear().position(position).limit(position + length);

            if (length <= MAX_TL_ARRAY_LEN || !allocator.isDirectBufferPooled()) {
                getBytes(buffer, threadLocalTempArray(chunkLen), 0, chunkLen, out, length);
            } else {
                // if direct buffers are pooled chances are good that heap buffers are pooled as well.
                ByteBuf tmpBuf = allocator.heapBuffer(chunkLen);
                try {
                    byte[] tmp = tmpBuf.array();
                    int offset = tmpBuf.arrayOffset();
                    getBytes(buffer, tmp, offset, chunkLen, out, length);
                } finally {
                    tmpBuf.release();
                }
            }
        }
    }

    private static void getBytes(ByteBuffer inBuffer, byte[] in, int inOffset, int inLen, OutputStream out, int outLen)
            throws IOException {
        do {
            int len = Math.min(inLen, outLen);
            inBuffer.get(in, inOffset, len);
            out.write(in, inOffset, len);
            outLen -= len;
        } while (outLen > 0);
    }

    private MiByteBufUtil() { }
}
