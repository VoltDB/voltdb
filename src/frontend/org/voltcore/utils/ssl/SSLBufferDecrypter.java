/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltcore.utils.ssl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.nio.channels.ScatteringByteChannel;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;

import org.voltcore.network.TLSException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

public class SSLBufferDecrypter {
    public final static int TLS_HEADER_SIZE = 5;

    private final SSLEngine m_sslEngine;

    public SSLBufferDecrypter(SSLEngine sslEngine) {
        this.m_sslEngine = sslEngine;
    }

    public static boolean readTLSFrame(ScatteringByteChannel chn, ByteBuf buf) throws IOException {
        int widx = buf.writerIndex();
        ByteBuf header = buf.slice(widx, TLS_HEADER_SIZE).clear();
        int rc = 0;
        do {
            rc = header.writeBytes(chn, header.writableBytes());
        } while(rc > 0 && header.isWritable());
        if (rc < 0) {
            throw new IOException("channel closed while reading tls frame header");
        }
        if (rc == 0) {
            return false;
        }

        int framesz = header.getShort(3);
        if (framesz+header.capacity() > buf.writableBytes()) {
            throw new IOException("destination buffer is too small to contain the whole frame");
        }
        buf.writerIndex(buf.writerIndex() + header.writerIndex());

        ByteBuf frame = buf.slice(buf.writerIndex(), framesz).clear();
        while (frame.isWritable()) {
            if (frame.writeBytes(chn, frame.writableBytes()) < 0) {
                throw new IOException("channel closed while reading tls frame header");
            }
        }
        buf.writerIndex(buf.writerIndex()+framesz);
        return true;
    }

    public int getPacketBufferSize() {
        return m_sslEngine.getSession().getPacketBufferSize();
    }

    /**
     * @see #tlsunwrap(ByteBuffer, ByteBuf, PooledByteBufAllocator)
     */
    public ByteBuf tlsunwrap(ByteBuffer srcBuffer, PooledByteBufAllocator allocator) {
        int size = m_sslEngine.getSession().getApplicationBufferSize();
        return tlsunwrap(srcBuffer, allocator.buffer(size), allocator);
    }

    /**
     * Encrypt data in {@code srcBuffer} into {@code dstBuf} if it is large enough. If an error occurs {@code dstBuf}
     * will be released. If {@code dstBuf} is not large enough a new buffer will be allocated from {@code allocator} and
     * {@code dstBuf} will be released.
     *
     * @param srcBuffer holding encrypted data
     * @param dstBuf    to attempt to write plain text data into
     * @param allocator to be used to allocate new buffers if {@code dstBuf} is too small
     * @return {@link ByteBuf} containing plain text data
     */
    public ByteBuf tlsunwrap(ByteBuffer srcBuffer, ByteBuf dstBuf, PooledByteBufAllocator allocator) {
        int writerIndex = dstBuf.writerIndex();
        ByteBuffer byteBuffer = dstBuf.nioBuffer(writerIndex, dstBuf.writableBytes());

        while (true) {
            SSLEngineResult result;
            try {
                result = m_sslEngine.unwrap(srcBuffer, byteBuffer);
            } catch (SSLException | ReadOnlyBufferException | IllegalArgumentException | IllegalStateException e) {
                dstBuf.release();
                throw new TLSException("ssl engine unwrap fault", e);
            } catch (Throwable t) {
                dstBuf.release();
                throw t;
            }

            switch (result.getStatus()) {
            case OK:
                int bytesProduced = result.bytesProduced();
                if (bytesProduced > 0) {
                    writerIndex += result.bytesProduced();
                }
                if (bytesProduced <= 0 || srcBuffer.hasRemaining()) {
                    continue;
                }
                dstBuf.writerIndex(writerIndex);
                return dstBuf;
            case BUFFER_OVERFLOW:
                dstBuf.release();
                if (m_sslEngine.getSession().getApplicationBufferSize() > dstBuf.writableBytes()) {
                    int size = m_sslEngine.getSession().getApplicationBufferSize();
                    dstBuf = allocator.buffer(size);
                    writerIndex = dstBuf.writerIndex();
                    byteBuffer = dstBuf.nioBuffer(writerIndex, dstBuf.writableBytes());
                    continue;
                }
                throw new TLSException("SSL engine unexpectedly overflowed when decrypting");
            case BUFFER_UNDERFLOW:
                if (srcBuffer.hasRemaining()) {
                    dstBuf.release();
                    throw new TLSException("SSL engine unexpectedly underflowed when decrypting");
                }
                return dstBuf;
            case CLOSED:
                dstBuf.release();
                throw new TLSException("SSL engine is closed on ssl unwrap of buffer.");
            }
        }
    }
}
