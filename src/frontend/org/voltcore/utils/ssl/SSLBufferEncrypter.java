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

import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;

import org.voltcore.network.TLSException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

public class SSLBufferEncrypter {

    private final SSLEngine m_sslEngine;

    public SSLBufferEncrypter(SSLEngine sslEngine) {
        this.m_sslEngine = sslEngine;
    }

    public ByteBuf tlswrap(ByteBuffer src, ByteBufAllocator allocator) {
        ByteBuf encrypted = tlswrap(Unpooled.wrappedBuffer(src), allocator);
        src.position(src.limit());
        return encrypted;
    }

    public ByteBuf tlswrap(ByteBuf src, ByteBufAllocator allocator) {
        SSLSession session = m_sslEngine.getSession();
        int packetBufferSize = session.getPacketBufferSize();

        CompositeByteBuf fullyEncrypted = null;
        ByteBuf piece = null;

        try {
            do {
                piece = allocator.buffer(packetBufferSize);
                assert piece.nioBufferCount() == 1 : "Should only have one buffer: " + piece.nioBufferCount();
                ByteBuffer destNioBuf = piece.nioBuffer(0, piece.writableBytes());

                ByteBuffer[] srcNioBuffers = src.nioBuffers();

                SSLEngineResult result = null;
                try {
                    result = m_sslEngine.wrap(srcNioBuffers, destNioBuf);
                } catch (SSLException | ReadOnlyBufferException | IllegalArgumentException | IllegalStateException e) {
                    throw new TLSException("ssl engine wrap fault", e);
                }
                switch (result.getStatus()) {
                case OK:
                    src.readerIndex(src.readerIndex() + result.bytesConsumed());
                    break;
                case BUFFER_OVERFLOW:
                    throw new TLSException("SSL engine unexpectedly overflowed when encrypting");
                case BUFFER_UNDERFLOW:
                    throw new TLSException("SSL engine unexpectedly underflowed when encrypting");
                case CLOSED:
                    throw new TLSException("SSL engine is closed on ssl wrap of buffer.");
                }
                piece.writerIndex(destNioBuf.position());

                if (fullyEncrypted == null) {
                    if (!src.isReadable()) {
                        // It all fit in one buffer just return that
                        return piece;
                    }
                    // Avoid buffer compaction and use a large maxNumComponents
                    fullyEncrypted = allocator.compositeBuffer(1024);
                }
                fullyEncrypted.addComponent(true, piece);
                piece = null;
            } while (src.isReadable());

            return fullyEncrypted;
        } catch (Throwable t) {
            if (piece != null) {
                piece.release();
            }
            fullyEncrypted.release();
            throw t;
        }
    }
}
