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
import java.nio.channels.SocketChannel;

import javax.net.ssl.SSLEngine;

import org.voltcore.network.CipherExecutor;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;

public class TLSMessagingChannel extends MessagingChannel {
    private final SSLEngine m_engine;
    private final CipherExecutor m_ce;
    private final SSLBufferDecrypter m_decrypter;
    private final SSLBufferEncrypter m_encrypter;

    public TLSMessagingChannel(SocketChannel socketChannel, SSLEngine engine) {
        super(socketChannel);
        m_engine = engine;
        m_ce = CipherExecutor.valueOf(engine);
        m_decrypter = new SSLBufferDecrypter(m_engine);
        m_encrypter = new SSLBufferEncrypter(m_engine);
    }

    /**
     * this values may change if a TLS session renegotiates its cipher suite
     */
    private int packetBufferSize() {
        return m_engine.getSession().getPacketBufferSize();
    }

    /**
     * this values may change if a TLS session renegotiates its cipher suite
     */
    private int applicationBufferSize() {
        return m_engine.getSession().getApplicationBufferSize();
    }

    private final static int NOT_AVAILABLE = -1;

    private int validateLength(int sz) throws IOException {
        if (sz < 1 || sz > (1<<25)) {
            throw new IOException("Invalid message length header value: " + sz
                    + ". It must be between 1 and " + (1<<25));
        }
        return sz;
    }

    /**
     * Reads the specified number of clear bytes and returns a ByteBuffer with the clear bytes.
     * If <code>numBytes</code> is <code>NOT_AVAILABLE</code>, it expects the first 4 clear bytes to
     * contain the number of bytes that should be read and reads accordingly.
     */
    @Override
    public ByteBuffer readBytes(int numBytes) throws IOException {
        final int appsz = applicationBufferSize();
        ByteBuf readbuf = m_ce.allocator().ioBuffer(packetBufferSize());
        CompositeByteBuf msgbb = Unpooled.compositeBuffer();

        try {
            ByteBuf clear = doUnwrap(readbuf, appsz);

            msgbb.addComponent(true, clear);
            int needed = numBytes;
            if (numBytes == NOT_AVAILABLE) {
                needed = msgbb.readableBytes() >= 4 ? validateLength(msgbb.readInt()) : NOT_AVAILABLE;
            }
            while (msgbb.readableBytes() < (needed == NOT_AVAILABLE ? 4 : needed)) {
                clear = doUnwrap(readbuf, appsz);

                msgbb.addComponent(true, clear);

                if (needed == NOT_AVAILABLE && msgbb.readableBytes() >= 4) {
                    needed = validateLength(msgbb.readInt());
                }
            }

            ByteBuffer retbb = ByteBuffer.allocate(needed);
            msgbb.readBytes(retbb);
            msgbb.discardReadComponents();

            assert !msgbb.isReadable() : "read from unblocked channel that received multiple messages?";

            return (ByteBuffer)retbb.flip();
        } finally {
            readbuf.release();
            msgbb.release();
        }
    }

    private ByteBuf doUnwrap(ByteBuf readbuf, int appsz) throws IOException {
        PooledByteBufAllocator allocator = m_ce.allocator();
        ByteBuf clear = allocator.buffer(appsz);
        ByteBuffer src;
        do {
            readbuf.clear();
            if (!SSLBufferDecrypter.readTLSFrame(m_socketChannel, readbuf)) {
                return null;
            }
            src = readbuf.nioBuffer();
        } while (!(clear = m_decrypter.tlsunwrap(src, clear, allocator)).isReadable());
        return clear;
    }

    @Override
    public ByteBuffer readMessage() throws IOException {
        return readBytes(NOT_AVAILABLE);
    }

    @Override
    public int writeMessage(ByteBuffer message) throws IOException {
        if (!message.hasRemaining()) {
            return 0;
        }

        int bytesWritten = 0;
        ByteBuf outputBuf = m_encrypter.tlswrap(message, m_ce.allocator());
        try {
            while (outputBuf.isReadable()) {
                bytesWritten += outputBuf.readBytes(m_socketChannel, outputBuf.readableBytes());
            }
        } finally {
            outputBuf.release();
        }

        return bytesWritten;
    }
}
