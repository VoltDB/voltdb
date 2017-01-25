/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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
import org.voltcore.network.TLSException;

import io.netty_voltpatches.buffer.ByteBuf;
import io.netty_voltpatches.buffer.CompositeByteBuf;
import io.netty_voltpatches.buffer.Unpooled;

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

    private int packetBufferSize() {
        return m_engine.getSession().getPacketBufferSize();
    }

    private int applicationBufferSize() {
        return m_engine.getSession().getApplicationBufferSize();
    }

    @Override
    public ByteBuffer readMessage() throws IOException {
        ByteBuf header = Unpooled.wrappedBuffer(new byte[5]);
        ByteBuffer hdrjbb = header.nioBuffer();

        int rc = 0;
        do {
            rc = m_socketChannel.read(hdrjbb);
        } while(rc > 0 && hdrjbb.hasRemaining());
        if (rc < 0) {
            throw new IOException("failed to read tls frame header");
        } else if (rc == 0) {
            return null;
        }

        int framesz = header.getShort(3);
        if (framesz <= 0) {
            throw new IOException("unexpected tls frame size " + framesz);
        }

        ByteBuf readbuf = m_ce.allocator().ioBuffer(packetBufferSize());
        ByteBuf frame = readbuf.slice(0, framesz+header.capacity()).clear();
        header.readBytes(frame, header.readableBytes());
        try {
            while (frame.isWritable()) {
                if (frame.writeBytes(m_socketChannel, frame.writableBytes()) < 0) {
                    throw new IOException("failed to read tls frame");
                }
            }
        } catch (IOException e) {
            readbuf.release();
            throw e;
        }
        final int appsz = applicationBufferSize();
        ByteBuf clear = m_ce.allocator().buffer(appsz).writerIndex(appsz);
        ByteBuffer src = frame.nioBuffer();
        ByteBuffer dst = clear.nioBuffer();
        try {
            m_decrypter.tlsunwrap(src, dst);
            clear.writerIndex(dst.limit());
            assert !src.hasRemaining() : "decrypter did not wholly consume the source buffer";
        } catch (TLSException e) {
            readbuf.release();
            clear.release();
            throw new IOException("failed to decrypt tls frame", e);
        }
        final int msgsz = clear.readInt();

        ByteBuf message = Unpooled.wrappedBuffer(new byte [msgsz]).clear();
        clear.readBytes(message, clear.readableBytes());
        clear.release();
        final int needed = CipherExecutor.framesFor(msgsz);
        try {
            for (int have = 1; have < needed; ++ have) {
                header.clear();
                while (header.isWritable()) {
                    if (header.writeBytes(m_socketChannel, header.writableBytes()) < 0) {
                        throw new IOException("failed to read tls frame header");
                    }
                }
                framesz = header.getShort(3);
                if (framesz <= 0) {
                    throw new IOException("inexpected tls frame size " + framesz);
                }
                frame = readbuf.slice(0, framesz+header.capacity()).clear();
                header.readBytes(frame, header.readableBytes());
                while (frame.isWritable()) {
                    if (frame.writeBytes(m_socketChannel, frame.writableBytes()) < 0) {
                        throw new IOException("failed to read tls frame");
                    }
                }
                int clearsz = Math.min(CipherExecutor.PAGE_SIZE, message.readableBytes());
                clear = message.slice(message.writerIndex(), clearsz).writerIndex(clearsz);
                src = frame.nioBuffer();
                dst = clear.nioBuffer();
                try {
                    m_decrypter.tlsunwrap(src, dst);
                } catch (TLSException e) {
                    throw new IOException("failed to decrypt tls frame", e);
                }
                assert !src.hasRemaining() : "decrypter did not wholly consume the source buffer";
                message.writerIndex(message.writerIndex()+dst.limit());
            }
        } catch (IOException e) {
            readbuf.release();
            throw e;
        }
        readbuf.release();
        assert message.readableBytes() == msgsz : "failed to compose a whole message from expected tls frames";
        return message.nioBuffer();
    }

    @Override
    public int writeMessage(ByteBuffer message) throws IOException {
        if (!message.hasRemaining()) {
            return 0;
        }

        CompositeByteBuf outbuf = Unpooled.compositeBuffer();
        ByteBuf msg = Unpooled.wrappedBuffer(message);
        final int needed = CipherExecutor.framesFor(msg.readableBytes());
        for (int have = 0; have < needed; ++have) {
            final int slicesz = Math.min(CipherExecutor.PAGE_SIZE, msg.readableBytes());
            ByteBuf clear = msg.readSlice(slicesz).writerIndex(slicesz);
            ByteBuf encr = m_ce.allocator().ioBuffer(packetBufferSize()).writerIndex(packetBufferSize());
            ByteBuffer src = clear.nioBuffer();
            ByteBuffer dst = encr.nioBuffer();
            try {
                m_encrypter.tlswrap(src, dst);
            } catch (TLSException e) {
                outbuf.release();
                encr.release();
                throw new IOException("failed to encrypt tls frame", e);
            }
            assert !src.hasRemaining() : "encryption wrap did not consume the whole source buffer";
            encr.writerIndex(dst.limit());
            outbuf.addComponent(true, encr);
        }
        int bytesWritten = 0;
        try {
            while (outbuf.isReadable()) {
                bytesWritten += outbuf.readBytes(m_socketChannel, outbuf.readableBytes());
            }
        } catch (IOException e) {
            outbuf.release();
            throw e;
        }

        outbuf.release();
        message.position(message.position() + msg.readerIndex());
        return bytesWritten;
    }
}
