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
import java.nio.ReadOnlyBufferException;
import java.nio.channels.ScatteringByteChannel;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;

import org.voltcore.network.TLSException;

import io.netty_voltpatches.buffer.ByteBuf;

public class SSLBufferDecrypter {
    public final static int TLS_HEADER_SIZE = 5;

    private final SSLEngine m_sslEngine;

    public SSLBufferDecrypter(SSLEngine sslEngine) {
        this.m_sslEngine = sslEngine;
    }

    public boolean readTLSFrame(ScatteringByteChannel chn, ByteBuf buf) throws IOException {
        int widx = buf.writerIndex();
        ByteBuf header = buf.slice(widx, TLS_HEADER_SIZE).clear();
        int rc = 0;
        do {
            rc = header.writeBytes(chn, header.writableBytes());
        } while(rc > 0 && header.isWritable());
        if (rc < 0) {
            throw new IOException("channel closed while reading tls frame header");
        }
        if (rc == 0) return false;

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

    public int tlsunwrap(ByteBuffer srcBuffer, ByteBuffer dstBuffer) {
        while (true) {
            SSLEngineResult result = null;
            ByteBuffer slice = dstBuffer.slice();
            try {
                result = m_sslEngine.unwrap(srcBuffer, slice);
            } catch (SSLException|ReadOnlyBufferException|IllegalArgumentException|IllegalStateException e) {
                throw new TLSException("ssl engine unwrap fault", e);
            }
            switch (result.getStatus()) {
                case OK:
                    if (result.bytesProduced() == 0 && !srcBuffer.hasRemaining()) {
                        return 0;
                    }
                    // in m_dstBuffer, newly decrtyped data is between pos and lim
                    if (result.bytesProduced() > 0) {
                        dstBuffer.limit(dstBuffer.position() + result.bytesProduced());
                        return result.bytesProduced();
                        }
                    else {
                        continue;
                    }
                case BUFFER_OVERFLOW:
                    throw new TLSException("SSL engine unexpectedly overflowed when decrypting");
                case BUFFER_UNDERFLOW:
                    throw new TLSException("SSL engine unexpectedly underflowed when decrypting");
                case CLOSED:
                    throw new TLSException("SSL engine is closed on ssl unwrap of buffer.");
            }
        }
    }
}
