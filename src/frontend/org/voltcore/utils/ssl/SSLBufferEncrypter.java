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

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;

import org.voltcore.network.TLSException;
import org.voltcore.utils.DBBPool;

public class SSLBufferEncrypter {

    private final SSLEngine m_sslEngine;
    private final int m_applicationBufferSize;
    private final int m_packetBufferSize;

    public SSLBufferEncrypter(SSLEngine sslEngine, int applicationBufferSize, int packetBufferSize) {
        this.m_sslEngine = sslEngine;
        this.m_applicationBufferSize = applicationBufferSize;
        this.m_packetBufferSize = packetBufferSize;
    }

    public SSLBufferEncrypter(SSLEngine sslEngine) {
        this.m_sslEngine = sslEngine;
        this.m_applicationBufferSize = applicationBufferSize();
        this.m_packetBufferSize = packetBufferSize();
    }

    private int applicationBufferSize() {
        return m_sslEngine.getSession().getApplicationBufferSize();
    }

    private int packetBufferSize() {
        return m_sslEngine.getSession().getPacketBufferSize();
    }

    public DBBPool.BBContainer encryptBuffer(ByteBuffer src) throws IOException {
        // may need to encrypt in more than one step.  Need to have packe buffer size
        // remaining in the dest buffer whenever wrap is called, will overflow otherwise.
        DBBPool.BBContainer dst = null;
        try {
            dst = DBBPool.allocateDirectAndPool(src.remaining() + m_packetBufferSize);
            dst.b().clear();
            int initialSrcLimit = src.limit();
            int i = 0;
            while (src.hasRemaining()) {
                if (src.remaining() > m_applicationBufferSize) {
                    src.limit(src.position() + m_applicationBufferSize);
                }
                SSLEngineResult result = null;
                try {
                    result = m_sslEngine.wrap(src.slice(), dst.b().slice());
                } catch (ReadOnlyBufferException|IllegalArgumentException|IllegalStateException e) {
                    throw new IOException("ssl engine wrap fault", e);
                }
                switch (result.getStatus()) {
                    case OK:
                        src.position(src.position() + result.bytesConsumed());
                        src.limit(initialSrcLimit);
                        dst.b().position(dst.b().position() + result.bytesProduced());
                        break;
                    case BUFFER_OVERFLOW:
                        throw new IOException("Overflow on ssl wrap of buffer");
                    case BUFFER_UNDERFLOW:
                        throw new IOException("Underflow on ssl wrap of buffer.");
                    case CLOSED:
                        throw new IOException("SSL engine is closed on ssl wrap of buffer.");
                    default:
                        throw new IOException("Unexpected SSLEngineResult.Status");
                }
            }
        } catch (IOException ioe) {
            if (dst != null) dst.discard();
            throw ioe;
        }
        dst.b().flip();
        return dst;
    }

    public void tlswrap(ByteBuffer srcBuffer, ByteBuffer dstBuffer)  {
        while (true) {
            SSLEngineResult result = null;
            try {
                result = m_sslEngine.wrap(srcBuffer, dstBuffer.slice());
            } catch (SSLException|ReadOnlyBufferException|IllegalArgumentException|IllegalStateException e) {
                throw new TLSException("ssl engine wrap fault", e);
            }
            switch (result.getStatus()) {
                case OK:
                    // in m_dstBuffer, newly decrtyped data is between pos and lim
                    if (result.bytesProduced() > 0) {
                        dstBuffer.limit(dstBuffer.position() + result.bytesProduced());
                        return;
                        }
                    else {
                        continue;
                    }
                case BUFFER_OVERFLOW:
                    throw new TLSException("SSL engine unexpectedly overflowed when enrypting");
                case BUFFER_UNDERFLOW:
                    throw new TLSException("SSL engine unexpectedly underflowed when encrypting");
                case CLOSED:
                    throw new TLSException("SSL engine is closed on ssl wrap of buffer.");
            }
        }
    }

}
