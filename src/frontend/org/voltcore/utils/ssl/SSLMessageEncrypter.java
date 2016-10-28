/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import org.voltdb.common.Constants;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class SSLMessageEncrypter {

    private final SSLEngine m_sslEngine;
    private ByteBuffer m_sslDst;

    public SSLMessageEncrypter(SSLEngine sslEngine) {
        this.m_sslEngine = sslEngine;
        this.m_sslDst = ByteBuffer.allocate(Constants.SSL_CHUNK_SIZE + 128);
    }

    public List<ByteBuffer> encryptBuffer(ByteBuffer src) throws IOException {

        List<ByteBuffer> encryptedBuffers = new ArrayList<>();
        while (src.remaining() > 0) {
            if (src.remaining() < Constants.SSL_CHUNK_SIZE) {
                ByteBuffer chunk = src.slice();
                wrap(chunk);
                ByteBuffer encyptedChunk = ByteBuffer.allocate(m_sslDst.remaining() + 4);
                encyptedChunk.putInt(m_sslDst.remaining());
                encyptedChunk.put(m_sslDst);
                encyptedChunk.flip();
                encryptedBuffers.add(encyptedChunk);
                src.position(src.limit());
            } else {
                int oldLimit = src.limit();
                int nextPosition = src.position() + Constants.SSL_CHUNK_SIZE;
                src.limit(nextPosition);
                ByteBuffer chunk = src.slice();
                wrap(chunk);
                ByteBuffer encyptedChunk = ByteBuffer.allocate(m_sslDst.remaining() + 4);
                encyptedChunk.putInt(m_sslDst.remaining());
                encyptedChunk.put(m_sslDst);
                encyptedChunk.flip();
                encryptedBuffers.add(encyptedChunk);
                src.position(nextPosition);
                src.limit(oldLimit);
            }
        }
        return encryptedBuffers;
    }

    private void wrap(ByteBuffer src) throws IOException {
        m_sslDst.clear();
        while (true) {
            SSLEngineResult result = m_sslEngine.wrap(src, m_sslDst);
            switch (result.getStatus()) {
                case OK:
                    m_sslDst.flip();
                    return;
                case BUFFER_OVERFLOW:
                    m_sslDst = ByteBuffer.allocate(m_sslDst.capacity() << 1);
                    break;
                case BUFFER_UNDERFLOW:
                    throw new IOException("Underflow on ssl wrap of buffer.");
                case CLOSED:
                    throw new IOException("SSL engine is closed on ssl wrap of buffer.");
                default:
                    throw new IOException("Unexpected SSLEngineResult.Status");
            }
        }
    }
}
