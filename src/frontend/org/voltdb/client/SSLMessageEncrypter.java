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

package org.voltdb.client;

import org.voltcore.network.VoltPort;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

// reuses the same chunk buffer.  So, when iterating, need to consume the result
// of next() before calling next() again.
public class SSLMessageEncrypter implements Iterator<ByteBuffer> {

    private final SSLEngine m_sslEngine;
    private  ByteBuffer m_encBuffer;

    private ByteBuffer m_message;

    public SSLMessageEncrypter(SSLEngine engine) {
        m_sslEngine = engine;
        m_encBuffer = ByteBuffer.allocate((int) (VoltPort.SSL_CHUNK_SIZE * 1.2));
    }

    public void setMessage(ByteBuffer message) {
        m_message = message;
    }

    @Override
    public boolean hasNext() {
        return (m_message != null) && (m_message.remaining() > 0);
    }

    @Override
    public ByteBuffer next() {
        ByteBuffer messageChunk = ByteBuffer.allocate(VoltPort.SSL_CHUNK_SIZE);
        if (m_message == null) {
            throw new IllegalStateException("no message");
        }
        if (m_message.remaining() < VoltPort.SSL_CHUNK_SIZE) {
            try {
                ByteBuffer encChunk = encryptChunk(m_message);
                m_message = null;  // done with this message
                return encChunk;
            } catch (IOException e) {
                return null;
            }
        } else {
            if (m_message.remaining() > VoltPort.SSL_CHUNK_SIZE) {
                m_message.get(messageChunk.array(), 0, VoltPort.SSL_CHUNK_SIZE);
                try {
                    return encryptChunk(messageChunk);
                } catch (IOException e) {
                    return null;
                }
            } else {
                messageChunk.put(m_message);
                messageChunk.flip();
                try {
                    ByteBuffer encChunk = encryptChunk(m_message);
                    m_message = null;  // done with this message
                    return encChunk;
                } catch (IOException e) {
                    return null;
                }
            }
        }
    }

    private ByteBuffer encryptChunk(ByteBuffer messageChunk) throws IOException {
        m_encBuffer.clear();
        encryptBuffer(messageChunk);
        ByteBuffer encryptedChunk = ByteBuffer.allocate(m_encBuffer.remaining() + 4);
        encryptedChunk.putInt(m_encBuffer.remaining());
        encryptedChunk.put(m_encBuffer);
        encryptedChunk.flip();
        return encryptedChunk;
    }

    private void encryptBuffer(ByteBuffer unwrapped) throws IOException {
        while (true) {
            SSLEngineResult result = m_sslEngine.wrap(unwrapped, m_encBuffer);
            switch (result.getStatus()) {
                case OK:
                    m_encBuffer.flip();
                    return;
                case BUFFER_OVERFLOW:
                    m_encBuffer = ByteBuffer.allocate(m_encBuffer.capacity() * 2);
                    break;  // try again
                case BUFFER_UNDERFLOW:
                    throw new IOException("Underflow on ssl wrap of buffer.");
                case CLOSED:
                    throw new IOException("SSL engine is closed on ssl wrap of buffer.");
            }
        }
    }

    @Override
    public void remove() {
        throw new IllegalStateException("remove is not implemented");
    }
}
