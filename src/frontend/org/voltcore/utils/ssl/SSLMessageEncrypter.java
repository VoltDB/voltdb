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

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import java.io.IOException;
import java.nio.ByteBuffer;

// doesn't support messages greater than SSL_CHUNK_SIZE.
public class SSLMessageEncrypter {

    private final SSLEngine m_sslEngine;
    private ByteBuffer m_encBuffer;

    public SSLMessageEncrypter(SSLEngine m_sslEngine) {
        this.m_sslEngine = m_sslEngine;
        int packetBufferSize = m_sslEngine.getSession().getPacketBufferSize();
        // wrap will overflow until the encryption buffer is this size.
        this.m_encBuffer = ByteBuffer.allocate(packetBufferSize);
    }

    public ByteBuffer encryptMessage(ByteBuffer message) throws IOException {
        m_encBuffer.clear();
        wrap(message);
        ByteBuffer encMessage = ByteBuffer.allocate(m_encBuffer.remaining() + 4);
        encMessage.putInt(m_encBuffer.remaining());
        encMessage.put(m_encBuffer);
        encMessage.flip();
       return encMessage;
    }

    private void wrap(ByteBuffer chunk) throws IOException {
        while (true) {
            SSLEngineResult result = m_sslEngine.wrap(chunk, m_encBuffer);
            switch (result.getStatus()) {
                case OK:
                    m_encBuffer.flip();
                    return;
                case BUFFER_OVERFLOW:
                    m_encBuffer = ByteBuffer.allocate(m_encBuffer.capacity() << 1);
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
