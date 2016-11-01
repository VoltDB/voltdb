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
import javax.net.ssl.SSLException;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.voltdb.common.Constants.SSL_CHUNK_SIZE;

/**
 * Doesn't support messages bigger then SSL_CHUNK_SIZE
 */
public class SSLMessageDecrypter {

    private final SSLEngine m_sslEngine;
    private ByteBuffer m_decBuffer;

    public SSLMessageDecrypter(SSLEngine m_sslEngine) {
        this.m_sslEngine = m_sslEngine;
        this.m_decBuffer = ByteBuffer.allocate(SSL_CHUNK_SIZE + 128);
    }

    public ByteBuffer decryptMessage(ByteBuffer message) throws IOException {
        m_decBuffer.clear();
        unwrapMessage(message);
        int messageLength = m_decBuffer.getInt();
        if (messageLength != m_decBuffer.remaining()) {
            throw new IOException("malformed ssl message, failed to decrypt");
        }
        ByteBuffer clearMessage = ByteBuffer.allocate(messageLength);
        clearMessage.put(m_decBuffer);
        clearMessage.flip();
        return clearMessage;
    }

    private void unwrapMessage(ByteBuffer message) throws IOException {
        while (true) {
            SSLEngineResult result = m_sslEngine.unwrap(message, m_decBuffer);
            switch (result.getStatus()) {
                case OK:
                    m_decBuffer.flip();
                    return;
                case BUFFER_OVERFLOW:
                    m_decBuffer = ByteBuffer.allocate(m_decBuffer.capacity() << 1);
                    break;  // try again
                case BUFFER_UNDERFLOW:
                    throw new SSLException("SSL engine should never underflow on ssl unwrapMessage of buffer.");
                case CLOSED:
                    throw new SSLException("SSL engine is closed on ssl unwrapMessage of buffer.");
            }
        }
    }
}
