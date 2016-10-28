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
import java.util.ArrayList;
import java.util.List;

import static org.voltdb.common.Constants.SSL_CHUNK_SIZE;

public class SSLMessageDecrypter {

    private final SSLEngine m_sslEngine;
    private ByteBuffer m_decBuffer;
    private ByteBuffer m_partialMessage;

    public SSLMessageDecrypter(SSLEngine sslEngine) {
        this.m_sslEngine = sslEngine;
        this.m_decBuffer = ByteBuffer.allocate(SSL_CHUNK_SIZE + 128);
    }

    public List<ByteBuffer> decryptMessages(ByteBuffer chunk) throws IOException {
        unwrap(chunk);
        List<ByteBuffer> decryptedMessages = new ArrayList<>();
        while (m_decBuffer.remaining() > 0) {
            if (m_partialMessage != null) {
                int bytesRemainingInPartial = m_partialMessage.capacity() - m_partialMessage.position();
                if (m_decBuffer.remaining() >= bytesRemainingInPartial) {
                    int oldLimit = m_decBuffer.limit();
                    m_decBuffer.limit(m_decBuffer.position() + bytesRemainingInPartial);
                    ByteBuffer restOfPartial = m_decBuffer.slice();
                    m_partialMessage.put(restOfPartial);
                    m_partialMessage.flip();
                    decryptedMessages.add(m_partialMessage);
                    m_partialMessage = null;
                    m_decBuffer.position(m_decBuffer.limit());
                    m_decBuffer.limit(oldLimit);
                } else {
                    m_partialMessage.put(m_decBuffer);
                    // there's nothing left in the dec buffer.
                    m_decBuffer.clear();
                    return decryptedMessages;
                }
            }

            // in this case there are leftover bytes in the decryption buffer, but not enough
            // to know the size of the next message.  Save these leftover bytes in the decryption
            // buffer itself.
            if (m_decBuffer.remaining() > 0 && m_decBuffer.remaining() < 4) {
                ByteBuffer leftover = m_decBuffer.slice();
                m_decBuffer.clear();
                m_decBuffer.put(leftover);
                return decryptedMessages;
            }
            if (m_decBuffer.remaining() >= 4) {
                int messageSize = m_decBuffer.getInt();
                m_partialMessage = ByteBuffer.allocate(messageSize);
                if (m_decBuffer.remaining() >= messageSize) {
                    int oldLimit = m_decBuffer.limit();
                    m_decBuffer.limit(m_decBuffer.position() + messageSize);
                    m_partialMessage.put(m_decBuffer.slice());
                    m_partialMessage.flip();
                    decryptedMessages.add(m_partialMessage);
                    m_partialMessage = null;
                    m_decBuffer.position(m_decBuffer.limit());
                    m_decBuffer.limit(oldLimit);
                } else {
                    m_partialMessage.put(m_decBuffer);
                }
            }
        }
        // all the bytes in the decryption buffer were processed.
        m_decBuffer.clear();
        return decryptedMessages;
    }

    private void unwrap(ByteBuffer chunk) throws IOException {
        while (true) {
            ByteBuffer decBuffer;
            boolean hadLeftover = false;
            if (m_decBuffer.position() > 0) {
                decBuffer = m_decBuffer.slice();
                hadLeftover = true;
            } else {
                decBuffer = m_decBuffer;
            }
            SSLEngineResult result = m_sslEngine.unwrap(chunk, decBuffer);
            switch (result.getStatus()) {
                case OK:
                    if (hadLeftover) {
                        m_decBuffer.limit(m_decBuffer.position() + decBuffer.position());
                        m_decBuffer.position(0);
                    } else {
                        m_decBuffer.flip();
                    }
                    return;
                case BUFFER_OVERFLOW:
                    if (m_decBuffer.position() > 0) {
                        ByteBuffer bigger = ByteBuffer.allocate(m_decBuffer.capacity() << 1);
                        m_decBuffer.flip();
                        bigger.put(m_decBuffer);
                        m_decBuffer = bigger;
                    }
                    m_decBuffer = ByteBuffer.allocate(m_decBuffer.capacity() << 1);
                    break;  // try again
                case BUFFER_UNDERFLOW:
                    throw new SSLException("SSL engine should never underflow on ssl unwrap of buffer.");
                case CLOSED:
                    throw new SSLException("SSL engine is closed on ssl unwrap of buffer.");
            }
        }
    }
}
