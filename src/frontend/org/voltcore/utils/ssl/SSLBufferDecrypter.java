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

public class SSLBufferDecrypter {

    private final SSLEngine m_sslEngine;
    private ByteBuffer m_partialMessage;
    private ByteBuffer m_partialLength;

    public SSLBufferDecrypter(SSLEngine sslEngine) {
        this.m_sslEngine = sslEngine;
    }

    public List<ByteBuffer> decryptBuffer(ByteBuffer chunk) throws IOException {
        // allocate a new dec buffer on each call, return slices of this buffer.
        ByteBuffer decBuffer = ByteBuffer.allocate(SSL_CHUNK_SIZE + 128);
        if (m_partialLength != null) {
            decBuffer.put(m_partialLength);
            m_partialLength = null;
            decBuffer = unwrapBuffer(chunk, decBuffer);
        } else {
            decBuffer = unwrapBuffer(chunk, decBuffer);
        }

        List<ByteBuffer> decryptedMessages = new ArrayList<>();
        while (decBuffer.remaining() > 0) {
            if (m_partialMessage != null) {
                int bytesRemainingInPartial = m_partialMessage.capacity() - m_partialMessage.position();
                if (decBuffer.remaining() >= bytesRemainingInPartial) {
                    int oldLimit = decBuffer.limit();
                    decBuffer.limit(decBuffer.position() + bytesRemainingInPartial);
                    ByteBuffer restOfPartial = decBuffer.slice();
                    m_partialMessage.put(restOfPartial);
                    m_partialMessage.flip();
                    decryptedMessages.add(m_partialMessage);
                    m_partialMessage = null;
                    decBuffer.position(decBuffer.limit());
                    decBuffer.limit(oldLimit);
                } else {
                    m_partialMessage.put(decBuffer);
                    return decryptedMessages;
                }
            }

            // in this case there are leftover bytes in the decryption buffer, but not enough
            // to know the size of the next message.  Save these leftover bytes in the decryption
            // buffer itself.
            if (decBuffer.remaining() > 0 && decBuffer.remaining() < 4) {
                m_partialLength = decBuffer.slice();
                return decryptedMessages;
            }
            if (decBuffer.remaining() >= 4) {
                int messageSize = decBuffer.getInt();
                if (decBuffer.remaining() >= messageSize) {
                    int oldLimit = decBuffer.limit();
                    decBuffer.limit(decBuffer.position() + messageSize);
                    ByteBuffer message = decBuffer.slice();
                    decryptedMessages.add(message);
                    decBuffer.position(decBuffer.limit());
                    decBuffer.limit(oldLimit);
                } else {
                    m_partialMessage = ByteBuffer.allocate(messageSize);
                    m_partialMessage.put(decBuffer);
                    return decryptedMessages;
                }
            }
        }
        // all the bytes in the decryption buffer were processed.
       return decryptedMessages;
    }

    private ByteBuffer unwrapBuffer(ByteBuffer chunk, ByteBuffer decBuffer) throws IOException {
        while (true) {
            ByteBuffer dstBuffer = decBuffer.slice();
            SSLEngineResult result = m_sslEngine.unwrap(chunk, dstBuffer);
            switch (result.getStatus()) {
                case OK:
                    decBuffer.limit(decBuffer.position() + dstBuffer.position());
                    decBuffer.position(0);
                    return decBuffer;
                case BUFFER_OVERFLOW:
                    if (decBuffer.position() > 0) {
                        ByteBuffer bigger = ByteBuffer.allocate(decBuffer.capacity() << 1);
                        decBuffer.flip();
                        bigger.put(decBuffer);
                        decBuffer = bigger;
                    } else {
                        decBuffer = ByteBuffer.allocate(decBuffer.capacity() << 1);
                    }
                    break;  // try again
                case BUFFER_UNDERFLOW:
                    throw new SSLException("SSL engine should never underflow on ssl unwrapBuffer of buffer.");
                case CLOSED:
                    throw new SSLException("SSL engine is closed on ssl unwrapBuffer of buffer.");
            }
        }
    }
}
