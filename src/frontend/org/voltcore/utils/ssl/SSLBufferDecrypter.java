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
import javax.net.ssl.SSLException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class SSLBufferDecrypter {

    private final SSLEngine m_sslEngine;
    private final ByteBuffer m_srcBuffer;
    private final ByteBuffer m_dstBuffer;
    private ByteBuffer m_partialMessage;
    private ByteBuffer m_partialLength;

    public SSLBufferDecrypter(SSLEngine sslEngine) {
        this.m_sslEngine = sslEngine;
        this.m_srcBuffer = ByteBuffer.allocateDirect(Constants.SSL_CHUNK_SIZE + 128);
        this.m_dstBuffer = ByteBuffer.allocateDirect(Constants.SSL_CHUNK_SIZE + 128);
        this.m_partialLength = ByteBuffer.allocate(4);
    }

    /**
     * Returns the read buffer.
     * @return  The read buffer.
     */
    public ByteBuffer getSrcBuffer() {
        return m_srcBuffer;
    }

    public List<ByteBuffer> decrypt() throws IOException {

        List<ByteBuffer> messages = new ArrayList<>();
        if (m_srcBuffer.position() > 0) {
            m_srcBuffer.flip();
            m_dstBuffer.clear();
            ByteBuffer chunk = unwrapBuffer(m_srcBuffer, m_dstBuffer);
            if (chunk != null) {
                while (chunk.hasRemaining()) {
                    if (m_partialLength.position() != 0) {
                        if (chunk.remaining() >= (m_partialLength.remaining())) {
                            int oldLimit = chunk.limit();
                            chunk.limit(chunk.position() + m_partialLength.remaining());
                            m_partialLength.put(chunk);
                            chunk.limit(oldLimit);
                            m_partialLength.flip();
                            int messageLength = m_partialLength.getInt();
                            m_partialLength.clear();
                            // m_partial message gets filled in next time through the loop.
                            m_partialMessage = ByteBuffer.allocate(messageLength);
                        } else {
                            // net enough remaining in chunk to finish length, loop again.
                            m_partialLength.put(chunk);
                        }
                    } else if (m_partialMessage != null) {
                        if (chunk.remaining() >= m_partialMessage.remaining()) {
                            int oldLimit = chunk.limit();
                            chunk.limit(chunk.position() + m_partialMessage.remaining());
                            m_partialMessage.put(chunk);
                            chunk.limit(oldLimit);
                            m_partialMessage.flip();
                            messages.add(m_partialMessage);
                            m_partialMessage = null;
                        } else {
                            m_partialMessage.put(chunk);
                        }
                    } else {
                        // at a message boundary.
                        if (chunk.remaining() >= 4) {
                            int messageLength = chunk.getInt();
                            if (chunk.remaining() >= messageLength) {
                                int oldLimit = chunk.limit();
                                chunk.limit(chunk.position() + messageLength);
                                ByteBuffer message = ByteBuffer.allocate(messageLength);
                                message.put(chunk);
                                chunk.limit(oldLimit);
                                message.flip();
                                messages.add(message);
                            } else {
                                m_partialMessage = ByteBuffer.allocate(messageLength);
                                m_partialMessage.put(chunk);
                            }
                        } else {
                            m_partialLength.put(chunk);
                        }
                    }
                }
            }
        }
        return messages.isEmpty() ? null : messages;
    }

    private ByteBuffer unwrapBuffer(ByteBuffer src, ByteBuffer dst) throws IOException {
        SSLEngineResult result = m_sslEngine.unwrap(src, dst);
        switch (result.getStatus()) {
            case OK:
                if (src.hasRemaining()) {
                    src.compact();
                } else {
                    src.clear();
                }
                dst.flip();
                return dst;
            case BUFFER_OVERFLOW:
                throw new SSLException("Unexpected overflow when unwrapping");
            case BUFFER_UNDERFLOW:
                // on underflow, want to read again.  There are unprocessed bytes up to limit.
                src.position(src.limit());
                src.limit(src.capacity());
               return null;
            case CLOSED:
                throw new SSLException("SSL engine is closed on ssl unwrapBuffer of buffer.");
        }
        return null;
    }
}
