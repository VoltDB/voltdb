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

    public int decrypt() throws IOException {

        List<ByteBuffer> messages = new ArrayList<>();
        if (m_srcBuffer.position() > 0) {
            m_srcBuffer.flip();
            m_dstBuffer.clear();
            return unwrap();
        }
        return 0;
    }

    public ByteBuffer message() {
        if (m_dstBuffer.hasRemaining()) {
            if (m_partialLength.position() != 0) {
                int length = handlePartialLength();
                if (length != 0) {
                    m_partialLength.clear();
                    // m_partial message gets filled in next time through the loop.
                    m_partialMessage = ByteBuffer.allocate(length);
                    ByteBuffer message = handlePartialMessage();
                    if (message != null) {
                        m_partialMessage = null;
                    }
                    return message;
                }
            } else if (m_partialMessage != null) {
                ByteBuffer message = handlePartialMessage();
                if (message != null) {
                    m_partialMessage = null;
                    return message;
                }
            } else {
                return handleMessage();
            }
        }
        return null;
    }

    // tries to complete a partial length.  Returns 0 if not.
    private int handlePartialLength() {
        if (m_dstBuffer.remaining() >= (m_partialLength.remaining())) {
            int oldLimit = m_dstBuffer.limit();
            m_dstBuffer.limit(m_dstBuffer.position() + m_partialLength.remaining());
            m_partialLength.put(m_dstBuffer);
            m_dstBuffer.limit(oldLimit);
            m_partialLength.flip();
            return m_partialLength.getInt();
        } else {
            // net enough remaining in chunk to finish length, loop again.
            m_partialLength.put(m_dstBuffer);
            return 0;
        }
    }

    private ByteBuffer handlePartialMessage() {
        if (m_dstBuffer.remaining() >= m_partialMessage.remaining()) {
            int oldLimit = m_dstBuffer.limit();
            m_dstBuffer.limit(m_dstBuffer.position() + m_partialMessage.remaining());
            m_partialMessage.put(m_dstBuffer);
            m_dstBuffer.limit(oldLimit);
            m_partialMessage.flip();
            return m_partialMessage;
        } else {
            m_partialMessage.put(m_dstBuffer);
            return null;
        }
    }

    private ByteBuffer handleMessage() {
        if (m_dstBuffer.remaining() >= 4) {
            int messageLength = m_dstBuffer.getInt();
            if (m_dstBuffer.remaining() >= messageLength) {
                int oldLimit = m_dstBuffer.limit();
                m_dstBuffer.limit(m_dstBuffer.position() + messageLength);
                ByteBuffer message = ByteBuffer.allocate(messageLength);
                message.put(m_dstBuffer);
                m_dstBuffer.limit(oldLimit);
                message.flip();
                return message;
            } else {
                m_partialMessage = ByteBuffer.allocate(messageLength);
                m_partialMessage.put(m_dstBuffer);
                return null;
            }
        } else {
            m_partialLength.put(m_dstBuffer);
            return null;
        }
    }

    // returns bytes consumed
    private int unwrap() throws IOException {
        SSLEngineResult result = m_sslEngine.unwrap(m_srcBuffer, m_dstBuffer);
        switch (result.getStatus()) {
            case OK:
                if (m_srcBuffer.hasRemaining()) {
                    m_srcBuffer.compact();
                } else {
                    m_srcBuffer.clear();
                }
                m_dstBuffer.flip();
                return result.bytesConsumed();
            case BUFFER_OVERFLOW:
                throw new SSLException("Unexpected overflow when unwrapping");
            case BUFFER_UNDERFLOW:
                // on underflow, want to read again.  There are unprocessed bytes up to limit.
                m_srcBuffer.position(m_srcBuffer.limit());
                m_srcBuffer.limit(m_srcBuffer.capacity());
               return 0;
            case CLOSED:
                throw new SSLException("SSL engine is closed on ssl unwrap of buffer.");
        }
        return 0;
    }
}
