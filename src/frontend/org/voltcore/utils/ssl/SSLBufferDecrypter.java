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

public class SSLBufferDecrypter {

    private final SSLEngine m_sslEngine;
    private ByteBuffer m_dstBuffer;
    private final ByteBuffer m_partialLength;
    private int m_currentMessageLength;
    private int m_currentMessageStart;

    public SSLBufferDecrypter(SSLEngine sslEngine) {
        this.m_sslEngine = sslEngine;
        this.m_dstBuffer = ByteBuffer.allocateDirect(1024 * 1024);
        this.m_partialLength = ByteBuffer.allocate(4);
        m_currentMessageLength = -1;
        m_currentMessageStart = -1;
    }

    public int decrypt(ByteBuffer srcBuffer) throws IOException {
        srcBuffer.flip();

        // if not in the middle of a message, clear the dst buffer.
        if (m_currentMessageLength == -1) {
            m_dstBuffer.clear();
        } else {
            // move the current message to the start of the dst buffer.
            if (m_currentMessageStart != 0) {
                m_dstBuffer.position(m_currentMessageStart);
                m_dstBuffer.compact();
                m_currentMessageStart = 0;
            } else {
                m_dstBuffer.limit(m_dstBuffer.capacity());
            }
        }

        return unwrap(srcBuffer);
    }

    public ByteBuffer message() {

        // all the newly descrypted data has been processeed.
        if (m_dstBuffer.limit() == m_dstBuffer.position()) {
            return null;
        }

        if (m_partialLength.position() != 0) {
            if (!startMessage()) {
                return null;
            }
        } else if (m_currentMessageLength == -1) {
            if (!startMessage()) {
                return null;
            }
        }

        int remainingInMessage = m_currentMessageLength - (m_dstBuffer.position() - m_currentMessageStart);
        if (m_dstBuffer.remaining() >= remainingInMessage) {
            int oldLimit = m_dstBuffer.limit();
            m_dstBuffer.position(m_currentMessageStart);
            m_dstBuffer.limit(m_dstBuffer.position() + m_currentMessageLength);

            ByteBuffer message = ByteBuffer.allocate(m_currentMessageLength);
            message.put(m_dstBuffer);
            message.flip();
            m_dstBuffer.limit(oldLimit);
            m_currentMessageLength = -1;
            return message;
        } else {
            // newly decrypted bytes are part of the existing message
            m_dstBuffer.position(m_dstBuffer.limit());
            return null;
        }
    }

    private boolean startMessage() {
        if (m_dstBuffer.remaining() >= m_partialLength.remaining()) {
            int oldLimit = m_dstBuffer.limit();
            m_dstBuffer.limit(m_dstBuffer.position() + m_partialLength.remaining());
            m_partialLength.put(m_dstBuffer);
            m_dstBuffer.limit(oldLimit);
            m_partialLength.flip();
            m_currentMessageLength = m_partialLength.getInt();
            m_partialLength.clear();
            m_currentMessageStart = m_dstBuffer.position();
            return true;
        } else {
            m_partialLength.put(m_dstBuffer);
            return false;
        }
    }

    // returns bytes consumed
    private int unwrap(ByteBuffer srcBuffer) throws IOException {
        // save initial state of dst buffer in case of underflow.
        int initialDstPos = m_dstBuffer.position();

        while (true) {
            SSLEngineResult result = m_sslEngine.unwrap(srcBuffer, m_dstBuffer.slice());
            switch (result.getStatus()) {
                case OK:
                    // in m_dstBuffer, newly decrtyped data is between pos and lim
                    m_dstBuffer.limit(m_dstBuffer.position() + result.bytesProduced());
                    return result.bytesConsumed();
                case BUFFER_OVERFLOW:
                    // the dst buffer holds partial volt messages, so its state needs to
                    // be retained on overflow.
                    ByteBuffer tmp = ByteBuffer.allocateDirect(m_dstBuffer.capacity() << 1);
                    m_dstBuffer.position(0);
                    tmp.put(m_dstBuffer);
                    tmp.position(initialDstPos);
                    m_dstBuffer = tmp;
                    break;
                case BUFFER_UNDERFLOW:
                    // Should never underflow as we know the size of the incoming frame.
                    throw new IOException("Unexpected underflow when unwrappling an ssl frame.");
                case CLOSED:
                    throw new SSLException("SSL engine is closed on ssl unwrap of buffer.");
            }
        }
    }
}
