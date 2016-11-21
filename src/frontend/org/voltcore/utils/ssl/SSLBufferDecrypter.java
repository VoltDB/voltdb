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

public class SSLBufferDecrypter {

    private final SSLEngine m_sslEngine;
    private ByteBuffer m_dstBuffer;
    private final ByteBuffer m_partialMessageLength;
    private int m_currentMessageLength;
    private int m_currentMessageStart;

    public SSLBufferDecrypter(SSLEngine sslEngine) {
        this.m_sslEngine = sslEngine;
        this.m_dstBuffer = ByteBuffer.allocateDirect(1024 * 1024);
        // newly decrypted data is held in the dst buffer between
        // position and limit.  Initially, there is no decrypted
        // data in the buffer and so the limit should be the same as
        // the position.
        m_dstBuffer.limit(m_dstBuffer.position());
        this.m_partialMessageLength = ByteBuffer.allocate(4);
        this.m_currentMessageLength = -1;
        this.m_currentMessageStart = -1;
    }

    public int decrypt(ByteBuffer srcBuffer) throws IOException {
        if (srcBuffer.position() > 0) {
            srcBuffer.flip();
        } else {
            // won't be able to decrypt is the src buffer is empty.
            return 0;
        }

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

        return unwrap(srcBuffer, m_dstBuffer);
    }

    public ByteBuffer message(ByteBuffer messagesBuf) {

        // all the newly descrypted data has been processeed.
        if (messagesBuf.limit() == messagesBuf.position()) {
            return null;
        }

        if (m_partialMessageLength.position() != 0) {
            if (!startMessage()) {
                return null;
            }
        } else if (m_currentMessageLength == -1) {
            if (!startMessage()) {
                return null;
            }
        }

        int remainingInMessage = m_currentMessageLength - (messagesBuf.position() - m_currentMessageStart);
        if (messagesBuf.remaining() >= remainingInMessage) {
            int oldLimit = messagesBuf.limit();
            messagesBuf.position(m_currentMessageStart);
            messagesBuf.limit(messagesBuf.position() + m_currentMessageLength);

            ByteBuffer message = ByteBuffer.allocate(m_currentMessageLength);
            message.put(messagesBuf);
            message.flip();
            messagesBuf.limit(oldLimit);
            m_currentMessageLength = -1;
            return message;
        } else {
            // newly decrypted bytes are part of the existing message
            messagesBuf.position(messagesBuf.limit());
            return null;
        }
    }

    private boolean startMessage() {
        if (m_dstBuffer.remaining() >= m_partialMessageLength.remaining()) {
            int oldLimit = m_dstBuffer.limit();
            m_dstBuffer.limit(m_dstBuffer.position() + m_partialMessageLength.remaining());
            m_partialMessageLength.put(m_dstBuffer);
            m_dstBuffer.limit(oldLimit);
            m_partialMessageLength.flip();
            m_currentMessageLength = m_partialMessageLength.getInt();
            m_partialMessageLength.clear();
            m_currentMessageStart = m_dstBuffer.position();
            return true;
        } else {
            m_partialMessageLength.put(m_dstBuffer);
            return false;
        }
    }

    // returns bytes consumed
    public int unwrap(ByteBuffer srcBuffer, ByteBuffer dstBuffer) throws IOException {
        // save initial state of dst buffer in case of underflow.
        int initialDstPos = dstBuffer.position();
        while (true) {
            SSLEngineResult result = m_sslEngine.unwrap(srcBuffer, dstBuffer.slice());
            switch (result.getStatus()) {
                case OK:
                    if (srcBuffer.hasRemaining()) {
                        srcBuffer.compact();
                    } else {
                        srcBuffer.clear();
                    }
                    // in m_dstBuffer, newly decrtyped data is between pos and lim
                    dstBuffer.limit(dstBuffer.position() + result.bytesProduced());
                    return result.bytesConsumed();
                case BUFFER_OVERFLOW:
                    // the dst buffer holds partial volt messages, so its state needs to
                    // be retained on overflow.
                    ByteBuffer tmp = ByteBuffer.allocateDirect(dstBuffer.capacity() << 1);
                    dstBuffer.position(0);
                    tmp.put(m_dstBuffer);
                    tmp.position(initialDstPos);
                    dstBuffer = tmp;
                    break;
                case BUFFER_UNDERFLOW:
                    // on underflow, want to read again.  There are unprocessed bytes up to limit.
                    // reset the buffers to their state prior to the underflow.
                    srcBuffer.position(srcBuffer.limit());
                    srcBuffer.limit(srcBuffer.capacity());
                    dstBuffer.position(initialDstPos);
                    dstBuffer.limit(initialDstPos);
                    return 0;
                case CLOSED:
                    throw new SSLException("SSL engine is closed on ssl unwrap of buffer.");
            }
        }
    }
}
