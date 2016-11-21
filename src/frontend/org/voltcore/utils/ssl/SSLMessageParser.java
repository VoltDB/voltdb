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

import java.nio.ByteBuffer;

/**
 * Parses messages out of a bytebuffer.  Keeps state between byte buffers,
 * the buffers need to be read sequentially from the network.  Also
 * compacts the given message buffer.
 */
public class SSLMessageParser {

    private final ByteBuffer m_partialMessageLength;
    private int m_currentMessageLength;
    private int m_currentMessageStart;

    public SSLMessageParser() {
        this.m_partialMessageLength = ByteBuffer.allocate(4);
        this.m_currentMessageLength = -1;
        this.m_currentMessageStart = -1;
    }

    public ByteBuffer message(ByteBuffer messagesBuf) {

        // all the newly descrypted data has been processeed.
        if (messagesBuf.limit() == messagesBuf.position()) {
            return null;
        }

        if (m_partialMessageLength.position() != 0) {
            if (!startMessage(messagesBuf)) {
                return null;
            }
        } else if (m_currentMessageLength == -1) {
            if (!startMessage(messagesBuf)) {
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
            int remainingMessageLength = messagesBuf.remaining();
            messagesBuf.compact();
            messagesBuf.flip();
            m_currentMessageLength = -1;
            return message;
        } else {
            // newly decrypted bytes are part of the existing message
            messagesBuf.position(messagesBuf.limit());
            return null;
        }
    }

    private boolean startMessage(ByteBuffer messagesBuf) {
        if (messagesBuf.remaining() >= m_partialMessageLength.remaining()) {
            int oldLimit = messagesBuf.limit();
            messagesBuf.limit(messagesBuf.position() + m_partialMessageLength.remaining());
            m_partialMessageLength.put(messagesBuf);
            messagesBuf.limit(oldLimit);
            m_partialMessageLength.flip();
            m_currentMessageLength = m_partialMessageLength.getInt();
            m_partialMessageLength.clear();
            m_currentMessageStart = messagesBuf.position();
            return true;
        } else {
            m_partialMessageLength.put(messagesBuf);
            return false;
        }
    }
}
