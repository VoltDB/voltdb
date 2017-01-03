/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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
    private ByteBuffer m_currentMessage;

    public SSLMessageParser() {
        this.m_partialMessageLength = ByteBuffer.allocate(4);
    }

    public ByteBuffer message(ByteBuffer messagesBuf) {

        // all the newly descrypted data has been processeed.
        if (messagesBuf.limit() == messagesBuf.position()) {
            return null;
        }

        // all the newly descrypted data has been processeed.
        if (m_partialMessageLength.position() != 0 || m_currentMessage == null) {
            if (!startMessage(messagesBuf)) {
                return null;
            }
        }

        if (messagesBuf.remaining() >= m_currentMessage.remaining()) {
            int oldLimit = messagesBuf.limit();
            messagesBuf.limit(messagesBuf.position() + m_currentMessage.remaining());
            m_currentMessage.put(messagesBuf);
            m_currentMessage.flip();
            messagesBuf.limit(oldLimit);
            ByteBuffer message = m_currentMessage;
            m_currentMessage = null;
            return message;
        } else {
            m_currentMessage.put(messagesBuf);
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
            int messageLength = m_partialMessageLength.getInt();
            m_partialMessageLength.clear();
            m_currentMessage = ByteBuffer.allocate(messageLength);
            return true;
        } else {
            m_partialMessageLength.put(messagesBuf);
            return false;
        }
    }
}
