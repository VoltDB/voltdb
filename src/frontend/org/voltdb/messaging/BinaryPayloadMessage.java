/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.voltdb.messaging;

import java.io.IOException;

import org.voltdb.utils.DBBPool;

public class BinaryPayloadMessage extends VoltMessage {

    public byte m_payload[];
    public byte m_metadata[];

    public BinaryPayloadMessage() {}
    public BinaryPayloadMessage( byte metadata[], byte payload[]) {
        m_payload = payload;
        m_metadata = metadata;
        if (metadata == null || metadata.length != 16) {
            throw new IllegalArgumentException();
        }
    }

    @Override
    protected void initFromBuffer() {
        m_buffer.position(HEADER_SIZE + 1); // skip the msg id
        m_metadata = new byte[16];
        m_buffer.get(m_metadata);
        final int payloadLength = m_buffer.getInt();
        if (payloadLength > -1) {
            m_payload = new byte[payloadLength];
            m_buffer.get(m_payload);
        }
        m_buffer = null;
        m_container.discard();
        m_container = null;
    }

    @Override
    protected void flattenToBuffer(DBBPool pool) throws IOException {
        int msgsize = m_metadata.length + 4;
        if (m_payload != null) {
            msgsize += m_payload.length;
        }

        if (m_buffer == null) {
            m_container = pool.acquire(msgsize + 1 + HEADER_SIZE);
            m_buffer = m_container.b;
        }
        setBufferSize(msgsize + 1, pool);

        m_buffer.position(HEADER_SIZE);
        m_buffer.put(BINARY_PAYLOAD_ID);
        m_buffer.put(m_metadata);
        if (m_payload == null) {
            m_buffer.putInt(-1);
        } else {
            m_buffer.putInt(m_payload.length);
            m_buffer.put(m_payload);
        }
    }
}
