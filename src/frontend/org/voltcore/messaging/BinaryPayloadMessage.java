/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
package org.voltcore.messaging;

import java.nio.ByteBuffer;

public class BinaryPayloadMessage extends VoltMessage {

    public byte m_payload[];
    public byte m_metadata[];
    private int m_startPos;
    private int m_length;

    public BinaryPayloadMessage() {}
    public BinaryPayloadMessage( byte metadata[], byte payload[]) {
        if (metadata == null || metadata.length > Short.MAX_VALUE) {
            throw new IllegalArgumentException();
        }
        m_payload = payload;
        m_metadata = metadata;
        m_startPos = 0;
        m_length = m_payload.length;
    }

    // Zero copy constructor to encapsulate part of input data into payload
    public BinaryPayloadMessage( byte metadata[], byte payload[], int startPos, int length) {
        if (metadata == null || metadata.length > Short.MAX_VALUE) {
            throw new IllegalArgumentException();
        }
        if (payload != null && length > payload.length) {
            throw new IllegalArgumentException();
        }
        m_payload = payload;
        m_metadata = metadata;
        m_startPos = startPos;
        m_length = length;
    }

    @Override
    public void initFromBuffer(ByteBuffer buf) {
        m_metadata = new byte[buf.getShort()];
        buf.get(m_metadata);
        m_startPos = 0;
        m_length = buf.getInt();
        if (m_length > -1) {
            m_payload = new byte[m_length];
            buf.get(m_payload);
        }
        assert(buf.capacity() == buf.position());
    }

    @Override
    public int getSerializedSize() {
        int msgsize = m_metadata.length + 6 + super.getSerializedSize();
        if (m_payload != null) {
            msgsize += m_length;
        }
        return msgsize;
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) {
        buf.put(VoltMessageFactory.BINARY_PAYLOAD_ID);
        buf.putShort((short)m_metadata.length);
        buf.put(m_metadata);
        if (m_payload == null) {
            buf.putInt(-1);
        } else {
            buf.putInt(m_length);
            buf.put(m_payload, m_startPos, m_length);
        }

        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }
}
