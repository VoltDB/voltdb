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

package org.voltdb.rejoin;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.voltcore.messaging.Subject;
import org.voltcore.messaging.VoltMessage;
import org.voltdb.messaging.VoltDbMessageFactory;

/**
 *
 */
public class RejoinDataMessage extends VoltMessage {
    private long m_targetId = -1;
    // compressed snapshot data
    private byte[] m_data = null;

    public RejoinDataMessage() {
        m_subject = Subject.DEFAULT.getId();
    }

    public RejoinDataMessage(long targetId, byte[] data) {
        m_subject = Subject.DEFAULT.getId();
        m_targetId = targetId;
        m_data = data;
    }

    public long getTargetId() {
        return m_targetId;
    }

    public byte[] getData() {
        return m_data;
    }

    @Override
    public int getSerializedSize() {
        int msgsize = super.getSerializedSize();
        msgsize +=
                8 + // m_targetId
                4 + // data length
                m_data.length;
        return msgsize;
    }

    @Override
    protected void initFromBuffer(ByteBuffer buf) throws IOException {
        m_targetId = buf.getLong();
        int len = buf.getInt();
        m_data = new byte[len];
        buf.get(m_data);
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException {
        buf.put(VoltDbMessageFactory.REJOIN_DATA_ID);
        buf.putLong(m_targetId);
        buf.putInt(m_data.length);
        buf.put(m_data);
        buf.limit(buf.position());
    }
}
