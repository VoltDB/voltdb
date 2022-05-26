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
public class RejoinDataAckMessage extends VoltMessage {
    private boolean m_isEOS = false;
    private long m_targetId = -1;
    private int m_blockIndex = -1;

    public RejoinDataAckMessage() {
        m_subject = Subject.DEFAULT.getId();
    }

    public RejoinDataAckMessage(boolean isEOS, long targetId, int blockIndex) {
        m_subject = Subject.DEFAULT.getId();
        m_isEOS = isEOS;
        m_targetId = targetId;
        m_blockIndex = blockIndex;
    }

    public long getTargetId() {
        return m_targetId;
    }

    public int getBlockIndex() {
        return m_blockIndex;
    }

    public boolean isEOS() {
        return m_isEOS;
    }

    @Override
    public int getSerializedSize() {
        int msgsize = super.getSerializedSize();
        msgsize += 1 + // m_isEOS
                8 + // m_targetId
                4;  // m_blockIndex
        return msgsize;
    }

    @Override
    protected void initFromBuffer(ByteBuffer buf) throws IOException {
        m_isEOS = buf.get() == 1;
        m_targetId = buf.getLong();
        m_blockIndex = buf.getInt();
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException {
        buf.put(VoltDbMessageFactory.REJOIN_DATA_ACK_ID);
        buf.put(m_isEOS ? 1 : (byte) 0);
        buf.putLong(m_targetId);
        buf.putInt(m_blockIndex);
        buf.limit(buf.position());
    }
}
