/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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
    private int m_blockIndex = -1;

    public RejoinDataAckMessage() {
        m_subject = Subject.DEFAULT.getId();
    }

    public RejoinDataAckMessage(int blockIndex) {
        m_subject = Subject.DEFAULT.getId();
        m_blockIndex = blockIndex;
    }

    public int getBlockIndex() {
        return m_blockIndex;
    }

    @Override
    public int getSerializedSize() {
        int msgsize = super.getSerializedSize();
        msgsize +=
                4; // m_blockIndex
        return msgsize;
    }

    @Override
    protected void initFromBuffer(ByteBuffer buf) throws IOException {
        m_blockIndex = buf.getInt();
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException {
        buf.put(VoltDbMessageFactory.REJOIN_DATA_ACK_ID);
        buf.putInt(m_blockIndex);
        buf.limit(buf.position());
    }
}
