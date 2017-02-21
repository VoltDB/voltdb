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

package org.voltcore.messaging;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.voltdb.messaging.VoltDbMessageFactory;

public class NodeFailureNotificationMessage extends VoltMessage {

    private int m_partitionId;
    private long m_initiatorHsid;

    public NodeFailureNotificationMessage () {}

    public NodeFailureNotificationMessage(int partitionId, long hsid) {
        m_partitionId = partitionId;
        m_initiatorHsid = hsid;
    }
    @Override
    protected void initFromBuffer(ByteBuffer buf) throws IOException {
        buf.put(VoltDbMessageFactory.NODE_FAILURE_NOTIFICATION_ID);
        buf.putInt(m_partitionId);
        buf.putLong(m_initiatorHsid);
        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException {
        m_partitionId = buf.getInt();
        m_initiatorHsid = buf.getLong();
    }

    @Override
    public int getSerializedSize() {
        int msgsize = super.getSerializedSize();
        msgsize +=
            4 + // m_partitionId
            8; // m_initiatorHsid
        return msgsize;
    }

    public int getPartitionId() {
        return m_partitionId;
    }

    public long getInitiatorHsid() {
        return m_initiatorHsid;
    }
}
