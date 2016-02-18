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

package org.voltdb.messaging;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltcore.utils.CoreUtils;

/**
 * Message from a client interface to an initiator, instructing the
 * site to begin executing a stored procedure, coordinating other
 * execution sites if needed.
 *
 */
public class Iv2GetDrTrackerRequestMessage extends TransactionInfoBaseMessage
{
    private long m_requestId = 0;
    private int m_producerClusterId = 0;
    private int m_producerPartitionId = 0;

    /** Empty constructor for de-serialization */
    Iv2GetDrTrackerRequestMessage() {
        super(0, 0, 0, 0, true, false);
    }

    public Iv2GetDrTrackerRequestMessage(long requestId, int producerClusterId, int producerPartitionId)
    {
        super();
        m_requestId = requestId;
        m_producerClusterId = producerClusterId;
        m_producerPartitionId = producerPartitionId;
    }

    public long getRequestId()
    {
        return m_requestId;
    }

    public int getProducerClusterId()
    {
        return m_producerClusterId;
    }

    public int getProducerPartitionId()
    {
        return m_producerPartitionId;
    }

    @Override
    public int getSerializedSize()
    {
        int msgsize = super.getSerializedSize();
        msgsize += 8; // requestId
        msgsize += 4; // producerClusterId
        msgsize += 4; // producerPartitionId
        return msgsize;
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException
    {
        buf.put(VoltDbMessageFactory.IV2_GET_DR_TRACKER_REQUEST);
        buf.putLong(m_requestId);
        buf.putInt(m_producerClusterId);
        buf.putInt(m_producerPartitionId);

        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    @Override
    public void initFromBuffer(ByteBuffer buf) throws IOException {
        m_requestId = buf.getLong();
        m_producerClusterId = buf.getInt();
        m_producerPartitionId = buf.getInt();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("IV2 GET_MAP_REQUEST (FROM ");
        sb.append(CoreUtils.hsIdToString(m_sourceHSId));
        sb.append(" REQID: ");
        sb.append(m_requestId);
        sb.append(" CLUSTERID: ");
        sb.append(m_producerClusterId);
        sb.append(" PARTITIONID: ");
        sb.append(m_producerPartitionId);
        return sb.toString();
    }
}
