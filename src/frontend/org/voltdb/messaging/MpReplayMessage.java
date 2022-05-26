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

package org.voltdb.messaging;

import com.google_voltpatches.common.collect.ImmutableSet;
import com.google_voltpatches.common.collect.Sets;
import org.voltcore.messaging.VoltMessage;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.iv2.TxnEgo;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Set;

/**
 * A message sent from the involved partitions to the
 * multipart replayer during command log replay.
 */
public class MpReplayMessage extends VoltMessage {
    long m_txnId;
    long m_uniqueId;
    int m_partitionId;
    Set<Integer> m_involvedPartitions;
    StoredProcedureInvocation m_invocation;

    /** Empty constructor for de-serialization */
    MpReplayMessage() {
        super();
    }

    public MpReplayMessage(long txnId, long uniqueId, int partitionId, Collection<Integer> involvedPartitions,
                           StoredProcedureInvocation invocation)
    {
        super();

        m_txnId = txnId;
        m_uniqueId = uniqueId;
        m_partitionId = partitionId;
        m_involvedPartitions = ImmutableSet.copyOf(involvedPartitions);
        m_invocation = invocation;
    }

    public long getTxnId()
    {
        return m_txnId;
    }

    public long getUniqueId()
    {
        return m_uniqueId;
    }

    public int getPartitionId()
    {
        return m_partitionId;
    }

    public Set<Integer> getInvolvedPartitions()
    {
        return m_involvedPartitions;
    }

    public StoredProcedureInvocation getInvocation()
    {
        return m_invocation;
    }

    @Override
    public int getSerializedSize()
    {
        int size = super.getSerializedSize();
        size +=   8 // m_txnId
                + 8 // m_uniqueId
                + 4 // m_partitionId
                + 4 // m_involvedPartitions.size()
                + 4 * m_involvedPartitions.size();

        if (m_invocation != null) {
            size += m_invocation.getSerializedSize();
        }

        return size;
    }

    @Override
    protected void initFromBuffer(ByteBuffer buf) throws IOException
    {
        m_txnId = buf.getLong();
        m_uniqueId = buf.getLong();
        m_partitionId = buf.getInt();
        int partitionCount = buf.getInt();
        m_involvedPartitions = Sets.newHashSet();
        for (int i = 0; i < partitionCount; i++) {
            m_involvedPartitions.add(buf.getInt());
        }

        if (buf.remaining() > 0) {
            m_invocation = new StoredProcedureInvocation();
            m_invocation.initFromBuffer(buf);
        } else {
            m_invocation = null;
        }
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException
    {
        buf.put(VoltDbMessageFactory.MP_REPLAY_ID);
        buf.putLong(m_txnId);
        buf.putLong(m_uniqueId);
        buf.putInt(m_partitionId);
        buf.putInt(m_involvedPartitions.size());
        for (int pid : m_involvedPartitions) {
            buf.putInt(pid);
        }

        if (m_invocation != null) {
            m_invocation.flattenToBuffer(buf);
        }

        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    @Override
    public String getMessageInfo() {
        return "MpReplayMessage TxnId:" + TxnEgo.txnIdToString(m_txnId);
    }
}
