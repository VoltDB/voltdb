/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

import org.voltcore.messaging.VoltMessage;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A message sent from the multipart replayer back to the involved
 * partitions after the multipart has finished.
 */
public class MpReplayAckMessage extends VoltMessage {
    private long m_txnId;

    /** Empty constructor for de-serialization */
    MpReplayAckMessage() {
        super();
    }

    public MpReplayAckMessage(long txnId)
    {
        super();
        m_txnId = txnId;
    }

    public long getTxnId() { return m_txnId; }

    @Override
    public int getSerializedSize()
    {
        int size = super.getSerializedSize();
        size += 8; // m_txnId
        return size;
    }

    @Override
    protected void initFromBuffer(ByteBuffer buf) throws IOException
    {
        m_txnId = buf.getLong();
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException
    {
        buf.put(VoltDbMessageFactory.MP_REPLAY_ACK_ID);
        buf.putLong(m_txnId);
    }
}
