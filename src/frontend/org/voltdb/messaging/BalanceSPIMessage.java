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
package org.voltdb.messaging;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;

//The message is used to notify the new partition leader that all transactions on old leader are drained
//or to request old partition leader to initiate @BalanceSPI
public class BalanceSPIMessage extends VoltMessage {

    private long m_destinationLeaderHSID;
    private int m_partitionId;
    private int m_partitionKey;

    public BalanceSPIMessage() {
    }

    public BalanceSPIMessage(long hsid) {
        super();
        m_destinationLeaderHSID = hsid;
    }

    public BalanceSPIMessage(long hsid, int partitionId, int partitionKey) {
        this(hsid);
        m_partitionId = partitionId;
        m_partitionKey = partitionKey;
    }

    @Override
    public int getSerializedSize() {
        int msgsize = super.getSerializedSize();
        msgsize += 8;  // m_destinationLeaderHSID
        msgsize += 4;  // m_partitionId
        msgsize += 4;  // m_partitionKey
        return msgsize;
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException {
        buf.put(VoltDbMessageFactory.BALANCE_SPI_MESSAGE_ID);
        buf.putLong(m_destinationLeaderHSID);
        buf.putInt(m_partitionId);
        buf.putInt(m_partitionKey);
        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    @Override
    public void initFromBuffer(ByteBuffer buf) throws IOException {
        m_destinationLeaderHSID = buf.getLong();
        m_partitionId = buf.getInt();
        m_partitionKey = buf.getInt();
    }

    public long getDestinationLeaderHSID() {
        return m_destinationLeaderHSID;
    }

    public int getDestinationHostId() {
        return CoreUtils.getHostIdFromHSId(m_destinationLeaderHSID);
    }

    public int getPartitionId() {
        return m_partitionId;
    }

    public int getPartitionKey() {
        return m_partitionKey;
    }
}
