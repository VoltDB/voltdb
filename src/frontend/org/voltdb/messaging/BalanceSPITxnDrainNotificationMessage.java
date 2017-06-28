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

//The message is used to notify the new partition leader that all transactions on old leader
//have been drained.
public class BalanceSPITxnDrainNotificationMessage extends VoltMessage {

    private long m_newLeaderHSID;

    public BalanceSPITxnDrainNotificationMessage() {

    }

    public BalanceSPITxnDrainNotificationMessage(long hsid) {
        super();
        m_newLeaderHSID = hsid;
    }

    @Override
    public int getSerializedSize() {
        int msgsize = super.getSerializedSize();
        msgsize += 8;  // m_newLeaderHSID
        return msgsize;
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException {
        buf.put(VoltDbMessageFactory.BALANCE_SPI_COMPLETE_MESSAGE_ID);
        buf.putLong(m_newLeaderHSID);
        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    @Override
    public void initFromBuffer(ByteBuffer buf) throws IOException {
        m_newLeaderHSID = buf.getLong();
    }

    public long getNewLeaderHSID() {
        return m_newLeaderHSID;
    }
}
