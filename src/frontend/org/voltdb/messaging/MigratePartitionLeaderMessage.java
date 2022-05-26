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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.voltcore.messaging.VoltMessage;

//The message is used to notify the new partition leader that all transactions on
//old leader are drained.
//It is also used to notify ClientInterface to start or stop MigratePartitionLeader
public class MigratePartitionLeaderMessage extends VoltMessage {

    private long m_newLeaderHSID = -1;
    private long m_priorLeaderHSID = -1;

    //if it is true, ClientInterface will start migrating partition leader service
    //otherwise, ClientInterface will stop migrating partition leader service if it has been started.
    private boolean m_startingService = false;

    private boolean m_resetStatus = false;

    private boolean m_stopNodeService = false;

    public MigratePartitionLeaderMessage() {
        super();
    }

    public MigratePartitionLeaderMessage(long priorHSID, long newHSID) {
        super();
        m_priorLeaderHSID = priorHSID;
        m_newLeaderHSID = newHSID;
    }

    @Override
    public int getSerializedSize() {
        int msgsize = super.getSerializedSize();
        msgsize += 8; // m_newLeaderHSID,
        msgsize += 8; // m_priorLeaderHSID
        msgsize += 1; // m_startingBalanceSpiService
        msgsize += 1; // m_resetStatus
        msgsize += 1; // m_stopNodeService
        return msgsize;
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException {
        buf.put(VoltDbMessageFactory.Migrate_Partition_Leader_MESSAGE_ID);
        buf.putLong(m_newLeaderHSID);
        buf.putLong(m_priorLeaderHSID);
        buf.put(m_startingService ? (byte) 1 : (byte) 0);
        buf.put(m_resetStatus ? (byte) 1 : (byte) 0);
        buf.put(m_stopNodeService ? (byte) 1 : (byte) 0);
        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    @Override
    public void initFromBuffer(ByteBuffer buf) throws IOException {
        m_newLeaderHSID = buf.getLong();
        m_priorLeaderHSID = buf.getLong();
        m_startingService = buf.get() == 1;
        m_resetStatus = buf.get() == 1;
        m_stopNodeService = buf.get() == 1;
    }

    public long getNewLeaderHSID() {
        return m_newLeaderHSID;
    }

    public long getPriorLeaderHSID() {
        return m_priorLeaderHSID;
    }

    public void setStartTask() {
        m_startingService = true;
    }

    public boolean startMigratingPartitionLeaders() {
        return m_startingService;
    }

    public void setStatusReset() {
        m_resetStatus = true;
    }

    public boolean isStatusReset() {
        return m_resetStatus;
    }

    public void setStopNodeService() {
        m_stopNodeService = true;
    }

    public boolean isForStopNode() {
        return m_stopNodeService;
    }
}
