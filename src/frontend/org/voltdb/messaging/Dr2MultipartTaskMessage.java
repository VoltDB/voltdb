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

import org.voltcore.messaging.VoltMessage;
import org.voltdb.StoredProcedureInvocation;

import java.io.IOException;
import java.nio.ByteBuffer;

public class Dr2MultipartTaskMessage extends VoltMessage {

    private int m_producerPID;
    private boolean m_drain;
    private boolean m_reneg;
    private byte m_producerClusterId;
    private short m_producerPartitionCnt;

    private long m_lastExecutedMPUniqueID;

    private StoredProcedureInvocation m_invocation;

    Dr2MultipartTaskMessage() {
        super();
    }

    public Dr2MultipartTaskMessage(StoredProcedureInvocation invocation, byte producerClusterId,
            int producerPID, short producerPartitionCnt, long lastExecutedMPUniqueID) {
        m_invocation = invocation;
        m_producerClusterId = producerClusterId;
        m_producerPartitionCnt = producerPartitionCnt;
        m_lastExecutedMPUniqueID = lastExecutedMPUniqueID;
        m_producerPID = producerPID;
        m_drain = false;
        m_reneg = false;
    }

    public static Dr2MultipartTaskMessage createDrainMessage(byte producerClusterId, int producerPID) {
        final Dr2MultipartTaskMessage msg = new Dr2MultipartTaskMessage();
        msg.m_producerPID = producerPID;
        msg.m_producerClusterId = producerClusterId;
        msg.m_producerPartitionCnt = -1;
        msg.m_drain = true;
        msg.m_reneg = false;
        msg.m_invocation = null;
        msg.m_lastExecutedMPUniqueID = Long.MIN_VALUE;
        return msg;
    }

    public static Dr2MultipartTaskMessage createRenegMessage(byte producerClusterId, int producerPID) {
        final Dr2MultipartTaskMessage msg = new Dr2MultipartTaskMessage();
        msg.m_producerPID = producerPID;
        msg.m_producerClusterId = producerClusterId;
        msg.m_producerPartitionCnt = -1;
        msg.m_drain = false;
        msg.m_reneg = true;
        msg.m_invocation = null;
        msg.m_lastExecutedMPUniqueID = Long.MIN_VALUE;
        return msg;
    }

    public StoredProcedureInvocation getSpi() {
        return m_invocation;
    }

    public long getLastExecutedMPUniqueID() {
        return m_lastExecutedMPUniqueID;
    }

    public boolean isDrain() {
        return m_drain;
    }

    public boolean isReneg() {
        return m_reneg;
    }

    public byte getProducerClusterId() {
        return m_producerClusterId;
    }

    public short getProducerPartitionCnt() {
        return m_producerPartitionCnt;
    }

    public int getProducerPID() {
        return m_producerPID;
    }

    @Override
    protected void initFromBuffer(ByteBuffer buf) throws IOException {
        m_producerPID = buf.getInt();
        m_drain = buf.get() == 1;
        m_reneg = buf.get() == 1;
        m_producerClusterId = buf.get();
        m_producerPartitionCnt = buf.getShort();
        m_lastExecutedMPUniqueID = buf.getLong();
        if (buf.remaining() > 0) {
            m_invocation = new StoredProcedureInvocation();
            m_invocation.initFromBuffer(buf);
        } else {
            m_invocation = null;
        }
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException {
        buf.put(VoltDbMessageFactory.DR2_MULTIPART_TASK_ID);
        buf.putInt(m_producerPID);
        buf.put((byte) (m_drain ? 1 : 0));
        buf.put((byte) (m_reneg ? 1 : 0));
        buf.put(m_producerClusterId);
        buf.putShort(m_producerPartitionCnt);
        buf.putLong(m_lastExecutedMPUniqueID);

        if (m_invocation != null) {
            m_invocation.flattenToBuffer(buf);
        }

        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    @Override
    public int getSerializedSize() {
        int size = super.getSerializedSize()
                   + 4  // producer partition ID
                   + 1  // is drain or not
                   + 1  // is reneg or not
                   + 1  // producer clusterId
                   + 2  // producer partition count
                   + 8; // last executed MP unique ID
        if (m_invocation != null) {
            size += m_invocation.getSerializedSize();
        }

        return size;
    }
}
