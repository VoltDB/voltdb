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

import org.voltcore.messaging.VoltMessage;
import org.voltdb.PrivateVoltTableFactory;
import org.voltdb.VoltTable;

import java.io.IOException;
import java.nio.ByteBuffer;

public class Dr2MultipartResponseMessage extends VoltMessage {

    private byte m_status = 0;
    private VoltTable[] m_results = new VoltTable[0];
    private byte m_appStatus = Byte.MIN_VALUE;
    private int m_producerPartitionId;

    Dr2MultipartResponseMessage() {
        super();
    }

    public Dr2MultipartResponseMessage(int producerPartitionId, byte status, byte appStatus, VoltTable[] results)  {
        m_producerPartitionId = producerPartitionId;
        m_status = status;
        m_appStatus = appStatus;
        m_results = results;
    }

    public byte getStatus() {
        return m_status;
    }

    public VoltTable[] getResults() {
        return m_results;
    }

    public byte getAppStatus() {
        return m_appStatus;
    }

    public int getProducerPartitionId() {
        return m_producerPartitionId;
    }

    @Override
    protected void initFromBuffer(ByteBuffer buf) throws IOException {
        if (buf.remaining() > 0) {
            m_producerPartitionId = buf.getInt();
            m_status = buf.get();
            m_appStatus = buf.get();
            int tableCount = buf.getInt();
            if (tableCount < 0) {
                throw new IOException("Table count is negative: " + tableCount);
            }
            m_results = new VoltTable[tableCount];
            for (int i = 0; i < tableCount; i++) {
                int tableSize = buf.getInt();
                final int originalLimit = buf.limit();
                buf.limit(buf.position() + tableSize);
                final ByteBuffer slice = buf.slice();
                buf.position(buf.position() + tableSize);
                buf.limit(originalLimit);
                m_results[i] = PrivateVoltTableFactory.createVoltTableFromBuffer(slice, false);
            }

        }
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException {
        buf.put(VoltDbMessageFactory.DR2_MULTIPART_RESPONSE_ID);

        buf.putInt(m_producerPartitionId);
        buf.put(m_status);
        buf.put(m_appStatus);
        buf.putInt(m_results.length);
        for (VoltTable vt : m_results) {
            vt.flattenToBuffer(buf);
        }

        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    @Override
    public int getSerializedSize() {
        int size = super.getSerializedSize();

        size += 4
             + 1
             + 1
             + 4;
        for (VoltTable vt: m_results) {
            size += vt.getSerializedSize();
        }

        return size;
    }
}
