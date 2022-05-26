/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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

package org.voltdb.e3;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.voltcore.messaging.VoltMessage;
import org.voltdb.utils.SerializationHelper;

abstract class GapFillMessage extends VoltMessage {
    String m_streamName;
    int m_partitionId;

    GapFillMessage(byte subject) {
        m_subject = subject;
    }

    GapFillMessage(byte subject, String streamName, int partitionId) {
        this(subject);
        m_streamName = streamName;
        m_partitionId = partitionId;
    }

    /**
     * @return The name of the stream this message is for
     */
    public String getStreamName() {
        return m_streamName;
    }

    /**
     * @return Partition ID this message is for
     */
    public int getPartitionId() {
        return m_partitionId;
    }

    @Override
    public int getSerializedSize() {
        // super + streamName + partitionId
        return super.getSerializedSize() + SerializationHelper.calculateSerializedSize(m_streamName) + Integer.BYTES;
    }

    @Override
    protected void initFromBuffer(ByteBuffer buf) throws IOException {
        m_streamName = SerializationHelper.getString(buf);
        m_partitionId = buf.getInt();
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException {
        buf.put(m_subject);
        SerializationHelper.writeString(m_streamName, buf);
        buf.putInt(m_partitionId);
    }
}
