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
import org.voltdb.ClientResponseImpl;

import java.io.IOException;
import java.nio.ByteBuffer;

public class Dr2MultipartResponseMessage extends VoltMessage {

    private int m_producerPartitionId;
    private ClientResponseImpl m_response;

    Dr2MultipartResponseMessage() {
        super();
    }

    public Dr2MultipartResponseMessage(int producerPartitionId, ClientResponseImpl response)  {
        m_producerPartitionId = producerPartitionId;
        m_response = response;
    }

    public int getProducerPartitionId() {
        return m_producerPartitionId;
    }

    public ClientResponseImpl getResponse() {
        return m_response;
    }

    @Override
    protected void initFromBuffer(ByteBuffer buf) throws IOException {
        if (buf.remaining() > 0) {
            m_producerPartitionId = buf.getInt();
            m_response = new ClientResponseImpl();
            m_response.initFromBuffer(buf);
        }
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException {
        buf.put(VoltDbMessageFactory.DR2_MULTIPART_RESPONSE_ID);

        buf.putInt(m_producerPartitionId);
        m_response.flattenToBuffer(buf);

        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    @Override
    public int getSerializedSize() {
        int size = super.getSerializedSize();
        size += (4 + m_response.getSerializedSize());
        return size;
    }
}
