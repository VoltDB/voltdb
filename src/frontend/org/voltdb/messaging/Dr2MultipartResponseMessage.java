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
import org.voltdb.ClientResponseImpl;

import java.io.IOException;
import java.nio.ByteBuffer;

public class Dr2MultipartResponseMessage extends VoltMessage {

    private boolean m_drain;
    private boolean m_reneg;
    private byte m_producerClusterId;
    private int m_producerPID;
    private ClientResponseImpl m_response;

    Dr2MultipartResponseMessage() {
        super();
    }

    public Dr2MultipartResponseMessage(byte producerClusterId, int producerPID, ClientResponseImpl response)  {
        m_drain = false;
        m_reneg = false;
        m_producerClusterId = producerClusterId;
        m_producerPID = producerPID;
        m_response = response;
    }

    public static Dr2MultipartResponseMessage createDrainOrRenegMessage(byte producerClusterId, int producerPID,
            boolean withDrainFlag, boolean withRenegFlag) {
        final Dr2MultipartResponseMessage msg = new Dr2MultipartResponseMessage();
        msg.m_drain = withDrainFlag;
        msg.m_reneg = withRenegFlag;
        msg.m_producerClusterId = producerClusterId;
        msg.m_producerPID = producerPID;
        msg.m_response = null;
        return msg;
    }

    public byte getProducerClusterId() {
        return m_producerClusterId;
    }

    public int getProducerPID() {
        return m_producerPID;
    }

    public ClientResponseImpl getResponse() {
        return m_response;
    }

    public boolean isDrain() {
        return m_drain;
    }

    public boolean isReneg() {
        return m_reneg;
    }

    @Override
    protected void initFromBuffer(ByteBuffer buf) throws IOException {
        m_drain = buf.get() == 1;
        m_reneg = buf.get() == 1;
        m_producerClusterId = buf.get();
        m_producerPID = buf.getInt();
        if (buf.remaining() > 0) {
            m_response = new ClientResponseImpl();
            m_response.initFromBuffer(buf);
        }
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException {
        buf.put(VoltDbMessageFactory.DR2_MULTIPART_RESPONSE_ID);
        buf.put((byte) (m_drain ? 1 : 0));
        buf.put((byte) (m_reneg ? 1 : 0));
        buf.put(m_producerClusterId);
        buf.putInt(m_producerPID);

        if (!m_drain && !m_reneg) {
            m_response.flattenToBuffer(buf);
        }

        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    @Override
    public int getSerializedSize() {
        int size = super.getSerializedSize()
                   + 1  // drain or not
                   + 1  // reneg or not
                   + 1  // producer cluster ID
                   + 4; // producer partition ID
        if (!m_drain && !m_reneg) {
            size += m_response.getSerializedSize();
        }
        return size;
    }
}
