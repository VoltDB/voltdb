/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.messaging;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;

/**
 * Message from a client interface to an initiator, instructing the
 * site to begin executing a stored procedure, coordinating other
 * execution sites if needed.
 *
 */
public class Iv2RepairLogResponseMessage extends VoltMessage
{
    private int m_requestId = 0;
    private int m_sequence = 0;
    private int m_ofTotal = 0;
    private long m_spHandle = Long.MIN_VALUE;

    // The original task that is must be replicated for
    // repair. Note: if the destination repair log is
    // empty, a repair log response message is returned
    // that has sequence = 0; ofTotal = 0 and a null
    // payload (because the requester must know that the
    // log request was processed and that no logs exist.)
    private VoltMessage m_payload = null;

    /** Empty constructor for de-serialization */
    Iv2RepairLogResponseMessage() {
        super();
    }

    public Iv2RepairLogResponseMessage(int requestId, int sequence,
            int ofTotal, long spHandle, VoltMessage payload)
    {
        super();
        m_requestId = requestId;
        m_sequence = sequence;
        m_ofTotal = ofTotal;
        m_spHandle = spHandle;
        m_payload = payload;
    }

    public int getRequestId()
    {
        return m_requestId;
    }

    public int getSequence()
    {
        return m_sequence;
    }

    public int getOfTotal()
    {
        return m_ofTotal;
    }

    public long getSpHandle()
    {
        return m_spHandle;
    }

    public VoltMessage getPayload()
    {
        return m_payload;
    }

    @Override
    public int getSerializedSize()
    {
        int msgsize = super.getSerializedSize();
        msgsize += 4; // requestId
        msgsize += 4; // sequence
        msgsize += 4; // ofTotal
        msgsize += 8; // spHandle
        if (m_payload != null) {
            msgsize += m_payload.getSerializedSize();
        }
        return msgsize;
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException
    {
        buf.put(VoltDbMessageFactory.IV2_REPAIR_LOG_RESPONSE);
        buf.putInt(m_requestId);
        buf.putInt(m_sequence);
        buf.putInt(m_ofTotal);
        buf.putLong(m_spHandle);

        if (m_payload != null) {
            ByteBuffer paybuf = ByteBuffer.allocate(m_payload.getSerializedSize());
            m_payload.flattenToBuffer(paybuf);
            if (paybuf.position() != 0) {
                paybuf.flip();
            }
            buf.put(paybuf);
        }

        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    @Override
    protected void initFromBuffer(ByteBuffer buf) throws IOException {
        m_requestId = buf.getInt();
        m_sequence = buf.getInt();
        m_ofTotal = buf.getInt();
        m_spHandle = buf.getLong();

        // going inception.
        if (m_ofTotal != 0) {
            VoltDbMessageFactory messageFactory = new VoltDbMessageFactory();
            m_payload = messageFactory.createMessageFromBuffer(buf, m_sourceHSId);
        }
        else {
            m_payload = null;
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("IV2 REPAIR_LOG_REQUEST (FROM ");
        sb.append(CoreUtils.hsIdToString(m_sourceHSId));
        sb.append(" REQID: ");
        sb.append(m_requestId);
        sb.append(" SEQ: ");
        sb.append(m_sequence);
        sb.append(" OF TOTAL: ");
        sb.append(m_ofTotal);
        sb.append(" SP HANDLE: ");
        sb.append(m_spHandle);
        sb.append(" PAYLOAD: ");
        if (m_payload == null) {
            sb.append("null");
        }
        else {
            sb.append(m_payload.toString());
        }
        return sb.toString();
    }
}
