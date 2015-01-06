/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
import org.voltcore.utils.Pair;

/**
 * Message from a client interface to an initiator, instructing the
 * site to begin executing a stored procedure, coordinating other
 * execution sites if needed.
 *
 */
public class Iv2RepairLogResponseMessage extends VoltMessage
{
    private long m_requestId = 0;
    private int m_sequence = 0;
    private int m_ofTotal = 0;
    private long m_handle = Long.MIN_VALUE;
    private long m_txnId;

    // Only set when sequence is 0
    private long m_hashinatorVersion = Long.MIN_VALUE;

    // The original task that is must be replicated for
    // repair. Note: if the destination repair log is
    // empty, a repair log response message is returned
    // that has sequence = 0; ofTotal = 0 and a null
    // payload (because the requester must know that the
    // log request was processed and that no logs exist.)
    private VoltMessage m_payload = null;

    // Only set when sequence is 0
    private byte [] m_hashinatorConfig = new byte[0];

    /** Empty constructor for de-serialization */
    Iv2RepairLogResponseMessage() {
        super();
    }

    public Iv2RepairLogResponseMessage(long requestId, int sequence,
            int ofTotal, long spHandle, long txnId, VoltMessage payload)
    {
        super();
        m_requestId = requestId;
        m_sequence = sequence;
        m_ofTotal = ofTotal;
        m_handle = spHandle;
        m_txnId = txnId;
        m_payload = payload;
    }

    public Iv2RepairLogResponseMessage(long requestId, int ofTotal,
            long spHandle, long txnId,
            Pair<Long, byte[]> versionedHashinatorConfig)
    {
        super();
        m_requestId = requestId;
        m_sequence = 0;
        m_ofTotal = ofTotal;
        m_handle = spHandle;
        m_txnId = txnId;
        m_hashinatorVersion = versionedHashinatorConfig.getFirst();
        m_hashinatorConfig = versionedHashinatorConfig.getSecond();
    }

    public long getRequestId()
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

    public long getHandle()
    {
        return m_handle;
    }

    public long getTxnId() {
        return m_txnId;
    }

    public VoltMessage getPayload()
    {
        return m_payload;
    }

    /**
     * Get version/config with the config in compressed (wire) format.
     * @return version/config pair
     */
    public Pair<Long, byte[]> getHashinatorVersionedConfig()
    {
        return Pair.of(m_hashinatorVersion, m_hashinatorConfig);
    }

    public boolean hasHashinatorConfig()
    {
        return m_sequence == 0 && m_hashinatorConfig.length > 0;
    }

    @Override
    public int getSerializedSize()
    {
        int msgsize = super.getSerializedSize();
        msgsize += 8; // requestId
        msgsize += 4; // sequence
        msgsize += 4; // ofTotal
        msgsize += 8; // spHandle
        msgsize += 8; // txnId
        if (m_payload != null) {
            msgsize += m_payload.getSerializedSize();
        }
        if (m_hashinatorConfig.length > 0) {
            msgsize += 8; // hashinator version
            msgsize += 4; // config size
            msgsize += m_hashinatorConfig.length;
        }
        return msgsize;
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException
    {
        buf.put(VoltDbMessageFactory.IV2_REPAIR_LOG_RESPONSE);
        buf.putLong(m_requestId);
        buf.putInt(m_sequence);
        buf.putInt(m_ofTotal);
        buf.putLong(m_handle);
        buf.putLong(m_txnId);

        if (m_payload != null) {
            ByteBuffer paybuf = ByteBuffer.allocate(m_payload.getSerializedSize());
            m_payload.flattenToBuffer(paybuf);
            if (paybuf.position() != 0) {
                paybuf.flip();
            }
            buf.put(paybuf);
        }
        if (m_hashinatorConfig.length > 0) {
            buf.putLong(m_hashinatorVersion);
            buf.putInt(m_hashinatorConfig.length);
            buf.put(m_hashinatorConfig);
        }

        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    @Override
    protected void initFromBuffer(ByteBuffer buf) throws IOException {
        m_requestId = buf.getLong();
        m_sequence = buf.getInt();
        m_ofTotal = buf.getInt();
        m_handle = buf.getLong();
        m_txnId = buf.getLong();

        // going inception.
        // The first message in the repair log response stream is always a null
        // ack, so don't try to deserialize a message that won't exist.
        if (m_sequence != 0) {
            VoltDbMessageFactory messageFactory = new VoltDbMessageFactory();
            m_payload = messageFactory.createMessageFromBuffer(buf, m_sourceHSId);
        }
        // only the first packet with sequence 0 has the hashinator configurations
        else {
            m_payload = null;
            m_hashinatorVersion = buf.getLong();
            m_hashinatorConfig = new byte[buf.getInt()];
            buf.get(m_hashinatorConfig);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("IV2 REPAIR_LOG_RESPONSE (FROM ");
        sb.append(CoreUtils.hsIdToString(m_sourceHSId));
        sb.append(" REQID: ");
        sb.append(m_requestId);
        sb.append(" SEQ: ");
        sb.append(m_sequence);
        sb.append(" OF TOTAL: ");
        sb.append(m_ofTotal);
        sb.append(" SP HANDLE: ");
        sb.append(m_handle);
        sb.append(" TXNID: ");
        sb.append(m_txnId);
        sb.append(" PAYLOAD: ");
        if (m_payload == null) {
            sb.append("null");
        }
        else {
            sb.append(m_payload.toString());
        }
        if (m_hashinatorConfig.length > 0)
        {
            sb.append( " HASHINATOR VERSION: ");
            sb.append(m_hashinatorVersion);
        }
        return sb.toString();
    }
}
