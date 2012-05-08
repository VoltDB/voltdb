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

import org.voltcore.messaging.Subject;
import org.voltcore.messaging.VoltMessage;

/**
 *
 */
public class RejoinResponseMessage extends VoltMessage {
    private long m_txnId;

    /** Empty constructor for de-serialization */
    public RejoinResponseMessage() {
        m_subject = Subject.DEFAULT.getId();
    }

    public RejoinResponseMessage(long sourceHSId, long txnId) {
        m_sourceHSId = sourceHSId;
        m_subject = Subject.DEFAULT.getId();
        m_txnId = txnId;
    }

    public long txnId() {
        return m_txnId;
    }

    @Override
    public int getSerializedSize() {
        int msgsize = super.getSerializedSize();
        msgsize +=
            8 + // m_sourceHSId
            8; // m_txnId
        return msgsize;
    }

    @Override
    protected void initFromBuffer(ByteBuffer buf) throws IOException {
        m_sourceHSId = buf.getLong();
        m_txnId = buf.getLong();

        assert(buf.capacity() == buf.position());
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException {
        buf.put(VoltDbMessageFactory.REJOIN_RESPONSE_ID);
        buf.putLong(m_sourceHSId);
        buf.putLong(m_txnId);

        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }
}
