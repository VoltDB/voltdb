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
import org.voltcore.utils.CoreUtils;
import org.voltdb.iv2.TxnEgo;
import org.voltdb.iv2.UniqueIdGenerator;

/**
 * Message from a client interface to an initiator, instructing the
 * site to begin executing a stored procedure, coordinating other
 * execution sites if needed.
 *
 */
public class Iv2LogFaultMessage extends VoltMessage
{
    private long m_spHandle;
    private long m_spUniqueId;

    /** Empty constructor for de-serialization */
    Iv2LogFaultMessage() {
        super();
    }

    public Iv2LogFaultMessage(long spHandle, long spUniqueId)
    {
        super();
        m_spHandle = spHandle;
        m_spUniqueId = spUniqueId;
    }

    public long getSpHandle()
    {
        return m_spHandle;
    }

    public long getSpUniqueId() {
        return m_spUniqueId;
    }

    @Override
    public int getSerializedSize()
    {
        int msgsize = super.getSerializedSize();
        msgsize += 8  // spHandle
                 + 8; // spUniqueId
        return msgsize;
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException
    {
        buf.put(VoltDbMessageFactory.IV2_LOG_FAULT_ID);
        buf.putLong(m_spHandle);
        buf.putLong(m_spUniqueId);

        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    @Override
    public void initFromBuffer(ByteBuffer buf) throws IOException {
        m_spHandle = buf.getLong();
        m_spUniqueId = buf.getLong();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("IV2 LOG_FAULT (FROM ");
        sb.append(CoreUtils.hsIdToString(m_sourceHSId));
        sb.append(" SPHANDLE: ");
        sb.append(TxnEgo.txnIdToString(m_spHandle));
        sb.append(" SPUNIQUEID: ");
        sb.append(UniqueIdGenerator.toShortString(m_spUniqueId));
        return sb.toString();
    }
}
