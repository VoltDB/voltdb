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
public class Iv2RepairLogRequestMessage extends VoltMessage
{
    private int m_requestId = 0;

    /** Empty constructor for de-serialization */
    Iv2RepairLogRequestMessage() {
        super();
    }

    public Iv2RepairLogRequestMessage(int requestId)
    {
        super();
        m_requestId = requestId;
    }

    public int getRequestId()
    {
        return m_requestId;
    }

    @Override
    public int getSerializedSize()
    {
        int msgsize = super.getSerializedSize();
        msgsize += 4; // requestId
        return msgsize;
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException
    {
        buf.put(VoltDbMessageFactory.IV2_REPAIR_LOG_REQUEST);
        buf.putInt(m_requestId);

        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    @Override
    public void initFromBuffer(ByteBuffer buf) throws IOException {
        m_requestId = buf.getInt();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("IV2 REPAIR_LOG_REQUEST (FROM ");
        sb.append(CoreUtils.hsIdToString(m_sourceHSId));
        sb.append(" REQID: ");
        sb.append(m_requestId);
        return sb.toString();
    }
}
