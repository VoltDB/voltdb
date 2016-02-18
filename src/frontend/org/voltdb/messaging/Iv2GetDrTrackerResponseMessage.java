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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;
import org.voltdb.DRConsumerDrIdTracker;

/**
 * Message from a client interface to an initiator, instructing the
 * site to begin executing a stored procedure, coordinating other
 * execution sites if needed.
 *
 */
public class Iv2GetDrTrackerResponseMessage extends VoltMessage
{
    private long m_requestId = 0;

    private DRConsumerDrIdTracker m_tracker = null;
    /** Empty constructor for de-serialization */
    Iv2GetDrTrackerResponseMessage() {
        super();
    }

    public Iv2GetDrTrackerResponseMessage(long requestId, DRConsumerDrIdTracker tracker)
    {
        super();
        m_requestId = requestId;
        m_tracker = tracker;
    }

    public long getRequestId()
    {
        return m_requestId;
    }


    public DRConsumerDrIdTracker getTracker()
    {
        return m_tracker;
    }


    @Override
    public int getSerializedSize()
    {
        int msgsize = super.getSerializedSize();
        msgsize += 8; // requestId
        msgsize += m_tracker.getSerializedSize();
        return msgsize;
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException
    {
        buf.put(VoltDbMessageFactory.IV2_REPAIR_LOG_RESPONSE);
        buf.putLong(m_requestId);
        m_tracker.serialize(buf);

        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    @Override
    protected void initFromBuffer(ByteBuffer buf) throws IOException {
        m_requestId = buf.getLong();
        m_tracker = new DRConsumerDrIdTracker(buf);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("IV2 GET_DR_TRACKER_RESPONSE (FROM ");
        sb.append(CoreUtils.hsIdToString(m_sourceHSId));
        sb.append(" REQID: ");
        sb.append(m_requestId);
        sb.append(" SP UNIQUEID: ");
        sb.append(m_tracker.getLastSpUniqueId());
        sb.append(" MP UNIQUEID: ");
        sb.append(m_tracker.getLastMpUniqueId());
        sb.append(" LAST ACKED DRID: ");
        sb.append(m_tracker.getLastAckedDrId());
        return sb.toString();
    }
}
