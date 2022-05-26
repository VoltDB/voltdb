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

import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltcore.utils.CoreUtils;

public class MultiPartitionParticipantMessage extends TransactionInfoBaseMessage {

    long m_ciHandle;
    long m_connectionId;

    MultiPartitionParticipantMessage() {
        super();
    }

    public MultiPartitionParticipantMessage(long initiatorHSId,
                                            long coordinatorHSId,
                                            long txnId,
                                            boolean isReadOnly) {
        super(initiatorHSId,
                coordinatorHSId,
                txnId,
                txnId,
                isReadOnly,
                false);

        m_ciHandle = -1;
        m_connectionId = -1;
    }

    public MultiPartitionParticipantMessage(long initiatorHSId,
                                            long coordinatorHSId,
                                            long uniqueId,
                                            long ciHandle,
                                            long connectionId,
                                            boolean isReadOnly,
                                            boolean isForReplay) {
        super(initiatorHSId,
                coordinatorHSId,
                -1,
                uniqueId,
                isReadOnly,
                isForReplay);

        m_ciHandle = ciHandle;
        m_connectionId = connectionId;
    }

    public long getClientInterfaceHandle()
    {
        return m_ciHandle;
    }

    public long getConnectionId()
    {
        return m_connectionId;
    }

    @Override
    public int getSerializedSize()
    {
        int msgsize = super.getSerializedSize()
                + 8 // m_ciHandle
                + 8; // m_connectionId
        return msgsize;
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException
    {
        buf.put(VoltDbMessageFactory.PARTICIPANT_NOTICE_ID);
        super.flattenToBuffer(buf);
        buf.putLong(m_ciHandle);
        buf.putLong(m_connectionId);
        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    @Override
    public void initFromBuffer(ByteBuffer buf) throws IOException {
        super.initFromBuffer(buf);
        m_ciHandle = buf.getLong();
        m_connectionId = buf.getLong();
    }

    @Override
    public void toDuplicateCounterString(StringBuilder sb) {
        sb.append("UNEXPECTED REPLAY SENTINAL");
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("MULTI_PARTITION_PARTICIPANT (FROM ");
        sb.append(CoreUtils.hsIdToString(getCoordinatorHSId()));
        sb.append(") FOR TXN ");
        sb.append(m_txnId);
        sb.append(" CLIENTINTERFACEHANDLE ");
        sb.append(m_ciHandle);
        sb.append(" CONNECTIONID ");
        sb.append(m_connectionId);

        return sb.toString();
    }
}
