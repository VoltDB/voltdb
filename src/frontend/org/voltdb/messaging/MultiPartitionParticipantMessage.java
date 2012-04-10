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

import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltcore.utils.CoreUtils;

public class MultiPartitionParticipantMessage extends TransactionInfoBaseMessage {

    MultiPartitionParticipantMessage() {
        super();
    }

    public MultiPartitionParticipantMessage(long initiatorHSId,
                                            long coordinatorHSId,
                                            long txnId,
                                            boolean isReadOnly) {
        super(initiatorHSId, coordinatorHSId, txnId, isReadOnly);
    }

    @Override
    public int getSerializedSize()
    {
        int msgsize = super.getSerializedSize();
        return msgsize;
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException
    {
        buf.put(VoltDbMessageFactory.PARTICIPANT_NOTICE_ID);
        super.flattenToBuffer(buf);
        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    @Override
    public void initFromBuffer(ByteBuffer buf) throws IOException {
        super.initFromBuffer(buf);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("MULTI_PARTITION_PARTICIPANT (FROM ");
        sb.append(CoreUtils.hsIdToString(getCoordinatorHSId()));
        sb.append(") FOR TXN ");
        sb.append(m_txnId);

        return sb.toString();
    }
}
