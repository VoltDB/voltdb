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

import java.nio.ByteBuffer;

import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;

public class CompleteTransactionResponseMessage extends VoltMessage
{
    long m_executionHSId;
    long m_txnId;

    /** Empty constructor for de-serialization */
    CompleteTransactionResponseMessage() {
        super();
    }

    public CompleteTransactionResponseMessage(CompleteTransactionMessage msg,
                                              long siteId)
    {
        m_executionHSId = siteId;
        m_txnId = msg.getTxnId();
    }

    public long getTxnId()
    {
        return m_txnId;
    }

    public long getExecutionSiteId()
    {
        return m_executionHSId;
    }

    @Override
    public int getSerializedSize()
    {
        int msgsize = super.getSerializedSize();
        msgsize += 8 + 8;
        return msgsize;
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf)
    {
        buf.put(VoltDbMessageFactory.COMPLETE_TRANSACTION_RESPONSE_ID);
        buf.putLong(m_executionHSId);
        buf.putLong(m_txnId);
        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    @Override
    protected void initFromBuffer(ByteBuffer buf)
    {
        m_executionHSId = buf.getLong();
        m_txnId = buf.getLong();
        assert(buf.capacity() == buf.position());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("COMPLETE_TRANSACTION_RESPONSE");
        sb.append(" (FROM EXEC SITE: ");
        sb.append(CoreUtils.hsIdToString(m_executionHSId));
        sb.append(") FOR TXN ID: ");
        sb.append(m_txnId);

        return sb.toString();
    }
}
