/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

import org.voltdb.utils.DBBPool;

public class CompleteTransactionResponseMessage extends VoltMessage
{
    int m_executionSiteId;
    long m_txnId;

    /** Empty constructor for de-serialization */
    CompleteTransactionResponseMessage() {
        super();
    }

    public CompleteTransactionResponseMessage(CompleteTransactionMessage msg,
                                              int siteId)
    {
        m_executionSiteId = siteId;
        m_txnId = msg.getTxnId();
    }

    public long getTxnId()
    {
        return m_txnId;
    }

    public int getExecutionSiteId()
    {
        return m_executionSiteId;
    }

    @Override
    protected void flattenToBuffer(DBBPool pool) throws IOException
    {
        int msgsize = 4 + 8;

        if (m_buffer == null) {
            m_container = pool.acquire(msgsize + 1 + HEADER_SIZE);
            m_buffer = m_container.b;
        }
        setBufferSize(msgsize + 1, pool);

        m_buffer.position(HEADER_SIZE);
        m_buffer.put(COMPLETE_TRANSACTION_RESPONSE_ID);

        m_buffer.putInt(m_executionSiteId);
        m_buffer.putLong(m_txnId);
        m_buffer.limit(m_buffer.position());
    }

    @Override
    protected void initFromBuffer()
    {
        m_buffer.position(HEADER_SIZE + 1); // skip the msg id

        m_executionSiteId = m_buffer.getInt();
        m_txnId = m_buffer.getLong();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("COMPLETE_TRANSACTION_RESPONSE");
        sb.append(" (FROM EXEC SITE: ");
        sb.append(m_executionSiteId);
        sb.append(") FOR TXN ID: ");
        sb.append(m_txnId);

        return sb.toString();
    }
}
