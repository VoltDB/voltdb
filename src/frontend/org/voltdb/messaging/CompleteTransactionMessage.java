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
import org.voltcore.utils.MiscUtils;

public class CompleteTransactionMessage extends TransactionInfoBaseMessage
{
    boolean m_isRollback;
    boolean m_requiresAck;

    /** Empty constructor for de-serialization */
    CompleteTransactionMessage() {
        super();
    }

    /**
     * These four args needed for base class
     * @param initiatorHSId
     * @param coordinatorHSId
     * @param txnId
     * @param isReadOnly
     *
     * @param isRollback  Should the recipient rollback this transaction to complete it?
     * @param requiresAck  Does the recipient need to respond to this message
     *                     with a CompleteTransactionResponseMessage?
     */
    public CompleteTransactionMessage(long initiatorHSId, long coordinatorHSId,
                                      long txnId, boolean isReadOnly,
                                      boolean isRollback, boolean requiresAck)
    {
        super(initiatorHSId, coordinatorHSId, txnId, isReadOnly);
        m_isRollback = isRollback;
        m_requiresAck = requiresAck;
    }

    public boolean isRollback()
    {
        return m_isRollback;
    }

    public boolean requiresAck()
    {
        return m_requiresAck;
    }

    @Override
    public int getSerializedSize()
    {
        int msgsize = super.getSerializedSize();
        msgsize += 1 + 1;
        return msgsize;
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException
    {
        buf.put(VoltDbMessageFactory.COMPLETE_TRANSACTION_ID);
        super.flattenToBuffer(buf);
        buf.put(m_isRollback ? (byte) 1 : (byte) 0);
        buf.put(m_requiresAck ? (byte) 1 : (byte) 0);
        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    @Override
    public void initFromBuffer(ByteBuffer buf) throws IOException
    {
        super.initFromBuffer(buf);
        m_isRollback = buf.get() == 1;
        m_requiresAck = buf.get() == 1;
        assert(buf.capacity() == buf.position());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("COMPLETE_TRANSACTION (FROM COORD: ");
        sb.append(MiscUtils.hsIdToString(m_coordinatorHSId));
        sb.append(") FOR TXN ");
        sb.append(m_txnId);

        if (m_isRollback)
            sb.append("\n  THIS IS AN ROLLBACK REQUEST");

        if (m_requiresAck)
            sb.append("\n  THIS MESSAGE REQUIRES AN ACK");

        return sb.toString();
    }
}
