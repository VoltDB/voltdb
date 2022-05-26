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
import org.voltdb.iv2.TxnEgo;

/**
 * Message issued from an initiator just after newly leader promotion, helping flushing
 * the transaction task queue on all partition replicas.
 */
public class DummyTransactionTaskMessage extends TransactionInfoBaseMessage
{
    public DummyTransactionTaskMessage()
    {
        super();
        m_isReadOnly = true;
        m_isForReplay = false;
    }

    public DummyTransactionTaskMessage (long initiatorHSId, long txnId, long uniqueId) {
        super(initiatorHSId, initiatorHSId, txnId, uniqueId, true, false);
        m_isReadOnly = true;
        m_isForReplay = false;
        setSpHandle(txnId);
    }

    @Override
    public boolean isSinglePartition() {
        return true;
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException
    {
        buf.put(VoltDbMessageFactory.DUMMY_TRANSACTION_TASK_ID);
        super.flattenToBuffer(buf);

        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    @Override
    public void toDuplicateCounterString(StringBuilder sb) {
        sb.append("Unexpected DummyTransactionTask");
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("DummyTaskMessage (FROM ");
        sb.append(CoreUtils.hsIdToString(m_sourceHSId));
        sb.append(" TO ");
        sb.append(CoreUtils.hsIdToString(getCoordinatorHSId()));
        sb.append(") FOR TXN ").append(TxnEgo.txnIdToString(m_txnId));
        sb.append("SP HANDLE: ").append(TxnEgo.txnIdToString(getSpHandle())).append("\n");
        return sb.toString();
    }
}
