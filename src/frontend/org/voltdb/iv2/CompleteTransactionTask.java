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

package org.voltdb.iv2;

import org.voltdb.SiteProcedureConnection;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.messaging.CompleteTransactionMessage;

public class CompleteTransactionTask extends TransactionTask
{
    final private CompleteTransactionMessage m_msg;

    public CompleteTransactionTask(TransactionState txn,
                                   TransactionTaskQueue queue,
                                   CompleteTransactionMessage msg)
    {
        super(txn, queue);
        m_msg = msg;
    }

    @Override
    public void run(SiteProcedureConnection siteConnection)
    {
        hostLog.debug("STARTING: " + this);
        siteConnection.truncateUndoLog(m_msg.isRollback(),
                                       m_txn.getBeginUndoToken(),
                                       m_msg.getTxnId());
        m_txn.setDone();
        m_queue.flush();
        hostLog.debug("COMPLETE: " + this);
    }

    @Override
    public long getMpTxnId()
    {
        return m_msg.getTxnId();
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("CompleteTransactionTask:");
        sb.append("  MP TXN ID: ").append(getMpTxnId());
        sb.append("  LOCAL TXN ID: ").append(getLocalTxnId());
        sb.append("  UNDO TOKEN: ").append(m_txn.getBeginUndoToken());
        sb.append("  MSG: ").append(m_msg.toString());
        return sb.toString();
    }
}
