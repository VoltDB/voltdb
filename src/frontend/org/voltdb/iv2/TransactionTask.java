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

import org.voltcore.logging.VoltLogger;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.dtxn.TransactionState;

public abstract class TransactionTask extends SiteTasker
{
    protected static final VoltLogger execLog = new VoltLogger("EXEC");
    protected static final VoltLogger hostLog = new VoltLogger("HOST");

    final protected TransactionState m_txn;
    final protected TransactionTaskQueue m_queue;

    public TransactionTask(TransactionState txn, TransactionTaskQueue queue)
    {
        m_txn = txn;
        m_queue = queue;
    }

    @Override
    abstract public void run(SiteProcedureConnection siteConnection);

    // run from the live rejoin task log.
    abstract public void runFromTaskLog(SiteProcedureConnection siteConnection);

    public TransactionState getTransactionState()
    {
        return m_txn;
    }

    public long getSpHandle()
    {
        return m_txn.spHandle;
    }

    public long getTxnId() {
        return m_txn.txnId;
    }
}
