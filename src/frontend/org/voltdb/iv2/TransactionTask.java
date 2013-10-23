/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

package org.voltdb.iv2;

import org.voltcore.logging.VoltLogger;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.dtxn.TransactionState;

public abstract class TransactionTask extends SiteTasker
{
    protected static final VoltLogger execLog = new VoltLogger("EXEC");
    protected static final VoltLogger hostLog = new VoltLogger("HOST");

    final protected TransactionState m_txnState;
    final protected TransactionTaskQueue m_queue;

    public TransactionTask(TransactionState txnState, TransactionTaskQueue queue)
    {
        m_txnState = txnState;
        m_queue = queue;
    }

    @Override
    abstract public void run(SiteProcedureConnection siteConnection);

    // run from the live rejoin task log.
    abstract public void runFromTaskLog(SiteProcedureConnection siteConnection);

    public TransactionState getTransactionState()
    {
        return m_txnState;
    }

    public long getSpHandle()
    {
        return m_txnState.m_spHandle;
    }

    public long getTxnId() {
        return m_txnState.txnId;
    }

    // Take actions common to all transactions in order to complete a transaction at an SPI
    // Nebulously defined, I know, but replicating these two lines in a bazillion places
    // began to offend me.
    void doCommonSPICompleteActions()
    {
        // Mark the transaction state as DONE
        m_txnState.setDone();
        // Flush us out of the head of the TransactionTaskQueue.  Null check so we're reusable
        // for live rejoin replay
        if (m_queue != null) {
            m_queue.flush(getTxnId());
        }
    }
}
