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

package org.voltdb.iv2;

import com.google_voltpatches.common.util.concurrent.ListenableFuture;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.VoltDB;
import org.voltdb.dtxn.TransactionState;

public abstract class TransactionTask extends SiteTasker
{
    protected static final VoltLogger execLog = new VoltLogger("EXEC");
    protected static final VoltLogger hostLog = new VoltLogger("HOST");

    final protected TransactionState m_txnState;
    final protected TransactionTaskQueue m_queue;
    protected ListenableFuture<Object> m_durabilityBackpressureFuture = CoreUtils.COMPLETED_FUTURE;

    public TransactionTask(TransactionState txnState, TransactionTaskQueue queue)
    {
        m_txnState = txnState;
        m_queue = queue;
    }

    public TransactionTask setDurabilityBackpressureFuture(ListenableFuture<Object> fut) {
        m_durabilityBackpressureFuture = fut;
        return this;
    }

    /*
     * If async command logging is in use and the command log isn't keeping up
     * an incomplete future will be populated here and can be waited on.
     *
     * Otherwise it will be null or will be populated with a completed future that
     * returns immediately.
     */
    protected void waitOnDurabilityBackpressureFuture() {
        try {
            m_durabilityBackpressureFuture.get();
        } catch (Throwable t) {
            VoltDB.crashLocalVoltDB("Unexpected exception waiting for durability future", true, t);
        }
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
