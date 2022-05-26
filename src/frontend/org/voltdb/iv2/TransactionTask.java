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

package org.voltdb.iv2;

import java.util.List;
import java.util.Map;

import org.voltcore.logging.VoltLogger;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.utils.VoltTableUtil;

import com.google_voltpatches.common.util.concurrent.ListenableFuture;

public abstract class TransactionTask extends SiteTasker
{
    protected static final VoltLogger execLog = new VoltLogger("EXEC");
    protected static final VoltLogger hostLog = new VoltLogger("HOST");
    public static VoltTable DUMMAY_RESULT_TABLE;
    protected static final byte[] RAW_DUMMY_RESULT;

    static {
        VoltTable dummyResult = new VoltTable(new ColumnInfo("UNUSED", VoltType.INTEGER));
        dummyResult.setStatusCode(VoltTableUtil.DUMMY_DEPENDENCY_STATUS);
        RAW_DUMMY_RESULT = dummyResult.buildReusableDependenyResult();
        DUMMAY_RESULT_TABLE = new VoltTable(new ColumnInfo("UNUSED", VoltType.INTEGER));
    }

    final protected TransactionState m_txnState;
    final protected TransactionTaskQueue m_queue;
    protected ListenableFuture<Object> m_durabilityBackpressureFuture = null;

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
        if (m_durabilityBackpressureFuture != null) {
            try {
                m_durabilityBackpressureFuture.get();
            } catch (Throwable t) {
                VoltDB.crashLocalVoltDB("Unexpected exception waiting for durability future", true, t);
            }
            durabilityTraceEnd();
        }
    }

    protected void durabilityTraceEnd() {}

    public void updateMasters(List<Long> masters, Map<Integer, Long> partitionMaster) {}

    @Override
    abstract public void run(SiteProcedureConnection siteConnection);

    // run from the live rejoin task log.
    abstract public void runFromTaskLog(SiteProcedureConnection siteConnection);

    // MP read-write task need to be coordinated.
    abstract public boolean needCoordination();

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

    @Override
    public String getTaskInfo() {
        return getClass().getSimpleName() + " TxnId:" + TxnEgo.txnIdToString(getTxnId());
    }
}
