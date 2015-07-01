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

import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

import org.voltcore.logging.Level;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltcore.utils.CoreUtils;

import org.voltdb.rejoin.TaskLog;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.utils.LogKeys;

/**
 * Implements the Single-partition everywhere procedure ProcedureTask.
 * Creates one SP transaction per partition. These are produced on
 * the MPI so that all involved SPs have the same happens-before
 * relationship with concurrent MPs.
 */
public class EveryPartitionTask extends TransactionTask
{
    final List<Long> m_initiatorHSIds = new ArrayList<Long>();
    final TransactionInfoBaseMessage m_initiationMsg;
    final Mailbox m_mailbox;

    EveryPartitionTask(Mailbox mailbox, TransactionTaskQueue queue,
                  TransactionInfoBaseMessage msg, List<Long> pInitiators)
    {
        super(new EveryPartTransactionState(msg), queue);
        m_initiationMsg = msg;
        m_initiatorHSIds.addAll(pInitiators);
        m_mailbox = mailbox;
    }

    /** Run is invoked by a run-loop to execute this transaction. */
    @Override
    public void run(SiteProcedureConnection siteConnection)
    {
        hostLog.debug("STARTING: " + this);
        m_mailbox.send(com.google_voltpatches.common.primitives.Longs.toArray(m_initiatorHSIds), m_initiationMsg);
        m_txnState.setDone();
        m_queue.flush(getTxnId());
        execLog.l7dlog( Level.TRACE, LogKeys.org_voltdb_ExecutionSite_SendingCompletedWUToDtxn.name(), null);
        hostLog.debug("COMPLETE: " + this);
    }

    @Override
    public void runForRejoin(SiteProcedureConnection siteConnection, TaskLog taskLog)
    throws IOException
    {
        throw new RuntimeException("MPI asked to execute everysite proc. while rejoining.");
    }

    @Override
    public void runFromTaskLog(SiteProcedureConnection siteConnection)
    {
        throw new RuntimeException("MPI asked to execute everysite proc from task log while rejoining.");
    }

    /**
     * Update the list of partition masters in the event of a failure/promotion.
     * Currently only thread-"safe" by virtue of only calling this on
     * ProcedureTasks which are not at the head of the MPI's TransactionTaskQueue.
     */
    public void updateMasters(List<Long> masters)
    {
        m_initiatorHSIds.clear();
        m_initiatorHSIds.addAll(masters);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("EveryPartitionTask:");
        sb.append("  TXN ID: ").append(getTxnId());
        sb.append("  SP HANDLE ID: ").append(getSpHandle());
        sb.append("  ON HSID: ").append(CoreUtils.hsIdToString(m_mailbox.getHSId()));
        return sb.toString();
    }
}
