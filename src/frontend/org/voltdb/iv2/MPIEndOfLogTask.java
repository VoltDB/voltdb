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
import java.util.List;

import org.voltcore.logging.Level;
import org.voltcore.messaging.Mailbox;
import org.voltcore.utils.CoreUtils;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.rejoin.TaskLog;
import org.voltdb.utils.LogKeys;

public class MPIEndOfLogTask extends TransactionTask
{
    final long[] m_initiatorHSIds;
    final Mailbox m_mailbox;

    MPIEndOfLogTask(Mailbox mailbox, TransactionTaskQueue queue,
                    MPIEndOfLogTransactionState txnState, List<Long> pInitiators)
    {
        super(txnState, queue);
        m_initiatorHSIds = com.google_voltpatches.common.primitives.Longs.toArray(pInitiators);
        m_mailbox = mailbox;
    }

    /** Run is invoked by a run-loop to execute this transaction. */
    @Override
    public void run(SiteProcedureConnection siteConnection)
    {
        hostLog.debug("STARTING: " + this);
        m_mailbox.send(m_initiatorHSIds, m_txnState.getNotice());
        m_txnState.setDone();
        m_queue.flush(getTxnId());
        execLog.l7dlog( Level.TRACE, LogKeys.org_voltdb_ExecutionSite_SendingCompletedWUToDtxn.name(), null);
        hostLog.debug("COMPLETE: " + this);
    }

    @Override
    public void runForRejoin(SiteProcedureConnection siteConnection, TaskLog taskLog)
    throws IOException
    {
        throw new RuntimeException("MPI asked to execute MPI end of log task while rejoining.");
    }

    @Override
    public void runFromTaskLog(SiteProcedureConnection siteConnection)
    {
        throw new RuntimeException("MPI asked to execute MPI end of log task from task log while rejoining.");
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("MPIEndOfLogTask:");
        sb.append("  ON HSID: ").append(CoreUtils.hsIdToString(m_mailbox.getHSId()));
        return sb.toString();
    }
}
