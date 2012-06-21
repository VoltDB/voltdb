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

package org.voltdb.dtxn;

import java.util.HashSet;

import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltdb.ExecutionSite;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskLogMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.messaging.InitiateTaskMessage;

/**
 * TransactionState subclass for replaying transactions during a rejoin,
 * or perhaps someday during DR data-movement. Runs either single partition
 * work, or a log of local FragmentTaskMessages run during a multi-part
 * procedure.
 *
 * Note that this should never block and can run as single part work.
 *
 * Unlike other TransactionState classes, this will never send a response
 * using a mailbox. It just silently does the work.
 *
 * It also assumes success, because hopeful, one wouldn't replay transactions
 * that failed.
 *
 */
public class ReplayedTxnState extends TransactionState {

    public ReplayedTxnState(ExecutionSite site, TransactionInfoBaseMessage notice)
    {
        super(null, site, notice);
        m_rejoinState = RejoinState.REPLAYING;

        // only these two notices are supported for replay
        assert((notice instanceof InitiateTaskMessage) ||
               (notice instanceof FragmentTaskLogMessage));
    }

    /**
     * Replaying a multi-partition transaction is independent and can
     * safely be treated as single-partition.
     */
    @Override
    public boolean isSinglePartition() {
        return true;
    }

    @Override
    public boolean isCoordinator() {
        return true;
    }

    @Override
    public boolean isBlocked() {
        return false;
    }

    @Override
    public boolean hasTransactionalWork() {
        return true;
    }

    @Override
    public boolean doWork(boolean rejoining) {
        if (!m_done) {
            m_site.beginNewTxn(this);
            if (m_notice instanceof InitiateTaskMessage) {
                InitiateTaskMessage task = (InitiateTaskMessage) m_notice;
                InitiateResponseMessage response = m_site.processInitiateTask(this, task);
                // we shouldn't replay transactions that rollback
                // if it rolls back here... that's non-deterministic and sad
                assert (response.shouldCommit() == false);
            }
            else if (m_notice instanceof FragmentTaskLogMessage) {
                FragmentTaskLogMessage taskLog = (FragmentTaskLogMessage) m_notice;
                for (FragmentTaskMessage ftask : taskLog.getFragmentTasks()) {
                    FragmentResponseMessage response = m_site.processFragmentTask(this, null, ftask);
                    assert (response.getStatusCode() == FragmentResponseMessage.SUCCESS);
                }
            }
            else {
                assert(false);
            }
            m_done = true;
        }
        return m_done;
    }

    @Override
    public StoredProcedureInvocation getInvocation() {
        return null;
    }

    @Override
    public void handleSiteFaults(HashSet<Long> failedSites) {
        // nothing to be done here.
    }

}
