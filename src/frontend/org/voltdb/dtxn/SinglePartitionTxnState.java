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

import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltdb.ClientResponseImpl;
import org.voltdb.ExecutionSite;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.TransactionIdManager;
import org.voltdb.VoltTable;
import org.voltdb.client.ClientResponse;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.messaging.InitiateTaskMessage;

public class SinglePartitionTxnState extends TransactionState {
    public SinglePartitionTxnState(Mailbox mbox,
                                   ExecutionSite site,
                                   TransactionInfoBaseMessage task)
    {
        super(mbox, site, task);
        assert(task instanceof InitiateTaskMessage) :
            "Creating single partition txn from invalid membership notice.";
    }

    @Override
    public boolean isSinglePartition()
    {
        return true;
    }

    // Single-partition transactions run only one place and it is always
    // the coordinator (replicas all run in parallel, not coordinated)
    @Override
    public boolean isCoordinator()
    {
        return true;
    }

    // Single-partition transactions should never block
    @Override
    public boolean isBlocked()
    {
        return false;
    }

    // Single-partition transactions better always touch persistent tables
    @Override
    public boolean hasTransactionalWork()
    {
        return true;
    }

    @Override
    public boolean doWork(boolean rejoining) {
        if (rejoining) {
            return doWorkRejoining();
        }
        if (!m_done) {
            m_site.beginNewTxn(this);
            InitiateTaskMessage task = (InitiateTaskMessage) m_notice;
            InitiateResponseMessage response = m_site.processInitiateTask(this, task);
            if (response.shouldCommit() == false) {
                m_needsRollback = true;
            }

            if (shouldSendResponse()) {
                m_mbox.send(initiatorHSId, response);
            }
            m_done = true;
        }
        return m_done;
    }

    private boolean doWorkRejoining() {
        if (!m_done) {
            InitiateTaskMessage task = (InitiateTaskMessage) m_notice;
            InitiateResponseMessage response = new InitiateResponseMessage(task);

            // add an empty dummy response
            response.setResults(new ClientResponseImpl(
                    ClientResponse.SUCCESS,
                    new VoltTable[0],
                    null));

            // this tells the initiator that the response is a dummy
            response.setRecovering(true);
            m_mbox.send(initiatorHSId, response);
            m_done = true;
        }
        return m_done;
    }

    @Override
    public String toString() {
        return "SinglePartitionTxnState initiator: " + initiatorHSId +
            " txnId: " + TransactionIdManager.toString(txnId);
    }

    @Override
    public void handleSiteFaults(HashSet<Long> failedSites) {
        // nothing to be done here.
    }

    @Override
    public boolean isDurable() {
        InitiateTaskMessage task = (InitiateTaskMessage) m_notice;
        java.util.concurrent.atomic.AtomicBoolean durableFlag = task.getDurabilityFlagIfItExists();
        return durableFlag == null ? true : durableFlag.get();
    }

    public InitiateTaskMessage getInitiateTaskMessage() {
        return (InitiateTaskMessage) m_notice;
    }

    @Override
    public StoredProcedureInvocation getInvocation() {
        InitiateTaskMessage task = (InitiateTaskMessage) m_notice;
        return task.getStoredProcedureInvocation();
    }
}
