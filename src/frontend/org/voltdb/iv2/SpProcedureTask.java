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

import java.io.IOException;

import org.voltcore.logging.Level;
import org.voltcore.messaging.Mailbox;
import org.voltcore.utils.CoreUtils;
import org.voltdb.ClientResponseImpl;
import org.voltdb.PartitionDRGateway;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.VoltTable;
import org.voltdb.client.ClientResponse;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;
import org.voltdb.rejoin.TaskLog;
import org.voltdb.utils.LogKeys;

/**
 * Implements the single partition procedure ProcedureTask.
 * Runs single partition procedures against a SiteConnection
 */
public class SpProcedureTask extends ProcedureTask
{
    final private PartitionDRGateway m_drGateway;

    SpProcedureTask(Mailbox initiator, String procName, TransactionTaskQueue queue,
                  Iv2InitiateTaskMessage msg,
                  PartitionDRGateway drGateway)
    {
       super(initiator, procName, new SpTransactionState(msg), queue);
       m_drGateway = drGateway;
    }

    /** Run is invoked by a run-loop to execute this transaction. */
    @Override
    public void run(SiteProcedureConnection siteConnection)
    {
        hostLog.debug("STARTING: " + this);
        if (!m_txn.isReadOnly()) {
            m_txn.setBeginUndoToken(siteConnection.getLatestUndoToken());
        }

        // cast up here .. ugly.
        SpTransactionState txn = (SpTransactionState)m_txn;
        final InitiateResponseMessage response = processInitiateTask(txn.m_task, siteConnection);
        int hash = m_txn.getHash();
        if (!response.shouldCommit()) {
            m_txn.setNeedsRollback();
        }
        completeInitiateTask(siteConnection);
        response.m_sourceHSId = m_initiator.getHSId();
        m_initiator.deliver(response);
        execLog.l7dlog( Level.TRACE, LogKeys.org_voltdb_ExecutionSite_SendingCompletedWUToDtxn.name(), null);
        hostLog.debug("COMPLETE: " + this);

        // Log invocation to DR
        if (m_drGateway != null && !m_txn.isReadOnly() && !m_txn.needsRollback()) {
            m_drGateway.onSuccessfulProcedureCall(txn.txnId, txn.timestamp, hash,
                                                  txn.getInvocation(),
                                                  response.getClientResponseData());
        }
    }

    @Override
    public void runForRejoin(SiteProcedureConnection siteConnection, TaskLog taskLog)
    throws IOException
    {
        taskLog.logTask(m_txn.getNotice());
        SpTransactionState txn = (SpTransactionState)m_txn;
        final InitiateResponseMessage response =
            new InitiateResponseMessage(txn.m_task);
        response.m_sourceHSId = m_initiator.getHSId();
        response.setRecovering(true);

        // add an empty dummy response
        response.setResults(new ClientResponseImpl(
                    ClientResponse.SUCCESS,
                    new VoltTable[0],
                    null));

        m_initiator.deliver(response);
    }

    // This is an ugly copy/paste mix of run() and completeInitiateTask()
    // that avoids using the mailbox -- since no response should be
    // generated...
    @Override
    public void runFromTaskLog(SiteProcedureConnection siteConnection)
    {
        if (hostLog.isTraceEnabled()) {
            hostLog.trace("START replaying txn: " + this);
        }
        if (!m_txn.isReadOnly()) {
            m_txn.setBeginUndoToken(siteConnection.getLatestUndoToken());
        }

        // cast up here .. ugly.
        SpTransactionState txn = (SpTransactionState)m_txn;
        final InitiateResponseMessage response = processInitiateTask(txn.m_task, siteConnection);
        if (!response.shouldCommit()) {
            m_txn.setNeedsRollback();
        }
        if (!m_txn.isReadOnly()) {
            assert(siteConnection.getLatestUndoToken() != Site.kInvalidUndoToken) :
                "[SP][RW] transaction found invalid latest undo token state in Iv2ExecutionSite.";
            assert(siteConnection.getLatestUndoToken() >= m_txn.getBeginUndoToken()) :
                "[SP][RW] transaction's undo log token farther advanced than latest known value.";
            assert (m_txn.getBeginUndoToken() != Site.kInvalidUndoToken) :
                "[SP][RW] with invalid undo token in completeInitiateTask.";

            // the truncation point token SHOULD be part of m_txn. However, the
            // legacy interaces don't work this way and IV2 hasn't changed this
            // ownership yet. But truncateUndoLog is written assuming the right
            // eventual encapsulation.
            siteConnection.truncateUndoLog(m_txn.needsRollback(), m_txn.getBeginUndoToken(), m_txn.txnId, m_txn.spHandle);
        }
        m_txn.setDone();
        execLog.l7dlog( Level.TRACE, LogKeys.org_voltdb_ExecutionSite_SendingCompletedWUToDtxn.name(), null);
        if (hostLog.isTraceEnabled()) {
            hostLog.trace("COMPLETE replaying txn: " + this);
        }
    }


    @Override
    void completeInitiateTask(SiteProcedureConnection siteConnection)
    {
        if (!m_txn.isReadOnly()) {
            assert(siteConnection.getLatestUndoToken() != Site.kInvalidUndoToken) :
                "[SP][RW] transaction found invalid latest undo token state in Iv2ExecutionSite.";
            assert(siteConnection.getLatestUndoToken() >= m_txn.getBeginUndoToken()) :
                "[SP][RW] transaction's undo log token farther advanced than latest known value.";
            assert (m_txn.getBeginUndoToken() != Site.kInvalidUndoToken) :
                "[SP][RW] with invalid undo token in completeInitiateTask.";

            // the truncation point token SHOULD be part of m_txn. However, the
            // legacy interaces don't work this way and IV2 hasn't changed this
            // ownership yet. But truncateUndoLog is written assuming the right
            // eventual encapsulation.
            siteConnection.truncateUndoLog(m_txn.needsRollback(), m_txn.getBeginUndoToken(), m_txn.txnId, m_txn.spHandle);
        }
        m_txn.setDone();
        m_queue.flush();
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("SpProcedureTask:");
        sb.append("  TXN ID: ").append(TxnEgo.txnIdToString(getTxnId()));
        sb.append("  SP HANDLE ID: ").append(TxnEgo.txnIdToString(getSpHandle()));
        sb.append("  ON HSID: ").append(CoreUtils.hsIdToString(m_initiator.getHSId()));
        return sb.toString();
    }
}
