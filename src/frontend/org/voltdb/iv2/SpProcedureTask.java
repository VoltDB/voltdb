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

import org.voltcore.logging.Level;
import org.voltcore.messaging.Mailbox;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.LatencyWatchdog;
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

    private static final boolean EXEC_TRACE_ENABLED;
    private static final boolean HOST_DEBUG_ENABLED;
    private static final boolean HOST_TRACE_ENABLED;
    static {
        EXEC_TRACE_ENABLED = execLog.isTraceEnabled();
        HOST_DEBUG_ENABLED = hostLog.isDebugEnabled();
        HOST_TRACE_ENABLED = hostLog.isTraceEnabled();
    }

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
        LatencyWatchdog.pet();

        waitOnDurabilityBackpressureFuture();
        if (HOST_DEBUG_ENABLED) {
            hostLog.debug("STARTING: " + this);
        }
        if (!m_txnState.isReadOnly()) {
            m_txnState.setBeginUndoToken(siteConnection.getLatestUndoToken());
        }

        // cast up here .. ugly.
        SpTransactionState txnState = (SpTransactionState)m_txnState;
        final InitiateResponseMessage response = processInitiateTask(txnState.m_initiationMsg, siteConnection);
        if (!response.shouldCommit()) {
            m_txnState.setNeedsRollback();
        }
        completeInitiateTask(siteConnection);
        response.m_sourceHSId = m_initiator.getHSId();
        m_initiator.deliver(response);
        if (EXEC_TRACE_ENABLED) {
            execLog.l7dlog( Level.TRACE, LogKeys.org_voltdb_ExecutionSite_SendingCompletedWUToDtxn.name(), null);
        }
        if (HOST_DEBUG_ENABLED) {
            hostLog.debug("COMPLETE: " + this);
        }

        logToDR(txnState, response);
    }

    @Override
    public void runForRejoin(SiteProcedureConnection siteConnection, TaskLog taskLog)
    throws IOException
    {
        LatencyWatchdog.pet();

        if (!m_txnState.isReadOnly()) {
            taskLog.logTask(m_txnState.getNotice());
        }

        SpTransactionState txnState = (SpTransactionState)m_txnState;
        final InitiateResponseMessage response =
            new InitiateResponseMessage(txnState.m_initiationMsg);
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
        LatencyWatchdog.pet();

        if (HOST_TRACE_ENABLED) {
            hostLog.trace("START replaying txn: " + this);
        }
        if (!m_txnState.isReadOnly()) {
            m_txnState.setBeginUndoToken(siteConnection.getLatestUndoToken());
        }

        // cast up here .. ugly.
        SpTransactionState txnState = (SpTransactionState)m_txnState;
        final InitiateResponseMessage response = processInitiateTask(txnState.m_initiationMsg, siteConnection);
        if (!response.shouldCommit()) {
            m_txnState.setNeedsRollback();
        }
        if (!m_txnState.isReadOnly()) {
            assert(siteConnection.getLatestUndoToken() != Site.kInvalidUndoToken) :
                "[SP][RW] transaction found invalid latest undo token state in Iv2ExecutionSite.";
            assert(siteConnection.getLatestUndoToken() >= m_txnState.getBeginUndoToken()) :
                "[SP][RW] transaction's undo log token farther advanced than latest known value.";
            assert (m_txnState.getBeginUndoToken() != Site.kInvalidUndoToken) :
                "[SP][RW] with invalid undo token in completeInitiateTask.";

            // the truncation point token SHOULD be part of m_txn. However, the
            // legacy interaces don't work this way and IV2 hasn't changed this
            // ownership yet. But truncateUndoLog is written assuming the right
            // eventual encapsulation.
            siteConnection.truncateUndoLog(m_txnState.needsRollback(),
                    m_txnState.getBeginUndoToken(),
                    m_txnState.m_spHandle,
                    m_txnState.getUndoLog());
        }
        m_txnState.setDone();
        if (EXEC_TRACE_ENABLED) {
            execLog.l7dlog( Level.TRACE, LogKeys.org_voltdb_ExecutionSite_SendingCompletedWUToDtxn.name(), null);
        }
        if (HOST_TRACE_ENABLED) {
            hostLog.trace("COMPLETE replaying txn: " + this);
        }

        logToDR(txnState, response);
    }

    private void logToDR(SpTransactionState txnState, InitiateResponseMessage response)
    {
        // Log invocation to DR
        if (m_drGateway != null && !txnState.isReadOnly() && !txnState.needsRollback()) {
            m_drGateway.onSuccessfulProcedureCall(txnState.txnId, txnState.uniqueId, txnState.getHash(),
                    txnState.getInvocation(), response.getClientResponseData());
        }
    }

    @Override
    void completeInitiateTask(SiteProcedureConnection siteConnection)
    {
        if (!m_txnState.isReadOnly()) {
            assert(siteConnection.getLatestUndoToken() != Site.kInvalidUndoToken) :
                "[SP][RW] transaction found invalid latest undo token state in Iv2ExecutionSite.";
            assert(siteConnection.getLatestUndoToken() >= m_txnState.getBeginUndoToken()) :
                "[SP][RW] transaction's undo log token farther advanced than latest known value.";
            assert (m_txnState.getBeginUndoToken() != Site.kInvalidUndoToken) :
                "[SP][RW] with invalid undo token in completeInitiateTask.";

            // the truncation point token SHOULD be part of m_txn. However, the
            // legacy interaces don't work this way and IV2 hasn't changed this
            // ownership yet. But truncateUndoLog is written assuming the right
            // eventual encapsulation.
            siteConnection.truncateUndoLog(m_txnState.needsRollback(),
                    m_txnState.getBeginUndoToken(),
                    m_txnState.m_spHandle,
                    m_txnState.getUndoLog());
        }
        doCommonSPICompleteActions();
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
