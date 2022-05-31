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

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.voltcore.messaging.Mailbox;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.LatencyWatchdog;
import org.voltdb.ClientResponseImpl;
import org.voltdb.PartitionDRGateway;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.VoltTable;
import org.voltdb.client.BatchTimeoutOverrideType;
import org.voltdb.client.ClientResponse;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;
import org.voltdb.rejoin.TaskLog;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.VoltTrace;

/**
 * Implements the single partition procedure ProcedureTask.
 * Runs single partition procedures against a SiteConnection
 */
public class SpProcedureTask extends ProcedureTask
{
    private static final boolean EXEC_TRACE_ENABLED;
    private static final boolean HOST_DEBUG_ENABLED;
    static {
        EXEC_TRACE_ENABLED = execLog.isTraceEnabled();
        HOST_DEBUG_ENABLED = hostLog.isDebugEnabled();
    }

    private final String m_traceLogName;

    public static SpProcedureTask create(Mailbox initiator, String procName, TransactionTaskQueue queue,
            Iv2InitiateTaskMessage msg) {
        StoredProcedureInvocation spi = msg.getStoredProcedureInvocation();
        return spi != null && spi.isBatchCall()
                ? new BatchProcedureTask.SpBatch(initiator, procName, queue, msg)
                : new SpProcedureTask(initiator, procName, queue, msg);
    }

    SpProcedureTask(Mailbox initiator, String procName, TransactionTaskQueue queue,
            Iv2InitiateTaskMessage msg) {
        this(initiator, procName, queue, msg, "runSpTask");
    }

    SpProcedureTask(Mailbox initiator, String procName, TransactionTaskQueue queue, Iv2InitiateTaskMessage msg,
            String traceLogName)
    {
       super(initiator, procName, new SpTransactionState(msg), queue);
       m_traceLogName = traceLogName;
    }

    @Override
    protected void durabilityTraceEnd() {
        final VoltTrace.TraceEventBatch traceLog = VoltTrace.log(VoltTrace.Category.SPI);
        if (traceLog != null) {
            traceLog.add(() -> VoltTrace.endAsync("durability",
                                                  MiscUtils.hsIdTxnIdToString(m_initiator.getHSId(), getSpHandle())));
        }
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
        final VoltTrace.TraceEventBatch traceLog = VoltTrace.log(VoltTrace.Category.SPI);
        if (traceLog != null) {
            traceLog.add(() -> VoltTrace.beginDuration(m_traceLogName,
                                                       "txnId", TxnEgo.txnIdToString(getTxnId()),
                                                       "partition", Integer.toString(siteConnection.getCorrespondingPartitionId())));
        }

        if (!m_txnState.isReadOnly()) {
            m_txnState.setBeginUndoToken(siteConnection.getLatestUndoToken());
        }

        // cast up here .. ugly.
        SpTransactionState txnState = (SpTransactionState)m_txnState;

        InitiateResponseMessage response;
        int originalTimeout = siteConnection.getBatchTimeout();
        int individualTimeout = m_txnState.getInvocation().getBatchTimeout();
        try {
            // run the procedure with a specific individual timeout
            if (BatchTimeoutOverrideType.isUserSetTimeout(individualTimeout) ) {
                siteConnection.setBatchTimeout(individualTimeout);
            }
            response = processInitiateTask(txnState.m_initiationMsg, siteConnection);
        } finally {
            // reset the deployment timeout value back to its original value
            if (BatchTimeoutOverrideType.isUserSetTimeout(individualTimeout) ) {
                siteConnection.setBatchTimeout(originalTimeout);
            }
        }

        if (!response.shouldCommit()) {
            m_txnState.setNeedsRollback(true);
        }
        completeInitiateTask(siteConnection);
        if (traceLog != null) {
            traceLog.add(() -> VoltTrace.endDuration(m_traceLogName,
                                                       "txnId", TxnEgo.txnIdToString(getTxnId()),
                                                       "partition", Integer.toString(siteConnection.getCorrespondingPartitionId())));
        }
        response.m_sourceHSId = m_initiator.getHSId();
        if (txnState.m_initiationMsg != null && !(txnState.m_initiationMsg.isForReplica())) {
            response.setExecutedOnPreviousLeader(true);
        }

        // Note: this call will re-queue the response to the site tasker queue,
        // thus delaying the transmission of the response behind other tasks, e.g
        // other invocations. Trying to deliver the response immediately to the network
        // does not improve latencies. See ENG-21040.
        m_initiator.deliver(response);
        if (EXEC_TRACE_ENABLED) {
            execLog.trace("ExecutionSite sending completed workunit to dtxn.");
        }
        if (HOST_DEBUG_ENABLED) {
            hostLog.debug("COMPLETE: " + this);
        }
        if (traceLog != null) {
            traceLog.add(VoltTrace::endDuration);
        }

        logToDR(siteConnection.getDRGateway(), txnState);
    }

    @Override
    public void runForRejoin(SiteProcedureConnection siteConnection, TaskLog taskLog)
    throws IOException
    {
        LatencyWatchdog.pet();

        if (!m_txnState.isReadOnly()) {
            taskLog.logTask(m_txnState.getNotice());
        }
        if (HOST_DEBUG_ENABLED) {
            hostLog.debug("START for rejoin: " + this);
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

        if (HOST_DEBUG_ENABLED) {
            hostLog.debug("START replaying txn: " + this);
        }
        if (!m_txnState.isReadOnly()) {
            m_txnState.setBeginUndoToken(siteConnection.getLatestUndoToken());
        }

        // cast up here .. ugly.
        SpTransactionState txnState = (SpTransactionState)m_txnState;
        final InitiateResponseMessage response = processInitiateTask(txnState.m_initiationMsg, siteConnection);
        if (!response.shouldCommit()) {
            m_txnState.setNeedsRollback(true);
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
            siteConnection.truncateUndoLog(m_txnState.needsRollback(), false,
                    m_txnState.getBeginUndoToken(),
                    m_txnState.m_spHandle,
                    m_txnState.getUndoLog());
        }
        m_txnState.setDone();
        if (EXEC_TRACE_ENABLED) {
            execLog.trace("ExecutionSite sending completed workunit to dtxn.", null);
        }
        if (HOST_DEBUG_ENABLED) {
            hostLog.debug("COMPLETE replaying txn: " + this);
        }

        logToDR(siteConnection.getDRGateway(), txnState);
    }

    private void logToDR(PartitionDRGateway drGateway, SpTransactionState txnState)
    {
        // Log invocation to DR
        if (drGateway != null && !txnState.isReadOnly() && !txnState.needsRollback()) {
            drGateway.onSuccessfulProcedureCall(txnState.getInvocation());
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
            // legacy interfaces don't work this way and IV2 hasn't changed this
            // ownership yet. But truncateUndoLog is written assuming the right
            // eventual encapsulation.
            siteConnection.truncateUndoLog(m_txnState.needsRollback(), false,
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
        sb.append(getClass().getSimpleName()).append(':');
        if (!StringUtils.isEmpty(m_procName)) {
            sb.append("  PROCNAME: ").append(m_procName);
        }
        sb.append("  TXN ID: ").append(TxnEgo.txnIdToString(getTxnId()));
        sb.append("  SP HANDLE ID: ").append(TxnEgo.txnIdToString(getSpHandle()));
        sb.append("  ON HSID: ").append(CoreUtils.hsIdToString(m_initiator.getHSId()));
        if (m_txnState != null) {
            SpTransactionState txnState = (SpTransactionState)m_txnState;
            if (txnState.m_initiationMsg != null) {
                sb.append("  TRUNCATION HANDLE: ").append(TxnEgo.txnIdToString(txnState.m_initiationMsg.getTruncationHandle()));
            }
        }
        return sb.toString();
    }

    @Override
    public boolean needCoordination() {
        return false;
    }
}
