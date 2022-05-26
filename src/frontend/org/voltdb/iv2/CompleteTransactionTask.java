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

import org.voltcore.messaging.Mailbox;
import org.voltdb.PartitionDRGateway;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.CompleteTransactionResponseMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;
import org.voltdb.rejoin.TaskLog;
import org.voltdb.utils.VoltTrace;

public class CompleteTransactionTask extends TransactionTask
{
    final private Mailbox m_initiator;
    final private CompleteTransactionMessage m_completeMsg;
    private boolean m_fragmentNotExecuted = false;
    private boolean m_repairCompletionMatched = false;
    public CompleteTransactionTask(Mailbox initiator,
                                   TransactionState txnState,
                                   TransactionTaskQueue queue,
                                   CompleteTransactionMessage msg)
    {
        super(txnState, queue);
        m_initiator = initiator;
        m_completeMsg = msg;
    }

    public void setFragmentNotExecuted()
    {
        m_fragmentNotExecuted = true;
    }

    public void setRepairCompletionMatched() {
        // When CompleteTransactionTask is released from Scorboard, all the sites will process it and forward it to its
        // replica. After a site has received CompleteTransactionResponseMessage from itself and its replica, the transaction state will be
        // removed from its duplicate counter and outstanding transaction list, i.e. the transaction state is removed.
        // When a repair process kicks in upon node failure or joining, some sites have cleaned up transaction state
        // and some have not. A repair CompleteTransactionMessage is collected and is broadcasted to all the partition leaders.
        // Then a repair CompelteTransactionTask is placed onto Scoreboard as unmatched (missing) from sites where the transaction state has been cleaned, and
        // as matched from sites where the transaction state is still available.
        // After Scoreboard has again collected all repair CompleteTransactionTask, CompleteTransactionTask is released.
        // If a site still has the transaction state, then the CompleteTransactionTask should flush its TransactionTaskQueue to unblock the site.
        // The flag is created for this purpose.
        m_repairCompletionMatched = true;
    }
    private void doUnexecutedFragmentCleanup()
    {
        // If the task is for restart, FragmentTasks could be at head of the TransactionTaskQueue.
        // The transaction should not be flushed at this moment.
        if (m_completeMsg.isAbortDuringRepair() || (m_repairCompletionMatched && !m_completeMsg.isRestart())) {
            if (hostLog.isDebugEnabled()) {
                hostLog.debug("releaseStashedCompleteTxns: flush non-restartable logs at " + TxnEgo.txnIdToString(getTxnId()));
            }
            // Mark the transaction state as DONE
            // Transaction state could be null when a CompleteTransactionTask is added to scoreboard.
            if (m_txnState != null) {
                m_txnState.setDone();
            }
            // Flush us out of the head of the TransactionTaskQueue.
            if (m_queue != null) {
                m_queue.flush(getTxnId());
            }
        }
    }

    @Override
    public void run(SiteProcedureConnection siteConnection)
    {
        if (m_fragmentNotExecuted) {
            if (hostLog.isDebugEnabled()) {
                hostLog.debug("SKIPPING (Never Executed): " + this);
            }
            doUnexecutedFragmentCleanup();
        }
        else {
            if (hostLog.isDebugEnabled()) {
                hostLog.debug("STARTING: " + this);
            }
            final VoltTrace.TraceEventBatch traceLog = VoltTrace.log(VoltTrace.Category.SPSITE);
            if (traceLog != null) {
                traceLog.add(() -> VoltTrace.beginDuration("execcompletetxn",
                                                           "txnId", TxnEgo.txnIdToString(getTxnId()),
                                                           "partition", Integer.toString(siteConnection.getCorrespondingPartitionId())));
            }

            if (!m_txnState.isReadOnly()) {
                // the truncation point token SHOULD be part of m_txn. However, the
                // legacy interfaces don't work this way and IV2 hasn't changed this
                // ownership yet. But truncateUndoLog is written assuming the right
                // eventual encapsulation.
                siteConnection.truncateUndoLog(m_completeMsg.isRollback(),
                        m_completeMsg.isEmptyDRTxn(),
                        m_txnState.getBeginUndoToken(),
                        m_txnState.m_spHandle,
                        m_txnState.getUndoLog());
            }
            if (!m_completeMsg.isRestart()) {
                doCommonSPICompleteActions();

                // Log invocation to DR
                logToDR(siteConnection.getDRGateway());
                if (hostLog.isDebugEnabled()) {
                    hostLog.debug("COMPLETE: " + this);
                }
            }
            else
            {
                // If we're going to restart the transaction, then reset the begin undo token so the
                // first FragmentTask will set it correctly.  Otherwise, don't set the Done state or
                // flush the queue; we want the TransactionTaskQueue to stay blocked on this TXN ID
                // for the restarted fragments.
                m_txnState.setBeginUndoToken(Site.kInvalidUndoToken);
                if (hostLog.isDebugEnabled()) {
                    hostLog.debug("RESTART: " + this);
                }
            }

            if (traceLog != null) {
                traceLog.add(VoltTrace::endDuration);
            }
        }
        final CompleteTransactionResponseMessage resp = new CompleteTransactionResponseMessage(m_completeMsg);
        resp.m_sourceHSId = m_initiator.getHSId();
        m_initiator.deliver(resp);
    }

    @Override
    public void runForRejoin(SiteProcedureConnection siteConnection, TaskLog taskLog)
    throws IOException
    {
        if (m_fragmentNotExecuted) {
            doUnexecutedFragmentCleanup();
        }
        else {
            if (!m_txnState.isReadOnly() && !m_completeMsg.isRollback()) {
                // ENG-5276: Need to set the last committed spHandle so that the rejoining site gets the accurate
                // per-partition txnId set for the next snapshot. Normally, this is done through undo log truncation.
                // Since the task is not run here, we need to set the last committed spHandle explicitly.
                //
                // How does this work?
                // - Blocking rejoin with idle cluster: The spHandle is updated here with the spHandle of the stream
                //   snapshot that transfers the rejoin data. So the snapshot right after rejoin should have the spHandle
                //   passed here.
                // - Live rejoin with idle cluster: Same as blocking rejoin.
                // - Live rejoin with workload: Transactions will be logged and replayed afterward. The spHandle will be
                //   updated when they commit and truncate undo logs. So at the end of replay,
                //   the spHandle should have the latest value. If all replayed transactions rolled back,
                //   the spHandle is still guaranteed to be the spHandle of the stream snapshot that transfered the
                //   rejoin data, which is the correct value.
                siteConnection.setSpHandleForSnapshotDigest(m_txnState.m_spHandle);
            }

            if (!m_completeMsg.isRestart()) {
                // future: offer to siteConnection.IBS for replay.
                doCommonSPICompleteActions();
            }

            if (!m_txnState.isReadOnly()) {
                // We need to log the restarting message to the task log so we'll replay the whole
                // stream faithfully
                taskLog.logTask(m_completeMsg);
            }
        }
        final CompleteTransactionResponseMessage resp = new CompleteTransactionResponseMessage(m_completeMsg);
        resp.setIsRecovering(true);
        resp.m_sourceHSId = m_initiator.getHSId();
        m_initiator.deliver(resp);
    }

    @Override
    public long getSpHandle()
    {
        return m_completeMsg.getSpHandle();
    }

    public long getTimestamp()
    {
        return m_completeMsg.getTimestamp();
    }

    public long getMsgTxnId()
    {
        return m_completeMsg.getTxnId();
    }

    public boolean isAbortDuringRepair() {
        return m_completeMsg.isAbortDuringRepair();
    }

    @Override
    public void runFromTaskLog(SiteProcedureConnection siteConnection)
    {
        if (hostLog.isDebugEnabled()) {
            hostLog.debug("START replaying txn: " + this);
        }
        if (!m_txnState.isReadOnly()) {
            // the truncation point token SHOULD be part of m_txn. However, the
            // legacy interfaces don't work this way and IV2 hasn't changed this
            // ownership yet. But truncateUndoLog is written assuming the right
            // eventual encapsulation.
            siteConnection.truncateUndoLog(m_completeMsg.isRollback(),
                    m_completeMsg.isEmptyDRTxn(),
                    m_txnState.getBeginUndoToken(),
                    m_txnState.m_spHandle,
                    m_txnState.getUndoLog());
        }
        if (!m_completeMsg.isRestart()) {
            // this call does the right thing with a null TransactionTaskQueue
            doCommonSPICompleteActions();
            logToDR(siteConnection.getDRGateway());
        }
        else {
            m_txnState.setBeginUndoToken(Site.kInvalidUndoToken);
        }
        if (hostLog.isDebugEnabled()) {
            hostLog.debug("COMPLETE replaying txn: " + this);
        }
    }

    private void logToDR(PartitionDRGateway drGateway)
    {
        // Log invocation to DR
        if (drGateway != null && !m_txnState.isForReplay() && !m_txnState.isReadOnly() &&
            !m_completeMsg.isRollback())
        {
            FragmentTaskMessage fragment = (FragmentTaskMessage) m_txnState.getNotice();
            Iv2InitiateTaskMessage initiateTask = fragment.getInitiateTask();
            assert(initiateTask != null);
            if (initiateTask == null) {
                hostLog.error("Unable to log MP transaction to DR because of missing InitiateTaskMessage, " +
                              "fragment: " + fragment.toString());
            }
            StoredProcedureInvocation invocation = initiateTask.getStoredProcedureInvocation().getShallowCopy();
            drGateway.onSuccessfulMPCall(invocation);
        }
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("CompleteTransactionTask:");
        if (m_txnState != null) {
            sb.append("  TXN ID: ").append(TxnEgo.txnIdToString(getTxnId()));
            sb.append("  SP HANDLE: ").append(TxnEgo.txnIdToString(getSpHandle()));
            sb.append("  UNDO TOKEN: ").append(m_txnState.getBeginUndoToken());
        }
        sb.append("  MSG:\n    ");
        m_completeMsg.indentedString(sb, 8);
        return sb.toString();
    }

    public boolean needCoordination() {
        return m_completeMsg.needsCoordination();
    }

    public CompleteTransactionMessage getCompleteMessage() {
        return m_completeMsg;
    }

    @Override
    public long getTxnId() {
        return getMsgTxnId();
    }
}
