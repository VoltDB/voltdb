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

import org.voltdb.PartitionDRGateway;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;
import org.voltdb.rejoin.TaskLog;

public class CompleteTransactionTask extends TransactionTask
{
    final private CompleteTransactionMessage m_completeMsg;
    final private PartitionDRGateway m_drGateway;

    public CompleteTransactionTask(TransactionState txnState,
                                   TransactionTaskQueue queue,
                                   CompleteTransactionMessage msg,
                                   PartitionDRGateway drGateway)
    {
        super(txnState, queue);
        m_completeMsg = msg;
        m_drGateway = drGateway;
    }

    @Override
    public void run(SiteProcedureConnection siteConnection)
    {
        hostLog.debug("STARTING: " + this);
        if (!m_txnState.isReadOnly()) {
            // the truncation point token SHOULD be part of m_txn. However, the
            // legacy interaces don't work this way and IV2 hasn't changed this
            // ownership yet. But truncateUndoLog is written assuming the right
            // eventual encapsulation.
            siteConnection.truncateUndoLog(m_completeMsg.isRollback(),
                    m_txnState.getBeginUndoToken(),
                    m_txnState.m_spHandle,
                    m_txnState.getUndoLog());
        }
        if (!m_completeMsg.isRestart()) {
            doCommonSPICompleteActions();

            // Log invocation to DR
            logToDR();
            hostLog.debug("COMPLETE: " + this);
        }
        else
        {
            // If we're going to restart the transaction, then reset the begin undo token so the
            // first FragmentTask will set it correctly.  Otherwise, don't set the Done state or
            // flush the queue; we want the TransactionTaskQueue to stay blocked on this TXN ID
            // for the restarted fragments.
            m_txnState.setBeginUndoToken(Site.kInvalidUndoToken);
            hostLog.debug("RESTART: " + this);
        }
    }

    @Override
    public void runForRejoin(SiteProcedureConnection siteConnection, TaskLog taskLog)
    throws IOException
    {
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

    @Override
    public long getSpHandle()
    {
        return m_completeMsg.getSpHandle();
    }

    @Override
    public void runFromTaskLog(SiteProcedureConnection siteConnection)
    {
        if (!m_txnState.isReadOnly()) {
            // the truncation point token SHOULD be part of m_txn. However, the
            // legacy interaces don't work this way and IV2 hasn't changed this
            // ownership yet. But truncateUndoLog is written assuming the right
            // eventual encapsulation.
            siteConnection.truncateUndoLog(m_completeMsg.isRollback(),
                    m_txnState.getBeginUndoToken(),
                    m_txnState.m_spHandle,
                    m_txnState.getUndoLog());
        }
        if (!m_completeMsg.isRestart()) {
            // this call does the right thing with a null TransactionTaskQueue
            doCommonSPICompleteActions();
            logToDR();
        }
        else {
            m_txnState.setBeginUndoToken(Site.kInvalidUndoToken);
        }
    }

    private void logToDR()
    {
        // Log invocation to DR
        if (m_drGateway != null && !m_txnState.isForReplay() && !m_txnState.isReadOnly() &&
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
            m_drGateway.onSuccessfulMPCall(m_txnState.m_spHandle,
                    m_txnState.txnId,
                    m_txnState.uniqueId,
                    m_completeMsg.getHash(),
                    invocation,
                    m_txnState.getResults());
        }
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("CompleteTransactionTask:");
        sb.append("  TXN ID: ").append(TxnEgo.txnIdToString(getTxnId()));
        sb.append("  SP HANDLE: ").append(TxnEgo.txnIdToString(getSpHandle()));
        sb.append("  UNDO TOKEN: ").append(m_txnState.getBeginUndoToken());
        sb.append("  MSG: ").append(m_completeMsg.toString());
        return sb.toString();
    }
}
