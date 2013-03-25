/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.SnapshotCompletionInterest;
import org.voltdb.VoltDB;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;
import org.voltdb.messaging.RejoinMessage;
import org.voltdb.rejoin.TaskLog;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;

public class JoinProducer extends JoinProducerBase implements TaskLog {
    private final LinkedBlockingDeque<Pair<Integer, ByteBuffer>> m_snapshotData =
            new LinkedBlockingDeque<Pair<Integer, ByteBuffer>>();
    // true if the site has received the first fragment task message
    private boolean m_receivedFirstFragment = false;
    // true if the site has notified the coordinator about the receipt of the first fragment
    // message
    private boolean m_firstFragResponseSent = false;

    private class CompletionAction extends JoinCompletionAction {
        @Override
        public void run()
        {
            // TODO: no-op for now
        }
    }

    public JoinProducer(int partitionId, SiteTaskerQueue taskQueue)
    {
        super(partitionId, "Join producer:" + partitionId + " ", taskQueue);
        m_completionAction = new CompletionAction();
    }

    /**
     * Notify the coordinator that this site has received the first fragment message
     */
    private void sendFirstFragResponse()
    {
        if (JOINLOG.isDebugEnabled()) {
            JOINLOG.debug("P" + m_partitionId + " sending first fragment response to coordinator " +
                    CoreUtils.hsIdToString(m_coordinatorHsId));
        }
        RejoinMessage msg = new RejoinMessage(m_mailbox.getHSId(),
                RejoinMessage.Type.FIRST_FRAGMENT_RECEIVED);
        m_mailbox.send(m_coordinatorHsId, msg);
        m_firstFragResponseSent = true;
    }

    @Override
    protected void kickWatchdog(boolean rearm)
    {

    }

    @Override
    public boolean acceptPromotion()
    {
        return true;
    }

    @Override
    public void deliver(RejoinMessage message)
    {
        if (message.getType() == RejoinMessage.Type.INITIATION) {
            m_coordinatorHsId = message.m_sourceHSId;
            String snapshotNonce = message.getSnapshotNonce();
            SnapshotCompletionAction interest = new SnapshotCompletionAction(snapshotNonce);
            interest.register();
            m_taskQueue.offer(this);
            JOINLOG.info("P" + m_partitionId + " received initiation");
        } else if (message.getType() == RejoinMessage.Type.SNAPSHOT_DATA) {
            if (JOINLOG.isTraceEnabled()) {
                JOINLOG.trace("P" + m_partitionId + " received snapshot data block");
            }
            m_snapshotData.offer(Pair.of(message.getTableId(), message.getTableBlock()));
        }
    }

    @Override
    public TaskLog constructTaskLog(String voltroot)
    {
        m_taskLog = initializeTaskLog(voltroot, m_partitionId);
        return this;
    }

    @Override
    public void run(SiteProcedureConnection siteConnection)
    {
        throw new RuntimeException("Unexpected execution of run method in rejoin producer");
    }

    @Override
    public void runForRejoin(SiteProcedureConnection siteConnection, TaskLog rejoinTaskLog) throws IOException
    {
        if (!m_receivedFirstFragment) {
            // no-op, wait for the first fragment
        } else if (!m_firstFragResponseSent) {
            // Received first fragment but haven't notified the coordinator
            sendFirstFragResponse();
        } else if (m_completionMonitorAwait.isDone()) {
            JOINLOG.info("P" + m_partitionId + " run for rejoin is complete");
            SnapshotCompletionInterest.SnapshotCompletionEvent event = null;
            try {
                event = m_completionMonitorAwait.get();
                assert(event != null);
            } catch (InterruptedException e) {
                // isDone() already returned true, this shouldn't happen
                VoltDB.crashLocalVoltDB("Impossible interruption happend", true, e);
            } catch (ExecutionException e) {
                VoltDB.crashLocalVoltDB("Error waiting for snapshot to finish", true, e);
            }
            RejoinMessage rm = new RejoinMessage(m_mailbox.getHSId(),
                    RejoinMessage.Type.SNAPSHOT_FINISHED);
            m_mailbox.send(m_coordinatorHsId, rm);
            setJoinComplete(siteConnection, event.exportSequenceNumbers);
            return;
        } else {
            Pair<Integer, ByteBuffer> dataPair = m_snapshotData.poll();
            if (dataPair != null) {
                JOINLOG.info("P" + m_partitionId + " restoring table " + dataPair.getFirst() +
                        " block of (" + dataPair.getSecond().position() + "," +
                        dataPair.getSecond().limit() + ")");
                restoreBlock(dataPair, siteConnection);
            }
        }

        m_taskQueue.offer(this);
    }

    @Override
    public void logTask(TransactionInfoBaseMessage message) throws IOException
    {
        assert(!(message instanceof Iv2InitiateTaskMessage));
        if (message instanceof FragmentTaskMessage) {
            if (JOINLOG.isTraceEnabled()) {
                JOINLOG.trace("P" + m_partitionId + " received first fragment");
            }
            m_receivedFirstFragment = true;
        }
        m_taskLog.logTask(message);
    }

    @Override
    public TransactionInfoBaseMessage getNextMessage() throws IOException
    {
        return m_taskLog.getNextMessage();
    }

    @Override
    public void setEarliestTxnId(long txnId)
    {
        m_taskLog.setEarliestTxnId(txnId);
    }

    @Override
    public boolean isEmpty() throws IOException
    {
        return m_taskLog.isEmpty();
    }

    @Override
    public void close() throws IOException
    {
        m_taskLog.close();
    }
}
