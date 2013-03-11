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
import org.voltdb.messaging.RejoinMessage;
import org.voltdb.rejoin.TaskLog;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;

public class JoinProducer extends JoinProducerBase implements TaskLog {
    private final LinkedBlockingDeque<Pair<Integer, ByteBuffer>> m_snapshotData =
            new LinkedBlockingDeque<Pair<Integer, ByteBuffer>>();
    private boolean m_receivedFirstFragment = false;

    private class CompletionAction extends JoinCompletionAction {
        @Override
        public void run()
        {
            // TODO: no-op for now
        }
    }

    JoinProducer(int partitionId, SiteTaskerQueue taskQueue)
    {
        super(partitionId, "Join producer:" + partitionId + " ", taskQueue);
        m_completionAction = new CompletionAction();
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
    public TaskLog getTaskLog()
    {
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
        } else if (m_completionMonitorAwait.isDone()) {
            JOINLOG.info("P" + m_partitionId + " run for rejoin is complete");
            SnapshotCompletionInterest.SnapshotCompletionEvent event = null;
            try {
                event = m_completionMonitorAwait.get();
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
        if (message instanceof FragmentTaskMessage) {
            JOINLOG.info("P" + m_partitionId + " received first fragment, " +
                    "sending to mailbox " + CoreUtils.hsIdToString(m_coordinatorHsId));
            if (!m_receivedFirstFragment) {
                RejoinMessage msg = new RejoinMessage(m_mailbox.getHSId(),
                        RejoinMessage.Type.FIRST_FRAGMENT_RECEIVED);
                m_mailbox.send(m_coordinatorHsId, msg);
                m_receivedFirstFragment = true;
            }
        }
    }

    @Override
    public TransactionInfoBaseMessage getNextMessage() throws IOException
    {
        return null;
    }

    @Override
    public void setEarliestTxnId(long txnId)
    {

    }

    @Override
    public boolean isEmpty() throws IOException
    {
        return true;
    }

    @Override
    public void close() throws IOException
    {

    }
}
