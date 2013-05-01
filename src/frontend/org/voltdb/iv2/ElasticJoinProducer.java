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

import com.google.common.util.concurrent.SettableFuture;
import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.SnapshotCompletionInterest.SnapshotCompletionEvent;
import org.voltdb.VoltDB;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;
import org.voltdb.messaging.RejoinMessage;
import org.voltdb.rejoin.StreamSnapshotSink;
import org.voltdb.rejoin.TaskLog;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;

public class ElasticJoinProducer extends JoinProducerBase implements TaskLog {
    private final LinkedBlockingDeque<Pair<Integer, ByteBuffer>> m_snapshotData =
            new LinkedBlockingDeque<Pair<Integer, ByteBuffer>>();
    // true if the site has received the first fragment task message
    private boolean m_receivedFirstFragment = false;
    // true if the site has notified the coordinator about the receipt of the first fragment
    // message
    private boolean m_firstFragResponseSent = false;
    // whether to wait for partitioned table snapshot completion or not
    private volatile boolean m_shouldWaitForSnapshotCompletion = true;

    // replicated table snapshot completion monitor
    private final SettableFuture<SnapshotCompletionEvent> m_replicatedCompletionMonitor =
            SettableFuture.create();
    // set to true when the coordinator is notified the completion of the replicated table snapshot
    private boolean m_replicatedStreamFinished = false;

    // a list of snapshot sinks used to stream partitioned table data from multiple sources
    private final List<StreamSnapshotSink> m_dataSinks = new ArrayList<StreamSnapshotSink>();
    // partitioned table snapshot completion monitor
    private final SettableFuture<SnapshotCompletionEvent> m_partitionedCompletionMonitor =
            SettableFuture.create();
    // set to true when the coordinator is notified the completion of the partitioned table snapshot
    private boolean m_partitionedStreamFinished = false;

    private class CompletionAction extends JoinCompletionAction {
        @Override
        public void run()
        {
            // TODO: no-op for now
        }
    }

    public ElasticJoinProducer(int partitionId, SiteTaskerQueue taskQueue)
    {
        super(partitionId, "Elastic join producer:" + partitionId + " ", taskQueue);
        m_completionAction = new CompletionAction();
    }

    private void doInitiation(RejoinMessage message)
    {
        m_coordinatorHsId = message.m_sourceHSId;
        m_shouldWaitForSnapshotCompletion = message.shouldWaitForSnapshotCompletion();
        String snapshotNonce = message.getSnapshotNonce();
        SnapshotCompletionAction interest =
                new SnapshotCompletionAction(snapshotNonce, m_replicatedCompletionMonitor);
        interest.register();

        Set<Long> sinkHSIds = new HashSet<Long>();
        for (int i = 0; i < message.getSnapshotSinkCount(); i++) {
            StreamSnapshotSink sink = new StreamSnapshotSink();
            m_dataSinks.add(sink);
            sinkHSIds.add(sink.initialize());
        }

        // respond to the coordinator with the sink HSIDs
        RejoinMessage msg = new RejoinMessage(m_mailbox.getHSId(), sinkHSIds);
        m_mailbox.send(m_coordinatorHsId, msg);

        m_taskQueue.offer(this);
        JOINLOG.info("P" + m_partitionId + " received initiation");
    }

    private void doPartitionSnapshot(RejoinMessage message)
    {
        String snapshotNonce = message.getSnapshotNonce();
        SnapshotCompletionAction interest =
                new SnapshotCompletionAction(snapshotNonce, m_partitionedCompletionMonitor);
        interest.register();
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

    /**
     * Restores a block of replicated table data if it's ready.
     * @param siteConnection
     */
    private void runForReplicatedDataTransfer(SiteProcedureConnection siteConnection)
    {
        Pair<Integer, ByteBuffer> dataPair = m_snapshotData.poll();
        if (dataPair != null) {
            if (JOINLOG.isTraceEnabled()) {
                JOINLOG.trace(m_whoami + "restoring replicated table " + dataPair.getFirst() +
                                  " block of (" + dataPair.getSecond().position() + "," +
                                  dataPair.getSecond().limit() + ")");
            }
            restoreBlock(dataPair, siteConnection);
        }
    }

    private void notifyReplicatedSnapshotFinished()
    {
        JOINLOG.info("P" + m_partitionId + " noticed replicated table snapshot completion");
        SnapshotCompletionEvent event;
        try {
            event = m_replicatedCompletionMonitor.get();
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
        m_replicatedStreamFinished = true;
    }

    /**
     * Blocking transfer all partitioned table data and notify the coordinator.
     * @param siteConnection
     */
    private void runForBlockingDataTransfer(SiteProcedureConnection siteConnection)
    {
        Pair<Integer, ByteBuffer> tableBlock = m_dataSinks.get(0).poll();
        // poll() could return null if the source indicated enf of stream,
        // need to check on that before retry
        if (tableBlock == null && !m_dataSinks.get(0).isEOF()) {
            m_taskQueue.offer(this);
            // The sources are not set up yet, don't block the site,
            // return here and retry later.
            return;
        }

        /*
         * Block until all blocks for partitioned tables are streamed over. There may
         * be more than one data sinks for partitioned snapshot transfer, each from a
         * different source partition.
         */
        JOINLOG.info("P" + m_partitionId + " blocking partitioned table transfer starts");
        while (!m_dataSinks.isEmpty()){
            if (tableBlock != null) {
                if (JOINLOG.isTraceEnabled()) {
                    JOINLOG.trace(m_whoami + "restoring partitioned table " + tableBlock.getFirst() +
                                      " block of (" + tableBlock.getSecond().position() + "," +
                                      tableBlock.getSecond().limit() + ")");
                }

                restoreBlock(tableBlock, siteConnection);
            }

            /*
             * Try all data sinks to see if there is a data block ready. If not, the outer
             * while loop will run again.
             *
             * Don't use take() here because it blocks. If one sink has reached the end,
             * it may block until the other sinks have finished. However, without reading
             * stuff from the other sinks, it will deadlock.
             */
            tableBlock = null;
            ListIterator<StreamSnapshotSink> sinkIter = m_dataSinks.listIterator();
            while (sinkIter.hasNext()) {
                StreamSnapshotSink sink = sinkIter.next();
                if (sink.isEOF()) {
                    // No more data from this data sink, close and remove it from the list
                    sink.close();
                    sinkIter.remove();

                    if (JOINLOG.isTraceEnabled()) {
                        JOINLOG.trace(m_whoami +
                                          " finished transfering partitioned table data from one partition");
                    }
                } else {
                    tableBlock = sink.poll();
                }

                if (tableBlock != null) {
                    // Got a block, restore it in the outer loop
                    break;
                }
            }
        }

        JOINLOG.debug(m_whoami + " partitioned table snapshot transfer is finished");

        SnapshotCompletionEvent event = null;
        try {
            event = m_partitionedCompletionMonitor.get();
            assert(event != null);
            JOINLOG.debug("P" + m_partitionId + " noticed partitioned table snapshot completion");
            m_completionAction.setSnapshotTxnId(event.multipartTxnId);
        } catch (InterruptedException e) {
            // isDone() already returned true, this shouldn't happen
            VoltDB.crashLocalVoltDB("Impossible interruption happend", true, e);
        } catch (ExecutionException e) {
            VoltDB.crashLocalVoltDB("Error waiting for snapshot to finish", true, e);
        }
        RejoinMessage rm = new RejoinMessage(m_mailbox.getHSId(),
                                             RejoinMessage.Type.SNAPSHOT_FINISHED);
        m_mailbox.send(m_coordinatorHsId, rm);
        m_partitionedStreamFinished = true;
        setJoinComplete(siteConnection, event.exportSequenceNumbers);
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
            doInitiation(message);
        } else if (message.getType() == RejoinMessage.Type.SNAPSHOT_DATA) {
            m_snapshotData.offer(Pair.of(message.getTableId(), message.getTableBlock()));
        } else if (message.getType() == RejoinMessage.Type.PARTITION_SNAPSHOT_INITIATION) {
            doPartitionSnapshot(message);
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
        } else if (!m_replicatedStreamFinished) {
            if (m_replicatedCompletionMonitor.isDone()) {
                notifyReplicatedSnapshotFinished();
                if (!m_shouldWaitForSnapshotCompletion) {
                    // TODO: hack to make producer finish early because
                    // partitioned table data transfer is not supported
                    // for k-safe join yet
                    HashMap<String, Map<Integer, Pair<Long, Long>>> export =
                        new HashMap<String, Map<Integer, Pair<Long, Long>>>();
                    setJoinComplete(siteConnection, export);
                    return;
                }
            } else {
                runForReplicatedDataTransfer(siteConnection);
            }
        } else if (!m_partitionedStreamFinished) {
            runForBlockingDataTransfer(siteConnection);
            return;
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
