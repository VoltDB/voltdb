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
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;

import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltcore.utils.CoreUtils;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.SnapshotCompletionInterest.SnapshotCompletionEvent;
import org.voltdb.VoltDB;
import org.voltdb.VoltZK;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;
import org.voltdb.messaging.RejoinMessage;
import org.voltdb.rejoin.StreamSnapshotSink;
import org.voltdb.rejoin.StreamSnapshotSink.RestoreWork;
import org.voltdb.rejoin.TaskLog;

public class ElasticJoinProducer extends JoinProducerBase implements TaskLog {
    private static final VoltLogger JOINLOG = new VoltLogger("JOIN");

    // true if the site has received the first fragment task message
    private boolean m_receivedFirstFragment = false;
    // true if the site has notified the coordinator about the receipt of the first fragment
    // message
    private boolean m_firstFragResponseSent = false;

    // a snapshot sink used to stream table data from multiple sources
    private final StreamSnapshotSink m_dataSink;
    private final Mailbox m_streamSnapshotMb;

    private class CompletionAction extends JoinCompletionAction {
        @Override
        public void run()
        {
            RejoinMessage rm = new RejoinMessage(m_mailbox.getHSId(),
                                                 RejoinMessage.Type.REPLAY_FINISHED);
            m_mailbox.send(m_coordinatorHsId, rm);
        }
    }

    public ElasticJoinProducer(int partitionId, SiteTaskerQueue taskQueue)
    {
        super(partitionId, "Elastic join producer:" + partitionId + " ", taskQueue);
        m_completionAction = new CompletionAction();
        m_streamSnapshotMb = VoltDB.instance().getHostMessenger().createMailbox();
        m_dataSink = new StreamSnapshotSink(m_streamSnapshotMb);
    }

    /*
     * Inherit the per partition txnid from the long since gone
     * partition that existed in the past
     */
    private long[] fetchPerPartitionTxnId() {
        ZooKeeper zk = VoltDB.instance().getHostMessenger().getZK();
        byte partitionTxnIdsBytes[] = null;
        try {
            partitionTxnIdsBytes = zk.getData(VoltZK.perPartitionTxnIds, false, null);
        } catch (KeeperException.NoNodeException e){return null;}//Can be no node if the cluster was never restored
        catch (Exception e) {
            VoltDB.crashLocalVoltDB("Error retrieving per partition txn ids", true, e);
        }
        ByteBuffer buf = ByteBuffer.wrap(partitionTxnIdsBytes);

        int count = buf.getInt();
        Long partitionTxnId = null;
        long partitionTxnIds[] = new long[count];
        for (int ii = 0; ii < count; ii++) {
            long txnId = buf.getLong();
            partitionTxnIds[ii] = txnId;
            int partitionId = TxnEgo.getPartitionId(txnId);
            if (partitionId == m_partitionId) {
                partitionTxnId = txnId;
                continue;
            }
        }
        if (partitionTxnId != null) {
            return partitionTxnIds;
        }
        return null;
    }

    /*
     * Fetch and set the per partition txnid if necessary
     */
    private void applyPerPartitionTxnId(SiteProcedureConnection connection) {
        //If there was no ID nothing to do
        long partitionTxnIds[] = fetchPerPartitionTxnId();
        if (partitionTxnIds == null) return;
        connection.setPerPartitionTxnIds(partitionTxnIds, true);
    }

    private void doInitiation(RejoinMessage message)
    {
        m_coordinatorHsId = message.m_sourceHSId;
        registerSnapshotMonitor(message.getSnapshotNonce());

        long sinkHSId = m_dataSink.initialize(message.getSnapshotSourceCount(),
                                              message.getSnapshotBufferPool());

        // respond to the coordinator with the sink HSID
        RejoinMessage msg = new RejoinMessage(m_mailbox.getHSId(), -1, sinkHSId);
        m_mailbox.send(m_coordinatorHsId, msg);

        m_taskQueue.offer(this);
        JOINLOG.info("P" + m_partitionId + " received initiation");
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
     * Blocking transfer all partitioned table data and notify the coordinator.
     * @param siteConnection
     */
    private void runForBlockingDataTransfer(SiteProcedureConnection siteConnection)
    {
        boolean sourcesReady = false;
        RestoreWork restoreWork = m_dataSink.poll(m_snapshotBufferAllocator);
        if (restoreWork != null) {
            restoreBlock(restoreWork, siteConnection);
            sourcesReady = true;
        }

        // The completion monitor may fire even if m_dataSink has not reached EOF in the case that there's no
        // replicated table in the database, so check for both conditions.
        if (m_dataSink.isEOF() || m_snapshotCompletionMonitor.isDone()) {
            // No more data from this data sink, close and remove it from the list
            m_dataSink.close();

            if (m_streamSnapshotMb != null) {
                VoltDB.instance().getHostMessenger().removeMailbox(m_streamSnapshotMb.getHSId());
            }

            JOINLOG.debug(m_whoami + " data transfer is finished");

            if (m_snapshotCompletionMonitor.isDone()) {
                try {
                    SnapshotCompletionEvent event = m_snapshotCompletionMonitor.get();
                    assert(event != null);
                    JOINLOG.debug("P" + m_partitionId + " noticed data transfer completion");
                    m_completionAction.setSnapshotTxnId(event.multipartTxnId);

                    setJoinComplete(siteConnection,
                                    event.exportSequenceNumbers,
                                    false /* requireExistingSequenceNumbers */);
                } catch (InterruptedException e) {
                    // isDone() already returned true, this shouldn't happen
                    VoltDB.crashLocalVoltDB("Impossible interruption happend", true, e);
                } catch (ExecutionException e) {
                    VoltDB.crashLocalVoltDB("Error waiting for snapshot to finish", true, e);
                }
            } else {
                m_taskQueue.offer(this);
            }
        } else {
            // The sources are not set up yet, don't block the site,
            // return here and retry later.
            returnToTaskQueue(sourcesReady);
        }
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
        }
    }

    @Override
    public TaskLog constructTaskLog(String voltroot)
    {
        m_taskLog = initializeTaskLog(voltroot, m_partitionId);
        return this;
    }

    @Override
    protected VoltLogger getLogger() {
        return JOINLOG;
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

            applyPerPartitionTxnId(siteConnection);
        } else {
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
    public boolean isEmpty() throws IOException
    {
        return m_taskLog.isEmpty();
    }

    @Override
    public void close() throws IOException
    {
        m_taskLog.close();
    }

    @Override
    public void enableRecording(long snapshotSpHandle) {
        //Implemented by the nest task log, it is enabled immediately on construction
    }
}
