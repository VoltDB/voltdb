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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;

import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.apache.zookeeper_voltpatches.data.Stat;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.SnapshotCompletionInterest.SnapshotCompletionEvent;
import org.voltdb.VoltDB;
import org.voltdb.VoltZK;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;
import org.voltdb.messaging.RejoinMessage;
import org.voltdb.rejoin.StreamSnapshotSink;
import org.voltdb.rejoin.TaskLog;

import com.google.common.util.concurrent.SettableFuture;

public class ElasticJoinProducer extends JoinProducerBase implements TaskLog {
    // true if the site has received the first fragment task message
    private boolean m_receivedFirstFragment = false;
    // true if the site has notified the coordinator about the receipt of the first fragment
    // message
    private boolean m_firstFragResponseSent = false;

    // data transfer snapshot completion monitor
    private final SettableFuture<SnapshotCompletionEvent> m_snapshotCompletionMonitor =
            SettableFuture.create();

    // a snapshot sink used to stream table data from multiple sources
    private final StreamSnapshotSink m_dataSink;
    private final Mailbox m_streamSnapshotMb;
    private Long m_partitionTxnId;
    private Stat m_partitionTxnIdStat = new Stat();
    private long m_partitionTxnIds[];

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
        m_streamSnapshotMb = VoltDB.instance().getHostMessenger().createMailbox();
        m_dataSink = new StreamSnapshotSink(m_streamSnapshotMb);
    }

    /*
     * Inherit the per partition txnid from the long since gone
     * partition that existed in the past
     */
    private void fetchPerPartitionTxnId() {
        ZooKeeper zk = VoltDB.instance().getHostMessenger().getZK();
        byte partitionTxnIdsBytes[] = null;
        try {
            partitionTxnIdsBytes = zk.getData(VoltZK.perPartitionTxnIds, false, m_partitionTxnIdStat);
        } catch (KeeperException.NoNodeException e){return;}//Can be no node if the cluster was never restored
        catch (Exception e) {
            VoltDB.crashLocalVoltDB("Error retrieving per partition txn ids", true, e);
        }
        ByteBuffer buf = ByteBuffer.wrap(partitionTxnIdsBytes);

        int count = buf.getInt();
        m_partitionTxnIds = new long[count];
        for (int ii = 0; ii < count; ii++) {
            long txnId = buf.getLong();
            m_partitionTxnIds[ii] = txnId;
            int partitionId = TxnEgo.getPartitionId(txnId);
            if (partitionId == m_partitionId) {
                m_partitionTxnId = txnId;
                return;
            }
        }
        if (m_partitionTxnId == null) {
            m_partitionTxnIds = null;
            m_partitionTxnIdStat = null;
        }
        System.out.println("Fetched " + count + " txnids and mine is " + m_partitionTxnId);
    }

    /*
     * Set the txnid on the site, clean up the ZK state by racing to remove self
     */
    private void applyAndCleanupPerPartitionTxnId(SiteProcedureConnection connection) {
        //If there was no ID nothing to do
        if (m_partitionTxnId == null) return;
        connection.setPerPartitionTxnIds(m_partitionTxnIds, true);
        ZooKeeper zk = VoltDB.instance().getHostMessenger().getZK();
        do {
            ByteBuffer txnIdsBuf = ByteBuffer.allocate(4 + (8 * (m_partitionTxnIds.length - 1)));
            txnIdsBuf.putInt(m_partitionTxnIds.length - 1);

            //Construct the new set of Ids without self, check to make sure it hasn't already been done
            boolean foundSelf = false;
            for (int ii = 0; ii < m_partitionTxnIds.length; ii++) {
                //Skip self and also not that self was present
                if (m_partitionTxnIds[ii] == m_partitionTxnId) {
                    foundSelf = true;
                    continue;
                }
                txnIdsBuf.putLong(m_partitionTxnIds[ii]);
            }

            //Some other replica already did the update, work is done so bail out
            if (!foundSelf) {
                m_partitionTxnIds = null;
                m_partitionTxnIdStat = null;
                return;
            }

            //Race to do the update
            try {
                zk.setData(VoltZK.perPartitionTxnIds, txnIdsBuf.array(), m_partitionTxnIdStat.getVersion());
                m_partitionTxnIds = null;
                m_partitionTxnIdStat = null;
                System.out.println("Updated per partition txnids for " + m_partitionTxnId);
                return;
            } catch (KeeperException.BadVersionException e) {
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB("Error updating per partition txn ids", true, e);
            }

            //Lost the race, get the new version and try and apply it again
            try {
                ByteBuffer update = ByteBuffer.wrap(zk.getData(VoltZK.perPartitionTxnIds, false, m_partitionTxnIdStat));
                int count = update.getInt();
                m_partitionTxnIds = new long[count];
                for (int ii = 0; ii < count; ii++) {
                    m_partitionTxnIds[ii] = update.getLong();
                }
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB("Error retrieving per partition txn ids", true, e);
            }
        } while (true);
    }

    private void doInitiation(RejoinMessage message)
    {
        m_coordinatorHsId = message.m_sourceHSId;
        fetchPerPartitionTxnId();
        String snapshotNonce = message.getSnapshotNonce();
        SnapshotCompletionAction interest =
                new SnapshotCompletionAction(snapshotNonce, m_snapshotCompletionMonitor);
        interest.register();

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
        Pair<Integer, ByteBuffer> tableBlock = m_dataSink.poll(m_snapshotBufferAllocator);
        // poll() could return null if the source indicated end of stream,
        // need to check on that before retry
        if (tableBlock == null && !m_dataSink.isEOF() && !m_snapshotCompletionMonitor.isDone()) {
            // The sources are not set up yet, don't block the site,
            // return here and retry later.
            m_taskQueue.offer(this);
            return;
        }

        // Block until all blocks for partitioned tables are streamed over.
        JOINLOG.info("P" + m_partitionId + " blocking data transfer starts");
        while (tableBlock != null) {
            if (JOINLOG.isTraceEnabled()) {
                JOINLOG.trace(m_whoami + "restoring table " + tableBlock.getFirst() +
                              " block of (" + tableBlock.getSecond().position() + "," +
                              tableBlock.getSecond().limit() + ")");
            }

            restoreBlock(tableBlock, siteConnection);

            // Block on the data sink for more data. If end of stream, it will return null.
            try {
                tableBlock = m_dataSink.take(m_snapshotBufferAllocator);
            } catch (InterruptedException e) {
                JOINLOG.warn("Transfer of data interrupted");
                tableBlock = null;
            }
        }

        // No more data from this data sink, close and remove it from the list
        assert m_dataSink.isEOF() || m_snapshotCompletionMonitor.isDone();
        m_dataSink.close();

        if (m_streamSnapshotMb != null) {
            VoltDB.instance().getHostMessenger().removeMailbox(m_streamSnapshotMb.getHSId());
        }

        JOINLOG.debug(m_whoami + " data transfer is finished");

        applyAndCleanupPerPartitionTxnId(siteConnection);

        SnapshotCompletionEvent event = null;
        try {
            event = m_snapshotCompletionMonitor.get();
            assert(event != null);
            JOINLOG.debug("P" + m_partitionId + " noticed data transfer completion");
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
        setJoinComplete(
                siteConnection,
                event.exportSequenceNumbers,
                false /* requireExistingSequenceNumbers */
                );
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
