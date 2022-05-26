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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.utils.CoreUtils;
import org.voltdb.DRConsumerDrIdTracker.DRSiteDrIdTracker;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.SnapshotCompletionInterest.SnapshotCompletionEvent;
import org.voltdb.SnapshotCompletionMonitor.ExportSnapshotTuple;
import org.voltdb.SnapshotSaveAPI;
import org.voltdb.VoltDB;
import org.voltdb.messaging.RejoinMessage;
import org.voltdb.messaging.RejoinMessage.Type;
import org.voltdb.rejoin.StreamSnapshotDataTarget;
import org.voltdb.rejoin.StreamSnapshotSink;
import org.voltdb.rejoin.StreamSnapshotSink.RestoreWork;
import org.voltdb.rejoin.TaskLog;

/**
 * Manages the lifecycle of snapshot serialization to a site
 * for the purposes of rejoin.
 */
public class RejoinProducer extends JoinProducerBase {
    private static final VoltLogger REJOINLOG = new VoltLogger("REJOIN");
    private static final long INITIAL_DATA_TIMEOUT_MS = Long.getLong("REJOIN_INITIAL_DATA_TIMEOUT_MS",
            TimeUnit.HOURS.toMillis(1));

    private final AtomicBoolean m_currentlyRejoining;
    private static ScheduledFuture<?> m_timeFuture;
    private Mailbox m_streamSnapshotMb = null;
    private StreamSnapshotSink m_rejoinSiteProcessor = null;

    // Barrier that prevents the finish task for firing until all sites have finished the stream snapshot
    private static AtomicInteger s_streamingSiteCount;


    // Get the snapshot nonce from the RejoinCoordinator's INITIATION message.
    // Then register the completion interest.
    //
    // When the completion interest callback fires for the nonce,
    // capture the snapshot txnid and tell the replay agent the snapshot is
    // complete and unregister the interest.
    //
    // The txnId informs post-rejoin replay where to start. TxnId
    // is stored in a ReplayCompletionAction which also bundles
    // the Replay complete logic; When the snapshot transfer is
    // complete, the RejoinProducer will hand the ReplayCompletionAction
    // to the Site which must take its own action to conclude rejoin and
    // then run the ReplayCompleteAction.
    //
    // The rest of RejoinProducer is driven by the snapshot sink..
    //
    // When the first data sink block arrives, offer the
    // producer to the SiteTaskerQueue.
    //
    // Blocking and nonblocking work the same now, the only difference
    // is that in blocking the snapshot transaction blocks on streaming all data
    //
    // In both cases, the site is responding with REJOINING to
    // all incoming tasks.
    //
    // Conditions to complete RejoinProducer and
    // transition to the Site controlled portion of rejoin:
    // 1. Snapshot must be fully transfered (sink EOF reached)
    // 2. The snapshot completion monitor callback must have been
    // triggered.
    //
    // The rejoin producer times out the snapshot block arrival.
    // If a block does not arrive within 60s, the RejoinProducer
    // terminates the current node. The watchdog timer is accessed
    // from multiple threads

    /**
     * ReplayCompletionAction communicates the snapshot txn id to
     * the site and offers a run() method that closes the open
     * rejoin state with the rejoin coordinator once the site has
     * concluded rejoin.
     */
    public class ReplayCompletionAction extends JoinCompletionAction
    {
        @Override
        public void run()
        {
            REJOINLOG.debug(m_whoami + "informing rejoinCoordinator "
                    + CoreUtils.hsIdToString(m_coordinatorHsId)
                    + " of REPLAY_FINISHED");
            RejoinMessage replay_complete = new RejoinMessage(
                    m_mailbox.getHSId(), RejoinMessage.Type.REPLAY_FINISHED);
            m_mailbox.send(m_coordinatorHsId, replay_complete);
            m_currentlyRejoining.set(false);

            SnapshotSaveAPI.recoveringSiteCount.decrementAndGet();
        }
    }

    public static void initBarrier(int siteCount) {
        s_streamingSiteCount = new AtomicInteger(siteCount);
    }

    // Run if the watchdog isn't cancelled within the timeout period
    private static class TimerCallback implements Runnable
    {
        final long m_timeout;
        private final String m_reason;

        static TimerCallback initialTimer() {
            return new TimerCallback(INITIAL_DATA_TIMEOUT_MS, "initial data not being sent from active nodes");
        }

        static TimerCallback dataTimer() {
            return new TimerCallback(StreamSnapshotDataTarget.DEFAULT_WRITE_TIMEOUT_MS,
                    "no data sent from active nodes");
        }

        private TimerCallback(long timeout, String reason) {
            super();
            m_timeout = timeout;
            m_reason = reason;
        }

        @Override
        public void run()
        {
            VoltDB.crashLocalVoltDB(String.format(
                    "Rejoin process timed out due to " + m_reason + " for %d seconds  Terminating rejoin.",
                    m_timeout / 1000),
                    false,
                    null);
        }
    }

    // Only instantiated if it must be used. This is important because
    // m_currentlyRejoining gates promotion to master. If the rejoin producer
    // is instantiated, it must complete its execution and set currentlyRejoining
    // to false.
    public RejoinProducer(int partitionId, SiteTaskerQueue taskQueue)
    {
        super(partitionId, "Rejoin producer:" + partitionId + " ", taskQueue);
        m_currentlyRejoining = new AtomicBoolean(true);
        m_completionAction = new ReplayCompletionAction();
        if (REJOINLOG.isDebugEnabled()) {
            REJOINLOG.debug(m_whoami + "created.");
        }
    }

    @Override
    public boolean acceptPromotion()
    {
        return !m_currentlyRejoining.get();
    }

    @Override
    public void deliver(RejoinMessage message)
    {
        if (message.getType() == RejoinMessage.Type.INITIATION
                || message.getType() == RejoinMessage.Type.INITIATION_COMMUNITY) {
            doInitiation(message);
        }
        else {
            VoltDB.crashLocalVoltDB(
                    "Unknown rejoin message type: " + message.getType(), false,
                    null);
        }
    }

    @Override
    public TaskLog constructTaskLog(String voltroot)
    {
        m_taskLog = initializeTaskLog(voltroot, m_partitionId);
        return m_taskLog;
    }

    @Override
    protected VoltLogger getLogger() {
        return REJOINLOG;
    }

    // cancel and maybe rearm the node-global snapshot data-segment watchdog.
    @Override
    protected void kickWatchdog(boolean rearm) {
        kickWatchdog(rearm ? TimerCallback.dataTimer() : null);
    }

    private static void kickWatchdog(TimerCallback callback)
    {
        synchronized (RejoinProducer.class) {
            if (m_timeFuture != null) {
                m_timeFuture.cancel(false);
                m_timeFuture = null;
            }
            if (callback != null) {
                m_timeFuture = VoltDB.instance().scheduleWork(callback, callback.m_timeout, 0, TimeUnit.MILLISECONDS);
            }
        }
    }

    /**
     * Runs when the RejoinCoordinator decides this site should start
     * rejoin.
     */
    void doInitiation(RejoinMessage message)
    {
        m_coordinatorHsId = message.m_sourceHSId;
        m_streamSnapshotMb = VoltDB.instance().getHostMessenger().createMailbox();
        m_rejoinSiteProcessor = new StreamSnapshotSink(m_streamSnapshotMb);
        // Start the watchdog so if we never get data it will notice
        kickWatchdog(TimerCallback.initialTimer());

        // MUST choose the leader as the source.
        long sourceSite = m_mailbox.getMasterHsId(m_partitionId);
        // The lowest partition has a single source for all messages whereas all other partitions have a real
        // data source and a dummy data source for replicated tables that are used to sync up replicated table changes.
        boolean haveTwoSources = VoltDB.instance().getLowestPartitionId() != m_partitionId;
        // Provide a valid sink host id unless it is an empty database.
        long hsId = (m_rejoinSiteProcessor != null
                        ? m_rejoinSiteProcessor.initialize(haveTwoSources?2:1,
                                                           message.getSnapshotDataBufferPool(),
                                                           message.getSnapshotCompressedDataBufferPool())
                        : Long.MIN_VALUE);

        REJOINLOG.debug(m_whoami
                + "received INITIATION message. Doing rejoin"
                + ". Source site is: "
                + CoreUtils.hsIdToString(sourceSite)
                + " and destination rejoin processor is: "
                + CoreUtils.hsIdToString(hsId)
                + " and snapshot nonce is: "
                + message.getSnapshotNonce());

        registerSnapshotMonitor(message.getSnapshotNonce());
        // Tell the RejoinCoordinator everything it will need to know to get us our snapshot stream.
        RejoinMessage initResp = new RejoinMessage(m_mailbox.getHSId(), sourceSite, hsId);
        m_mailbox.send(m_coordinatorHsId, initResp);

        // Start waiting for snapshot data
        m_taskQueue.offer(this);
    }

    /**
     * SiteTasker run -- load this site!
     *
     * run() is invoked when the RejoinProducer (this) submits itself to the
     * site tasker. RejoinProducer submits itself to the site tasker queue
     * when rejoin data is available. Rejoin data is available after the
     * snapshot request is fulfilled. The snapshot request is made by the rejoin
     * coordinator on our behalf.
     */
    @Override
    public void run(SiteProcedureConnection siteConnection)
    {
        throw new RuntimeException(
                "Unexpected execution of run method in rejoin producer.");
    }

    /**
     * An implementation of run() that does not block the site thread.
     * The Site has responsibility for transactions that occur between
     * schedulings of this task.
     */
    @Override
    public void runForRejoin(SiteProcedureConnection siteConnection,
            TaskLog m_taskLog) throws IOException
    {
        // After doInitialization(), the rejoin producer is inserted into the task queue,
        // and then we come here repeatedly until the stream snapshot restore finishes.
        // The first time when this producer method is run (m_commaSeparatedNameOfViewsToPause is null),
        // we need to figure out which views to pause so that they are handled properly
        // before the snapshot streams arrive.
        if (m_commaSeparatedNameOfViewsToPause == null) {
            initListOfViewsToPause();
            // Set enabled to false for the views we found.
            siteConnection.setViewsEnabled(m_commaSeparatedNameOfViewsToPause, false);
        }
        boolean sourcesReady = false;
        RestoreWork rejoinWork = m_rejoinSiteProcessor.poll(m_snapshotBufferAllocator);
        if (rejoinWork != null) {
            restoreBlock(rejoinWork, siteConnection);
            sourcesReady = true;
        }

        if (m_rejoinSiteProcessor.isEOF() == false) {
            returnToTaskQueue(sourcesReady);
        } else {
            REJOINLOG.debug(m_whoami + "Rejoin snapshot transfer is finished");
            m_rejoinSiteProcessor.close();

            boolean allSitesFinishStreaming;
            if (m_streamSnapshotMb != null) {
                VoltDB.instance().getHostMessenger().removeMailbox(m_streamSnapshotMb.getHSId());
                m_streamSnapshotMb = null;
                allSitesFinishStreaming = s_streamingSiteCount.decrementAndGet() == 0;
            } else {
                int pendingSites = s_streamingSiteCount.get();
                assert (pendingSites >= 0);
                allSitesFinishStreaming = pendingSites == 0;
            }
            if (allSitesFinishStreaming) {
                doFinishingTask(siteConnection);
            }
            else {
                returnToTaskQueue(sourcesReady);
            }
        }
    }

    private void doFinishingTask(final SiteProcedureConnection siteConnection) {
        /*
         * Don't notify the rejoin coordinator yet. The stream snapshot may
         * have not finished on all nodes, let the snapshot completion
         * monitor tell the rejoin coordinator.
         *
         * This used to block on the completion interest, but this raced
         * with fragments from the MPI that needed dummy responses. If the fragments
         * came after the EOF then they wouldn't receive dummy responses
         * and then the MPI wouldn't invoke SnapshotSaveAPI.logParticipatingHostCount
         */
        final SiteTasker finishingTask = new SiteTasker() {

            @Override
            public void run(SiteProcedureConnection siteConnection) {
                throw new RuntimeException(
                        "Unexpected execution of run method in rejoin producer.");
            }

            @Override
            public void runForRejoin(SiteProcedureConnection siteConnection, TaskLog rejoinTaskLog) throws IOException {
                if (!m_snapshotCompletionMonitor.isDone()) {
                    m_taskQueue.offer(this);
                    return;
                }
                assert(m_commaSeparatedNameOfViewsToPause != null);
                // Resume the views.
                siteConnection.setViewsEnabled(m_commaSeparatedNameOfViewsToPause, true);
                SnapshotCompletionEvent event = null;
                Map<String, Map<Integer, ExportSnapshotTuple>> exportSequenceNumbers = null;
                Map<Integer, Long> drSequenceNumbers = null;
                Map<Integer, Map<Integer, Map<Integer, DRSiteDrIdTracker>>> allConsumerSiteTrackers = null;
                Map<Byte, byte[]> drCatalogCommands = null;
                Map<Byte, String[]> replicableTables = null;
                long clusterCreateTime = -1;
                try {
                    event = m_snapshotCompletionMonitor.get();
                    REJOINLOG.debug(m_whoami + " waiting on snapshot completion monitor.");
                    exportSequenceNumbers = event.exportSequenceNumbers;
                    m_completionAction.setSnapshotTxnId(event.multipartTxnId);

                    drSequenceNumbers = event.drSequenceNumbers;
                    allConsumerSiteTrackers = event.drMixedClusterSizeConsumerState;
                    clusterCreateTime = event.clusterCreateTime;
                    drCatalogCommands = event.drCatalogCommands;
                    replicableTables = event.replicableTables;

                    // Tells EE which DR version going to use
                    if (event.drVersion != 0) {
                        siteConnection.setDRProtocolVersion(event.drVersion);
                    }

                    REJOINLOG.debug(m_whoami + " monitor completed. Sending SNAPSHOT_FINISHED "
                            + "and handing off to site.");
                    RejoinMessage snap_complete = new RejoinMessage(
                            m_mailbox.getHSId(), Type.SNAPSHOT_FINISHED);
                    m_mailbox.send(m_coordinatorHsId, snap_complete);
                } catch (InterruptedException crashme) {
                    VoltDB.crashLocalVoltDB(
                            "Interrupted awaiting snapshot completion.", true, crashme);
                } catch (ExecutionException e) {
                    VoltDB.crashLocalVoltDB(
                            "Unexpected exception awaiting snapshot completion.", true,
                            e);
                }
                if (exportSequenceNumbers == null) {
                    // Send empty sequence number map if the schema is empty (no tables).
                    exportSequenceNumbers = new HashMap<String, Map<Integer, ExportSnapshotTuple>>();
                }
                setJoinComplete(
                        siteConnection,
                        exportSequenceNumbers,
                        drSequenceNumbers,
                        allConsumerSiteTrackers,
                        drCatalogCommands,
                        replicableTables,
                        true /* requireExistingSequenceNumbers */,
                        clusterCreateTime);
            }
        };
        try {
            finishingTask.runForRejoin(siteConnection, null);
        } catch (IOException e) {
            VoltDB.crashLocalVoltDB("Unexpected IOException in rejoin", true, e);
        }
    }
}
