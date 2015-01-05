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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.SnapshotCompletionInterest.SnapshotCompletionEvent;
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

    private final AtomicBoolean m_currentlyRejoining;
    private static ScheduledFuture<?> m_timeFuture;
    private Mailbox m_streamSnapshotMb = null;
    private StreamSnapshotSink m_rejoinSiteProcessor = null;

    // True if we're handling a table-less rejoin.
    boolean m_schemaHasNoTables = false;

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

    // Run if the watchdog isn't cancelled within the timeout period
    private static class TimerCallback implements Runnable
    {
        @Override
        public void run()
        {
            VoltDB.crashLocalVoltDB(String.format(
                    "Rejoin process timed out due to no data sent from active nodes for %d seconds  Terminating rejoin.",
                    StreamSnapshotDataTarget.DEFAULT_WRITE_TIMEOUT_MS / 1000),
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
        REJOINLOG.debug(m_whoami + "created.");
    }

    @Override
    public boolean acceptPromotion()
    {
        return !m_currentlyRejoining.get();
    }

    @Override
    public void deliver(RejoinMessage message)
    {
        if (message.getType() == RejoinMessage.Type.INITIATION) {
            doInitiation(message);
        }
        else if (message.getType() == RejoinMessage.Type.INITIATION_COMMUNITY) {
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
    protected void kickWatchdog(boolean rearm)
    {
        synchronized (RejoinProducer.class) {
            if (m_timeFuture != null) {
                m_timeFuture.cancel(false);
                m_timeFuture = null;
            }
            if (rearm) {
                m_timeFuture = VoltDB.instance().scheduleWork(
                        new TimerCallback(),
                        StreamSnapshotDataTarget.DEFAULT_WRITE_TIMEOUT_MS,
                        0,
                        TimeUnit.MILLISECONDS);
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
        m_schemaHasNoTables = message.schemaHasNoTables();
        if (!m_schemaHasNoTables) {
            m_streamSnapshotMb = VoltDB.instance().getHostMessenger().createMailbox();
            m_rejoinSiteProcessor = new StreamSnapshotSink(m_streamSnapshotMb);
        }
        else {
            m_streamSnapshotMb = null;
            m_rejoinSiteProcessor = null;
        }

        // MUST choose the leader as the source.
        long sourceSite = m_mailbox.getMasterHsId(m_partitionId);
        // Provide a valid sink host id unless it is an empty database.
        long hsId = (m_rejoinSiteProcessor != null
                        ? m_rejoinSiteProcessor.initialize(message.getSnapshotSourceCount(),
                                                           message.getSnapshotBufferPool())
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
        if (!m_schemaHasNoTables) {
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

                if (m_streamSnapshotMb != null) {
                    VoltDB.instance().getHostMessenger().removeMailbox(m_streamSnapshotMb.getHSId());
                }

                doFinishingTask(siteConnection);
            }
        }
        else {
            doFinishingTask(siteConnection);
            // Remove the completion monitor for an empty (zero table) rejoin.
            m_snapshotCompletionMonitor.set(null);
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
                if (!m_schemaHasNoTables && !m_snapshotCompletionMonitor.isDone()) {
                    m_taskQueue.offer(this);
                    return;
                }
                SnapshotCompletionEvent event = null;
                Map<String, Map<Integer, Pair<Long,Long>>> exportSequenceNumbers = null;
                try {
                    if (!m_schemaHasNoTables) {
                        REJOINLOG.debug(m_whoami + "waiting on snapshot completion monitor.");
                        event = m_snapshotCompletionMonitor.get();
                        exportSequenceNumbers = event.exportSequenceNumbers;
                        m_completionAction.setSnapshotTxnId(event.multipartTxnId);
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
                    exportSequenceNumbers = new HashMap<String, Map<Integer, Pair<Long,Long>>>();
                }
                setJoinComplete(
                        siteConnection,
                        exportSequenceNumbers,
                        true /* requireExistingSequenceNumbers */);
            }
        };
        try {
            finishingTask.runForRejoin(siteConnection, null);
        } catch (IOException e) {
            VoltDB.crashLocalVoltDB("Unexpected IOException in rejoin", true, e);
        }
    }
}
