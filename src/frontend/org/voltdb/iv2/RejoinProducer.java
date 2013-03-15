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

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.SnapshotCompletionInterest.SnapshotCompletionEvent;
import org.voltdb.SnapshotSaveAPI;
import org.voltdb.VoltDB;
import org.voltdb.messaging.RejoinMessage;
import org.voltdb.messaging.RejoinMessage.Type;
import org.voltdb.rejoin.RejoinSiteProcessor;
import org.voltdb.rejoin.StreamSnapshotSink;
import org.voltdb.rejoin.TaskLog;

import org.voltdb.utils.MiscUtils;

/**
 * Manages the lifecycle of snapshot serialization to a site
 * for the purposes of rejoin.
 */
public class RejoinProducer extends JoinProducerBase {
    private static final VoltLogger REJOINLOG = new VoltLogger("REJOIN");

    private final AtomicBoolean m_currentlyRejoining;
    private ScheduledFuture<?> m_timeFuture;
    private RejoinSiteProcessor m_rejoinSiteProcessor;

    // True: use live rejoin; false use community blocking implementation.
    private boolean m_liveRejoin;
    private final TaskLog m_rejoinTaskLog;

    boolean useLiveRejoin()
    {
        return m_liveRejoin;
    }

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
    // Blocking rejoin blocks the tasker queue until
    // all snapshot data is transferred.
    //
    // Non-blocking rejoin releases the queue and reschedules
    // the producer with the site to incrementally transfer
    // snapshot data.
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
        private final String m_whoami;

        TimerCallback(String whoami)
        {
            m_whoami = whoami;
        }

        @Override
        public void run()
        {
            VoltDB.crashLocalVoltDB(m_whoami
                    + " timed out. Terminating rejoin.", false, null);
        }
    }

    // Only instantiated if it must be used. This is important because
    // m_currentlyRejoining gates promotion to master. If the rejoin producer
    // is instantiated, it must complete its execution and set currentlyRejoining
    // to false.
    public RejoinProducer(int partitionId, SiteTaskerQueue taskQueue, String voltroot,
                          boolean isLiveRejoin)
    {
        super(partitionId, "Rejoin producer:" + partitionId + " ", taskQueue);
        m_currentlyRejoining = new AtomicBoolean(true);
        m_completionAction = new ReplayCompletionAction();

        if (isLiveRejoin) {
            m_rejoinTaskLog = initializeForLiveRejoin(voltroot, m_partitionId);
        } else {
            m_rejoinTaskLog = initializeForCommunityRejoin();
        }
        REJOINLOG.debug(m_whoami + "created.");
    }

    private static TaskLog initializeForLiveRejoin(String voltroot, int pid)
    {
        // Construct task log and start logging task messages
        File overflowDir = new File(voltroot, "rejoin_overflow");
        Class<?> taskLogKlass =
                MiscUtils.loadProClass("org.voltdb.rejoin.TaskLogImpl", "Rejoin", false);
        if (taskLogKlass != null) {
            Constructor<?> taskLogConstructor;
            try {
                taskLogConstructor = taskLogKlass.getConstructor(int.class, File.class, boolean.class);
                return (TaskLog) taskLogConstructor.newInstance(pid, overflowDir, true);
            } catch (InvocationTargetException e) {
                VoltDB.crashLocalVoltDB("Unable to construct rejoin task log", true, e.getCause());
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB("Unable to construct rejoin task log", true, e);
            }
        }
        return null;
    }

    private static TaskLog initializeForCommunityRejoin()
    {
        return new TaskLog() {
            @Override
            public void logTask(TransactionInfoBaseMessage message)
                    throws IOException {
            }

            @Override
            public TransactionInfoBaseMessage getNextMessage()
                    throws IOException {
                return null;
            }

            @Override
            public void setEarliestTxnId(long txnId) {
            }

            @Override
            public boolean isEmpty() throws IOException {
                return true;
            }

            @Override
            public void close() throws IOException {
            }
        };
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
    public TaskLog getTaskLog()
    {
        return m_rejoinTaskLog;
    }

    // cancel and maybe rearm the snapshot data-segment watchdog.
    @Override
    protected void kickWatchdog(boolean rearm)
    {
        synchronized (this) {
            if (m_timeFuture != null) {
                m_timeFuture.cancel(false);
                m_timeFuture = null;
            }
            if (rearm) {
                m_timeFuture = VoltDB.instance().scheduleWork(
                        new TimerCallback(m_whoami), 60, 0, TimeUnit.SECONDS);
            }
        }
    }

    final AtomicReference<Pair<Integer, ByteBuffer>> firstWork =
        new AtomicReference<Pair<Integer, ByteBuffer>>();

    /**
     * Runs when the RejoinCoordinator decides this site should start
     * rejoin.
     */
    void doInitiation(RejoinMessage message)
    {
        m_liveRejoin = message.getType() == RejoinMessage.Type.INITIATION;
        m_coordinatorHsId = message.m_sourceHSId;
        m_rejoinSiteProcessor = new StreamSnapshotSink();
        String snapshotNonce = message.getSnapshotNonce();

        // MUST choose the leader as the source.
        long sourceSite = m_mailbox.getMasterHsId(m_partitionId);
        long hsId = m_rejoinSiteProcessor.initialize();

        REJOINLOG.debug(m_whoami
                + "received INITIATION message. Doing liverejoin: "
                + m_liveRejoin + ". Source site is: "
                + CoreUtils.hsIdToString(sourceSite)
                + " and destination rejoin processor is: "
                + CoreUtils.hsIdToString(hsId)
                + " and snapshot nonce is: "
                + snapshotNonce);

        SnapshotCompletionAction interest = new SnapshotCompletionAction(snapshotNonce);
        interest.register();
        // Tell the RejoinCoordinator everything it will need to know to get us our snapshot stream.
        RejoinMessage initResp = new RejoinMessage(m_mailbox.getHSId(), sourceSite, hsId);
        m_mailbox.send(m_coordinatorHsId, initResp);

        // A little awkward here...
        // The site must stay unblocked until the first snapshot data block arrrives.
        // Do a messy blocking poll on the first unit of work (by spinning!).
        // Save that first unit in "m_firstWork" and then enter the taskQueue.
        // Future: need a blocking peek on rejoinSiteProcessor(); then firstWork
        // can go away and the weird special casing of the first block in
        // run() can also go away.
        Thread firstSnapshotBlock = new Thread() {
            @Override
            public void run()
            {
                Pair<Integer, ByteBuffer> rejoinWork = null;
                try {
                    rejoinWork = m_rejoinSiteProcessor.take();
                } catch (InterruptedException e) {
                    VoltDB.crashLocalVoltDB(
                            "Interrupted in take()ing first snapshot block for rejoin",
                            true, e);
                }
                firstWork.set(rejoinWork);
                m_taskQueue.offer(RejoinProducer.this);
            }
        };
        firstSnapshotBlock.start();
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

    @Override
    public void runForRejoin(SiteProcedureConnection siteConnection,
            TaskLog m_taskLog) throws IOException
    {
        if (useLiveRejoin()) {
            runForLiveRejoin(siteConnection);
        } else {
            runForCommunityRejoin(siteConnection);
        }
    }

    /**
     * An implementation of run() that does not block the site thread.
     * The Site has responsibility for transactions that occur between
     * schedulings of this task.
     */
    void runForLiveRejoin(SiteProcedureConnection siteConnection)
    {
        // the first block is a special case.
        Pair<Integer, ByteBuffer> rejoinWork = firstWork.get();
        if (rejoinWork != null) {
            REJOINLOG.debug(m_whoami
                    + " executing first snapshot transfer for live rejoin.");
            firstWork.set(null);
        } else {
            rejoinWork = m_rejoinSiteProcessor.poll();
        }
        if (rejoinWork != null) {
            restoreBlock(rejoinWork, siteConnection);
        }

        if (m_rejoinSiteProcessor.isEOF() == false) {
            m_taskQueue.offer(this);
        } else {
            REJOINLOG.debug(m_whoami + "Rejoin snapshot transfer is finished");
            m_rejoinSiteProcessor.close();

            // m_rejoinSnapshotBytes = m_rejoinSiteProcessor.bytesTransferred();
            // m_rejoinSiteProcessor = null;

            // Don't notify the rejoin coordinator yet. The stream snapshot may
            // have not finished on all nodes, let the snapshot completion
            // monitor tell the rejoin coordinator.

            SnapshotCompletionEvent event = null;
            // Block until the snapshot interest triggers.
            try {
                REJOINLOG.debug(m_whoami
                        + "waiting on snapshot completion monitor.");
                event = m_completionMonitorAwait.get();
                REJOINLOG.debug(m_whoami
                        + "snapshot monitor completed. "
                        + "Sending SNAPSHOT_FINISHED and Handing off to site.");
                RejoinMessage snap_complete = new RejoinMessage(
                        m_mailbox.getHSId(), Type.SNAPSHOT_FINISHED);
                m_mailbox.send(m_coordinatorHsId, snap_complete);

            } catch (InterruptedException crashme) {
                VoltDB.crashLocalVoltDB(
                        "Interrupted awaiting snapshot completion.", true,
                        crashme);
            } catch (ExecutionException e) {
                VoltDB.crashLocalVoltDB(
                        "Unexpected exception awaiting snapshot completion.",
                        true, e);
            }
            setJoinComplete(siteConnection, event.exportSequenceNumbers);
        }
    }

    /**
     * An implementation of run() that blocks the site thread
     * until the streaming snapshot queue is emptied.
     */
    void runForCommunityRejoin(SiteProcedureConnection siteConnection)
    {
        Pair<Integer, ByteBuffer> rejoinWork = firstWork.get();
        REJOINLOG.debug(m_whoami
                + " executing first snapshot transfer for community rejoin.");
        firstWork.set(null);
        while (rejoinWork != null) {
            restoreBlock(rejoinWork, siteConnection);
            try {
                rejoinWork = m_rejoinSiteProcessor.take();
            } catch (InterruptedException e) {
                REJOINLOG.warn("RejoinProducer interrupted at take()");
                rejoinWork = null;
            }
        }
        REJOINLOG.debug(m_whoami + "Rejoin snapshot transfer is finished");
        m_rejoinSiteProcessor.close();

        // m_rejoinSnapshotBytes = m_rejoinSiteProcessor.bytesTransferred();
        // m_rejoinSiteProcessor = null;

        // A bit of a hack; in some large database cases with more than 2 nodes,
        // one of the nodes can finish streaming its sites LONG before the other
        // nodes.  Since we won't see the SnapshotCompletionMonitor fire until
        // all sites are done, the watchdog can fire first and abort the rejoin.
        // Just turn the watchdog off after we've gotten the last block.
        kickWatchdog(false);

        /*
         * Don't notify the rejoin coordinator yet. The stream snapshot may
         * have not finished on all nodes, let the snapshot completion
         * monitor tell the rejoin coordinator.
         */

        // Block until the snapshot interest triggers.
        // -- rtb: not sure this race can happen in the live rejoin case?
        // Maybe with tiny data sets? I'm going to accept
        // the simple and correct action of blocking until there
        // is an indication that a non-blocking wait is necesary.
        SnapshotCompletionEvent event = null;
        try {
            REJOINLOG.debug(m_whoami
                    + "waiting on snapshot completion monitor.");
            event = m_completionMonitorAwait.get();
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
        setJoinComplete(siteConnection, event.exportSequenceNumbers);
    }
}
