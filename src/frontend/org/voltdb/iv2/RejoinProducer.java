/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.iv2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;
import org.voltdb.ClientResponseImpl;
import org.voltdb.PrivateVoltTableFactory;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.SnapshotCompletionInterest;
import org.voltdb.SnapshotCompletionInterest.SnapshotCompletionEvent;
import org.voltdb.SnapshotFormat;
import org.voltdb.SnapshotSaveAPI;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.client.ClientResponse;
import org.voltdb.messaging.RejoinMessage;
import org.voltdb.messaging.RejoinMessage.Type;
import org.voltdb.rejoin.RejoinSiteProcessor;
import org.voltdb.rejoin.StreamSnapshotSink;
import org.voltdb.rejoin.TaskLog;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;
import org.voltdb.sysprocs.saverestore.SnapshotUtil.SnapshotResponseHandler;

import com.google.common.util.concurrent.SettableFuture;

/**
 * Manages the lifecycle of snapshot serialization to a site
 * for the purposes of rejoin.
 */
public class RejoinProducer extends SiteTasker
{
    private static final VoltLogger HOSTLOG = new VoltLogger("HOST");
    private static final VoltLogger REJOINLOG = new VoltLogger("REJOIN");

    private final CountDownLatch m_snapshotAdapterAwait;
    private final SettableFuture<SnapshotCompletionEvent> m_completionMonitorAwait;
    private final SiteTaskerQueue m_taskQueue;
    private final int m_partitionId;
    private final String m_snapshotNonce;
    private final String m_whoami;
    private final AtomicBoolean m_currentlyRejoining;
    private ScheduledFuture<?> m_timeFuture;
    private InitiatorMailbox m_mailbox;
    private long m_rejoinCoordinatorHsId;
    private RejoinSiteProcessor m_rejoinSiteProcessor;
    private ReplayCompletionAction m_replayCompleteAction;

    // True: use live rejoin; false use community blocking implementation.
    private boolean m_liveRejoin;

    boolean useLiveRejoin()
    {
        return m_liveRejoin;
    }

    // Some notes for rtb's bad memory...
    //
    // Calculate the snapshot nonce first.
    //
    // Then register the completion interest.
    //
    // When the completion interest callback fires for the nonce,
    // tell the replay agent the snapshot is complete. And
    // unregister the interest. The only purpose of SnapshotCompletionAction
    // is to tell the replay agent the snapshot ended.
    //
    // When the snapshot *request* finishes (not the
    // snapshot itself), it will call the SnapshotResponseHandler
    // callback. This handler must capture the snapshot's txnId
    // so that post-rejoin replay knows where to start. This txnId
    // is stored in a ReplayCompletionAction, which also bundles
    // the Replay complete logic; When the snapshot transfer is
    // complete, the RejoinProducer will hand the ReplayCompletionAction
    // to the Site which must take its own action to conclude rejoin.
    //
    // The rest of RejoinProducer is driven by the snapshot sink..
    //
    // When the first data sink block arrives, offer the
    // producer to the SiteTaskerQueue.
    //
    // Blocking rejoin will block block the tasker queue until
    // all snapshot data is transferred.
    //
    // Non-blocking rejoin will release the queue and reschedule
    // the producer with the site to incrementally transfer
    // snapshot data.
    //
    // In both cases, the site is responding with REJOINING to
    // all incoming tasks.
    //
    // There are three conditions to complete RejoinProducer and
    // transition the Site controlled portion of rejoin. The last
    // two are controlled by CountDown monitors and block the
    // site thread until complete.
    // 1. Snapshot must be fully transfered (sink EOF reached)
    // 2. The replay complete action must be ready, meaning the
    // snapshot handler has responded with the snapshot txnid.
    // 3. The snapshot completion monitor callback must have been
    // triggered.
    //
    // When all of these events have been observed, the Site
    // is handed a ReplayCompletionAction and instructed to
    // complete its portion of rejoin.
    //
    // The rejoin producer times out the snapshot block arrival.
    // If a block does not arrive within 60s, the RejoinProducer
    // will terminate the current node. The watchdog timer
    // is accessed from multiple threads

    /**
     * SnapshotCompletionAction waits for the completion
     * notice of m_snapshotNonce and instructs the replay agent
     * that the snapshot completed.
     *
     * Inner class references m_mailbox, m_nonce, m_rejoinCoordinatorHsId.
     */
    private class SnapshotCompletionAction implements SnapshotCompletionInterest
    {
        private void register()
        {
            REJOINLOG.debug(m_whoami + "registering snapshot completion action");
            VoltDB.instance().getSnapshotCompletionMonitor().addInterest(this);
        }

        private void deregister()
        {
            REJOINLOG.debug(m_whoami + "deregistering snapshot completion action");
            VoltDB.instance().getSnapshotCompletionMonitor().removeInterest(this);
        }

        @Override
        public CountDownLatch snapshotCompleted(SnapshotCompletionEvent event)
        {
            if (event.nonce.equals(m_snapshotNonce)) {
                REJOINLOG.debug(m_whoami + "counting down snapshot monitor completion.");
                deregister();
                kickWatchdog(true);
                m_completionMonitorAwait.set(event);
            } else {
                REJOINLOG.debug(m_whoami
                        + " observed completion of irrelevant nonce: "
                        + event.nonce);
            }
            return null;
        }
    }

    /**
     * ReplayCompletionAction communicates the snapshot txn id to
     * the site and offers a run() method that closes the open
     * rejoin state with the rejoin coordinator once the site has
     * concluded rejoin.
     */
    public static class ReplayCompletionAction implements Runnable
    {
        private final long m_snapshotTxnId;
        private final Runnable m_action;

        ReplayCompletionAction(long snapshotTxnId, Runnable action)
        {
            m_snapshotTxnId = snapshotTxnId;
            m_action = action;
        }

        long getSnapshotTxnId()
        {
            return m_snapshotTxnId;
        }

        @Override
        public void run()
        {
            m_action.run();
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
    public RejoinProducer(int partitionId, SiteTaskerQueue taskQueue)
    {
        m_partitionId = partitionId;
        m_taskQueue = taskQueue;
        m_snapshotNonce = "Rejoin_" + m_partitionId + "_" + System.currentTimeMillis();
        m_completionMonitorAwait = SettableFuture.create();
        m_snapshotAdapterAwait = new CountDownLatch(1);
        m_whoami = "Rejoin producer:" + m_partitionId + " ";
        m_currentlyRejoining = new AtomicBoolean(true);
        REJOINLOG.debug(m_whoami + "created.");
    }

    public void setMailbox(InitiatorMailbox mailbox)
    {
        m_mailbox = mailbox;
    }


    public boolean acceptPromotion()
    {
        return !m_currentlyRejoining.get();
    }

    public void deliver(RejoinMessage message)
    {
        if (message.getType() == RejoinMessage.Type.INITIATION) {
            doInitiation(message);
        }
        else if (message.getType() == RejoinMessage.Type.INITIATION_COMMUNITY) {
            doInitiation(message);
        }
        else if (message.getType() == RejoinMessage.Type.REQUEST_RESPONSE) {
            doRequestResponse(message);
        } else {
            VoltDB.crashLocalVoltDB(
                    "Unknown rejoin message type: " + message.getType(), false,
                    null);
        }
    }

    // cancel and maybe rearm the snapshot data-segment watchdog.
    private void kickWatchdog(boolean rearm)
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
        m_rejoinCoordinatorHsId = message.m_sourceHSId;
        m_rejoinSiteProcessor = new StreamSnapshotSink();

        // MUST choose the leader as the source.
        long sourceSite = m_mailbox.getMasterHsId(m_partitionId);
        long hsId = m_rejoinSiteProcessor.initialize();

        REJOINLOG.debug(m_whoami
                + "received INITIATION message. Doing liverejoin: "
                + m_liveRejoin + " source site is: "
                + CoreUtils.hsIdToString(sourceSite)
                + " destination rejoin processor is: "
                + CoreUtils.hsIdToString(hsId));

        // Initiate a snapshot with stream snapshot target
        String data = makeSnapshotRequest(hsId, sourceSite);
        SnapshotCompletionAction interest = new SnapshotCompletionAction();
        interest.register();

        SnapshotUtil.requestSnapshot(0l, "", m_snapshotNonce, !useLiveRejoin(), // community rejoin uses blocking snapshot (true)
                SnapshotFormat.STREAM, data, m_handler, true);

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

    private String makeSnapshotRequest(long hsId, long sourceSite)
    {
        try {
            JSONStringer jsStringer = new JSONStringer();
            jsStringer.object();
            jsStringer.key("hsId").value(hsId);
            jsStringer.key("target_hsid").value(sourceSite);
            jsStringer.endObject();
            return jsStringer.toString();
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Failed to serialize to JSON", true, e);
        }
        // unreachable;
        return null;
    }

    /*
     * m_handler is called when a SnapshotUtil.requestSnapshot response occurs.
     * This callback runs on the snapshot daemon thread.
     */
    SnapshotResponseHandler m_handler = new SnapshotResponseHandler() {
        @Override
        public void handleResponse(ClientResponse resp)
        {
            if (resp == null) {
                VoltDB.crashLocalVoltDB("Failed to initiate rejoin snapshot",
                        false, null);
            } else if (resp.getStatus() != ClientResponseImpl.SUCCESS) {
                VoltDB.crashLocalVoltDB("Failed to initiate rejoin snapshot: "
                        + resp.getStatusString(), false, null);
            }

            VoltTable[] results = resp.getResults();
            if (SnapshotUtil.didSnapshotRequestSucceed(results)) {
                long txnId = -1;
                String appStatus = resp.getAppStatusString();
                if (appStatus == null) {
                    VoltDB.crashLocalVoltDB("Rejoin snapshot request failed: "
                            + resp.getStatusString(), false, null);
                }

                try {
                    JSONObject jsObj = new JSONObject(appStatus);
                    txnId = jsObj.getLong("txnId");
                } catch (JSONException e) {
                    VoltDB.crashLocalVoltDB(
                            "Failed to get the rejoin snapshot txnId", true, e);
                    return;
                }

                REJOINLOG.debug(m_whoami
                        + "received Snapshotresponse handler callback."
                        + " Snapshot txnId is: " + txnId);

                // Send a RequestResponse message to self to avoid synchronization
                RejoinMessage msg = new RejoinMessage(txnId);
                m_mailbox.send(m_mailbox.getHSId(), msg);
            } else {
                VoltDB.crashLocalVoltDB("Snapshot request for rejoin failed",
                        false, null);
            }
        }
    };

    // m_handler sends this message to communicate the snapshot txnid after
    // the snapshot has been initiated. Setup the ReplayCompleteAction
    // that the Site will need.
    void doRequestResponse(RejoinMessage message)
    {
        REJOINLOG.debug(m_whoami
                + "SnapshotResponse forwarded to RejoinProducer mbox.");

        Runnable action = new Runnable() {
            @Override
            public void run()
            {
                REJOINLOG.debug(m_whoami + "informing rejoinCoordinator "
                        + CoreUtils.hsIdToString(m_rejoinCoordinatorHsId)
                        + " of REPLAY_FINISHED");
                RejoinMessage replay_complete = new RejoinMessage(
                        m_rejoinCoordinatorHsId, Type.REPLAY_FINISHED);
                m_mailbox.send(m_rejoinCoordinatorHsId, replay_complete);
                m_currentlyRejoining.set(false);
            }
        };

        m_replayCompleteAction = new ReplayCompletionAction(
                message.getSnapshotTxnId(), action);
        m_snapshotAdapterAwait.countDown();
    }

    /**
     * SiteTasker run -- load this site!
     *
     * run() is invoked when the RejoinProducer (this) submits itself to the
     * site tasker. RejoinProducer submits itself to the site tasker queue
     * when rejoin data is available. Rejoin data is available after the
     * snapshot request is fulfilled. The snapshot request is triggered
     * by the node-wise snapshot coordinator telling this producer that it's
     * its turn to start the rejoin sequence.
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
                        + "waiting on snapshot response adapter.");
                m_snapshotAdapterAwait.await();
                REJOINLOG.debug(m_whoami
                        + "snapshot monitor and adapter complete. "
                        + "Sending SNAPSHOT_FINISHED and Handing off to site.");
                // Send snapshot finished message once both monitor and adapter complete.
                RejoinMessage snap_complete = new RejoinMessage(
                        m_rejoinCoordinatorHsId, Type.SNAPSHOT_FINISHED);
                m_mailbox.send(m_rejoinCoordinatorHsId, snap_complete);

            } catch (InterruptedException crashme) {
                VoltDB.crashLocalVoltDB(
                        "Interrupted awaiting snapshot completion.", true,
                        crashme);
            } catch (ExecutionException e) {
                VoltDB.crashLocalVoltDB(
                        "Unexpected exception awaiting snapshot completion.",
                        true, e);
            }
            setRejoinComplete(siteConnection, event.exportSequenceNumbers);
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
                HOSTLOG.warn("RejoinProducer interrupted at take()");
                rejoinWork = null;
            }
        }
        REJOINLOG.debug(m_whoami + "Rejoin snapshot transfer is finished");
        m_rejoinSiteProcessor.close();

        // m_rejoinSnapshotBytes = m_rejoinSiteProcessor.bytesTransferred();
        // m_rejoinSiteProcessor = null;

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
            REJOINLOG.debug(m_whoami + "waiting on snapshot response adapter.");
            m_snapshotAdapterAwait.await();
            REJOINLOG
                    .debug(m_whoami
                            + "snapshot monitor and adapter complete. Handing off to site.");
            // Send snapshot finished message once both monitor and adapter complete.
            RejoinMessage snap_complete = new RejoinMessage(
                    m_rejoinCoordinatorHsId, Type.SNAPSHOT_FINISHED);
            m_mailbox.send(m_rejoinCoordinatorHsId, snap_complete);
        } catch (InterruptedException crashme) {
            VoltDB.crashLocalVoltDB(
                    "Interrupted awaiting snapshot completion.", true, crashme);
        } catch (ExecutionException e) {
            VoltDB.crashLocalVoltDB(
                    "Unexpected exception awaiting snapshot completion.", true,
                    e);
        }
        setRejoinComplete(siteConnection, event.exportSequenceNumbers);
    }

    // Received a datablock. Reset the watchdog timer and hand the block to the Site.
    void restoreBlock(Pair<Integer, ByteBuffer> rejoinWork,
            SiteProcedureConnection siteConnection)
    {
        kickWatchdog(true);

        int tableId = rejoinWork.getFirst();
        ByteBuffer buffer = rejoinWork.getSecond();
        VoltTable table = PrivateVoltTableFactory.createVoltTableFromBuffer(
                buffer.duplicate(), true);

        // Currently, only export cares about this TXN ID.  Since we don't have one handy, and IV2
        // doesn't yet care about export, just use Long.MIN_VALUE
        siteConnection.loadTable(Long.MIN_VALUE, tableId, table);
    }

    // Completed all criteria: Kill the watchdog and inform the site.
    void setRejoinComplete(SiteProcedureConnection siteConnection,
            Map<String, Map<Integer, Pair<Long, Long>>> exportSequenceNumbers)
    {
        kickWatchdog(false);
        siteConnection.setRejoinComplete(m_replayCompleteAction, exportSequenceNumbers);
    }
}
