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

package org.voltdb;

import java.io.File;
import java.net.URI;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.KeeperException.NodeExistsException;
import org.apache.zookeeper_voltpatches.WatchedEvent;
import org.apache.zookeeper_voltpatches.Watcher;
import org.apache.zookeeper_voltpatches.Watcher.Event.EventType;
import org.apache.zookeeper_voltpatches.Watcher.Event.KeeperState;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.apache.zookeeper_voltpatches.data.Stat;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltcore.logging.VoltLogger;
import org.voltcore.network.Connection;
import org.voltcore.utils.CoreUtils;
import org.voltdb.catalog.SnapshotSchedule;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.sysprocs.SnapshotSave;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;

import com.google.common.base.Throwables;

/**
 * A scheduler of automated snapshots and manager of archived and retained snapshots.
 * The new functionality for handling truncation snapshots operates separately from
 * the old automated snapshots. They just share the same event processing threads. Future work
 * should merge them.
 *
 */
public class SnapshotDaemon implements SnapshotCompletionInterest {
    private class TruncationSnapshotAttempt {
        private String path;
        private String nonce;
        private boolean finished;
    }

    static int m_periodicWorkInterval = 2000;
    public static volatile int m_userSnapshotRetryInterval = 30;

    /*
     * Something that initiates procedures for the snapshot daemon.
     */
    public interface DaemonInitiator {
        public void initiateSnapshotDaemonWork(final String procedureName, long clientData, Object params[]);
    };

    private static final VoltLogger hostLog = new VoltLogger("HOST");
    private static final VoltLogger loggingLog = new VoltLogger("LOGGING");
    private final ScheduledThreadPoolExecutor m_es =
            new ScheduledThreadPoolExecutor(1, CoreUtils.getThreadFactory("SnapshotDaemon"),
                                            new java.util.concurrent.ThreadPoolExecutor.DiscardPolicy());

    private ZooKeeper m_zk;
    private DaemonInitiator m_initiator;
    private long m_nextCallbackHandle;
    private String m_truncationSnapshotPath;

    /*
     * Before doing truncation snapshot operations, wait a few seconds
     * to give a few nodes a chance to get into the same state WRT to truncation
     * so that a truncation snapshot will can service multiple truncation requests
     * that arrive at the same time.
     */
    int m_truncationGatheringPeriod = 10;

    private final TreeMap<Long, TruncationSnapshotAttempt> m_truncationSnapshotAttempts =
        new TreeMap<Long, TruncationSnapshotAttempt>();
    private Future<?> m_truncationSnapshotScanTask;

    private TimeUnit m_frequencyUnit;
    private long m_frequencyInMillis;
    private int m_frequency;
    private int m_retain;
    private String m_path;
    private String m_prefix;
    private String m_prefixAndSeparator;
    private Future<?> m_snapshotTask;

    private final HashMap<Long, ProcedureCallback> m_procedureCallbacks = new HashMap<Long, ProcedureCallback>();

    private final SimpleDateFormat m_dateFormat = new SimpleDateFormat("'_'yyyy.MM.dd.HH.mm.ss");

    // true if this SnapshotDaemon is the one responsible for generating
    // snapshots
    private boolean m_isActive = false;
    private long m_nextSnapshotTime;

    /**
     * Don't invoke sysprocs too close together.
     * Keep track of the last call and only do it after
     * enough time has passed.
     */
    private long m_lastSysprocInvocation = System.currentTimeMillis();
    static long m_minTimeBetweenSysprocs = 3000;

    /**
     * List of snapshots on disk sorted by creation time
     */
    final LinkedList<Snapshot> m_snapshots = new LinkedList<Snapshot>();

    /**
     * States the daemon can be in
     *
     */
    enum State {
        /*
         * Initial state
         */
        STARTUP,
        /*
         * Invoked @SnapshotScan, waiting for results.
         * Done once on startup to find number of snapshots on disk
         * at path with prefix
         */
        SCANNING,
        /*
         * Waiting in between snapshots
         */
        WAITING,
        /*
         * Deleting snapshots that are no longer going to be retained.
         */
        DELETING,
        /*
         * Initiated a snapshot. Will call snapshot scan occasionally to find out
         * when it completes.
         */
        SNAPSHOTTING,

        /*
         * Failure state. This state is entered when a sysproc
         * fails and the snapshot Daemon can't recover. An error is logged
         * and the Daemon stops working
         */
        FAILURE;
    }

    private State m_state = State.STARTUP;

    SnapshotDaemon() {
        m_es.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        m_es.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);

        m_frequencyUnit = null;
        m_retain = 0;
        m_frequency = 0;
        m_frequencyInMillis = 0;
        m_prefix = null;
        m_path = null;
        m_prefixAndSeparator = null;



        // Register the snapshot status to the StatsAgent
        SnapshotStatus snapshotStatus = new SnapshotStatus();
        VoltDB.instance().getStatsAgent().registerStatsSource(SysProcSelector.SNAPSHOTSTATUS,
                                                              0,
                                                              snapshotStatus);
        VoltDB.instance().getSnapshotCompletionMonitor().addInterest(this);
    }

    public void init(DaemonInitiator initiator, ZooKeeper zk, Runnable threadLocalInit, GlobalServiceElector gse) {
        m_initiator = initiator;
        m_zk = zk;

        try {
            zk.create(VoltZK.nodes_currently_snapshotting, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (Exception e) {}
        try {
            zk.create(VoltZK.completed_snapshots, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (Exception e) {}

        if (threadLocalInit != null) {
            m_es.execute(threadLocalInit);
        }

        /*
         *  Really shouldn't leak this from a constructor, and twice to boot
         *  If IV2 is enabled leader election for the snapshot daemon is always tied to
         *  leader election for the MP coordinator so that they can't be partitioned
         *  from each other.
         */
        if (gse == null) {
            m_es.execute(new Runnable() {
                @Override
                public void run() {
                    leaderElection();
                }
            });
        } else {
            gse.registerService(new Promotable() {
                @Override
                public void acceptPromotion() throws InterruptedException,
                        ExecutionException, KeeperException {
                    m_es.submit(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                electedTruncationLeader();
                            } catch (Exception e) {
                                VoltDB.crashLocalVoltDB("Exception in snapshot daemon electing master via ZK", true, e);
                            }
                        }
                    });
                }

            });
        }
    }

    /*
     *  Search for truncation snapshots, after a failure there may be
     *  ones we don't know about, there may be ones from a previous instance etc.
     *  Do this every five minutes as an easy hack to make sure we don't leak them.
     *  Next time groom is called it will delete the old ones after a success.
     */
    private void scanTruncationSnapshots() {
        if (m_truncationSnapshotPath == null) {
            try {
                m_truncationSnapshotPath = new String(m_zk.getData(VoltZK.test_scan_path, false, null), "UTF-8");
            } catch (Exception e) {
                return;
            }
        }

        Object params[] = new Object[1];
        params[0] = m_truncationSnapshotPath;
        long handle = m_nextCallbackHandle++;
        m_procedureCallbacks.put(handle, new ProcedureCallback() {

            @Override
            public void clientCallback(final ClientResponse clientResponse)
                    throws Exception {
                if (clientResponse.getStatus() != ClientResponse.SUCCESS){
                    hostLog.error(clientResponse.getStatusString());
                    return;
                }

                final VoltTable results[] = clientResponse.getResults();
                if (results.length == 1) {
                    setState(State.FAILURE);
                    final VoltTable result = results[0];
                    boolean advanced = result.advanceRow();
                    assert(advanced);
                    assert(result.getColumnCount() == 1);
                    assert(result.getColumnType(0) == VoltType.STRING);
                    loggingLog.error("Snapshot scan failed with failure response: " + result.getString("ERR_MSG"));
                    return;
                }
                assert(results.length == 3);

                final VoltTable snapshots = results[0];
                assert(snapshots.getColumnCount() == 9);

                TreeMap<Long, TruncationSnapshotAttempt> foundSnapshots =
                    new TreeMap<Long, TruncationSnapshotAttempt>();
                while (snapshots.advanceRow()) {
                    final String path = snapshots.getString("PATH");
                    final String nonce = snapshots.getString("NONCE");
                    final Long txnId = snapshots.getLong("TXNID");
                    TruncationSnapshotAttempt snapshotAttempt = new TruncationSnapshotAttempt();
                    snapshotAttempt.path = path;
                    snapshotAttempt.nonce = nonce;
                    foundSnapshots.put(txnId, snapshotAttempt);
                }

                for (Map.Entry<Long, TruncationSnapshotAttempt> entry : foundSnapshots.entrySet()) {
                    if (!m_truncationSnapshotAttempts.containsKey(entry.getKey())) {
                        loggingLog.info("Truncation snapshot scan discovered new snapshot txnid " + entry.getKey() +
                                " path " + entry.getValue().path + " nonce " + entry.getValue().nonce);
                        m_truncationSnapshotAttempts.put(entry.getKey(), entry.getValue());
                    }
                }
            }

        });
        m_initiator.initiateSnapshotDaemonWork("@SnapshotScan", handle, params);
    }

    /*
     * Delete all snapshots older then the last successful snapshot.
     * This only effects snapshots used for log truncation
     */
    private void groomTruncationSnapshots() {
        ArrayList<TruncationSnapshotAttempt> toDelete = new ArrayList<TruncationSnapshotAttempt>();
        boolean foundMostRecentSuccess = false;
        Iterator<Map.Entry<Long, TruncationSnapshotAttempt>> iter =
            m_truncationSnapshotAttempts.descendingMap().entrySet().iterator();
        loggingLog.info("Snapshot daemon grooming truncation snapshots");
        while (iter.hasNext()) {
            Map.Entry<Long, TruncationSnapshotAttempt> entry = iter.next();
            TruncationSnapshotAttempt snapshotAttempt = entry.getValue();
            if (!foundMostRecentSuccess) {
                if (snapshotAttempt.finished) {
                    loggingLog.info("Found most recent successful snapshot txnid " + entry.getKey()
                            + " path " + entry.getValue().path + " nonce " + entry.getValue().nonce);
                    foundMostRecentSuccess = true;
                } else {
                    loggingLog.info("Retaining possible partial snapshot txnid " + entry.getKey()
                            + " path " + entry.getValue().path + " nonce " + entry.getValue().nonce);
                }
            } else {
                loggingLog.info("Deleting old unecessary snapshot txnid " + entry.getKey()
                        + " path " + entry.getValue().path + " nonce " + entry.getValue().nonce);
                toDelete.add(entry.getValue());
                iter.remove();
            }
        }

        String paths[] = new String[toDelete.size()];
        String nonces[] = new String[toDelete.size()];

        int ii = 0;
        for (TruncationSnapshotAttempt attempt : toDelete) {
            paths[ii] = attempt.path;
            nonces[ii++] = attempt.nonce;
        }

        Object params[] =
            new Object[] {
                paths,
                nonces,
                };
        long handle = m_nextCallbackHandle++;
        m_procedureCallbacks.put(handle, new ProcedureCallback() {

            @Override
            public void clientCallback(ClientResponse clientResponse)
                    throws Exception {
                if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                    hostLog.error(clientResponse.getStatusString());
                }
            }

        });
        m_initiator.initiateSnapshotDaemonWork("@SnapshotDelete", handle, params);
    }

    /*
     * If this cluster has per partition transactions ids carried over from
     * previous instances, retrieve them from ZK and pass them to snapshot save so that it can
     * include them in the snapshot
     */
    private JSONArray retrievePerPartitionTransactionIds() {
        JSONArray retval = new JSONArray();
        try {
            ByteBuffer values = ByteBuffer.wrap(m_zk.getData(VoltZK.perPartitionTxnIds, false, null));
            int numKeys = values.getInt();
            for (int ii = 0; ii < numKeys; ii++) {
                retval.put(values.getLong());
            }
        } catch (KeeperException.NoNodeException e) {/*doesn't have to exist*/}
        catch (Exception e) {
            VoltDB.crashLocalVoltDB("Failed to retrieve per partition transaction ids for snapshot", false, e);
        }
        return retval;
    }


    /**
     * Leader election for snapshots.
     * Leader will watch for truncation and user snapshot requests
     */
    private void leaderElection() {
        loggingLog.info("Starting leader election for snapshot truncation daemon");
        try {
            while (true) {
                Stat stat = m_zk.exists(VoltZK.snapshot_truncation_master, new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                        switch(event.getType()) {
                        case NodeDeleted:
                            loggingLog.info("Detected the snapshot truncation leader's ephemeral node deletion");
                            m_es.execute(new Runnable() {
                                @Override
                                public void run() {
                                    leaderElection();
                                }
                            });
                            break;
                            default:
                                break;
                        }
                    }
                });
                if (stat == null) {
                    try {
                        m_zk.create(VoltZK.snapshot_truncation_master, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                        electedTruncationLeader();
                        return;
                    } catch (NodeExistsException e) {
                    }
                } else {
                    loggingLog.info("Leader election concluded, a leader already exists");
                    break;
                }
            }
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Exception in snapshot daemon electing master via ZK", true, e);
        }
    }

    /*
     * Invoked when this snapshot daemon has been elected as leader
     */
    private void electedTruncationLeader() throws Exception {
        loggingLog.info("This node was selected as the leader for snapshot truncation");
        m_truncationSnapshotScanTask = m_es.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    scanTruncationSnapshots();
                } catch (Exception e) {
                    loggingLog.error("Error during scan and group of truncation snapshots");
                }
            }
        }, 0, 1, TimeUnit.HOURS);
        truncationRequestExistenceCheck();
        userSnapshotRequestExistenceCheck();
    }

    /*
     * Process the event generated when the node for a truncation request
     * is created, reschedules it for a few seconds later
     */
    private void processTruncationRequestEvent(final WatchedEvent event) {
        if (event.getType() == EventType.NodeCreated) {
            /*
             * Do it 10 seconds later because these requests tend to come in bunches
             * and we want one truncation snapshot to do truncation for all nodes
             * so we don't get them back to back
             */
            m_es.schedule(new Runnable() {
                @Override
                public void run() {
                    processSnapshotTruncationRequestCreated(event);
                }
            }, m_truncationGatheringPeriod, TimeUnit.SECONDS);
            return;
        }
    }

    /*
     * A ZK event occured requestion a truncation snapshot be taken
     */
    private void processSnapshotTruncationRequestCreated(
            final WatchedEvent event) {
        loggingLog.info("Snapshot truncation leader received snapshot truncation request");
        String snapshotPathTemp;
        try {
            snapshotPathTemp = new String(m_zk.getData(VoltZK.truncation_snapshot_path, false, null), "UTF-8");
        } catch (Exception e) {
            loggingLog.error("Unable to retrieve truncation snapshot path from ZK, log can't be truncated");
            return;
        }
        m_truncationSnapshotPath = snapshotPathTemp;
        final String snapshotPath = snapshotPathTemp;
        final long now = System.currentTimeMillis();
        final String nonce = Long.toString(now);
        //Allow nodes to check and see if the nonce incoming for a snapshot is
        //for a truncation snapshot. In that case they will mark the completion node
        //to be for a truncation snapshot. SnapshotCompletionMonitor notices the mark.
        try {
            ByteBuffer payload = ByteBuffer.allocate(8);
            payload.putLong(0, now);
            m_zk.setData(VoltZK.request_truncation_snapshot, payload.array(), -1);
        } catch (Exception e) {
            //Cause a cascading failure?
            VoltDB.crashLocalVoltDB("Setting data on the truncation snapshot request in ZK should never fail", true, e);
        }
        JSONObject jsObj = new JSONObject();
        try {
            jsObj.put("path", snapshotPath );
            jsObj.put("nonce", nonce);
            jsObj.put("perPartitionTxnIds", retrievePerPartitionTransactionIds());
        } catch (JSONException e) {
            /*
             * Should never happen, so fail fast
             */
            VoltDB.crashLocalVoltDB("", false, e);
        }

        long handle = m_nextCallbackHandle++;

        m_procedureCallbacks.put(handle, new ProcedureCallback() {

            @Override
            public void clientCallback(ClientResponse clientResponse)
                    throws Exception {
                if (clientResponse.getStatus() != ClientResponse.SUCCESS){
                    loggingLog.warn(
                            "Attempt to initiate a truncation snapshot was not successful: " +
                            clientResponse.getStatusString());
                    loggingLog.warn("Retrying log truncation snapshot in 5 minutes");
                    /*
                     * Try again in a few minute
                     */
                    m_es.schedule(new Runnable() {
                        @Override
                        public void run() {
                            processTruncationRequestEvent(event);
                        }
                    }, 5, TimeUnit.MINUTES);
                    return;
                }

                final VoltTable results[] = clientResponse.getResults();
                final VoltTable result = results[0];
                boolean success = true;
                if (result.getColumnCount() == 1) {
                    boolean advanced = result.advanceRow();
                    assert(advanced);
                    assert(result.getColumnCount() == 1);
                    assert(result.getColumnType(0) == VoltType.STRING);
                    loggingLog.error("Snapshot failed with failure response: " + result.getString(0));
                    success = false;
                }

                //assert(result.getColumnName(1).equals("TABLE"));
                if (success) {
                    while (result.advanceRow()) {
                        if (!result.getString("RESULT").equals("SUCCESS")) {
                            success = false;
                            loggingLog.warn("Snapshot save feasibility test failed for host "
                                    + result.getLong("HOST_ID") + " table " + result.getString("TABLE") +
                                    " with error message " + result.getString("ERR_MSG"));
                        }
                    }
                }

                if (success) {
                    /*
                     * Race to create the completion node before deleting
                     * the request node so that we can guarantee that the
                     * completion node will have the correct information
                     */
                    JSONObject obj = new JSONObject(clientResponse.getAppStatusString());
                    final long snapshotTxnId = Long.valueOf(obj.getLong("txnId"));
                    try {
                        m_zk.delete(VoltZK.request_truncation_snapshot, -1);
                    } catch (Exception e) {
                        VoltDB.crashLocalVoltDB(
                                "Unexpected error deleting truncation snapshot request", true, e);
                    }

                    SiteTracker st = VoltDB.instance().getSiteTrackerForSnapshot();
                    int hostId = SiteTracker.getHostForSite(st.getLocalSites()[0]);
                    if (!SnapshotSaveAPI.createSnapshotCompletionNode(nonce, snapshotTxnId,
                                                                      hostId, true)) {
                        SnapshotSaveAPI.increaseParticipateHostCount(snapshotTxnId, hostId);
                    }

                    try {
                        TruncationSnapshotAttempt snapshotAttempt =
                            m_truncationSnapshotAttempts.get(snapshotTxnId);
                        if (snapshotAttempt == null) {
                            snapshotAttempt = new TruncationSnapshotAttempt();
                            m_truncationSnapshotAttempts.put(snapshotTxnId, snapshotAttempt);
                        }
                        snapshotAttempt.nonce = nonce;
                        snapshotAttempt.path = snapshotPath;
                    } finally {
                        try {
                            truncationRequestExistenceCheck();
                        } catch (Exception e) {
                            VoltDB.crashLocalVoltDB(
                                    "Unexpected error checking for existence of truncation snapshot request"
                                    , true, e);
                        }
                    }
                } else {
                    loggingLog.info("Retrying log truncation snapshot in 60 seconds");
                    /*
                     * Try again in a few minutes
                     */
                    m_es.schedule(new Runnable() {
                        @Override
                        public void run() {
                            processTruncationRequestEvent(event);
                        }
                    }, 1, TimeUnit.MINUTES);
                }
            }

        });
        try {
            m_initiator.initiateSnapshotDaemonWork("@SnapshotSave", handle, new Object[] { jsObj.toString(4) });
        } catch (JSONException e) {
            /*
             * Should never happen, so fail fast
             */
            VoltDB.crashLocalVoltDB("", false, e);
        }
        return;
    }

    /*
     * Watcher that handles changes to the ZK node for
     * internal truncation snapshot requests
     */
    private final Watcher m_truncationRequestExistenceWatcher = new Watcher() {

        @Override
        public void process(final WatchedEvent event) {
            if (event.getState() == KeeperState.Disconnected) return;

            m_es.execute(new Runnable() {
                @Override
                public void run() {
                    processTruncationRequestEvent(event);
                }
            });
        }
    };

    /*
     * Watcher that handles events to the user snapshot request node
     * in ZK
     */
    private final Watcher m_userSnapshotRequestExistenceWatcher = new Watcher() {

        @Override
        public void process(final WatchedEvent event) {
            if (event.getState() == KeeperState.Disconnected) return;

            m_es.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        processUserSnapshotRequestEvent(event);
                    } catch (Exception e) {
                        VoltDB.crashLocalVoltDB("Error processing user snapshot request event", false, e);
                    }
                }
            });
        }
    };

    /*
     * Process the event generated when the node for a user snapshot request
     * is created.
     */
    private void processUserSnapshotRequestEvent(final WatchedEvent event) throws Exception {
        if (event.getType() == EventType.NodeCreated) {
            byte data[] = m_zk.getData(event.getPath(), false, null);
            String jsonString = new String(data, "UTF-8");
            final JSONObject jsObj = new JSONObject(jsonString);
            final String requestId = jsObj.getString("requestId");
            /*
             * Going to reuse the request object, remove the requestId
             * field now that it is consumed
             */
            jsObj.remove("requestId");
            jsObj.put("perPartitionTxnIds", retrievePerPartitionTransactionIds());
            final long handle = m_nextCallbackHandle++;
            m_procedureCallbacks.put(handle, new ProcedureCallback() {

                @Override
                public void clientCallback(ClientResponse clientResponse)
                        throws Exception {
                    /*
                     * If there is an error then we are done.
                     */
                    if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                        ClientResponseImpl rimpl = (ClientResponseImpl)clientResponse;
                        ByteBuffer buf = ByteBuffer.allocate(rimpl.getSerializedSize());
                        m_zk.create(
                                VoltZK.user_snapshot_response + requestId,
                                rimpl.flattenToBuffer(buf).array(),
                                Ids.OPEN_ACL_UNSAFE,
                                CreateMode.PERSISTENT);
                        //Reset the watch
                        userSnapshotRequestExistenceCheck();
                        return;
                    }

                    /*
                     * Now analyze the response. If a snapshot was in progress
                     * we have to reattempt it later, and send a response to the client
                     * saying it was queued. Otherwise, forward the response
                     * failure/success to the client.
                     */
                    if (isSnapshotInProgressResponse(clientResponse)) {
                        scheduleSnapshotForLater( jsObj.toString(4), requestId, true);
                    } else {
                        ClientResponseImpl rimpl = (ClientResponseImpl)clientResponse;
                        ByteBuffer buf = ByteBuffer.allocate(rimpl.getSerializedSize());
                        m_zk.create(
                                VoltZK.user_snapshot_response + requestId,
                                rimpl.flattenToBuffer(buf).array(),
                                Ids.OPEN_ACL_UNSAFE,
                                CreateMode.PERSISTENT);
                        //Reset the watch
                        userSnapshotRequestExistenceCheck();
                        return;
                    }
                }
            });
            m_initiator.initiateSnapshotDaemonWork(
                    "@SnapshotSave",
                    handle,
                    new Object[] { jsObj.toString(4) });
            return;
        }
    }

    /*
     * Schedule a user snapshot request for later since the database was busy.
     * Continue doing this as long as the error response returned by the DB is snapshot in progress.
     * Since the snapshot is being scheduled for later we will send an immediate response to the client
     * via ZK relay.
     */
    private void scheduleSnapshotForLater(
            final String requestObj,
            final String requestId,
            final boolean isFirstAttempt
            ) throws Exception {
        /*
         * Only need to send the queue response the first time we attempt to schedule the snapshot
         * for later. It may be necessary to reschedule via this function multiple times.
         */
        if (isFirstAttempt) {
            hostLog.info("A user snapshot request could not be immediately fulfilled and will be reattempted later");
            /*
             * Construct a result to send to the client right now via ZK
             * saying we queued it to run later
             */
            VoltTable result = SnapshotSave.constructNodeResultsTable();
            result.addRow(-1,
                    CoreUtils.getHostnameOrAddress(),
                    "",
                    "SUCCESS",
                    "SNAPSHOT REQUEST QUEUED");
            final ClientResponseImpl queuedResponse =
                new ClientResponseImpl(ClientResponseImpl.SUCCESS,
                                       new VoltTable[] { result },
                                       "Snapshot request could not be fulfilled because a snapshot " +
                                         "is in progress. It was queued for execution",
                                       0);
            ByteBuffer buf = ByteBuffer.allocate(queuedResponse.getSerializedSize());
            m_zk.create(VoltZK.user_snapshot_response + requestId,
                        queuedResponse.flattenToBuffer(buf).array(),
                        Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
        }

        /*
         * Now queue the request for later
         */
        final Runnable r = new Runnable() {
            @Override
            public void run() {
                try {
                    /*
                     * Construct a callback to handle the response to the
                     * @SnapshotSave invocation that will reattempt the user snapshot
                     */
                    final long handle = m_nextCallbackHandle++;
                    m_procedureCallbacks.put(handle, new ProcedureCallback() {
                        @Override
                        public void clientCallback(ClientResponse clientResponse)
                                throws Exception {
                            /*
                             * If there is an error then we are done
                             * attempting this user snapshot. The params must be bad
                             * or things are broken.
                             */
                            if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                                hostLog.error(clientResponse.getStatusString());
                                //Reset the watch, in case this is recoverable
                                userSnapshotRequestExistenceCheck();
                                return;
                            }

                            VoltTable results[] = clientResponse.getResults();
                            //Do this check to avoid an NPE
                            if (results == null || results.length == 0 || results[0].getRowCount() < 1) {
                                hostLog.error("Queued user snapshot request reattempt received an unexpected response" +
                                        " and will not be reattempted");
                                /*
                                 * Don't think this should happen, reset the watch to allow later requests
                                 */
                                userSnapshotRequestExistenceCheck();
                                return;
                            }

                            VoltTable result = results[0];
                            boolean snapshotInProgress = false;
                            boolean haveFailure = false;
                            while (result.advanceRow()) {
                                if (result.getString("RESULT").equals("FAILURE")) {
                                    if (result.getString("ERR_MSG").equals("SNAPSHOT IN PROGRESS")) {
                                        snapshotInProgress = true;
                                    } else {
                                        haveFailure = true;
                                    }
                                }
                            }

                            /*
                             * If a snapshot was in progress, reattempt later, otherwise,
                             * if there was a failure, abort the attempt and log.
                             */
                            if (snapshotInProgress) {
                                hostLog.info("Queued user snapshot was reattempted, but a snapshot was " +
                                        " still in progress. It will be reattempted.");
                                //Turtles all the way down
                                scheduleSnapshotForLater(
                                        requestObj,
                                        null,//null because it shouldn't be used, request already responded to
                                        false);
                            } else if (haveFailure) {
                                hostLog.info("Queued user snapshot was attempted, but there was a failure.");
                                if (requestId != null) {
                                    ClientResponseImpl rimpl = (ClientResponseImpl)clientResponse;
                                    ByteBuffer buf = ByteBuffer.allocate(rimpl.getSerializedSize());
                                    m_zk.create(
                                            VoltZK.user_snapshot_response + requestId,
                                            rimpl.flattenToBuffer(buf).array(),
                                            Ids.OPEN_ACL_UNSAFE,
                                            CreateMode.PERSISTENT);
                                }
                                //Reset the watch, in case this is recoverable
                                userSnapshotRequestExistenceCheck();
                                //Log the details of the failure, after resetting the watch in case of some odd NPE
                                result.resetRowPosition();
                                hostLog.info(result);
                            } else {
                                if (requestId != null) {
                                    hostLog.debug("Queued user snapshot was successfully requested, saving to path " +
                                            VoltZK.user_snapshot_response + requestId);
                                    /*
                                     * Snapshot was started no problem, reset the watch for new requests
                                     */
                                    ClientResponseImpl rimpl = (ClientResponseImpl)clientResponse;
                                    ByteBuffer buf = ByteBuffer.allocate(rimpl.getSerializedSize());
                                    m_zk.create(
                                            VoltZK.user_snapshot_response + requestId,
                                            rimpl.flattenToBuffer(buf).array(),
                                            Ids.OPEN_ACL_UNSAFE,
                                            CreateMode.PERSISTENT);
                                }
                                userSnapshotRequestExistenceCheck();
                                return;
                            }
                        }
                    });

                    m_initiator.initiateSnapshotDaemonWork("@SnapshotSave", handle,
                            new Object[] { requestObj });
                } catch (Exception e) {
                }
            }
        };
        m_es.schedule(r, m_userSnapshotRetryInterval, TimeUnit.SECONDS);
    }

    /*
     * Check a client response to and determine if it is a snapshot in progress response
     * to a snapshot request
     */
    private boolean isSnapshotInProgressResponse(
            ClientResponse response) {
        if (response.getStatus() != ClientResponse.SUCCESS) {
            return false;
        }

        if (response.getResults() == null) {
            return false;
        }

        if (response.getResults().length < 1) {
            return false;
        }

        VoltTable results = response.getResults()[0];
        if (results.getRowCount() < 1) {
            return false;
        }

        boolean snapshotInProgress = false;
        while (results.advanceRow()) {
            if (results.getString("RESULT").equals("FAILURE")) {
                if (results.getString("ERR_MSG").equals("SNAPSHOT IN PROGRESS")) {
                    snapshotInProgress = true;
                }
            }
        }
        return snapshotInProgress;
    }

    /*
     * Set the watch in ZK on the node that represents an internal request
     * for a truncation snapshot
     */
    void truncationRequestExistenceCheck() throws KeeperException, InterruptedException {
        if (m_zk.exists(VoltZK.request_truncation_snapshot, m_truncationRequestExistenceWatcher) != null) {
            processTruncationRequestEvent(new WatchedEvent(
                    EventType.NodeCreated,
                    KeeperState.SyncConnected,
                    VoltZK.snapshot_truncation_master));
        }
    }

    /*
     * Set the watch in ZK on the node that represents a user
     * request for a snapshot
     */
    void userSnapshotRequestExistenceCheck() throws Exception {
        m_zk.delete(VoltZK.user_snapshot_request, -1, null, null);
        if (m_zk.exists(VoltZK.user_snapshot_request, m_userSnapshotRequestExistenceWatcher) != null) {
            processUserSnapshotRequestEvent(new WatchedEvent(
                    EventType.NodeCreated,
                    KeeperState.SyncConnected,
                    VoltZK.user_snapshot_request));
        }
    }

    /**
     * Make this SnapshotDaemon responsible for generating snapshots
     */
    public Future<Void> makeActive(final SnapshotSchedule schedule)
    {
        return m_es.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                makeActivePrivate(schedule);
                return null;
            }
        });
    }

    private void makeActivePrivate(final SnapshotSchedule schedule) {
        m_isActive = true;
        m_frequency = schedule.getFrequencyvalue();
        m_retain = schedule.getRetain();
        m_path = schedule.getPath();
        m_prefix = schedule.getPrefix();
        m_prefixAndSeparator = m_prefix + "_";
        final String frequencyUnitString = schedule.getFrequencyunit().toLowerCase();
        assert(frequencyUnitString.length() == 1);
        final char frequencyUnit = frequencyUnitString.charAt(0);

        switch (frequencyUnit) {
        case 's':
            m_frequencyUnit = TimeUnit.SECONDS;
            break;
        case 'm':
            m_frequencyUnit = TimeUnit.MINUTES;
            break;
        case 'h':
            m_frequencyUnit = TimeUnit.HOURS;
            break;
            default:
                throw new RuntimeException("Frequency unit " + frequencyUnitString + "" +
                        " in snapshot schedule is not one of d,m,h");
        }
        m_frequencyInMillis = TimeUnit.MILLISECONDS.convert( m_frequency, m_frequencyUnit);
        m_nextSnapshotTime = System.currentTimeMillis() + m_frequencyInMillis;
        m_es.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    doPeriodicWork(System.currentTimeMillis());
                } catch (Exception e) {

                }
            }
        }, 0, m_periodicWorkInterval, TimeUnit.MILLISECONDS);
    }

    public void makeInactive() {
        m_es.execute(new Runnable() {
            @Override
            public void run() {
                m_isActive = false;
                m_snapshots.clear();
            }
        });
    }

    private class Snapshot implements Comparable<Snapshot> {
        private final String path;
        private final String nonce;
        private final Long txnId;

        private Snapshot (String path, String nonce, Long txnId) {
            this.path = path;
            this.nonce = nonce;
            this.txnId = txnId;
        }

        @Override
        public int compareTo(Snapshot o) {
            return txnId.compareTo(o.txnId);
        }

        @Override
        public String toString() {
            return path + "/" + nonce;
        }
    }

    /**
     * Invoked by the client interface occasionally. Returns null
     * if nothing needs to be done or the name of a sysproc along with procedure parameters
     * if there is work to be done. Responses come back later via invocations
     * of processClientResponse
     * @param now Current time
     * @return null if there is no work to do or a sysproc with parameters if there is work
     */
    private void doPeriodicWork(final long now) {
        if (!m_isActive)
        {
            setState(State.STARTUP);
            return;
        }

        if (m_frequencyUnit == null) {
            return;
        }

        if (m_state == State.STARTUP) {
            initiateSnapshotScan();
        } else if (m_state == State.SCANNING) {
            return;
        } else if (m_state == State.FAILURE) {
            return;
        } else if (m_state == State.WAITING){
            processWaitingPeriodicWork(now);
        } else if (m_state == State.SNAPSHOTTING) {
            return;
        } else if (m_state == State.DELETING){
            return;
        }
    }

    /**
     * Do periodic work when the daemon is in the waiting state. The
     * daemon paces out sysproc invocations over time
     * to avoid disrupting regular work. If the time for the next
     * snapshot has passed it attempts to initiate a new snapshot.
     * If there are too many snapshots being retains it attempts to delete
     * the extras. Then it attempts to initiate a new snapshot if
     * one is due
     */
    private void processWaitingPeriodicWork(long now) {
        if (now - m_lastSysprocInvocation < m_minTimeBetweenSysprocs) {
            return;
        }

        if (m_snapshots.size() > m_retain) {
            //Quick hack to make sure we don't delete while the snapshot is running.
            //Deletes work really badly during a snapshot because the FS is occupied
            if (SnapshotSiteProcessor.ExecutionSitesCurrentlySnapshotting.get() > 0) {
                m_lastSysprocInvocation = System.currentTimeMillis() + 3000;
                return;
            }
            deleteExtraSnapshots();
            return;
        }

        if (m_nextSnapshotTime < now) {
            initiateNextSnapshot(now);
            return;
        }
    }

    private void initiateNextSnapshot(long now) {
        setState(State.SNAPSHOTTING);
        m_lastSysprocInvocation = now;
        final Date nowDate = new Date(now);
        final String dateString = m_dateFormat.format(nowDate);
        final String nonce = m_prefix + dateString;
        JSONObject jsObj = new JSONObject();
        try {
            jsObj.put("path", m_path);
            jsObj.put("nonce", nonce);
            jsObj.put("perPartitionTxnIds", retrievePerPartitionTransactionIds());
            m_snapshots.offer(new Snapshot(m_path, nonce, now));
            long handle = m_nextCallbackHandle++;
            m_procedureCallbacks.put(handle, new ProcedureCallback() {

                @Override
                public void clientCallback(final ClientResponse clientResponse)
                        throws Exception {
                    processClientResponsePrivate(clientResponse);
                }

            });
            m_initiator.initiateSnapshotDaemonWork("@SnapshotSave", handle, new Object[] { jsObj.toString(4) });
        } catch (JSONException e) {
            /*
             * Should never happen, so fail fast
             */
            VoltDB.crashLocalVoltDB("", false, e);
        }
    }

    /**
     * Invoke the @SnapshotScan system procedure to discover
     * snapshots on disk that are managed by this daemon
     * @return
     */
    private void initiateSnapshotScan() {
        m_lastSysprocInvocation = System.currentTimeMillis();
        Object params[] = new Object[1];
        params[0] = m_path;
        setState(State.SCANNING);
        long handle = m_nextCallbackHandle++;
        m_procedureCallbacks.put(handle, new ProcedureCallback() {

            @Override
            public void clientCallback(final ClientResponse clientResponse)
                    throws Exception {
                processClientResponsePrivate(clientResponse);
            }

        });
        m_initiator.initiateSnapshotDaemonWork("@SnapshotScan", handle, params);
    }

    /**
     * Process responses to sysproc invocations generated by this daemon
     * via processPeriodicWork
     * @param response
     * @return
     */
    public Future<Void> processClientResponse(final Callable<ClientResponseImpl> response) {
        return m_es.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                try {
                    ClientResponseImpl resp = response.call();
                    long handle = resp.getClientHandle();
                    m_procedureCallbacks.remove(handle).clientCallback(resp);
                } catch (Exception e) {
                    hostLog.warn("Error when SnapshotDaemon invoked callback for a procedure invocation", e);
                    throw e;
                }
                return null;
            }
        });
    }

    private void processClientResponsePrivate(ClientResponse response) {
        if (m_frequencyUnit == null) {
            throw new RuntimeException("SnapshotDaemon received a response when it has not been configured to run");
        }

        if (m_state == State.STARTUP) {
            throw new RuntimeException("SnapshotDaemon received a response in the startup state");
        } else if (m_state == State.SCANNING) {
            processScanResponse(response);
        } else if (m_state == State.FAILURE) {
            return;
        } else if (m_state == State.DELETING){
            processDeleteResponse(response);
            return;
        } else if (m_state == State.SNAPSHOTTING){
            processSnapshotResponse(response);
            return;
        }
    }

    /**
     * Confirm and log that the snapshot was a success
     * @param response
     */
    private void processSnapshotResponse(ClientResponse response) {
        setState(State.WAITING);
        final long now = System.currentTimeMillis();
        m_nextSnapshotTime += m_frequencyInMillis;
        if (m_nextSnapshotTime < now) {
            m_nextSnapshotTime = now - 1;
        }

        if (response.getStatus() != ClientResponse.SUCCESS){
            setState(State.FAILURE);
            logFailureResponse("Snapshot failed", response);
            return;
        }

        final VoltTable results[] = response.getResults();
        final VoltTable result = results[0];

        if (result.getColumnCount() == 1) {
            boolean advanced = result.advanceRow();
            assert(advanced);
            assert(result.getColumnCount() == 1);
            assert(result.getColumnType(0) == VoltType.STRING);
            hostLog.error("Snapshot failed with failure response: " + result.getString(0));
            m_snapshots.removeLast();
            return;
        }

        //assert(result.getColumnName(1).equals("TABLE"));
        boolean success = true;
        while (result.advanceRow()) {
            if (!result.getString("RESULT").equals("SUCCESS")) {
                success = false;
                hostLog.warn("Snapshot save feasibility test failed for host "
                        + result.getLong("HOST_ID") + " table " + result.getString("TABLE") +
                        " with error message " + result.getString("ERR_MSG"));
            }
        }
        if (!success) {
            m_snapshots.removeLast();
        }
    }

    /**
     * Process a response to a request to delete snapshots.
     * Always transitions to the waiting state even if the delete
     * fails. This ensures the system will continue to snapshot
     * until the disk is full in the event that there is an administration
     * error or a bug.
     * @param response
     */
    private void processDeleteResponse(ClientResponse response) {
        //Continue snapshotting even if a delete fails.
        setState(State.WAITING);
        if (response.getStatus() != ClientResponse.SUCCESS){
            /*
             * The delete may fail but the procedure should at least return success...
             */
            setState(State.FAILURE);
            logFailureResponse("Delete of snapshots failed", response);
            return;
        }

        final VoltTable results[] = response.getResults();
        assert(results.length > 0);
        if (results[0].getColumnCount() == 1) {
            final VoltTable result = results[0];
            boolean advanced = result.advanceRow();
            assert(advanced);
            assert(result.getColumnCount() == 1);
            assert(result.getColumnType(0) == VoltType.STRING);
            hostLog.error("Snapshot delete failed with failure response: " + result.getString("ERR_MSG"));
            return;
        }
    }

    /**
     * Process the response to a snapshot scan. Find the snapshots
     * that are managed by this daemon by path and nonce
     * and add it the list. Initiate a delete of any that should
     * not be retained
     * @param response
     * @return
     */
    private void processScanResponse(ClientResponse response) {
        if (response.getStatus() != ClientResponse.SUCCESS){
            setState(State.FAILURE);
            logFailureResponse("Initial snapshot scan failed", response);
            return;
        }

        final VoltTable results[] = response.getResults();
        if (results.length == 1) {
            setState(State.FAILURE);
            final VoltTable result = results[0];
            boolean advanced = result.advanceRow();
            assert(advanced);
            assert(result.getColumnCount() == 1);
            assert(result.getColumnType(0) == VoltType.STRING);
            hostLog.error("Initial snapshot scan failed with failure response: " + result.getString("ERR_MSG"));
            return;
        }
        assert(results.length == 3);

        final VoltTable snapshots = results[0];
        assert(snapshots.getColumnCount() == 9);

        final File myPath = new File(m_path);
        while (snapshots.advanceRow()) {
            final String path = snapshots.getString("PATH");
            final File pathFile = new File(path);
            if (pathFile.equals(myPath)) {
                final String nonce = snapshots.getString("NONCE");
                if (nonce.startsWith(m_prefixAndSeparator)) {
                    final Long txnId = snapshots.getLong("TXNID");
                    m_snapshots.add(new Snapshot(path, nonce, txnId));
                }
            }
        }

        java.util.Collections.sort(m_snapshots);

        deleteExtraSnapshots();
    }

    /**
     * Check if there are extra snapshots and initiate deletion
     * @return
     */
    private void deleteExtraSnapshots() {
        if (m_snapshots.size() <= m_retain) {
            setState(State.WAITING);
        } else {
            m_lastSysprocInvocation = System.currentTimeMillis();
            setState(State.DELETING);
            final int numberToDelete = m_snapshots.size() - m_retain;
            String pathsToDelete[] = new String[numberToDelete];
            String noncesToDelete[] = new String[numberToDelete];
            for (int ii = 0; ii < numberToDelete; ii++) {
                final Snapshot s = m_snapshots.poll();
                pathsToDelete[ii] = s.path;
                noncesToDelete[ii] = s.nonce;
                hostLog.info("Snapshot daemon deleting " + s.nonce);
            }
            Object params[] =
                new Object[] {
                    pathsToDelete,
                    noncesToDelete,
                    };
            long handle = m_nextCallbackHandle++;
            m_procedureCallbacks.put(handle, new ProcedureCallback() {

                @Override
                public void clientCallback(final ClientResponse clientResponse)
                        throws Exception {
                    processClientResponsePrivate(clientResponse);
                }

            });
            m_initiator.initiateSnapshotDaemonWork("@SnapshotDelete", handle, params);
        }
    }

    private void logFailureResponse(String message, ClientResponse response) {
        hostLog.error(message, response.getException());
        if (response.getStatusString() != null) {
            hostLog.error(response.getStatusString());
        }
    }

    State getState() {
        return m_state;
    }

    void setState(State state) {
        m_state = state;
    }

    public void shutdown() throws InterruptedException {
        if (m_snapshotTask != null) {
            m_snapshotTask.cancel(false);
        }
        if (m_truncationSnapshotScanTask != null) {
            m_truncationSnapshotScanTask.cancel(false);
        }

        m_es.shutdown();
        m_es.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    }

    /*
     * If we are the leader, go ahead an create the procedure invocation to do the work.
     * We aren't going to journal this in ZK. if the leader dies there will be no
     * one to try and complete the work. C'est la vie.
     */
    public void requestUserSnapshot(final StoredProcedureInvocation invocation, final Connection c) {
        m_es.submit(new Runnable() {
            @Override
            public void run() {
                submitUserSnapshotRequest(invocation, c);
            }
        });
    }

    public static final class ForwardClientException extends Exception {
        /**
         *
         */
        private static final long serialVersionUID = 1L;
        private final VoltTable m_table;
        public ForwardClientException(String msg, VoltTable table) {
            super(msg);
            m_table = table;
        }

    }

    private void submitUserSnapshotRequest(final StoredProcedureInvocation invocation, final Connection c) {
        Object params[] = invocation.getParams().toArray();
        String path = null;
        String nonce = null;
        boolean blocking = false;
        SnapshotFormat format = SnapshotFormat.NATIVE;

        try {
            /*
             * Dang it, have to parse the params here to validate
             */
            if (params.length != 3 && params.length != 1) {
                throw new Exception("@SnapshotSave requires 3 parameters or alternatively a single JSON blob. " +
                        "Path, nonce, and blocking");
            }

            if (params[0] == null) {
                throw new Exception("@SnapshotSave path is null");
            }

            if (params.length == 3) {
                if (params[1] == null) {
                    throw new Exception("@SnapshotSave nonce is null");
                }

                if (params[2] == null) {
                    throw new Exception("@SnapshotSave blocking is null");
                }
            }

            if (!(params[0] instanceof String)) {
                throw new Exception("@SnapshotSave path param is a " +
                        params[0].getClass().getSimpleName() +
                        " and should be a java.lang.String");
            }

            if (params.length == 3) {
                if (!(params[1] instanceof String)) {
                    throw new Exception("@SnapshotSave nonce param is a " +
                            params[0].getClass().getSimpleName() +
                            " and should be a java.lang.String");
                }

                if (!(params[2] instanceof Byte ||
                        params[2] instanceof Short ||
                        params[2] instanceof Integer ||
                        params[2] instanceof Long)) {
                    throw new Exception("@SnapshotSave blocking param is a " +
                            params[0].getClass().getSimpleName() +
                            " and should be a java.lang.[Byte|Short|Integer|Long]");
                }
            }

            if (params.length == 1) {
                final JSONObject jsObj = new JSONObject((String)params[0]);

                path = jsObj.getString("uripath");
                if (path.isEmpty()) {
                    throw new Exception("uripath cannot be empty");
                }
                URI pathURI = new URI(path);
                String pathURIScheme = pathURI.getScheme();
                if (pathURIScheme == null) {
                    throw new Exception("URI scheme cannot be null");
                }
                if (!pathURIScheme.equals("file")) {
                    throw new Exception("Unsupported URI scheme " + pathURIScheme +
                            " if this is a file path then you must prepend file://");
                }
                path = pathURI.getPath();

                nonce = jsObj.getString("nonce");
                if (nonce.isEmpty()) {
                    throw new Exception("nonce cannot be empty");
                }

                Object blockingObj = false;
                if (jsObj.has("block")) {
                    blockingObj = jsObj.get("block");
                }
                if (blockingObj instanceof Number) {
                    blocking = ((Number)blockingObj).byteValue() == 0 ? false : true;
                } else if (blockingObj instanceof Boolean) {
                    blocking = (Boolean)blockingObj;
                } else if (blockingObj instanceof String) {
                    blocking = Boolean.valueOf((String)blockingObj);
                } else {
                    throw new Exception(blockingObj.getClass().getName() + " is not supported as " +
                            " type for the block parameter");
                }

                String formatString = jsObj.optString("format",SnapshotFormat.NATIVE.toString());
                /*
                 * Try and be very flexible about what we will accept
                 * as the type of the block parameter.
                 */
                try {
                    format = SnapshotFormat.getEnumIgnoreCase(formatString);
                } catch (IllegalArgumentException argException) {
                    throw new Exception("@SnapshotSave format param is a " + format +
                            " and should be one of [\"native\" | \"csv\"]");
                }
            } else {
                path = (String)params[0];
                nonce = (String)params[1];
                blocking = ((Number)params[2]).byteValue() == 0 ? false : true;
            }

            if (nonce.contains("-") || nonce.contains(",")) {
                throw new Exception("Provided nonce " + nonce + " contains a prohibited character (- or ,)");
            }

            createAndWatchRequestNode(invocation.clientHandle, c, path, nonce, blocking, format, null, false);
        } catch (Exception e) {
            VoltTable tables[] = new VoltTable[0];
            byte status = ClientResponseImpl.GRACEFUL_FAILURE;
            if (e instanceof ForwardClientException && ((ForwardClientException)e).m_table != null) {
                tables = new VoltTable[] { ((ForwardClientException)e).m_table };
                status = ClientResponseImpl.SUCCESS;
            }
            final ClientResponseImpl errorResponse =
                    new ClientResponseImpl(status,
                                           tables,
                                           Throwables.getStackTraceAsString(e),
                                           invocation.clientHandle);
            ByteBuffer buf = ByteBuffer.allocate(errorResponse.getSerializedSize() + 4);
            buf.putInt(buf.capacity() - 4);
            errorResponse.flattenToBuffer(buf).flip();
            c.writeStream().enqueue(buf);
            return;
        }
    }

    /**
     * Try to create the ZK request node and watch it if created successfully.
     *
     * @param clientHandle
     * @param c
     * @param path
     * @param nonce
     * @param blocking
     * @param format
     * @param data
     * @throws ForwardClientException
     */
    public void createAndWatchRequestNode(final long clientHandle,
                                          final Connection c,
                                          String path,
                                          String nonce,
                                          boolean blocking,
                                          SnapshotFormat format,
                                          String data,
                                          boolean notifyChanges) throws ForwardClientException {
        boolean requestExists = false;
        final String requestId = createRequestNode(path, nonce, blocking, format, data);
        if (requestId == null) {
            requestExists = true;
        } else {
            try {
                registerUserSnapshotResponseWatch(requestId, clientHandle, c, notifyChanges);
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB("Failed to register ZK watch on snapshot response", true, e);
            }
        }

        if (requestExists) {
            VoltTable result = org.voltdb.sysprocs.SnapshotSave.constructNodeResultsTable();
            result.addRow(-1,
                    CoreUtils.getHostnameOrAddress(),
                    "",
                    "FAILURE",
                    "SNAPSHOT IN PROGRESS");
            throw new ForwardClientException("A request to perform a user snapshot already exists", result);
        }
    }

    /**
     * Try to create the ZK node to request the snapshot.
     *
     * @param path can be null if the target is not a file target
     * @param nonce
     * @param blocking
     * @param format
     * @param data Any data to pass to the snapshot target
     * @return The request ID if succeeded, otherwise null.
     */
    private String createRequestNode(String path, String nonce,
                                     boolean blocking, SnapshotFormat format,
                                     String data) {
        String requestId = null;

        try {
            final JSONObject jsObj = new JSONObject();
            jsObj.put("path", path);
            jsObj.put("nonce", nonce);
            jsObj.put("block", blocking);
            jsObj.put("format", format.toString());
            requestId = java.util.UUID.randomUUID().toString();
            jsObj.put("requestId", requestId);
            jsObj.putOpt("data", data);
            String zkString = jsObj.toString(4);
            byte zkBytes[] = zkString.getBytes("UTF-8");

            m_zk.create(VoltZK.user_snapshot_request, zkBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException e) {
            return null;
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Exception while attempting to create user snapshot request in ZK", true, e);
        }

        return requestId;
    }

    private void registerUserSnapshotResponseWatch(
            final String requestId,
            final long clientHandle,
            final Connection c,
            final boolean notifyChanges) throws Exception {
        final String responseNode = VoltZK.user_snapshot_response + requestId;
        Stat exists = m_zk.exists(responseNode, new Watcher() {
            @Override
            public void process(final WatchedEvent event) {
                if (event.getState() == KeeperState.Disconnected) return;
                switch (event.getType()) {
                case NodeCreated:
                    m_es.submit(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                processUserSnapshotRequestResponse(
                                            event,
                                            clientHandle,
                                            c,
                                            notifyChanges);
                            } catch (Exception e) {
                                VoltDB.crashLocalVoltDB(
                                        "Error retrieving user snapshot request response from ZK",
                                        true,
                                        e);
                            }
                        }
                    });
                    break;
                default:
                }
            }
        });

        if (exists != null) {
            processUserSnapshotRequestResponse(
                    new WatchedEvent(
                        EventType.NodeCreated,
                        KeeperState.SyncConnected,
                        responseNode),
                        clientHandle,
                        c,
                        notifyChanges);
        }
    }

    void processUserSnapshotRequestResponse(
            final WatchedEvent event,
            final long clientHandle,
            final Connection c,
            final boolean notifyChanges) throws Exception {
        byte responseBytes[] = m_zk.getData(event.getPath(), false, null);
        try {
            m_zk.delete(event.getPath(), -1, null, null);
        } catch (Exception e) {
            hostLog.error("Error cleaning up user snapshot request response in ZK", e);
        }
        ByteBuffer buf = ByteBuffer.wrap(responseBytes);
        ClientResponseImpl response = new ClientResponseImpl();
        response.initFromBuffer(buf);
        response.setClientHandle(clientHandle);

        // Not sure if we need to preserve the original byte buffer here, playing it safe
        ByteBuffer buf2 = ByteBuffer.allocate(response.getSerializedSize() + 4);
        buf2.putInt(buf2.capacity() - 4);
        response.flattenToBuffer(buf2).flip();
        c.writeStream().enqueue(buf2);

        /*
         * If the caller wants to be notified of final results for the snapshot
         * request, set up a watcher only if the snapshot is queued.
         */
        if (notifyChanges && (response.getStatus() == ClientResponse.SUCCESS) &&
            SnapshotUtil.isSnapshotQueued(response.getResults())) {
            Watcher watcher = new Watcher() {
                @Override
                public void process(final WatchedEvent event) {
                    if (event.getState() == KeeperState.Disconnected) return;
                    switch (event.getType()) {
                    case NodeCreated:
                        m_es.submit(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    processUserSnapshotRequestResponse(
                                                event,
                                                clientHandle,
                                                c,
                                                false);
                                } catch (Exception e) {
                                    VoltDB.crashLocalVoltDB(
                                                "Error retrieving user snapshot request response from ZK",
                                                true,
                                                e);
                                }
                            }
                        });
                        break;
                    default:
                    }
                }
            };

            // Set the watcher
            if (m_zk.exists(event.getPath(), watcher) != null) {
                processUserSnapshotRequestResponse(event, clientHandle, c, false);
            }
        }
    }

    @Override
    public CountDownLatch snapshotCompleted(
            final String nonce, final long txnId, final long partitionTxnIds[], final boolean truncation) {
        if (!truncation) {
            return new CountDownLatch(0);
        }
        final CountDownLatch latch = new CountDownLatch(1);
        m_es.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    TruncationSnapshotAttempt snapshotAttempt = m_truncationSnapshotAttempts.get(txnId);
                    if (snapshotAttempt == null) {
                        snapshotAttempt = new TruncationSnapshotAttempt();
                        m_truncationSnapshotAttempts.put(txnId, snapshotAttempt);
                    }
                    snapshotAttempt.finished = true;
                    groomTruncationSnapshots();
                } finally {
                    latch.countDown();
                }
            }
        });
        return latch;
    }

}
