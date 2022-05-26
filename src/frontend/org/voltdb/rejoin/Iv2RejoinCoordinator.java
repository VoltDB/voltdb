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

package org.voltdb.rejoin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.SnapshotFormat;
import org.voltdb.VoltDB;
import org.voltdb.VoltZK;
import org.voltdb.catalog.Database;
import org.voltdb.messaging.RejoinMessage;
import org.voltdb.messaging.RejoinMessage.Type;
import org.voltdb.sysprocs.saverestore.SnapshotPathType;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;
import org.voltdb.sysprocs.saverestore.StreamSnapshotRequestConfig;

import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.base.Stopwatch;
import com.google_voltpatches.common.collect.ArrayListMultimap;
import com.google_voltpatches.common.collect.Multimap;

/**
 * Thread Safety: this is a reentrant class. All mutable datastructures
 * must be thread-safe. They use m_lock to do this. DO NOT hold m_lock
 * when leaving this class.
 */
public class Iv2RejoinCoordinator extends JoinCoordinator {
    private static final VoltLogger REJOINLOG = new VoltLogger("REJOIN");

    static final int REJOIN_ACTION_BLOCKER_INTERVAL = Integer.getInteger("REJOIN_ACTION_BLOCKER_INTERVAL", 1000);

    private long m_startTime;

    // This lock synchronizes all data structure access. Do not hold this
    // across blocking external calls.
    private final Object m_lock = new Object();

    // triggers specific test code for TestMidRejoinDeath
    private static final boolean m_rejoinDeathTestMode   = System.getProperties()
                                                                 .containsKey("rejoindeathtestonrejoinside");
    private static final boolean m_rejoinDeathTestCancel = System.getProperties()
                                                                 .containsKey("rejoindeathtestcancel");

    private Database m_catalog;
    // contains all sites that haven't started rejoin initialization
    private final Queue<Long> m_pendingSites;
    // contains all sites that are waiting to start a snapshot
    private final Queue<Long> m_snapshotSites  = new LinkedList<Long>();
    // Mapping of source to destination HSIds for the current snapshot
    private final ArrayListMultimap<Long, Long> m_srcToDest = ArrayListMultimap.create();
    // contains all sites that haven't finished replaying transactions
    private final Queue<Long>  m_rejoiningSites = new LinkedList<Long>();
    // true if performing live rejoin
    private final boolean m_liveRejoin;
    // Need to remember the nonces we're using here (for now)
    private final Map<Long, String> m_nonces = new HashMap<Long, String>();
    // Node-wise stream snapshot receiver buffer pool
    private final Queue<BBContainer> m_snapshotDataBufPool;
    private final Queue<BBContainer> m_snapshotCompressedDataBufPool;
    // For progress tracking
    private int m_initialSiteCount;

    private String m_hostId;

    private Long m_lowestDestSiteHSId = CoreUtils.getHSIdFromHostAndSite(0, Integer.MAX_VALUE);
    private Long m_lowestSiteSinkHSId = 0L;

    public Iv2RejoinCoordinator(HostMessenger messenger,
                                Collection<Long> sites,
                                String voltroot,
                                boolean liveRejoin)
    {
        super(messenger);
        synchronized (m_lock) {
            m_liveRejoin = liveRejoin;
            m_pendingSites = new LinkedList<Long>(sites);
            if (m_pendingSites.isEmpty()) {
                VoltDB.crashLocalVoltDB("No execution sites to rejoin", false, null);
            }

            // clear overflow dir in case there are files left from previous runs
            clearOverflowDir(voltroot);

            // Create a buffer pool for uncompressed stream snapshot data
            m_snapshotDataBufPool = new ConcurrentLinkedQueue<BBContainer>();
            // Create a buffer pool for compressed stream snapshot data
            m_snapshotCompressedDataBufPool = new ConcurrentLinkedQueue<BBContainer>();

            m_hostId = String.valueOf(m_messenger.getHostId());
            Preconditions.checkArgument(
                    m_hostId != null && !m_hostId.trim().isEmpty(),
                    "m_hostId is null or empty"
                    );
        }
    }

    private void initiateRejoinOnSites(List<Long> HSIds)
    {
        // We're going to share this snapshot across the provided HSIDs.
        // Steal just the first one to disabiguate it.
        String nonce = SnapshotUtil.makeSnapshotNonce("Rejoin", HSIds.get(0));
        // Must not hold m_lock across the send() call to manage lock
        // acquisition ordering with other in-process mailboxes.
        synchronized (m_lock) {
            for (long HSId : HSIds) {
                m_nonces.put(HSId, nonce);
            }
        }
        RejoinMessage msg = new RejoinMessage(getHSId(),
                                              m_liveRejoin ? RejoinMessage.Type.INITIATION :
                                              RejoinMessage.Type.INITIATION_COMMUNITY,
                                              nonce,
                                              m_snapshotDataBufPool,
                                              m_snapshotCompressedDataBufPool);
        send(com.google_voltpatches.common.primitives.Longs.toArray(HSIds), msg);

        // For testing, exit if only one property is set...
        // Because we start all sites at the same time, we can't stop the rejoin after one site has finished anymore
        if (m_rejoinDeathTestMode && !m_rejoinDeathTestCancel) {
            System.exit(0);
        }
    }

    private String makeSnapshotRequest(Multimap<Long, Long> sourceToDests, Long lowestSiteSinkHSId)
    {
        StreamSnapshotRequestConfig.Stream stream =
            new StreamSnapshotRequestConfig.Stream(sourceToDests, lowestSiteSinkHSId);
        StreamSnapshotRequestConfig config =
            new StreamSnapshotRequestConfig(SnapshotUtil.getTablesToSave(m_catalog), Arrays.asList(stream), false);
        return SnapshotUtil.makeSnapshotRequest(config);
    }

    public static void acquireLock(HostMessenger messenger )
    {
        final long maxWaitTime = TimeUnit.MINUTES.toSeconds(10); // 10 minutes

        Stopwatch sw = Stopwatch.createStarted();
        long elapsed = 0;
        while ((elapsed = sw.elapsed(TimeUnit.SECONDS)) < maxWaitTime) {
            String blockerError = VoltZK.createActionBlocker(messenger.getZK(), VoltZK.rejoinInProgress,
                                                            CreateMode.EPHEMERAL, REJOINLOG, "node rejoin");
            if (blockerError == null) {
                sw.stop();
                return;
            }
            if (VoltZK.zkNodeExists(messenger.getZK(), VoltZK.reducedClusterSafety)) {
                VoltDB.crashLocalVoltDB("Cluster is in reduced ksafety state before this node could finish rejoin. " +
                        "As a result, the rejoin operation has been canceled.");
                return;
            }

            if (elapsed % 10 == 5) {
                // log the info message every 10 seconds, log the initial message under 5 seconds
                REJOINLOG.info("Rejoin node is waiting " + blockerError + " time elapsed " + elapsed + " seconds");
            }

            try {
                Thread.sleep(REJOIN_ACTION_BLOCKER_INTERVAL);
            } catch (InterruptedException ignoreIt) {
            }
        }

        // Print out ZK info
        StringBuilder builder = new StringBuilder("Contect on ZK:\n");
        VoltZK.printZKDir(messenger.getZK(), VoltZK.actionBlockers, builder);
        VoltZK.printZKDir(messenger.getZK(), VoltZK.actionLock, builder);
        REJOINLOG.info(builder.toString());
        VoltDB.crashLocalVoltDB("Rejoin node is timed out " + maxWaitTime +
                " seconds waiting for catalog update or elastic join, please retry node rejoin later manually.");
    }

    @Override
    public boolean startJoin(Database catalog) {
        m_catalog = catalog;
        m_startTime = System.currentTimeMillis();
        List<Long> firstSites = new ArrayList<Long>();
        synchronized (m_lock) {
            m_initialSiteCount = m_pendingSites.size();
            firstSites.addAll(m_pendingSites);
            m_snapshotSites.addAll(m_pendingSites);
            m_pendingSites.clear();
            VoltDB.instance().reportNodeStartupProgress(0, m_initialSiteCount);
        }
        REJOINLOG.info("Initiating snapshot stream to sites: " + CoreUtils.hsIdCollectionToString(firstSites));
        initiateRejoinOnSites(firstSites);

        return true;
    }

    private void onReplayFinished(long HSId) {
        boolean allDone = false;
        synchronized (m_lock) {
            if (!m_rejoiningSites.remove(HSId)) {
                VoltDB.crashLocalVoltDB("Unknown site " + CoreUtils.hsIdToString(HSId) +
                        " finished rejoin", false, null);
            }
            String msg = "Finished rejoining site " + CoreUtils.hsIdToString(HSId);
            ArrayList<Long> remainingSites = new ArrayList<Long>(m_pendingSites);
            remainingSites.addAll(m_rejoiningSites);
            remainingSites.addAll(m_snapshotSites);
            if (!remainingSites.isEmpty()) {
                msg += ". Remaining sites to rejoin: " +
                    CoreUtils.hsIdCollectionToString(remainingSites);
            }
            else {
                msg += ". All sites completed rejoin.";
            }
            REJOINLOG.info(msg);
            VoltDB.instance().reportNodeStartupProgress(m_initialSiteCount-remainingSites.size(),
                                                        m_initialSiteCount);
            allDone = m_snapshotSites.isEmpty() && m_rejoiningSites.isEmpty();
        }

        if (allDone) {
            VoltZK.removeActionBlocker(m_messenger.getZK(), VoltZK.rejoinInProgress, REJOINLOG);

            // All sites have finished snapshot streaming, clear buffer pool
            while (m_snapshotDataBufPool.size() > 0) {
                m_snapshotDataBufPool.poll().discard();
            }
            while (m_snapshotCompressedDataBufPool.size() > 0) {
                m_snapshotCompressedDataBufPool.poll().discard();
            }

            long delta = (System.currentTimeMillis() - m_startTime) / 1000;
            REJOINLOG.info("" + (m_liveRejoin ? "Live" : "Blocking") + " rejoin data transfer completed in " +
                    delta + " seconds.");
            // no more sites to rejoin, we're done
            VoltDB.instance().onExecutionSiteRejoinCompletion(0l);
        }
    }

    private void onSiteInitialized(long HSId, long masterHSId, long dataSinkHSId) {
        String nonce = null;
        String data = null;
        synchronized(m_lock) {
            if (CoreUtils.getSiteIdFromHSId(m_lowestDestSiteHSId) > CoreUtils.getSiteIdFromHSId(HSId)) {
                m_lowestDestSiteHSId = HSId;
                m_lowestSiteSinkHSId = dataSinkHSId;
            }
            m_snapshotSites.remove(HSId);
            // Long.MIN_VALUE is used when there are no tables in the database and
            // no snapshot transfer is needed.
            if (dataSinkHSId != Long.MIN_VALUE) {
                m_srcToDest.put(masterHSId, dataSinkHSId);
            }
            m_rejoiningSites.add(HSId);
            nonce = m_nonces.get(HSId);
            if (m_snapshotSites.isEmpty()) {
                data = makeSnapshotRequest(m_srcToDest, m_lowestSiteSinkHSId);
                m_srcToDest.clear();
            }
        }
        if (nonce == null) {
            // uh-oh, shouldn't be possible
            throw new RuntimeException("Received an INITIATION_RESPONSE for an HSID for which no nonce exists: " +
                    CoreUtils.hsIdToString(HSId));
        }
        if (data != null) {
            REJOINLOG.debug("Snapshot request: " + data);
            SnapshotUtil.requestSnapshot(0l, "", nonce, !m_liveRejoin, SnapshotFormat.STREAM, SnapshotPathType.SNAP_NO_PATH, data,
                    SnapshotUtil.fatalSnapshotResponseHandler, true);
        }
    }

    @Override
    public void deliver(VoltMessage message) {
        if (!(message instanceof RejoinMessage)) {
            VoltDB.crashLocalVoltDB("Unknown message type " +
                    message.getClass().toString() + " sent to the rejoin coordinator",
                    false, null);
        }

        RejoinMessage rm = (RejoinMessage) message;
        Type type = rm.getType();
        if (type == RejoinMessage.Type.SNAPSHOT_FINISHED) {
            REJOINLOG.info("Finished streaming snapshot to site: " +
                           CoreUtils.hsIdToString(rm.m_sourceHSId));
        } else if (type == RejoinMessage.Type.REPLAY_FINISHED) {
            assert(m_catalog != null);
            onReplayFinished(rm.m_sourceHSId);
        } else if (type == RejoinMessage.Type.INITIATION_RESPONSE) {
            onSiteInitialized(rm.m_sourceHSId, rm.getMasterHSId(), rm.getSnapshotSinkHSId());
        } else {
            VoltDB.crashLocalVoltDB("Wrong rejoin message of type " + type +
                                    " sent to the rejoin coordinator", false, null);
        }
    }
}
