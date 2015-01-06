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

package org.voltdb.rejoin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.zookeeper_voltpatches.KeeperException;
import org.json_voltpatches.JSONException;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;
import org.voltdb.SnapshotFormat;
import org.voltdb.SnapshotSiteProcessor;
import org.voltdb.VoltDB;
import org.voltdb.VoltZK;
import org.voltdb.catalog.Database;
import org.voltdb.messaging.RejoinMessage;
import org.voltdb.messaging.RejoinMessage.Type;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;
import org.voltdb.sysprocs.saverestore.StreamSnapshotRequestConfig;
import org.voltdb.utils.FixedDBBPool;

import com.google_voltpatches.common.collect.ArrayListMultimap;
import com.google_voltpatches.common.collect.Multimap;

/**
 * Thread Safety: this is a reentrant class. All mutable datastructures
 * must be thread-safe. They use m_lock to do this. DO NOT hold m_lock
 * when leaving this class.
 */
public class Iv2RejoinCoordinator extends JoinCoordinator {
    private static final VoltLogger REJOINLOG = new VoltLogger("REJOIN");

    private long m_startTime;

    // This lock synchronizes all data structure access. Do not hold this
    // across blocking external calls.
    private final Object m_lock = new Object();

    // triggers specific test code for TestMidRejoinDeath
    private static final boolean m_rejoinDeathTestMode   = System.getProperties()
                                                                 .containsKey("rejoindeathtestonrejoinside");
    private static final boolean m_rejoinDeathTestCancel = System.getProperties()
                                                                 .containsKey("rejoindeathtestcancel");

    private static AtomicLong m_sitesRejoinedCount = new AtomicLong(0);

    private Database m_catalog;
    // contains all sites that haven't started rejoin initialization
    private final Queue<Long> m_pendingSites;
    // contains all sites that are waiting to start a snapshot
    private final Queue<Long>                   m_snapshotSites  = new LinkedList<Long>();
    // Mapping of source to destination HSIds for the current snapshot
    private final ArrayListMultimap<Long, Long> m_srcToDest = ArrayListMultimap.create();
    // contains all sites that haven't finished replaying transactions
    private final Queue<Long>                   m_rejoiningSites = new LinkedList<Long>();
    // true if performing live rejoin
    private final boolean m_liveRejoin;
    // Need to remember the nonces we're using here (for now)
    private final Map<Long, String> m_nonces = new HashMap<Long, String>();
    // Node-wise stream snapshot receiver buffer pool
    private final FixedDBBPool m_snapshotBufPool;

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

            // The buffer pool capacity is min(numOfSites to rejoin times 3, 16)
            // or any user specified value.
            Integer userPoolSize = Integer.getInteger("REJOIN_RECEIVE_BUFFER_POOL_SIZE");
            int poolSize = 0;
            if (userPoolSize != null) {
                poolSize = userPoolSize;
            } else {
                poolSize = 3;
            }

            m_snapshotBufPool = new FixedDBBPool();
            // Create a buffer pool for uncompressed stream snapshot data
            m_snapshotBufPool.allocate(SnapshotSiteProcessor.m_snapshotBufferLength, poolSize);
            // Create a buffer pool for compressed stream snapshot data
            m_snapshotBufPool.allocate(SnapshotSiteProcessor.m_snapshotBufferCompressedLen, poolSize);
        }
    }

    /**
     * Send rejoin initiation message to the local site
     * @param HSId
     */
    private void initiateRejoinOnSites(long HSId, boolean schemaHasNoTables)
    {
        List<Long> HSIds = new ArrayList<Long>();
        HSIds.add(HSId);
        initiateRejoinOnSites(HSIds, schemaHasNoTables);
    }

    private void initiateRejoinOnSites(List<Long> HSIds, boolean schemaHasNoTables)
    {
        // We're going to share this snapshot across the provided HSIDs.
        // Steal just the first one to disabiguate it.
        String nonce = makeSnapshotNonce("Rejoin", HSIds.get(0));
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
                                              1, // 1 source per rejoining site
                                              m_snapshotBufPool,
                                              schemaHasNoTables);
        send(com.google_voltpatches.common.primitives.Longs.toArray(HSIds), msg);

        // For testing, exit if only one property is set...
        if (m_rejoinDeathTestMode && !m_rejoinDeathTestCancel &&
                (m_sitesRejoinedCount.incrementAndGet() == 2)) {
            System.exit(0);
        }
    }

    private String makeSnapshotRequest(Multimap<Long, Long> sourceToDests)
    {
        StreamSnapshotRequestConfig.Stream stream =
            new StreamSnapshotRequestConfig.Stream(sourceToDests, null);
        StreamSnapshotRequestConfig config =
            new StreamSnapshotRequestConfig(SnapshotUtil.getTablesToSave(m_catalog), Arrays.asList(stream), false);
        return makeSnapshotRequest(config);
    }

    @Override
    public void initialize(int kfactor) throws JSONException, KeeperException, InterruptedException, ExecutionException
    {
        VoltZK.createCatalogUpdateBlocker(m_messenger.getZK(), VoltZK.rejoinActiveBlocker);
    }

    @Override
    public boolean startJoin(Database catalog) {
        m_catalog = catalog;
        boolean schemaHasNoTables = catalog.getTables().isEmpty();
        m_startTime = System.currentTimeMillis();
        if (m_liveRejoin) {
            long firstSite;
            synchronized (m_lock) {
                firstSite = m_pendingSites.poll();
                m_snapshotSites.add(firstSite);
            }
            String HSIdString = CoreUtils.hsIdToString(firstSite);
            REJOINLOG.info("Initiating snapshot stream to first site: " + HSIdString);
            initiateRejoinOnSites(firstSite, schemaHasNoTables);
        }
        else {
            List<Long> firstSites = new ArrayList<Long>();
            synchronized (m_lock) {
                firstSites.addAll(m_pendingSites);
                m_snapshotSites.addAll(m_pendingSites);
                m_pendingSites.clear();
            }
            REJOINLOG.info("Initiating snapshot stream to sites: " + CoreUtils.hsIdCollectionToString(firstSites));
            initiateRejoinOnSites(firstSites, schemaHasNoTables);
        }

        return true;
    }

    private void initiateNextSite(boolean schemaHasNoTables) {
        // make all the decisions under lock.
        Long nextSite = null;
        synchronized (m_lock) {
            if (!m_pendingSites.isEmpty()) {
                nextSite = m_pendingSites.poll();
                m_snapshotSites.add(nextSite);
                REJOINLOG.info("Initiating snapshot stream to next site: " +
                        CoreUtils.hsIdToString(nextSite));
            }
        }
        if (nextSite != null) {
            initiateRejoinOnSites(nextSite, schemaHasNoTables);
        }
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
            allDone = m_snapshotSites.isEmpty() && m_rejoiningSites.isEmpty();
        }

        if (allDone) {
            VoltZK.removeCatalogUpdateBlocker(m_messenger.getZK(), VoltZK.rejoinActiveBlocker, REJOINLOG);

            // All sites have finished snapshot streaming, clear buffer pool
            m_snapshotBufPool.clear();

            long delta = (System.currentTimeMillis() - m_startTime) / 1000;
            REJOINLOG.info("" + (m_liveRejoin ? "Live" : "Blocking") + " rejoin data transfer completed in " +
                    delta + " seconds.");
            // no more sites to rejoin, we're done
            VoltDB.instance().onExecutionSiteRejoinCompletion(0l);
        }
    }

    private void onSiteInitialized(long HSId, long masterHSId, long dataSinkHSId,
                                   boolean schemaHasNoTables)
    {
        String nonce = null;
        String data = null;
        synchronized(m_lock) {
            m_snapshotSites.remove(HSId);
            // Long.MIN_VALUE is used when there are no tables in the database and
            // no snapshot transfer is needed.
            if (dataSinkHSId != Long.MIN_VALUE) {
                m_srcToDest.put(masterHSId, dataSinkHSId);
            }
            m_rejoiningSites.add(HSId);
            nonce = m_nonces.get(HSId);
            if (m_snapshotSites.isEmpty()) {
                data = makeSnapshotRequest(m_srcToDest);
                m_srcToDest.clear();
            }
        }
        if (nonce == null) {
            // uh-oh, shouldn't be possible
            throw new RuntimeException("Received an INITIATION_RESPONSE for an HSID for which no nonce exists: " +
                    CoreUtils.hsIdToString(HSId));
        }
        if (data != null && !schemaHasNoTables) {
            REJOINLOG.debug("Snapshot request: " + data);
            SnapshotUtil.requestSnapshot(0l, "", nonce, !m_liveRejoin, SnapshotFormat.STREAM, data,
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
            boolean schemaHasNoTables = m_catalog.getTables().isEmpty();
            initiateNextSite(schemaHasNoTables);
            onReplayFinished(rm.m_sourceHSId);
        } else if (type == RejoinMessage.Type.INITIATION_RESPONSE) {
            onSiteInitialized(rm.m_sourceHSId, rm.getMasterHSId(), rm.getSnapshotSinkHSId(),
                              rm.schemaHasNoTables());
        } else {
            VoltDB.crashLocalVoltDB("Wrong rejoin message of type " + type +
                                    " sent to the rejoin coordinator", false, null);
        }
    }
}
