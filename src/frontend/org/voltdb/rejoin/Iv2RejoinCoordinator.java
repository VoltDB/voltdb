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

package org.voltdb.rejoin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;

import org.voltdb.SnapshotSiteProcessor;

import org.voltdb.SnapshotFormat;

import org.voltdb.catalog.Database;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;
import org.voltdb.VoltDB;
import org.voltdb.messaging.RejoinMessage;
import org.voltdb.messaging.RejoinMessage.Type;
import org.voltdb.sysprocs.saverestore.StreamSnapshotRequestConfig;
import org.voltdb.utils.FixedDBBPool;

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
            int poolSize = userPoolSize != null ? userPoolSize : Math.min(sites.size() * 3, 16);
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
    private void initiateRejoinOnSites(long HSId)
    {
        List<Long> HSIds = new ArrayList<Long>();
        HSIds.add(HSId);
        initiateRejoinOnSites(HSIds);
    }

    private void initiateRejoinOnSites(List<Long> HSIds)
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
                                              m_snapshotBufPool);
        send(com.google.common.primitives.Longs.toArray(HSIds), msg);

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
    public boolean startJoin(Database catalog) {
        m_catalog = catalog;
        m_startTime = System.currentTimeMillis();
        if (m_liveRejoin) {
            long firstSite;
            synchronized (m_lock) {
                firstSite = m_pendingSites.poll();
                m_snapshotSites.add(firstSite);
            }
            String HSIdString = CoreUtils.hsIdToString(firstSite);
            REJOINLOG.info("Initiating snapshot stream to first site: " + HSIdString);
            initiateRejoinOnSites(firstSite);
        }
        else {
            List<Long> firstSites = new ArrayList<Long>();
            synchronized (m_lock) {
                firstSites.addAll(m_pendingSites);
                m_snapshotSites.addAll(m_pendingSites);
                m_pendingSites.clear();
            }
            REJOINLOG.info("Initiating snapshot stream to sites: " + CoreUtils.hsIdCollectionToString(firstSites));
            initiateRejoinOnSites(firstSites);
        }

        return true;
    }

    private void initiateNextSite() {
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
            initiateRejoinOnSites(nextSite);
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
            // All sites have finished snapshot streaming, clear buffer pool
            m_snapshotBufPool.clear();

            long delta = (System.currentTimeMillis() - m_startTime) / 1000;
            REJOINLOG.info("" + (m_liveRejoin ? "Live" : "Blocking") + " rejoin data transfer completed in " +
                    delta + " seconds.");
            // no more sites to rejoin, we're done
            VoltDB.instance().onExecutionSiteRejoinCompletion(0l);
        }
    }

    private void onSiteInitialized(long HSId, long masterHSId, long dataSinkHSId)
    {
        String nonce = null;
        String data = null;
        synchronized(m_lock) {
            m_snapshotSites.remove(HSId);
            m_srcToDest.put(masterHSId, dataSinkHSId);
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
        if (data != null) {
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
            initiateNextSite();
            onReplayFinished(rm.m_sourceHSId);
        } else if (type == RejoinMessage.Type.INITIATION_RESPONSE) {
            onSiteInitialized(rm.m_sourceHSId, rm.getMasterHSId(), rm.getSnapshotSinkHSId());
        } else {
            VoltDB.crashLocalVoltDB("Wrong rejoin message of type " + type +
                                    " sent to the rejoin coordinator", false, null);
        }
    }
}
