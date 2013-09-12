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

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;
import org.voltdb.VoltDB;
import org.voltdb.catalog.Database;
import org.voltdb.iv2.Cartographer;
import org.voltdb.messaging.RejoinMessage;
import org.voltdb.messaging.RejoinMessage.Type;
import org.voltdb.utils.VoltFile;

/**
 * Sequentially rejoins each site.
 *
 * Thread Safety: this is a reentrant class. All mutable datastructures
 * must be thread-safe. They use m_lock to do this. DO NOT hold m_lock
 * when leaving this class.
 */
public class SequentialRejoinCoordinator extends JoinCoordinator {
    private static final VoltLogger rejoinLog = new VoltLogger("REJOIN");

    // This lock synchronizes all data structure access. Do not hold this
    // across blocking external calls.
    private final Object m_lock = new Object();

    // triggers specific test code for TestMidRejoinDeath
    private static final boolean m_rejoinDeathTestMode = System.getProperties().containsKey("rejoindeathtestonrejoinside");
    private static final boolean m_rejoinDeathTestCancel = System.getProperties().containsKey("rejoindeathtestcancel");

    private static AtomicLong m_sitesRejoinedCount = new AtomicLong(0);

    // contains all sites that haven't started streaming snapshot
    private final Queue<Long> m_pendingSites;
    // contains all sites that haven't finished replaying transactions
    private final Queue<Long> m_rejoiningSites = new LinkedList<Long>();
    // true if performing live rejoin
    private final boolean m_liveRejoin;

    public SequentialRejoinCoordinator(HostMessenger messenger,
                                       List<Long> sites,
                                       String voltroot,
                                       boolean liveRejoin) {
        super(messenger);
        synchronized (m_lock) {
            m_liveRejoin = liveRejoin;
            m_pendingSites = new LinkedList<Long>(sites);
            if (m_pendingSites.isEmpty()) {
                VoltDB.crashLocalVoltDB("No execution sites to rejoin", false, null);
            }

            // clear overflow dir in case there are files left from previous runs
            try {
                File overflowDir = new File(voltroot, "rejoin_overflow");
                if (overflowDir.exists()) {
                    VoltFile.recursivelyDelete(overflowDir);
                }
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB("Fail to clear rejoin overflow directory", false, e);
            }
        }
    }

    /**
     * Send rejoin initiation message to the local site
     * @param HSId
     */
    private void initiateRejoinOnSite(long HSId) {
        // Must not hold m_lock across the send() call to manage lock
        // acquisition ordering with other in-process mailboxes.
        RejoinMessage msg = new RejoinMessage(getHSId(),
                m_liveRejoin ? RejoinMessage.Type.INITIATION :
                               RejoinMessage.Type.INITIATION_COMMUNITY);
        send(HSId, msg);

        // For testing, exit if only one property is set...
        if (m_rejoinDeathTestMode && !m_rejoinDeathTestCancel &&
                (m_sitesRejoinedCount.incrementAndGet() == 2)) {
            System.exit(0);
        }
    }

    @Override
    public boolean startJoin(Database catalog, Cartographer cartographer, String clSnapshotPath) {
        long firstSite;
        synchronized (m_lock) {
            firstSite = m_pendingSites.poll();
            m_rejoiningSites.add(firstSite);
        }
        String HSIdString = CoreUtils.hsIdToString(firstSite);
        rejoinLog.info("Initiating snapshot stream to first site: " + HSIdString);
        initiateRejoinOnSite(firstSite);
        return true;
    }

    private void onSnapshotStreamFinished(long HSId) {
        // make all the decisions under lock.
        Long nextSite = null;
        synchronized (m_lock) {
            if (!m_pendingSites.isEmpty()) {
                nextSite = m_pendingSites.poll();
                m_rejoiningSites.add(nextSite);
                rejoinLog.info("Finished streaming snapshot to site: " +
                        CoreUtils.hsIdToString(HSId) +
                        " and initiating snapshot stream to next site: " +
                        CoreUtils.hsIdToString(nextSite));
            }
            else {
                rejoinLog.info("Finished streaming snapshot to site: " +
                        CoreUtils.hsIdToString(HSId));
            }
        }
        if (nextSite != null) {
            initiateRejoinOnSite(nextSite);
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
            if (!remainingSites.isEmpty()) {
                msg += ". Remaining sites to rejoin: " +
                    CoreUtils.hsIdCollectionToString(remainingSites);
            }
            else {
                msg += ". All sites completed rejoin.";
            }
            rejoinLog.info(msg);
            allDone = m_rejoiningSites.isEmpty();
        }

        if (allDone) {
            // no more sites to rejoin, we're done
            VoltDB.instance().onExecutionSiteRejoinCompletion(0l);
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
            onSnapshotStreamFinished(rm.m_sourceHSId);
        } else if (type == RejoinMessage.Type.REPLAY_FINISHED) {
            onReplayFinished(rm.m_sourceHSId);
        } else {
            VoltDB.crashLocalVoltDB("Wrong rejoin message of type " + type +
                                    " sent to the rejoin coordinator", false, null);
        }
    }
}
