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

package org.voltdb.rejoin;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;
import org.voltdb.VoltDB;
import org.voltdb.messaging.RejoinMessage;
import org.voltdb.messaging.RejoinMessage.Type;
import org.voltdb.utils.VoltFile;

/**
 * Sequentially rejoins each site. This rejoin coordinator is accessed by sites
 * sequentially, so no need to synchronize.
 */
public class SequentialRejoinCoordinator extends RejoinCoordinator {
    private static final VoltLogger rejoinLog = new VoltLogger("JOIN");

    // contains all sites that haven't started streaming snapshot
    private final Queue<Long> m_pendingSites;
    // contains all sites that haven't finished replaying transactions
    private final Queue<Long> m_rejoiningSites = new LinkedList<Long>();

    public SequentialRejoinCoordinator(HostMessenger messenger,
                                       List<Long> sites,
                                       String voltroot) {
        super(messenger);
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

    /**
     * Send rejoin initiation message to the local site
     * @param HSId
     */
    private void initiateRejoinOnSite(long HSId) {
        RejoinMessage msg = new RejoinMessage(getHSId(),
                                              RejoinMessage.Type.INITIATION);
        send(HSId, msg);
    }

    @Override
    public void startRejoin() {
        long firstSite = m_pendingSites.poll();
        m_rejoiningSites.add(firstSite);
        String HSIdString = CoreUtils.hsIdToString(firstSite);
        rejoinLog.debug("Start rejoining site " + HSIdString);
        initiateRejoinOnSite(firstSite);
    }

    private void onSnapshotStreamFinished(long HSId) {
        rejoinLog.info("Finished streaming snapshot to site " +
                CoreUtils.hsIdToString(HSId));

        if (!m_pendingSites.isEmpty()) {
            long nextSite = m_pendingSites.poll();
            m_rejoiningSites.add(nextSite);
            String HSIdString = CoreUtils.hsIdToString(nextSite);
            rejoinLog.debug("Start rejoining site " + HSIdString);
            initiateRejoinOnSite(nextSite);
        }
    }

    private void onReplayFinished(long HSId) {
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

        if (m_rejoiningSites.isEmpty()) {
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
