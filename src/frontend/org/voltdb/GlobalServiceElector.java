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
package org.voltdb;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.voltcore.logging.VoltLogger;
import org.voltcore.zk.LeaderElector;
import org.voltcore.zk.LeaderNoticeHandler;

/**
 * GlobalServiceElector performs leader election to determine which VoltDB cluster node
 * will be responsible for leading various cluster-wide services, particularly those which must
 * run on the same node.
 */
class GlobalServiceElector implements LeaderNoticeHandler
{
    private static final VoltLogger hostLog = new VoltLogger("HOST");
    private final LeaderElector m_leaderElector;
    private final List<Promotable> m_services = new ArrayList<Promotable>();
    private final int m_hostId;
    private boolean m_isLeader = false;

    GlobalServiceElector(ZooKeeper zk, int hostId)
    {
        m_leaderElector = new LeaderElector(zk, VoltZK.leaders_globalservice,
                "globalservice", null, this);
        m_hostId = hostId;
    }

    /** Add a service to be notified if this node becomes the global leader */
    synchronized void registerService(Promotable service)
    {
        m_services.add(service);
        if (m_isLeader) {
            try {
                service.acceptPromotion();
            }
            catch (Exception e) {
                VoltDB.crashLocalVoltDB("Unable to promote global service.", true, e);
            }
        }
    }

    /** Kick off the leader election.
     * Will block until all of the acceptPromotion() calls to all of the services return at the initial leader.
     */
    void start() throws KeeperException, InterruptedException, ExecutionException
    {
        m_leaderElector.start(true);
    }

    @Override
    synchronized public void becomeLeader()
    {
        hostLog.info("Host " + m_hostId + " promoted to be the global service provider");
        m_isLeader = true;
        for (Promotable service : m_services) {
            try {
                service.acceptPromotion();
            }
            catch (Exception e) {
                VoltDB.crashLocalVoltDB("Unable to promote global service.", true, e);
            }
        }
    }

    void shutdown()
    {
        try {
            m_leaderElector.shutdown();
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Error shutting down GlobalServiceElector's LeaderElector", true, e);
        }
    }

    @Override
    public void noticedTopologyChange(boolean added, boolean removed) {
    }
}
