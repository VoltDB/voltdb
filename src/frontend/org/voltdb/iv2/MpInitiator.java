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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import java.util.List;

import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooKeeper;

import org.json_voltpatches.JSONObject;

import org.voltcore.messaging.HostMessenger;

import org.voltcore.utils.Pair;

import org.voltcore.zk.LeaderElector;
import org.voltcore.zk.LeaderNoticeHandler;
import org.voltcore.zk.MapCache;
import org.voltcore.zk.MapCacheWriter;

import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.CatalogSpecificPlanner;
import org.voltdb.VoltDB;
import org.voltdb.VoltZK;

/**
 * Subclass of Initiator to manage multi-partition operations.
 * This class is primarily used for object construction and configuration plumbing;
 * Try to avoid filling it with lots of other functionality.
 */
public class MpInitiator extends BaseInitiator implements LeaderNoticeHandler
{
    private static final int MP_INIT_PID = -1;
    private CountDownLatch m_missingStartupSites;
    protected LeaderElector m_leaderElector = null;

    public MpInitiator(HostMessenger messenger, long buddyHSId)
    {
        super(VoltZK.iv2mpi,
                messenger,
                MP_INIT_PID,
                new MpScheduler(
                    buddyHSId,
                    new SiteTaskerQueue()),
                "MP");
    }

    @Override
    public void configure(BackendTarget backend, String serializedCatalog,
                          CatalogContext catalogContext,
                          int kfactor, CatalogSpecificPlanner csp,
                          int numberOfPartitions,
                          boolean createForRejoin)
        throws KeeperException, InterruptedException, ExecutionException
    {
        super.configureCommon(backend, serializedCatalog, catalogContext,
                numberOfPartitions, csp, numberOfPartitions,
                createForRejoin && isRejoinable());
        // Join the leader election process after the object is fully
        // configured.  If we do this earlier, rejoining sites will be
        // given transactions before they're ready to handle them.
        // FUTURE: Consider possibly returning this and the
        // m_siteThread.start() in a Runnable which RealVoltDB can use for
        // configure/run sequencing in the future.
        joinElectoralCollege(numberOfPartitions);
    }

    @Override
    public void acceptPromotion()
    {
        try {
            long startTime = System.currentTimeMillis();
            Boolean success = false;
            m_term = createTerm(m_missingStartupSites, m_messenger.getZK(),
                    m_partitionId, getInitiatorHSId(), m_initiatorMailbox,
                    m_whoami);
            m_term.start();
            while (!success) {
                RepairAlgo repair = null;
                if (m_missingStartupSites != null) {
                    repair = new StartupAlgo(m_missingStartupSites, m_whoami);
                }
                else {
                    repair = createPromoteAlgo(m_term.getInterestingHSIds(),
                            m_initiatorMailbox, m_whoami);
                }

                m_initiatorMailbox.setRepairAlgo(repair);
                // term syslogs the start of leader promotion.
                Pair<Boolean, Long> result = repair.start().get();
                success = result.getFirst();
                if (success) {
                    m_initiatorMailbox.setLeaderState(result.getSecond());
                    tmLog.info(m_whoami
                            + "finished leader promotion. Took "
                            + (System.currentTimeMillis() - startTime) + " ms.");

                    // THIS IS where map cache should be updated, not
                    // in the promotion algorithm.
                    MapCacheWriter iv2masters = new MapCache(m_messenger.getZK(),
                            m_zkMailboxNode);
                    iv2masters.put(Integer.toString(m_partitionId),
                            new JSONObject("{hsid:" + m_initiatorMailbox.getHSId() + "}"));
                }
                else {
                    // The only known reason to fail is a failed replica during
                    // recovery; that's a bounded event (by k-safety).
                    // CrashVoltDB here means one node failure causing another.
                    // Don't create a cascading failure - just try again.
                    tmLog.info(m_whoami
                            + "interrupted during leader promotion after "
                            + (System.currentTimeMillis() - startTime) + " ms. of "
                            + "trying. Retrying.");
                }
            }
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Terminally failed leader promotion.", true, e);
        }
    }

    /**
     * The MPInitiator does not have user data to rejoin.
     */
    @Override
    public boolean isRejoinable()
    {
        return false;
    }

    @Override
    public Term createTerm(CountDownLatch missingStartupSites, ZooKeeper zk,
            int partitionId, long initiatorHSId, InitiatorMailbox mailbox,
            String whoami)
    {
        return new MpTerm(missingStartupSites, zk, initiatorHSId, mailbox, whoami);
    }

    @Override
    public RepairAlgo createPromoteAlgo(List<Long> survivors, InitiatorMailbox mailbox,
            String whoami)
    {
        return new MpPromoteAlgo(m_term.getInterestingHSIds(), m_initiatorMailbox, m_whoami);
    }

    @Override
    public void becomeLeader()
    {
        acceptPromotion();
    }

    // Register with m_partition's leader elector node
    // On the leader, becomeLeader() will run before joinElectoralCollage returns.
    boolean joinElectoralCollege(int startupCount) throws InterruptedException, ExecutionException, KeeperException
    {
        m_missingStartupSites = new CountDownLatch(startupCount);
        m_leaderElector = new LeaderElector(m_messenger.getZK(),
                LeaderElector.electionDirForPartition(m_partitionId),
                Long.toString(getInitiatorHSId()), null, this);
        m_leaderElector.start(true);
        m_missingStartupSites = null;
        boolean isLeader = m_leaderElector.isLeader();
        if (isLeader) {
            tmLog.info(m_whoami + "published as leader.");
        }
        else {
            tmLog.info(m_whoami + "running as replica.");
        }
        return isLeader;
    }

    @Override
    public void shutdown()
    {
        try {
            if (m_leaderElector != null) {
                m_leaderElector.shutdown();
            }
        } catch (Exception e) {
            tmLog.info("Exception during shutdown.", e);
        }
        super.shutdown();
    }
}
