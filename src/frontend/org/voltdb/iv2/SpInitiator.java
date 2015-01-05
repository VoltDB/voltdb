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

package org.voltdb.iv2;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.zk.LeaderElector;
import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.CatalogSpecificPlanner;
import org.voltdb.CommandLog;
import org.voltdb.MemoryStats;
import org.voltdb.NodeDRGateway;
import org.voltdb.PartitionDRGateway;
import org.voltdb.Promotable;
import org.voltdb.SnapshotCompletionMonitor;
import org.voltdb.StartAction;
import org.voltdb.StatsAgent;
import org.voltdb.VoltDB;
import org.voltdb.VoltZK;
import org.voltdb.export.ExportManager;

import com.google_voltpatches.common.collect.ImmutableMap;

/**
 * Subclass of Initiator to manage single-partition operations.
 * This class is primarily used for object construction and configuration plumbing;
 * Try to avoid filling it with lots of other functionality.
 */
public class SpInitiator extends BaseInitiator implements Promotable
{
    final private LeaderCache m_leaderCache;
    private boolean m_promoted = false;
    private final TickProducer m_tickProducer;

    LeaderCache.Callback m_leadersChangeHandler = new LeaderCache.Callback()
    {
        @Override
        public void run(ImmutableMap<Integer, Long> cache)
        {
            for (Long HSId : cache.values()) {
                if (HSId == getInitiatorHSId()) {
                    if (!m_promoted) {
                        acceptPromotion();
                        m_promoted = true;
                    }
                    break;
                }
            }
        }
    };

    public SpInitiator(HostMessenger messenger, Integer partition, StatsAgent agent,
            SnapshotCompletionMonitor snapMonitor,
            StartAction startAction)
    {
        super(VoltZK.iv2masters, messenger, partition,
                new SpScheduler(partition, new SiteTaskerQueue(), snapMonitor),
                "SP", agent, startAction);
        m_leaderCache = new LeaderCache(messenger.getZK(), VoltZK.iv2appointees, m_leadersChangeHandler);
        m_tickProducer = new TickProducer(m_scheduler.m_tasks);
    }

    @Override
    public void configure(BackendTarget backend,
                          CatalogContext catalogContext,
                          int kfactor, CatalogSpecificPlanner csp,
                          int numberOfPartitions,
                          StartAction startAction,
                          StatsAgent agent,
                          MemoryStats memStats,
                          CommandLog cl,
                          NodeDRGateway nodeDRGateway,
                          String coreBindIds)
        throws KeeperException, InterruptedException, ExecutionException
    {
        try {
            m_leaderCache.start(true);
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Unable to configure SpInitiator.", true, e);
        }

        // configure DR
        PartitionDRGateway drGateway =
                PartitionDRGateway.getInstance(m_partitionId, nodeDRGateway,
                        startAction.doesRejoin());
        ((SpScheduler) m_scheduler).setDRGateway(drGateway);

        super.configureCommon(backend, catalogContext,
                csp, numberOfPartitions, startAction, agent, memStats, cl, coreBindIds, drGateway);

        m_tickProducer.start();

        // add ourselves to the ephemeral node list which BabySitters will watch for this
        // partition
        LeaderElector.createParticipantNode(m_messenger.getZK(),
                LeaderElector.electionDirForPartition(m_partitionId),
                Long.toString(getInitiatorHSId()), null);
    }

    @Override
    public void acceptPromotion()
    {
        try {

            long startTime = System.currentTimeMillis();
            Boolean success = false;
            m_term = createTerm(m_messenger.getZK(),
                    m_partitionId, getInitiatorHSId(), m_initiatorMailbox,
                    m_whoami);
            m_term.start();
            while (!success) {
                RepairAlgo repair =
                        m_initiatorMailbox.constructRepairAlgo(m_term.getInterestingHSIds(), m_whoami);

                // if rejoining, a promotion can not be accepted. If the rejoin is
                // in-progress, the loss of the master will terminate the rejoin
                // anyway. If the rejoin has transferred data but not left the rejoining
                // state, it will respond REJOINING to new work which will break
                // the MPI and/or be unexpected to external clients.
                if (!m_initiatorMailbox.acceptPromotion()) {
                    tmLog.error(m_whoami
                            + "rejoining site can not be promoted to leader. Terminating.");
                    VoltDB.crashLocalVoltDB("A rejoining site can not be promoted to leader.", false, null);
                    return;
                }

                // term syslogs the start of leader promotion.
                Long txnid = Long.MIN_VALUE;
                try {
                    txnid = repair.start().get();
                    success = true;
                } catch (CancellationException e) {
                    success = false;
                }
                if (success) {
                    m_initiatorMailbox.setLeaderState(txnid);
                    tmLog.info(m_whoami
                             + "finished leader promotion. Took "
                             + (System.currentTimeMillis() - startTime) + " ms.");

                    // THIS IS where map cache should be updated, not
                    // in the promotion algorithm.
                    LeaderCacheWriter iv2masters = new LeaderCache(m_messenger.getZK(),
                            m_zkMailboxNode);
                    iv2masters.put(m_partitionId, m_initiatorMailbox.getHSId());
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
            // Tag along and become the export master too
            ExportManager.instance().acceptMastership(m_partitionId);
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Terminally failed leader promotion.", true, e);
        }
    }

    /**
     * SpInitiator has userdata that must be rejoined.
     */
    @Override
    public boolean isRejoinable()
    {
        return true;
    }

    @Override
    public Term createTerm(ZooKeeper zk, int partitionId, long initiatorHSId, InitiatorMailbox mailbox,
            String whoami)
    {
        return new SpTerm(zk, partitionId, initiatorHSId, mailbox, whoami);
    }

    @Override
    public void enableWritingIv2FaultLog() {
        m_initiatorMailbox.enableWritingIv2FaultLog();
    }

    @Override
    public void shutdown() {
        try {
            m_leaderCache.shutdown();
        } catch (InterruptedException e) {
            tmLog.info("Interrupted during shutdown", e);
        }
        super.shutdown();
    }
}
