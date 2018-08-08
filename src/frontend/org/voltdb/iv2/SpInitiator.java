/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

import java.util.Arrays;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.utils.CoreUtils;
import org.voltcore.zk.LeaderElector;
import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.CommandLog;
import org.voltdb.MemoryStats;
import org.voltdb.PartitionDRGateway;
import org.voltdb.ProducerDRGateway;
import org.voltdb.Promotable;
import org.voltdb.SnapshotCompletionMonitor;
import org.voltdb.StartAction;
import org.voltdb.StatsAgent;
import org.voltdb.VoltDB;
import org.voltdb.VoltZK;
import org.voltdb.export.ExportManager;
import org.voltdb.iv2.LeaderCache.LeaderCallBackInfo;
import org.voltdb.iv2.RepairAlgo.RepairResult;
import org.voltdb.iv2.SpScheduler.DurableUniqueIdListener;
import org.voltdb.messaging.MigratePartitionLeaderMessage;

import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.Sets;

/**
 * Subclass of Initiator to manage single-partition operations.
 * This class is primarily used for object construction and configuration plumbing;
 * Try to avoid filling it with lots of other functionality.
 */
public class SpInitiator extends BaseInitiator implements Promotable
{
    final private LeaderCache m_leaderCache;
    private final TickProducer m_tickProducer;
    private boolean m_promoted = false;

    private static final VoltLogger exportLog = new VoltLogger("EXPORT");

    LeaderCache.Callback m_leadersChangeHandler = new LeaderCache.Callback()
    {
        @Override
        public void run(ImmutableMap<Integer, LeaderCallBackInfo> cache)
        {
            String hsidStr = CoreUtils.hsIdToString(m_initiatorMailbox.getHSId());
            if (cache != null && tmLog.isDebugEnabled()) {
                tmLog.debug(hsidStr + " [SpInitiator] cache keys: " + Arrays.toString(cache.keySet().toArray()));
                tmLog.debug(hsidStr + " [SpInitiator] cache values: " + Arrays.toString(cache.values().toArray()));
            }

            Set<Long> leaders = Sets.newHashSet();
            for (Entry<Integer, LeaderCallBackInfo> entry: cache.entrySet()) {
                Long HSId = entry.getValue().m_HSId;
                leaders.add(HSId);
                if (HSId == getInitiatorHSId()){
                    if (!m_promoted) {
                        acceptPromotionImpl(entry.getValue().m_lastHSId,
                                entry.getValue().m_isMigratePartitionLeaderRequested);
                        m_promoted = true;
                    }
                    break;
                }
            }

            if (!leaders.contains(getInitiatorHSId())) {
                m_promoted = false;
                if (m_term != null) {
                    m_term.shutdown();
                }
                if (tmLog.isDebugEnabled()) {
                    tmLog.debug(CoreUtils.hsIdToString(getInitiatorHSId()) + " is not a partition leader.");
                }
            }
        }
    };

    public SpInitiator(HostMessenger messenger, Integer partition, StatsAgent agent,
            SnapshotCompletionMonitor snapMonitor,
            StartAction startAction)
    {
        super(VoltZK.iv2masters, messenger, partition,
                new SpScheduler(partition, new SiteTaskerQueue(partition), snapMonitor,
                        startAction != StartAction.JOIN),
                "SP", agent, startAction);
        ((SpScheduler)m_scheduler).initializeScoreboard(CoreUtils.getSiteIdFromHSId(getInitiatorHSId()),
                m_initiatorMailbox);
        m_leaderCache = new LeaderCache(messenger.getZK(), VoltZK.iv2appointees, m_leadersChangeHandler);
        m_tickProducer = new TickProducer(m_scheduler.m_tasks);
        ((SpScheduler)m_scheduler).m_repairLog = m_repairLog;
    }

    @Override
    public void configure(BackendTarget backend,
                          CatalogContext catalogContext,
                          String serializedCatalog,
                          int numberOfPartitions,
                          StartAction startAction,
                          StatsAgent agent,
                          MemoryStats memStats,
                          CommandLog cl,
                          String coreBindIds,
                          boolean isLowestSiteId)
        throws KeeperException, InterruptedException, ExecutionException
    {
        try {
            m_leaderCache.start(true);
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Unable to configure SpInitiator.", true, e);
        }

        super.configureCommon(backend, catalogContext, serializedCatalog,
                numberOfPartitions, startAction, agent, memStats, cl,
                coreBindIds, isLowestSiteId);

        m_tickProducer.start();

        // add ourselves to the ephemeral node list which BabySitters will watch for this
        // partition
        LeaderElector.createParticipantNode(m_messenger.getZK(),
                LeaderElector.electionDirForPartition(VoltZK.leaders_initiators, m_partitionId),
                Long.toString(getInitiatorHSId()), null);
    }

    @Override
    public void initDRGateway(StartAction startAction, ProducerDRGateway nodeDRGateway, boolean createMpDRGateway)
    {
        CommandLog commandLog = VoltDB.instance().getCommandLog();
        boolean asyncCommandLogEnabled = commandLog.isEnabled() && !commandLog.isSynchronous();

        // DRProducerProtocol.NO_REPLICATED_STREAM_PROTOCOL_VERSION
        // TODO get rid of createMpDRGateway and related code in the .1 release once
        // the next compatible version doesn't use replicated stream

        // configure DR
        PartitionDRGateway drGateway = PartitionDRGateway.getInstance(m_partitionId, nodeDRGateway, startAction);
        if (asyncCommandLogEnabled) {
            configureDurableUniqueIdListener(drGateway, true);
        }

        final PartitionDRGateway mpPDRG;
        if (createMpDRGateway) {
            mpPDRG = PartitionDRGateway.getInstance(MpInitiator.MP_INIT_PID, nodeDRGateway, startAction);
            if (asyncCommandLogEnabled) {
                configureDurableUniqueIdListener(mpPDRG, true);
            }
        } else {
            mpPDRG = null;
        }

        m_scheduler.getQueue().offer(new SiteTasker.SiteTaskerRunnable() {
            @Override
            void run()
            {
                m_executionSite.setDRGateway(drGateway, mpPDRG);
            }
            private SiteTasker.SiteTaskerRunnable init(){
                taskInfo = "Set DRGateway";
                return this;
            }
        }.init());
    }

    @Override
    public void acceptPromotion() {
        acceptPromotionImpl(Long.MAX_VALUE, false);
    }

    private void acceptPromotionImpl(long lastLeaderHSId, boolean migratePartitionLeader)
    {
        try {
            long startTime = System.currentTimeMillis();
            Boolean success = false;
            m_term = createTerm(m_messenger.getZK(),
                    m_partitionId, getInitiatorHSId(), m_initiatorMailbox,
                    m_whoami);
            m_term.start();
            int deadSPIHost = lastLeaderHSId == Long.MAX_VALUE ?
                    Integer.MAX_VALUE : CoreUtils.getHostIdFromHSId(lastLeaderHSId);
            while (!success) {

                // if rejoining, a promotion can not be accepted. If the rejoin is
                // in-progress, the loss of the master will terminate the rejoin
                // anyway. If the rejoin has transferred data but not left the rejoining
                // state, it will respond REJOINING to new work which will break
                // the MPI and/or be unexpected to external clients.
                if (!migratePartitionLeader && !m_initiatorMailbox.acceptPromotion()) {
                    tmLog.error(m_whoami
                            + "rejoining site can not be promoted to leader. Terminating.");
                    VoltDB.crashLocalVoltDB("A rejoining site can not be promoted to leader.", false, null);
                    return;
                }

                // term syslogs the start of leader promotion.
                long txnid = Long.MIN_VALUE;
                RepairAlgo repair =
                        m_initiatorMailbox.constructRepairAlgo(m_term.getInterestingHSIds(),
                                deadSPIHost, m_whoami, migratePartitionLeader);
                try {
                    RepairResult res = repair.start().get();
                    txnid = res.m_txnId;
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

                    if (migratePartitionLeader) {
                        String hsidStr = LeaderCache.suffixHSIdsWithMigratePartitionLeaderRequest(
                                m_initiatorMailbox.getHSId());
                        iv2masters.put(m_partitionId, hsidStr);
                        tmLog.info(m_whoami + "becomes new leader from MigratePartitionLeader request.");
                    } else {
                        iv2masters.put(m_partitionId, m_initiatorMailbox.getHSId());
                    }
                    m_initiatorMailbox.setMigratePartitionLeaderStatus(migratePartitionLeader);
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
            // leave the export on the former leader, now a replica
            if (!migratePartitionLeader) {
                if (exportLog.isDebugEnabled()) {
                    exportLog.debug("Export Manager has been notified that local partition " +
                            m_partitionId + " to accept export stream mastership.");
                }
                ExportManager.instance().acceptMastership(m_partitionId);
            }
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
    public void configureDurableUniqueIdListener(DurableUniqueIdListener listener, boolean install)
    {
        m_scheduler.configureDurableUniqueIdListener(listener, install);
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

    public void setMigratePartitionLeaderStatus(long hsId) {
        MigratePartitionLeaderMessage message = new MigratePartitionLeaderMessage(hsId, getInitiatorHSId());
        message.setStatusReset();
        m_initiatorMailbox.deliver(message);
    }

    public boolean isLeader() {
        return m_scheduler.isLeader();
    }

    public void resetMigratePartitionLeaderStatus(int failedHostId) {
        m_initiatorMailbox.resetMigratePartitionLeaderStatus();
        ((SpScheduler)m_scheduler).updateReplicasFromMigrationLeaderFailedHost(failedHostId);
    }

    public Scheduler getScheduler() {
        return m_scheduler;
    }

    //This will be called from Snapshot in elastic joining or rejoining cases.
    public void updateReplicasForJoin(long snapshotSaveTxnId) {
        long[] replicasAdded = new long[0];
        if (m_term != null) {
            replicasAdded = ((SpTerm)m_term).updateReplicas(snapshotSaveTxnId);
        }
        ((SpScheduler)m_scheduler).forwardPendingTaskToRejoinNode(replicasAdded, snapshotSaveTxnId);
    }
}
