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

package org.voltdb.iv2;

import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.utils.CoreUtils;
import org.voltcore.zk.LeaderElector;
import org.voltcore.zk.ZKUtil;
import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.CommandLog;
import org.voltdb.MemoryStats;
import org.voltdb.PartitionDRGateway;
import org.voltdb.ProducerDRGateway;
import org.voltdb.Promotable;
import org.voltdb.RealVoltDB;
import org.voltdb.SnapshotCompletionMonitor;
import org.voltdb.StartAction;
import org.voltdb.StatsAgent;
import org.voltdb.VoltDB;
import org.voltdb.VoltZK;
import org.voltdb.dtxn.TransactionState;
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
public class SpInitiator extends BaseInitiator<SpScheduler> implements Promotable
{
    final private LeaderCache m_leaderCache;
    private boolean m_promoted = false;
    public static enum ServiceState {
        NORMAL(0),
        ELIGIBLE_REMOVAL(1),
        REMOVED(2);
        final int state;
        ServiceState(int state) {
            this.state = state;
        }
        int get() {
            return state;
        }
        public boolean isNormal() {
            return state == NORMAL.get();
        }
        public boolean isEligibleForRemoval() {
            return state == ELIGIBLE_REMOVAL.get();
        }
        public boolean isRemoved() {
            return state == REMOVED.get();
        }
    }

    volatile ServiceState m_serviceState;

    LeaderCache.Callback m_leadersChangeHandler = new LeaderCache.Callback()
    {
        @Override
        public void run(ImmutableMap<Integer, LeaderCallBackInfo> cache)
        {
            if (failedLeaderMigration(cache)) {
                return;
            }

            String hsidStr = CoreUtils.hsIdToString(m_initiatorMailbox.getHSId());
            if (cache != null && tmLog.isDebugEnabled()) {
                tmLog.debug(hsidStr + " [SpInitiator] cache keys: " + Arrays.toString(cache.keySet().toArray()));
                tmLog.debug(hsidStr + " [SpInitiator] cache values: " + Arrays.toString(cache.values().toArray()));
            }

            Set<Long> leaders = Sets.newHashSet();
            for (Entry<Integer, LeaderCallBackInfo> entry: cache.entrySet()) {
                LeaderCallBackInfo info = entry.getValue();
                leaders.add(info.m_HSId);
                if (info.m_HSId == getInitiatorHSId()){

                    // Special case for testing
                    if (info.m_lastHSId < 0) {
                        break;
                    }
                    boolean reinstate = reinstateAsLeader(info);
                    if (!m_promoted || reinstate) {
                        acceptPromotionImpl(info.m_lastHSId, (reinstate || info.m_isMigratePartitionLeaderRequested));
                        m_promoted = true;
                    }
                    break;
                }
            }

            if (!leaders.contains(getInitiatorHSId())) {
                // We used to shutdown SpTerm when the site is no longer leader,
                // however during leader migration there is a short window between
                // the new leader fails before leader migration comes to finish and
                // the old leader accepts promotion. If rejoin happens in this window,
                // SpTerm will fail to add the new site id into the replica list, it
                // can cause rejoin node hangs on receiving stream snapshot.
                // Because of this reason we keep the SpTerm even if it's no longer the
                // leader.
                m_promoted = false;
                if (tmLog.isDebugEnabled()) {
                    tmLog.debug(CoreUtils.hsIdToString(getInitiatorHSId()) + " is not a partition leader.");
                }
            }
        }
    };

    private boolean failedLeaderMigration(ImmutableMap<Integer, LeaderCallBackInfo> cache) {
        RealVoltDB db = (RealVoltDB) VoltDB.instance();
        if (!db.isRunning()) {
            return false;
        }
        // The supposed new leader is gone, Catographer(ZooKeeper) still gets the older
        // leader, so LeaderAppointer won't act. Re-instate the leader
        if (getInitiatorHSId() == db.getCartographer().getHSIdForMaster(m_partitionId) &&
                !m_scheduler.m_isLeader) {
            for (Entry<Integer, LeaderCallBackInfo> entry: cache.entrySet()) {
                LeaderCallBackInfo info = entry.getValue();
                if (!info.m_isMigratePartitionLeaderRequested ||
                        m_messenger.getLiveHostIds().contains(CoreUtils.getHostIdFromHSId(info.m_HSId))) {
                    continue;
                }
                m_scheduler.m_isLeader = true;
                m_initiatorMailbox.m_repairLog.m_isLeader = true;
                m_promoted = true;
                return true;
            }
        }
        return false;
    }

    // When the leader is migrated away from this site, m_scheduler is marked as not-a-leader. If the host for new leader fails
    // before leader migration is completed. The previous leader, the current site, must be reinstated.
    private boolean reinstateAsLeader(LeaderCallBackInfo info) {
        return (!m_scheduler.m_isLeader && info.m_lastHSId == info.m_HSId);
    }

    public SpInitiator(HostMessenger messenger, Integer partition, StatsAgent agent,
            SnapshotCompletionMonitor snapMonitor,
            StartAction startAction)
    {
        super(VoltZK.iv2masters, messenger, partition,
                new SpScheduler(partition, new SiteTaskerQueue(partition), snapMonitor,
                        startAction != StartAction.JOIN),
                "SP", agent, startAction);
        m_scheduler.initializeScoreboard(CoreUtils.getSiteIdFromHSId(getInitiatorHSId()));
        m_leaderCache = new LeaderCache(messenger.getZK(), "SpInitiator-iv2appointees-" + partition,
                ZKUtil.joinZKPath(VoltZK.iv2appointees, Integer.toString(partition)), m_leadersChangeHandler);
        m_scheduler.m_repairLog = m_repairLog;
        m_serviceState = ServiceState.NORMAL;
        m_scheduler.setServiceState(m_serviceState);
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
            // Put child watch on /db/iv2appointees/<partition> node
            m_leaderCache.startPartitionWatch();
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Unable to configure SpInitiator.", true, e);
        }

        super.configureCommon(backend, catalogContext, serializedCatalog,
                numberOfPartitions, startAction, agent, memStats, cl,
                coreBindIds, isLowestSiteId);

        m_executionSite.setServiceState(m_serviceState);
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

        // configure DR
        PartitionDRGateway drGateway = PartitionDRGateway.getInstance(m_partitionId, nodeDRGateway, startAction);
        if (asyncCommandLogEnabled) {
            configureDurableUniqueIdListener(drGateway, true);
        }
        m_repairLog.registerTransactionCommitInterest(drGateway);

        final PartitionDRGateway mpPDRG;
        if (createMpDRGateway) {
            mpPDRG = PartitionDRGateway.getInstance(MpInitiator.MP_INIT_PID, nodeDRGateway, startAction);
            if (asyncCommandLogEnabled) {
                configureDurableUniqueIdListener(mpPDRG, true);
            }
            m_repairLog.registerTransactionCommitInterest(mpPDRG);
        } else {
            mpPDRG = null;
        }

        SiteTasker.SiteTaskerRunnable task = new SiteTasker.SiteTaskerRunnable() {
            @Override
            void run()
            {
                m_executionSite.setDRGateway(drGateway, mpPDRG);
            }
            private SiteTasker.SiteTaskerRunnable init(){
                taskInfo = "Set DRGateway";
                return this;
            }
        }.init();

        Iv2Trace.logSiteTaskerQueueOffer(task);
        m_scheduler.getQueue().offer(task);
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
            if (m_term != null) {
                m_term.shutdown();
            }
            // When the leader is migrated away from this site, the term is still active
            // If the leader is moved back to the site, recreate the term to ensure no-missed
            // update.
            m_term = createTerm(m_messenger.getZK(),
                    m_partitionId, getInitiatorHSId(), m_initiatorMailbox,
                    m_whoami);
            m_term.start();
            int deadSPIHost = Integer.MAX_VALUE;
            if ((lastLeaderHSId != Long.MAX_VALUE && lastLeaderHSId != m_initiatorMailbox.getHSId())) {
                deadSPIHost = CoreUtils.getHostIdFromHSId(lastLeaderHSId);
            }
            while (!success) {

                // if rejoining, a promotion can not be accepted. If the rejoin is
                // in-progress, the loss of the master will terminate the rejoin
                // anyway. If the rejoin has transferred data but not left the rejoining
                // state, it will respond REJOINING to new work which will break
                // the MPI and/or be unexpected to external clients.
                if (!migratePartitionLeader && !m_initiatorMailbox.acceptPromotion()) {
                    tmLog.info(m_whoami
                            + "rejoining site can not be promoted to leader. Terminating.");
                    // rejoining not completed. The node will be shutdown @RealVoltDB.hostFailed() anyway.
                    // do not log extra fatal message.
                    VoltDB.crashLocalVoltDB("A rejoining site can not be promoted to leader.", false, null, false);
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
                            "SpInitiator-iv2masters-" + m_partitionId,
                            m_zkMailboxNode);

                    if (migratePartitionLeader) {
                        String hsidStr = LeaderCache.suffixHSIdsWithMigratePartitionLeaderRequest(
                                m_initiatorMailbox.getHSId());
                        iv2masters.put(m_partitionId, hsidStr);
                        if (lastLeaderHSId == m_initiatorMailbox.getHSId()) {
                            tmLog.info(m_whoami + "reinstate as partition leader.");
                            m_initiatorMailbox.setLeaderMigrationState(false, deadSPIHost);
                        } else {
                            tmLog.info(m_whoami + "becomes new leader from MigratePartitionLeader request.");
                            m_initiatorMailbox.setLeaderMigrationState(true, deadSPIHost);
                        }
                    } else {
                        iv2masters.put(m_partitionId, m_initiatorMailbox.getHSId());
                    }
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

    public Scheduler getScheduler() {
        return m_scheduler;
    }

    //This will be called from Snapshot in elastic joining or rejoining cases.
    public void updateReplicasForJoin(TransactionState snapshotTransactionState) {
        long[] replicasAdded = new long[0];
        if (m_term != null) {
            replicasAdded = ((SpTerm) m_term).updateReplicas(snapshotTransactionState);
        }
        m_scheduler.forwardPendingTaskToRejoinNode(replicasAdded, snapshotTransactionState.m_spHandle);
    }

    @Override
    protected InitiatorMailbox createInitiatorMailbox(JoinProducerBase joinProducer) {
        return new InitiatorMailbox(m_partitionId, m_scheduler, m_messenger, m_repairLog, joinProducer);
    }

    public void updateServiceState(ServiceState state) {
        m_serviceState = state;
        m_executionSite.setServiceState(m_serviceState);
        m_scheduler.setServiceState(m_serviceState);
    }

    public ServiceState getServiceState() {
        return m_serviceState;
    }

    @Override
    public void shutdownService() {
        // do not shutdown leader
        if (isLeader()) {
            return;
        }
        logMasterMode();
        try {
            final String partitionPath = LeaderElector.electionDirForPartition(
                    VoltZK.leaders_initiators, m_partitionId);
            List<String> children = m_messenger.getZK().getChildren(partitionPath, null);
            for (String child : children) {
                if (child.startsWith(Long.toString(getInitiatorHSId()) + "_")) {
                    final String path = ZKUtil.joinZKPath(partitionPath, child);
                    m_messenger.getZK().delete(path, -1);
                    break;
                }
            }
        } catch (KeeperException e) {
            if (e.code() != KeeperException.Code.NONODE) {
                VoltDB.crashLocalVoltDB("This ZK call should never fail", true, e);
            }
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("This ZK call should never fail", true, e);
        }

        try {
            m_leaderCache.shutdown();
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("This ZK call should never fail", true, e);
        }
        TransactionTaskQueue.removeScoreboard(CoreUtils.getSiteIdFromHSId(getInitiatorHSId()));
        super.shutdownService();

        if (tmLog.isDebugEnabled()) {
            tmLog.debug(String.format("Shutdown leader initiator, leader cache, update scoreboard, execution engine for partition %d", m_partitionId));
        }
        m_serviceState = ServiceState.REMOVED;
        m_executionSite.setServiceState(m_serviceState);
        m_scheduler.setServiceState(m_serviceState);
    }

    public void logMasterMode() {
        m_scheduler.logMasterMode();
    }
}
