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

import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltcore.utils.CoreUtils;
import org.voltcore.zk.LeaderElector;
import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.CommandLog;
import org.voltdb.MemoryStats;
import org.voltdb.ProducerDRGateway;
import org.voltdb.Promotable;
import org.voltdb.RealVoltDB;
import org.voltdb.StartAction;
import org.voltdb.StatsAgent;
import org.voltdb.VoltDB;
import org.voltdb.VoltZK;
import org.voltdb.iv2.LeaderCache.LeaderCallBackInfo;
import org.voltdb.iv2.RepairAlgo.RepairResult;
import org.voltdb.messaging.DumpMessage;
import org.voltdb.messaging.HashMismatchMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;

import com.google_voltpatches.common.collect.ImmutableMap;

/**
 * Subclass of Initiator to manage multi-partition operations.
 * This class is primarily used for object construction and configuration plumbing;
 * Try to avoid filling it with lots of other functionality.
 */
public class MpInitiator extends BaseInitiator<MpScheduler> implements Promotable
{
    public static final int MP_INIT_PID = TxnEgo.PARTITIONID_MAX_VALUE;

    LeaderCache.Callback m_replicaRemovalHandler = new LeaderCache.Callback() {
        @Override
        public void run(ImmutableMap<Integer, LeaderCallBackInfo> cache) {
            if (cache == null || cache.isEmpty()) {
                return;
            }
            if (m_replicaRemovalInvoked.compareAndSet(false, true)) {
                if (tmLog.isDebugEnabled()) {
                    tmLog.debug("Replica removal started.");
                }
                RealVoltDB db = (RealVoltDB) VoltDB.instance();
                m_scheduler.updateBuddyHSIds(db.getLeaderSites());
                m_initiatorMailbox.send(CoreUtils.getHSIdFromHostAndSite(
                        m_messenger.getHostId(), HostMessenger.CLIENT_INTERFACE_SITE_ID),
                        new HashMismatchMessage());
            }
        }
    };

    LeaderCache m_replicaRemovalCache;
    private AtomicBoolean m_replicaRemovalInvoked = new AtomicBoolean(false);
    public MpInitiator(HostMessenger messenger, List<Long> buddyHSIds, StatsAgent agent, int leaderId)
    {
        super(VoltZK.iv2mpi,
                messenger,
                MP_INIT_PID,
                new MpScheduler(
                    MP_INIT_PID,
                    buddyHSIds,
                    new SiteTaskerQueue(MP_INIT_PID),
                    leaderId),
                "MP",
                agent,
                StartAction.CREATE /* never for rejoin */);
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
        // note the mp initiator always uses a non-ipc site, even though it's never used for anything
        if (backend.isValgrindTarget) {
            backend = BackendTarget.NATIVE_EE_JNI;
        }

        super.configureCommon(backend, catalogContext, serializedCatalog,
                numberOfPartitions, startAction, null, null, cl, coreBindIds, false);
        // Hacky
        MpScheduler sched = m_scheduler;
        MpRoSitePool sitePool = new MpRoSitePool(m_initiatorMailbox.getHSId(),
                backend,
                catalogContext,
                m_partitionId,
                m_initiatorMailbox);
        sched.setMpRoSitePool(sitePool);

        // add ourselves to the ephemeral node list which BabySitters will watch for this
        // partition
        LeaderElector.createParticipantNode(m_messenger.getZK(),
                LeaderElector.electionDirForPartition(VoltZK.leaders_initiators, m_partitionId),
                Long.toString(getInitiatorHSId()), null);

        m_replicaRemovalCache = new LeaderCache(m_messenger.getZK(), "MpInitiator-replicas-removal",
                VoltZK.hashMismatchedReplicas, m_replicaRemovalHandler);
    }

    @Override
    public void initDRGateway(StartAction startAction, ProducerDRGateway nodeDRGateway, boolean createMpDRGateway)
    {
        // No-op on MPI
    }

    @Override
    public void acceptPromotion()
    {
        try {
            long startTime = System.currentTimeMillis();
            Boolean success = false;
            // Look for the previous MPI (if any)
            int deadMPIHost = Integer.MAX_VALUE;
            Cartographer cartographer = VoltDB.instance().getCartographer();
            if (cartographer != null) {
                Long deadMPIHSId = cartographer.getHSIdForMultiPartitionInitiator();
                if (deadMPIHSId != null) {
                    deadMPIHost = CoreUtils.getHostIdFromHSId(deadMPIHSId);
                }
            }
            m_term = createTerm(m_messenger.getZK(),
                    m_partitionId, getInitiatorHSId(), m_initiatorMailbox,
                    m_whoami);
            m_term.start();
            while (!success) {
                final RepairAlgo repair = m_initiatorMailbox.constructRepairAlgo(m_term.getInterestingHSIds(),
                                deadMPIHost, m_whoami, false);

                // term syslogs the start of leader promotion.
                long txnid = Long.MIN_VALUE;
                long repairTruncationHandle = Long.MIN_VALUE;
                try {
                    RepairResult res = repair.start().get();
                    txnid = res.m_txnId;
                    repairTruncationHandle = res.m_repairTruncationHandle;
                    success = true;
                } catch (CancellationException e) {
                    success = false;
                }
                if (success) {
                    ((MpInitiatorMailbox)m_initiatorMailbox).setLeaderState(txnid, repairTruncationHandle);
                    List<Iv2InitiateTaskMessage> restartTxns = ((MpPromoteAlgo)repair).getInterruptedTxns();
                    if (!restartTxns.isEmpty()) {
                        // Should only be one restarting MP txn
                        if (restartTxns.size() > 1) {
                            tmLog.fatal("Detected a fatal condition while repairing multipartition transactions " +
                                    "following a cluster topology change.");
                            tmLog.fatal("The MPI found multiple transactions requiring restart: ");
                            for (Iv2InitiateTaskMessage txn : restartTxns) {
                                tmLog.fatal("Restart candidate: " + txn);
                            }
                            tmLog.fatal("This node will fail.  Please contact VoltDB support with your cluster's " +
                                    "log files.");
                            m_initiatorMailbox.send(
                                    com.google_voltpatches.common.primitives.Longs.toArray(m_term.getInterestingHSIds().get()),
                                    new DumpMessage());
                            throw new RuntimeException("Failing promoted MPI node with unresolvable repair condition.");
                        }
                        if (tmLog.isDebugEnabled()) {
                            tmLog.debug(m_whoami + " restarting MP transaction: " + restartTxns.get(0));
                        }
                        Iv2InitiateTaskMessage firstMsg = restartTxns.get(0);
                        assert(firstMsg.getTruncationHandle() == TransactionInfoBaseMessage.UNUSED_TRUNC_HANDLE);
                        m_initiatorMailbox.repairReplicasWith(null, firstMsg);
                    }
                    tmLog.info(m_whoami
                            + "finished leader promotion. Took "
                            + (System.currentTimeMillis() - startTime) + " ms. Leader ID: "
                            + m_scheduler.getLeaderId());

                    // THIS IS where map cache should be updated, not
                    // in the promotion algorithm.
                    LeaderCacheWriter iv2masters = new LeaderCache(m_messenger.getZK(), "MpInitiator", m_zkMailboxNode);
                    iv2masters.put(m_partitionId, m_initiatorMailbox.getHSId());
                    VoltDB.getTTLManager().scheduleTTLTasks();
                    m_replicaRemovalCache.start(true);
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
    public Term createTerm(ZooKeeper zk, int partitionId, long initiatorHSId, InitiatorMailbox mailbox,
            String whoami)
    {
        return new MpTerm(zk, initiatorHSId, mailbox, whoami);
    }

    /**
     * Update the MPI's Site's catalog.  Unlike the SPI, this is not going to
     * run from the same Site's thread; this is actually going to run from some
     * other local SPI's Site thread.  Since the MPI's site thread is going to
     * be blocked running the EveryPartitionTask for the catalog update, this
     * is currently safe with no locking.  And yes, I'm a horrible person.
     */
    public void updateCatalog(String diffCmds, CatalogContext context, boolean isReplay,
            boolean requireCatalogDiffCmdsApplyToEE, boolean requiresNewExportGeneration)
    {
        // note this will never require snapshot isolation because the MPI has no snapshot funtionality
        m_executionSite.updateCatalog(diffCmds, context, false, true, Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE,
                isReplay, requireCatalogDiffCmdsApplyToEE, requiresNewExportGeneration, null);
        m_scheduler.updateCatalog(diffCmds, context);
    }

    public void updateSettings(CatalogContext context)
    {
        m_executionSite.updateSettings(context);
        m_scheduler.updateSettings(context);
    }

    @Override
    public void enableWritingIv2FaultLog() {
        m_initiatorMailbox.enableWritingIv2FaultLog();
    }

    @Override
    protected InitiatorMailbox createInitiatorMailbox(JoinProducerBase joinProducer) {
        return new MpInitiatorMailbox(m_partitionId, m_scheduler, m_messenger, m_repairLog, joinProducer);
    }
}
