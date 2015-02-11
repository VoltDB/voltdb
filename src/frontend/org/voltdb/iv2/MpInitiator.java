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

import java.util.List;
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
import org.voltdb.Promotable;
import org.voltdb.StartAction;
import org.voltdb.StatsAgent;
import org.voltdb.VoltDB;
import org.voltdb.VoltZK;
import org.voltdb.messaging.DumpMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;

/**
 * Subclass of Initiator to manage multi-partition operations.
 * This class is primarily used for object construction and configuration plumbing;
 * Try to avoid filling it with lots of other functionality.
 */
public class MpInitiator extends BaseInitiator implements Promotable
{
    public static final int MP_INIT_PID = TxnEgo.PARTITIONID_MAX_VALUE;

    public MpInitiator(HostMessenger messenger, List<Long> buddyHSIds, StatsAgent agent)
    {
        super(VoltZK.iv2mpi,
                messenger,
                MP_INIT_PID,
                new MpScheduler(
                    MP_INIT_PID,
                    buddyHSIds,
                    new SiteTaskerQueue()),
                "MP",
                agent,
                StartAction.CREATE /* never for rejoin */);
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
                          NodeDRGateway drGateway,
                          String coreBindIds)
        throws KeeperException, InterruptedException, ExecutionException
    {
        // note the mp initiator always uses a non-ipc site, even though it's never used for anything
        if ((backend == BackendTarget.NATIVE_EE_IPC) || (backend == BackendTarget.NATIVE_EE_VALGRIND_IPC)) {
            backend = BackendTarget.NATIVE_EE_JNI;
        }

        super.configureCommon(backend, catalogContext,
                csp, numberOfPartitions, startAction, null, null, cl, coreBindIds, null);
        // Hacky
        MpScheduler sched = (MpScheduler)m_scheduler;
        MpRoSitePool sitePool = new MpRoSitePool(m_initiatorMailbox.getHSId(),
                backend,
                catalogContext,
                m_partitionId,
                m_initiatorMailbox,
                csp);
        sched.setMpRoSitePool(sitePool);

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
                final RepairAlgo repair =
                        m_initiatorMailbox.constructRepairAlgo(m_term.getInterestingHSIds(), m_whoami);

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
                        tmLog.debug(m_whoami + " restarting MP transaction: " + restartTxns.get(0));
                        m_initiatorMailbox.repairReplicasWith(null, restartTxns.get(0));
                    }
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
    public void updateCatalog(String diffCmds, CatalogContext context, CatalogSpecificPlanner csp)
    {
        // note this will never require snapshot isolation because the MPI has no snapshot funtionality
        m_executionSite.updateCatalog(diffCmds, context, csp, false, true);
        MpScheduler sched = (MpScheduler)m_scheduler;
        sched.updateCatalog(diffCmds, context, csp);
    }

    @Override
    public void enableWritingIv2FaultLog() {
        m_initiatorMailbox.enableWritingIv2FaultLog();
    }
}
