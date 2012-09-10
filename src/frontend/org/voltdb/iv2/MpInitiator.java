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

import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.utils.Pair;
import org.voltcore.zk.LeaderElector;
import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.CatalogSpecificPlanner;
import org.voltdb.CommandLog;
import org.voltdb.Promotable;
import org.voltdb.StatsAgent;
import org.voltdb.VoltDB;
import org.voltdb.VoltZK;

/**
 * Subclass of Initiator to manage multi-partition operations.
 * This class is primarily used for object construction and configuration plumbing;
 * Try to avoid filling it with lots of other functionality.
 */
public class MpInitiator extends BaseInitiator implements Promotable
{
    public static final int MP_INIT_PID = TxnEgo.PARTITIONID_MAX_VALUE;

    public MpInitiator(HostMessenger messenger, long buddyHSId, StatsAgent agent)
    {
        super(VoltZK.iv2mpi,
                messenger,
                MP_INIT_PID,
                new MpScheduler(
                    MP_INIT_PID,
                    buddyHSId,
                    new SiteTaskerQueue()),
                "MP",
                agent);
    }

    @Override
    public void configure(BackendTarget backend, String serializedCatalog,
                          CatalogContext catalogContext,
                          int kfactor, CatalogSpecificPlanner csp,
                          int numberOfPartitions,
                          boolean createForRejoin,
                          CommandLog cl)
        throws KeeperException, InterruptedException, ExecutionException
    {
        super.configureCommon(backend, serializedCatalog, catalogContext,
                csp, numberOfPartitions,
                createForRejoin && isRejoinable(), cl);
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
                RepairAlgo repair = null;
                repair = createPromoteAlgo(m_term.getInterestingHSIds(),
                        m_initiatorMailbox, m_whoami);

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

    @Override
    public RepairAlgo createPromoteAlgo(List<Long> survivors, InitiatorMailbox mailbox,
            String whoami)
    {
        return new MpPromoteAlgo(m_term.getInterestingHSIds(), m_initiatorMailbox, m_whoami);
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
        m_executionSite.updateCatalog(diffCmds, context, csp, true);
    }
}
