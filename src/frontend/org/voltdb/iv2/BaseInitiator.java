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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import org.apache.zookeeper_voltpatches.KeeperException;
import org.json_voltpatches.JSONObject;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;
import org.voltcore.zk.LeaderElector;
import org.voltcore.zk.LeaderNoticeHandler;
import org.voltcore.zk.MapCache;
import org.voltcore.zk.MapCacheWriter;
import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.CatalogSpecificPlanner;
import org.voltdb.CommandLog;
import org.voltdb.LoadedProcedureSet;
import org.voltdb.ProcedureRunnerFactory;
import org.voltdb.VoltDB;

/**
 * Subclass of Initiator to manage single-partition operations.
 * This class is primarily used for object construction and configuration plumbing;
 * Try to avoid filling it with lots of other functionality.
 */
public abstract class BaseInitiator implements Initiator, LeaderNoticeHandler
{
    VoltLogger tmLog = new VoltLogger("TM");

    // External references/config
    protected final HostMessenger m_messenger;
    protected final int m_partitionId;
    private CountDownLatch m_missingStartupSites;
    protected final String m_zkMailboxNode;
    protected final String m_whoami;

    // Encapsulated objects
    protected final Scheduler m_scheduler;
    protected final InitiatorMailbox m_initiatorMailbox;
    protected Term m_term = null;
    protected Site m_executionSite = null;
    protected LeaderElector m_leaderElector = null;
    protected Thread m_siteThread = null;
    protected final RepairLog m_repairLog = new RepairLog();

    // conversion helper.
    static List<Long> childrenToReplicaHSIds(Collection<String> children)
    {
        List<Long> replicas = new ArrayList<Long>(children.size());
        for (String child : children) {
            long HSId = Long.parseLong(LeaderElector.getPrefixFromChildName(child));
            replicas.add(HSId);
        }
        return replicas;
    }

    public BaseInitiator(String zkMailboxNode, HostMessenger messenger, Integer partition,
            Scheduler scheduler, String whoamiPrefix)
    {
        m_zkMailboxNode = zkMailboxNode;
        m_messenger = messenger;
        m_partitionId = partition;
        m_scheduler = scheduler;
        RejoinProducer rejoinProducer =
            new RejoinProducer(m_partitionId, scheduler.m_tasks);
        m_initiatorMailbox = new InitiatorMailbox(
                m_partitionId,
                m_scheduler,
                m_messenger,
                m_repairLog,
                rejoinProducer);

        // Now publish the initiator mailbox to friends and family
        m_messenger.createMailbox(null, m_initiatorMailbox);
        rejoinProducer.setMailbox(m_initiatorMailbox);
        m_scheduler.setMailbox(m_initiatorMailbox);

        String partitionString = " ";
        if (m_partitionId != -1) {
            partitionString = " for partition " + m_partitionId + " ";
        }
        m_whoami = whoamiPrefix +  " " +
            CoreUtils.hsIdToString(getInitiatorHSId()) + partitionString;
    }

    protected void configureCommon(BackendTarget backend, String serializedCatalog,
                          CatalogContext catalogContext,
                          int startupCount, CatalogSpecificPlanner csp,
                          int numberOfPartitions,
                          boolean createForRejoin,
                          CommandLog cl)
        throws KeeperException, ExecutionException, InterruptedException
    {
            int snapshotPriority = 6;
            if (catalogContext.cluster.getDeployment().get("deployment") != null) {
                snapshotPriority = catalogContext.cluster.getDeployment().get("deployment").
                    getSystemsettings().get("systemsettings").getSnapshotpriority();
            }
            m_executionSite = new Site(m_scheduler.getQueue(),
                                       m_initiatorMailbox.getHSId(),
                                       backend, catalogContext,
                                       serializedCatalog,
                                       catalogContext.m_transactionId,
                                       m_partitionId,
                                       numberOfPartitions,
                                       createForRejoin,
                                       snapshotPriority);
            ProcedureRunnerFactory prf = new ProcedureRunnerFactory();
            prf.configure(m_executionSite, m_executionSite.m_sysprocContext);

            LoadedProcedureSet procSet = new LoadedProcedureSet(
                    m_executionSite,
                    prf,
                    m_initiatorMailbox.getHSId(),
                    0, // this has no meaning
                    numberOfPartitions);
            procSet.loadProcedures(catalogContext, backend, csp);
            m_executionSite.setLoadedProcedures(procSet);
            m_scheduler.setProcedureSet(procSet);
            m_scheduler.setCommandLog(cl);

            m_siteThread = new Thread(m_executionSite);
            m_siteThread.start();

            // Join the leader election process after the object is fully
            // configured.  If we do this earlier, rejoining sites will be
            // given transactions before they're ready to handle them.
            // FUTURE: Consider possibly returning this and the
            // m_siteThread.start() in a Runnable which RealVoltDB can use for
            // configure/run sequencing in the future.
            joinElectoralCollege(startupCount);
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

    // runs on the leader elector callback thread.
    @Override
    public void becomeLeader()
    {
        try {
            long startTime = System.currentTimeMillis();
            Boolean success = false;
            m_term = createTerm(m_missingStartupSites, m_messenger.getZK(),
                    m_partitionId, getInitiatorHSId(), m_initiatorMailbox,
                    m_zkMailboxNode, m_whoami);
            m_term.start();
            while (!success) {
                RepairAlgo repair = null;
                if (m_missingStartupSites != null) {
                    repair = new StartupAlgo(m_missingStartupSites, m_whoami, m_partitionId);
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

    @Override
    public void shutdown()
    {
        // set the shutdown flag on the site thread.
        if (m_executionSite != null) {
            m_executionSite.startShutdown();
        }
        try {
            if (m_leaderElector != null) {
                m_leaderElector.shutdown();
            }
        } catch (Exception e) {
            tmLog.info("Exception during shutdown.", e);
        }

        try {
            if (m_term != null) {
                m_term.shutdown();
            }
        } catch (Exception e) {
            tmLog.info("Exception during shutdown.", e );
        }

        try {
            if (m_initiatorMailbox != null) {
                m_initiatorMailbox.shutdown();
            }
        } catch (Exception e) {
            tmLog.info("Exception during shutdown.", e);
        }

        try {
            if (m_siteThread != null) {
                m_siteThread.interrupt();
            }
        } catch (Exception e) {
            tmLog.info("Exception during shutdown.");
        }
    }

    @Override
    public long getInitiatorHSId()
    {
        return m_initiatorMailbox.getHSId();
    }
}
