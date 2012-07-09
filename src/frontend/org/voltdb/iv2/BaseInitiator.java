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
import org.apache.zookeeper_voltpatches.KeeperException;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;

import org.voltcore.utils.CoreUtils;
import org.voltcore.zk.LeaderElector;
import org.voltcore.zk.LeaderNoticeHandler;
import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.CatalogSpecificPlanner;
import org.voltdb.LoadedProcedureSet;
import org.voltdb.ProcedureRunnerFactory;
import org.voltdb.iv2.Site;
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
    private final String m_zkMailboxNode;
    protected final String m_whoami;

    // Encapsulated objects
    protected final Scheduler m_scheduler;
    protected final InitiatorMailbox m_initiatorMailbox;
    protected Term m_term = null;
    protected Site m_executionSite = null;
    protected LeaderElector m_leaderElector = null;
    protected Thread m_siteThread = null;
    protected final RepairLog m_repairLog = new RepairLog();

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

    @Override
    public void configure(BackendTarget backend, String serializedCatalog,
                          CatalogContext catalogContext,
                          int kfactor, CatalogSpecificPlanner csp,
                          int numberOfPartitions,
                          boolean createForRejoin)
    {
        try {
            m_executionSite = new Site(m_scheduler.getQueue(),
                                       m_initiatorMailbox.getHSId(),
                                       backend, catalogContext,
                                       serializedCatalog,
                                       catalogContext.m_transactionId,
                                       m_partitionId,
                                       numberOfPartitions,
                                       createForRejoin);
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

            m_scheduler.start();
            m_siteThread = new Thread(m_executionSite);
            m_siteThread.start();

            // Join the leader election process after the object is fully
            // configured.  If we do this earlier, rejoining sites will be
            // given transactions before they're ready to handle them.
            // FUTURE: Consider possibly returning this and the
            // m_siteThread.start() in a Runnable which RealVoltDB can use for
            // configure/run sequencing in the future.
            joinElectoralCollege(kfactor);

            // Leader elector chains are built, let scheduler do final
            // initialization (the MPI needs to setup its MapCache)
            m_scheduler.setProcedureSet(procSet);
        }
        catch (Exception e) {
           VoltDB.crashLocalVoltDB("Failed to configure initiator", true, e);
        }
    }

    // Register with m_partition's leader elector node
    // On the leader, becomeLeader() will run before joinElectoralCollage returns.
    boolean joinElectoralCollege(int kfactor) throws InterruptedException, ExecutionException, KeeperException
    {
        m_missingStartupSites = new CountDownLatch(kfactor + 1);
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
            while (!success) {
                // term syslogs the start of leader promotion.
                m_term = new Term(m_missingStartupSites, m_messenger.getZK(),
                        m_partitionId, getInitiatorHSId(), m_initiatorMailbox,
                        m_zkMailboxNode, m_whoami);
                m_initiatorMailbox.setTerm(m_term);
                success = m_term.start().get();
                if (success) {
                    m_repairLog.setLeaderState(true);
                    m_scheduler.setLeaderState(true);
                    tmLog.info(m_whoami
                            + "finished leader promotion. Took "
                            + (System.currentTimeMillis() - startTime) + " ms.");
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
        // rtb: better to schedule a shutdown SiteTasker?
        // than to play java interrupt() games?
        if (m_scheduler != null) {
            m_scheduler.shutdown();
        }
        if (m_executionSite != null) {
            m_executionSite.startShutdown();
        }
        try {
            if (m_leaderElector != null) {
                m_leaderElector.shutdown();
            }
            if (m_term != null) {
                m_term.shutdown();
            }
            if (m_initiatorMailbox != null) {
                m_initiatorMailbox.shutdown();
            }
        } catch (InterruptedException e) {
            // what to do here?
        } catch (KeeperException e) {
            // What to do here?
        }
        if (m_siteThread != null) {
            try {
                m_siteThread.interrupt();
                m_siteThread.join();
            }
            catch (InterruptedException giveup) {
            }
        }
    }

    @Override
    public long getInitiatorHSId()
    {
        return m_initiatorMailbox.getHSId();
    }
}
