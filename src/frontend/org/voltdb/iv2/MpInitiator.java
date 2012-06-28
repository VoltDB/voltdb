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
import org.voltcore.zk.MapCache;
import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.CatalogSpecificPlanner;
import org.voltdb.LoadedProcedureSet;
import org.voltdb.ProcedureRunnerFactory;
import org.voltdb.iv2.Site;
import org.voltdb.VoltDB;
import org.voltdb.VoltZK;

/**
 * Subclass of Initiator to manage multi-partition operations.
 * This class is primarily used for object construction and configuration plumbing;
 * Try to avoid filling it with lots of other functionality.
 */
public class MpInitiator implements Initiator, LeaderNoticeHandler
{
    VoltLogger tmLog = new VoltLogger("TM");

    // External references/config
    private HostMessenger m_messenger = null;
    private final int m_partitionId;

    // Encapsulated objects
    private InitiatorMailbox m_initiatorMailbox = null;
    private Term m_term = null;
    private Site m_executionSite = null;
    private Scheduler m_scheduler = null;
    private LoadedProcedureSet m_procSet = null;
    private Thread m_siteThread = null;
    private MapCache m_iv2masters = null;
    private LeaderElector m_leaderElector = null;
    private final RepairLog m_repairLog = new RepairLog();

    private final String m_whoami;

    public MpInitiator(HostMessenger messenger)
    {
        m_messenger = messenger;
        // MPI currently pretends to have partition ID -1 just as a placeholder value
        m_partitionId = -1;
        m_iv2masters = new MapCache(m_messenger.getZK(), VoltZK.iv2masters);
        m_scheduler = new MpScheduler(new SiteTaskerQueue(), m_iv2masters);
        // don't create a rejoin producer for the MPI quite yet.
        m_initiatorMailbox = new InitiatorMailbox(m_scheduler, m_messenger, m_repairLog, null);

        // Now publish the initiator mailbox to friends and family
        m_messenger.createMailbox(null, m_initiatorMailbox);
        m_scheduler.setMailbox(m_initiatorMailbox);

        m_whoami = "MP " +  CoreUtils.hsIdToString(getInitiatorHSId())
            + " for partition " + m_partitionId + " ";
    }

    @Override
    public void becomeLeader()
    {
        try {
            long startTime = System.currentTimeMillis();
            tmLog.info(m_whoami + "starting leader promotion");
            m_term = new Term(new CountDownLatch(1), m_messenger.getZK(), m_partitionId,
                    getInitiatorHSId(), m_initiatorMailbox, VoltZK.iv2mpi);
            m_initiatorMailbox.setTerm(m_term);
            boolean success = m_term.start().get();
            if (!success) {
                throw new RuntimeException("Screwed!");
            }
            m_repairLog.setLeaderState(true);
            m_scheduler.setLeaderState(true);
            tmLog.info(m_whoami
                    + "finished leader promotion. Took "
                    + (System.currentTimeMillis() - startTime) + " ms.");
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Bad news.", true, e);
        }
    }

    /** Register with m_partition's leader elector node */
    public boolean joinElectoralCollege()
        throws InterruptedException, ExecutionException
    {
        // perform leader election before continuing configuration.
        m_leaderElector = new LeaderElector(m_messenger.getZK(),
                LeaderElector.electionDirForPartition(m_partitionId),
                Long.toString(getInitiatorHSId()), null, this);
        try {
            // becomeLeader() will run before start(true) returns (if this is the leader).
            m_leaderElector.start(true);
        } catch (Exception ex) {
            VoltDB.crashLocalVoltDB("Partition " + m_partitionId + " failed to initialize " +
                    "leader elector. ", false, ex);
        }

        return m_leaderElector.isLeader();
    }

    @Override
    public void configure(BackendTarget backend, String serializedCatalog,
                          CatalogContext catalogContext,
                          Cartographer cartographer, int kfactor, CatalogSpecificPlanner csp,
                          boolean createForRejoin)
    {
        assert(createForRejoin == false);

        try {
            m_iv2masters.start(true);
            boolean isLeader = joinElectoralCollege();
            if (isLeader) {
                tmLog.info(m_whoami + "published as leader.");
            }
            else {
                tmLog.info(m_whoami + "running as replica.");
            }

            // ugh
            ((MpScheduler)m_scheduler).setBuddyHSId(cartographer.getBuddySiteForMPI(m_initiatorMailbox.getHSId()));
            m_executionSite = new Site(m_scheduler.getQueue(),
                    m_initiatorMailbox.getHSId(),
                    backend, catalogContext,
                    serializedCatalog,
                    catalogContext.m_transactionId,
                    m_partitionId,
                    cartographer.getNumberOfPartitions(),
                    createForRejoin);
            ProcedureRunnerFactory prf = new ProcedureRunnerFactory();
            prf.configure(m_executionSite,
                    m_executionSite.m_sysprocContext);
            m_procSet = new LoadedProcedureSet(m_executionSite,
                    prf,
                    m_initiatorMailbox.getHSId(),
                    0, // this has no meaning
                    cartographer.getNumberOfPartitions());
            m_procSet.loadProcedures(catalogContext, backend, csp);
            m_scheduler.setProcedureSet(m_procSet);
            m_executionSite.setLoadedProcedures(m_procSet);

            m_siteThread = new Thread(m_executionSite);
            m_siteThread.start(); // Maybe this moves --izzy
        } catch (InterruptedException e) {
            VoltDB.crashLocalVoltDB("Error initializing MP initiator.", true, e);
        } catch (ExecutionException e) {
            VoltDB.crashLocalVoltDB("Error initializing MP initiator.", true, e);
        }
    }

    @Override
    public void shutdown()
    {
        // rtb: better to schedule a shutdown SiteTasker?
        // than to play java interrupt() games?
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
        if (m_iv2masters != null) {
            try {
                m_iv2masters.shutdown();
            } catch (Exception e) {
                // nobody cares at shutdown.
            }
        }
    }

    @Override
    public long getInitiatorHSId()
    {
        return m_initiatorMailbox.getHSId();
    }
}
