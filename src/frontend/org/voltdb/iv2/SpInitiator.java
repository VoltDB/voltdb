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

import org.apache.zookeeper_voltpatches.KeeperException;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.zk.BabySitter;
import org.voltcore.zk.BabySitter.Callback;
import org.voltcore.zk.LeaderElector;
import org.voltcore.zk.LeaderNoticeHandler;
import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.LoadedProcedureSet;
import org.voltdb.ProcedureRunnerFactory;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.iv2.Site;
import org.voltdb.VoltDB;

/**
 * Subclass of Initiator to manage single-partition operations.
 * This class is primarily used for object construction and configuration plumbing;
 * Try to avoid filling it with lots of other functionality.
 */
public class SpInitiator implements Initiator, LeaderNoticeHandler
{
    VoltLogger hostLog = new VoltLogger("HOST");

    // External references/config
    private HostMessenger m_messenger = null;
    private int m_partitionId;

    // Encapsulated objects
    private InitiatorMailbox m_initiatorMailbox = null;
    private Site m_executionSite = null;
    private Scheduler m_scheduler = null;
    private LoadedProcedureSet m_procSet = null;
    private InitiatorMessageHandler m_msgHandler = null;
    private LeaderElector m_leaderElector = null;
    // Only gets set non-null for the leader
    private BabySitter m_babySitter = null;
    private Thread m_siteThread = null;

    Callback m_replicasChangeHandler = new Callback()
    {
        @Override
        public void run(List<String> children) {
            hostLog.info("Babysitter for zkLeaderNode: " +
                    LeaderElector.electionDirForPartition(m_partitionId) + ":");
            hostLog.info("children: " + children);
            // make an HSId array out of the children
            // The list contains the leader, skip it
            long[] tmpArray = new long[children.size() -1];
            int i = 0;
            for (String child : children) {
                long HSId = Long.parseLong(LeaderElector.getPrefixFromChildName(child));
                if (HSId != getInitiatorHSId())
                {
                    tmpArray[i++] = HSId;
                    hostLog.info("Child: " + i + ", HSID: " + HSId);
                }
            }
            m_initiatorMailbox.updateReplicas(tmpArray);
        }
    };

    public SpInitiator(HostMessenger messenger, Integer partition, PartitionClerk clerk)
    {
        m_messenger = messenger;
        m_partitionId = partition;
        m_scheduler = new SpScheduler(clerk);
        m_msgHandler = new SpInitiatorMessageHandler(m_scheduler);
        m_initiatorMailbox = new InitiatorMailbox(m_msgHandler, m_messenger, clerk);
    }

    @Override
    public void becomeLeader()
    {
    }

    /** Register with m_partition's leader elector node */
    public boolean joinElectoralCollege()
    {
        // perform leader election before continuing configuration.
        m_leaderElector = new LeaderElector(m_messenger.getZK(),
                LeaderElector.electionDirForPartition(m_partitionId),
                Long.toString(getInitiatorHSId()), null, this);
        try {
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
                          SiteTracker siteTracker, int kfactor)
    {
        boolean isLeader = joinElectoralCollege();
        if (isLeader) {
            hostLog.info("Chosen as leader for partition " + m_partitionId);
            m_babySitter = new BabySitter(m_messenger.getZK(),
                    LeaderElector.electionDirForPartition(m_partitionId),
                    m_replicasChangeHandler);
            // This block-on-all-the-replicas-at-startup thing sucks.  Hopefully this can
            // go away when we get rejoin working.
            List<String> children = m_babySitter.lastSeenChildren();
            while (children.size() < kfactor) {
                try {
                    Thread.sleep(5);
                } catch (InterruptedException e) {
                }
                children = m_babySitter.lastSeenChildren();
            }
        }
        else {
            hostLog.info("Chosen as replica for partition " + m_partitionId);
        }

        m_executionSite = new Site(m_scheduler.getQueue(),
                                   m_initiatorMailbox.getHSId(),
                                   backend, catalogContext,
                                   serializedCatalog,
                                   catalogContext.m_transactionId,
                                   m_partitionId,
                                   siteTracker.m_numberOfPartitions);
        ProcedureRunnerFactory prf = new ProcedureRunnerFactory();
        prf.configure(m_executionSite,
                m_executionSite.m_sysprocContext);
        m_procSet = new LoadedProcedureSet(m_executionSite,
                                           prf,
                                           m_initiatorMailbox.getHSId(),
                                           0, // this has no meaning
                                           siteTracker.m_numberOfPartitions);
        m_procSet.loadProcedures(catalogContext, backend);
        m_scheduler.setProcedureSet(m_procSet);
        m_executionSite.setLoadedProcedures(m_procSet);


        m_siteThread = new Thread(m_executionSite);
        m_siteThread.start();
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
            if (m_babySitter != null) {
                m_babySitter.shutdown();
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
