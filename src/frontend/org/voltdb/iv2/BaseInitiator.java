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

import java.util.concurrent.ExecutionException;

import org.apache.zookeeper_voltpatches.KeeperException;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.CatalogSpecificPlanner;
import org.voltdb.CommandLog;
import org.voltdb.LoadedProcedureSet;
import org.voltdb.MemoryStats;
import org.voltdb.PartitionDRGateway;
import org.voltdb.ProcedureRunnerFactory;
import org.voltdb.StartAction;
import org.voltdb.StarvationTracker;
import org.voltdb.StatsAgent;
import org.voltdb.StatsSelector;
import org.voltdb.rejoin.TaskLog;

/**
 * Subclass of Initiator to manage single-partition operations.
 * This class is primarily used for object construction and configuration plumbing;
 * Try to avoid filling it with lots of other functionality.
 */
public abstract class BaseInitiator implements Initiator
{
    VoltLogger tmLog = new VoltLogger("TM");

    // External references/config
    protected final HostMessenger m_messenger;
    protected final int m_partitionId;
    protected final String m_zkMailboxNode;
    protected final String m_whoami;

    // Encapsulated objects
    protected final Scheduler m_scheduler;
    protected final InitiatorMailbox m_initiatorMailbox;
    protected Term m_term = null;
    protected Site m_executionSite = null;
    protected Thread m_siteThread = null;
    protected final RepairLog m_repairLog = new RepairLog();
    public BaseInitiator(String zkMailboxNode, HostMessenger messenger, Integer partition,
            Scheduler scheduler, String whoamiPrefix, StatsAgent agent,
            StartAction startAction)
    {
        m_zkMailboxNode = zkMailboxNode;
        m_messenger = messenger;
        m_partitionId = partition;
        m_scheduler = scheduler;
        JoinProducerBase joinProducer;

        if (startAction == StartAction.JOIN) {
            joinProducer = new ElasticJoinProducer(m_partitionId, scheduler.m_tasks);
        } else if (startAction.doesRejoin()) {
            joinProducer = new RejoinProducer(m_partitionId, scheduler.m_tasks);
        } else {
            joinProducer = null;
        }

        if (m_partitionId == MpInitiator.MP_INIT_PID) {
            m_initiatorMailbox = new MpInitiatorMailbox(
                    m_partitionId,
                    m_scheduler,
                    m_messenger,
                    m_repairLog,
                    joinProducer);
        } else {
            m_initiatorMailbox = new InitiatorMailbox(
                    m_partitionId,
                    m_scheduler,
                    m_messenger,
                    m_repairLog,
                    joinProducer);
        }

        // Now publish the initiator mailbox to friends and family
        m_messenger.createMailbox(null, m_initiatorMailbox);
        if (joinProducer != null) {
            joinProducer.setMailbox(m_initiatorMailbox);
        }
        m_scheduler.setMailbox(m_initiatorMailbox);
        m_repairLog.setHSId(m_initiatorMailbox.getHSId());
        StarvationTracker st = new StarvationTracker(getInitiatorHSId());
        m_scheduler.setStarvationTracker(st);
        m_scheduler.setLock(m_initiatorMailbox);
        agent.registerStatsSource(StatsSelector.STARVATION,
                                  getInitiatorHSId(),
                                  st);

        String partitionString = " ";
        if (m_partitionId != -1) {
            partitionString = " for partition " + m_partitionId + " ";
        }
        m_whoami = whoamiPrefix +  " " +
            CoreUtils.hsIdToString(getInitiatorHSId()) + partitionString;
    }

    protected void configureCommon(BackendTarget backend,
                          CatalogContext catalogContext,
                          CatalogSpecificPlanner csp,
                          int numberOfPartitions,
                          StartAction startAction,
                          StatsAgent agent,
                          MemoryStats memStats,
                          CommandLog cl,
                          String coreBindIds,
                          PartitionDRGateway drGateway)
        throws KeeperException, ExecutionException, InterruptedException
    {
            int snapshotPriority = 6;
            if (catalogContext.cluster.getDeployment().get("deployment") != null) {
                snapshotPriority = catalogContext.cluster.getDeployment().get("deployment").
                    getSystemsettings().get("systemsettings").getSnapshotpriority();
            }

            // demote rejoin to create for initiators that aren't rejoinable.
            if (startAction.doesJoin() && !isRejoinable()) {
                startAction = StartAction.CREATE;
            }

            TaskLog taskLog = null;
            if (m_initiatorMailbox.getJoinProducer() != null) {
                taskLog = m_initiatorMailbox.getJoinProducer().constructTaskLog(catalogContext.cluster.getVoltroot());
            }

            m_executionSite = new Site(m_scheduler.getQueue(),
                                       m_initiatorMailbox.getHSId(),
                                       backend, catalogContext,
                                       m_partitionId,
                                       numberOfPartitions,
                                       startAction,
                                       snapshotPriority,
                                       m_initiatorMailbox,
                                       agent,
                                       memStats,
                                       coreBindIds,
                                       taskLog,
                                       drGateway);
            ProcedureRunnerFactory prf = new ProcedureRunnerFactory();
            prf.configure(m_executionSite, m_executionSite.m_sysprocContext);

            LoadedProcedureSet procSet = new LoadedProcedureSet(
                    m_executionSite,
                    prf,
                    m_initiatorMailbox.getHSId(),
                    0); // this has no meaning
            procSet.loadProcedures(catalogContext, backend, csp);
            m_executionSite.setLoadedProcedures(procSet);
            m_scheduler.setCommandLog(cl);

            m_siteThread = new Thread(m_executionSite);
            m_siteThread.setDaemon(false);
            m_siteThread.start();
    }

    @Override
    public void shutdown()
    {
        // set the shutdown flag on the site thread.
        if (m_executionSite != null) {
            m_executionSite.startShutdown();
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

        if (m_siteThread != null) {
            try {
                m_siteThread.join();
            } catch (InterruptedException e) {
                tmLog.info("Interrupted during shutdown", e);
            }
        }
    }

    @Override
    public int getPartitionId()
    {
        return m_partitionId;
    }

    @Override
    public long getInitiatorHSId()
    {
        return m_initiatorMailbox.getHSId();
    }

    abstract protected void acceptPromotion() throws Exception;
}
