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

import java.util.concurrent.ExecutionException;

import org.apache.zookeeper_voltpatches.KeeperException;
import org.json_voltpatches.JSONStringer;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.BinaryPayloadMessage;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.BackendTarget;

import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Connector;
import org.voltdb.catalog.Database;
import org.voltdb.CatalogContext;
import org.voltdb.CatalogSpecificPlanner;
import org.voltdb.CommandLog;
import org.voltdb.LoadedProcedureSet;
import org.voltdb.ProcedureRunnerFactory;
import org.voltdb.StarvationTracker;
import org.voltdb.StatsAgent;
import org.voltdb.SysProcSelector;

/**
 * Subclass of Initiator to manage single-partition operations.
 * This class is primarily used for object construction and configuration plumbing;
 * Try to avoid filling it with lots of other functionality.
 */
public abstract class BaseInitiator implements Initiator
{
    VoltLogger tmLog = new VoltLogger("TM");

    public static final String JSON_PARTITION_ID = "partitionId";
    public static final String JSON_INITIATOR_HSID = "initiatorHSId";

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
    private final TickProducer m_tickProducer;

    public BaseInitiator(String zkMailboxNode, HostMessenger messenger, Integer partition,
            Scheduler scheduler, String whoamiPrefix, StatsAgent agent)
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

        m_tickProducer = new TickProducer(m_scheduler.m_tasks);

        // Now publish the initiator mailbox to friends and family
        m_messenger.createMailbox(null, m_initiatorMailbox);
        rejoinProducer.setMailbox(m_initiatorMailbox);
        m_scheduler.setMailbox(m_initiatorMailbox);
        StarvationTracker st = new StarvationTracker(getInitiatorHSId());
        m_scheduler.setStarvationTracker(st);
        m_scheduler.setLock(m_initiatorMailbox);
        agent.registerStatsSource(SysProcSelector.STARVATION,
                                  getInitiatorHSId(),
                                  st);

        String partitionString = " ";
        if (m_partitionId != -1) {
            partitionString = " for partition " + m_partitionId + " ";
        }
        m_whoami = whoamiPrefix +  " " +
            CoreUtils.hsIdToString(getInitiatorHSId()) + partitionString;
    }

    private boolean isExportEnabled(CatalogContext catalogContext)
    {
        final Cluster cluster = catalogContext.catalog.getClusters().get("cluster");
        final Database db = cluster.getDatabases().get("database");
        final Connector conn= db.getConnectors().get("0");
        return (conn != null && conn.getEnabled() == true);
    }

    protected void configureCommon(BackendTarget backend, String serializedCatalog,
                          CatalogContext catalogContext,
                          CatalogSpecificPlanner csp,
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
                                       snapshotPriority,
                                       m_initiatorMailbox);
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
            m_scheduler.setCommandLog(cl);

            if (isExportEnabled(catalogContext)) {
                m_tickProducer.start();
            }

            m_siteThread = new Thread(m_executionSite);
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

    protected void acceptPromotion() throws Exception {
        /*
         * Notify all known client interfaces that the mastership has changed
         * for the specified partition and that no responses from previous masters will be forthcoming
         */
        JSONStringer stringer = new JSONStringer();
        stringer.object();
        stringer.key(JSON_PARTITION_ID).value(m_partitionId);
        stringer.key(JSON_INITIATOR_HSID).value(m_initiatorMailbox.getHSId());
        stringer.endObject();
        BinaryPayloadMessage bpm = new BinaryPayloadMessage(new byte[0], stringer.toString().getBytes("UTF-8"));
        for (Integer hostId : m_messenger.getLiveHostIds()) {
            m_messenger.send(CoreUtils.getHSIdFromHostAndSite(hostId, HostMessenger.CLIENT_INTERFACE_SITE_ID), bpm);
        }
    }
}
