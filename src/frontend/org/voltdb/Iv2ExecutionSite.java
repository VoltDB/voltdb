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

package org.voltdb;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.VoltProcedure.VoltAbortException;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.exceptions.EEException;
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.jni.ExecutionEngineIPC;
import org.voltdb.jni.ExecutionEngineJNI;
import org.voltdb.jni.MockExecutionEngine;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.LogKeys;

public class Iv2ExecutionSite implements Runnable,SiteProcedureConnection
{
    private static final VoltLogger hostLog = new VoltLogger("HOST");

    // Set to false trigger shutdown.
    volatile boolean m_shouldContinue = true;

    // HSId of this site's initiator.
    final long m_siteId;

    // Enumerate execution sites by host.
    private static final AtomicInteger siteIndexCounter = new AtomicInteger(0);
    private final int m_siteIndex = siteIndexCounter.getAndIncrement();

    // Manages pending tasks.
    final SiteTaskerScheduler m_scheduler;

    // Almighty execution engine and its HSQL sidekick
    final ExecutionEngine m_ee;
    final HsqlBackend m_hsql;

    // Current catalog
    CatalogContext m_context;

    // Current topology
    SiteTracker m_tracker;

    /** Create a new execution site and the corresponding EE */
    Iv2ExecutionSite(
            long siteId,
            BackendTarget backend,
            String serializedCatalog,
            long txnId,
            int numPartitions)
    {
        m_siteId = siteId;
        m_scheduler = new SiteTaskerScheduler();

        if (backend == BackendTarget.NONE) {
            m_hsql = null;
            m_ee = new MockExecutionEngine();
        }
        else if (backend == BackendTarget.HSQLDB_BACKEND) {
            m_hsql = initializeHSQLBackend();
            m_ee = new MockExecutionEngine();
        }
        else {
            m_hsql = null;
            m_ee = initializeEE(backend, serializedCatalog, txnId, numPartitions);
        }
    }

    /** Create an HSQL backend */
    HsqlBackend initializeHSQLBackend()
    {
        HsqlBackend hsqlTemp = null;
        try {
            hsqlTemp = new HsqlBackend(m_siteId);
            final String hexDDL = m_context.database.getSchema();
            final String ddl = Encoder.hexDecodeToString(hexDDL);
            final String[] commands = ddl.split("\n");
            for (String command : commands) {
                String decoded_cmd = Encoder.hexDecodeToString(command);
                decoded_cmd = decoded_cmd.trim();
                if (decoded_cmd.length() == 0) {
                    continue;
                }
                hsqlTemp.runDDL(decoded_cmd);
            }
        }
        catch (final Exception ex) {
            hostLog.l7dlog( Level.FATAL, LogKeys.host_ExecutionSite_FailedConstruction.name(),
                            new Object[] { m_siteId, m_siteIndex }, ex);
            VoltDB.crashLocalVoltDB(ex.getMessage(), true, ex);
        }
        return hsqlTemp;
    }


    /** Create a native VoltDB execution engine */
    ExecutionEngine initializeEE(BackendTarget target, String serializedCatalog,
            final long txnId, int configuredNumberOfPartitions)
    {
        String hostname = CoreUtils.getHostnameOrAddress();

        ExecutionEngine eeTemp = null;
        try {
            if (target == BackendTarget.NATIVE_EE_JNI) {
                eeTemp =
                    new ExecutionEngineJNI(
                        m_context.cluster.getRelativeIndex(),
                        m_siteId,
                        m_tracker.getPartitionForSite(m_siteId),
                        SiteTracker.getHostForSite(m_siteId),
                        hostname,
                        m_context.cluster.getDeployment().get("deployment").
                        getSystemsettings().get("systemsettings").getMaxtemptablesize(),
                        configuredNumberOfPartitions);
                eeTemp.loadCatalog( txnId, serializedCatalog);
                // TODO: export integration will require a tick.
                // lastTickTime = EstTime.currentTimeMillis();
                // eeTemp.tick(lastTickTime, txnId);
            }
            else {
                // set up the EE over IPC
                eeTemp =
                    new ExecutionEngineIPC(
                            m_context.cluster.getRelativeIndex(),
                            m_siteId,
                            m_tracker.getPartitionForSite(m_siteId),
                            SiteTracker.getHostForSite(m_siteId),
                            hostname,
                            m_context.cluster.getDeployment().get("deployment").
                            getSystemsettings().get("systemsettings").getMaxtemptablesize(),
                            target,
                            VoltDB.instance().getConfig().m_ipcPorts.remove(0),
                            m_tracker.m_numberOfPartitions);
                eeTemp.loadCatalog( 0, serializedCatalog);
                // TODO: export integration will require a tick.
                // lastTickTime = EstTime.currentTimeMillis();
                // eeTemp.tick( lastTickTime, 0);
            }
        }
        // just print error info an bail if we run into an error here
        catch (final Exception ex) {
            hostLog.l7dlog( Level.FATAL, LogKeys.host_ExecutionSite_FailedConstruction.name(),
                            new Object[] { m_siteId, m_siteIndex }, ex);
            VoltDB.crashLocalVoltDB(ex.getMessage(), true, ex);
        }
        return eeTemp;
    }


    @Override
    public void run()
    {
        Thread.currentThread().setName("Iv2ExecutionSite: " + String.valueOf(m_siteId));
        try {
            while (m_shouldContinue) {
                runLoop();
            }
        }
        catch (final RuntimeException e) {
            hostLog.l7dlog(Level.ERROR, LogKeys.host_ExecutionSite_RuntimeException.name(), e);
            throw e;
        }
        shutdown();
    }

    void runLoop()
    {
        SiteTasker task = m_scheduler.poll();
        if (task != null) {
            task.run(m_ee);
        }
    }

    void shutdown()
    {
    }


    //
    // Legacy SiteProcedureConnection needed by ProcedureRunner
    //
    @Override
    public void registerPlanFragment(long pfId, ProcedureRunner proc)
    {
        throw new RuntimeException("Not supported in IV2");
    }

    @Override
    public long getCorrespondingSiteId()
    {
        return m_siteId;
    }

    @Override
    public int getCorrespondingPartitionId()
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getCorrespondingHostId()
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void loadTable(long txnId, String clusterName, String databaseName,
            String tableName, VoltTable data) throws VoltAbortException
    {
        throw new RuntimeException("Ain't gonna do it.");
    }

    @Override
    public VoltTable[] executeQueryPlanFragmentsAndGetResults(
            long[] planFragmentIds, int numFragmentIds,
            ParameterSet[] parameterSets, int numParameterSets, long txnId,
            boolean readOnly) throws EEException
    {
        return m_ee.executeQueryPlanFragmentsAndGetResults(
            planFragmentIds,
            numFragmentIds,
            parameterSets,
            numParameterSets,
            txnId,
            lastCommittedTxnId,
            readOnly ? Long.MAX_VALUE : getNextUndoToken());
    }

    @Override
    public long getReplicatedDMLDivisor()
    {
        // TODO Auto-generated method stub
        return 1;
    }

    @Override
    public void simulateExecutePlanFragments(long txnId, boolean readOnly)
    {
        throw new RuntimeException("Not supported in IV2.");
    }

    @Override
    public Map<Integer, List<VoltTable>> recursableRun(
            TransactionState currentTxnState)
    {
        throw new RuntimeException("Not supported in IV2");
    }
}
