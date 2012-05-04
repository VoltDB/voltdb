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
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.HsqlBackend;
import org.voltdb.iv2.SiteTasker;
import org.voltdb.iv2.SiteTaskerScheduler;
import org.voltdb.ParameterSet;
import org.voltdb.ProcedureRunner;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.VoltDB;
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
import org.voltdb.VoltTable;

public class Site implements Runnable, SiteProcedureConnection
{
    private static final VoltLogger hostLog = new VoltLogger("HOST");

    // Set to false trigger shutdown.
    volatile boolean m_shouldContinue = true;

    // HSId of this site's initiator.
    final long m_siteId;

    // Partition count is important for some reason.
    final int m_numberOfPartitions;

    // What type of EE is controlled
    final BackendTarget m_backend;

    // Enumerate execution sites by host.
    private static final AtomicInteger siteIndexCounter = new AtomicInteger(0);
    private final int m_siteIndex = siteIndexCounter.getAndIncrement();

    // Manages pending tasks.
    final SiteTaskerScheduler m_scheduler;

    // Almighty execution engine and its HSQL sidekick
    ExecutionEngine m_ee;
    HsqlBackend m_hsql;

    // Current catalog
    CatalogContext m_context;

    // Current topology
    int m_partitionId;

    // Need temporary access to some startup parameters in order to
    // initialize EEs in the right thread.
    private static class StartupConfig
    {
        final String m_serializedCatalog;
        final long m_txnId;
        StartupConfig(final String serCatalog, final long txnId)
        {
            m_serializedCatalog = serCatalog;
            m_txnId = txnId;
        }
    }
    private StartupConfig m_startupConfig = null;


    // Undo token state for the corresponding EE.
    public final static long kInvalidUndoToken = -1L;
    long latestUndoToken = 0L;

    @Override
    public long getNextUndoToken()
    {
        return ++latestUndoToken;
    }

    @Override
    public long getLatestUndoToken()
    {
        return latestUndoToken;
    }

    // TODO: need this for real?
    long m_lastCommittedTxnId = 0L;

    SiteProcedureConnection getSiteProcedureConnection()
    {
        return this;
    }

    /** Create a new execution site and the corresponding EE */
    public Site(
            SiteTaskerScheduler scheduler,
            long siteId,
            BackendTarget backend,
            CatalogContext context,
            String serializedCatalog,
            long txnId,
            int partitionId,
            int numPartitions)
    {
        m_siteId = siteId;
        m_context = context;
        m_partitionId = partitionId;
        m_numberOfPartitions = numPartitions;
        m_scheduler = scheduler;
        m_backend = backend;

        // need this later when running in the final thread.
        m_startupConfig = new StartupConfig(serializedCatalog, txnId);
    }

    /** Thread specific initialization */
    void initialize(String serializedCatalog, long txnId)
    {
        if (m_backend == BackendTarget.NONE) {
            m_hsql = null;
            m_ee = new MockExecutionEngine();
        }
        else if (m_backend == BackendTarget.HSQLDB_BACKEND) {
            m_hsql = initializeHSQLBackend();
            m_ee = new MockExecutionEngine();
        }
        else {
            m_hsql = null;
            m_ee = initializeEE(serializedCatalog, txnId);
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
    ExecutionEngine initializeEE(String serializedCatalog, final long txnId)
    {
        String hostname = CoreUtils.getHostnameOrAddress();
        ExecutionEngine eeTemp = null;
        try {
            if (m_backend == BackendTarget.NATIVE_EE_JNI) {
                System.out.println("Creating JNI EE.");
                eeTemp =
                    new ExecutionEngineJNI(
                        m_context.cluster.getRelativeIndex(),
                        m_siteId,
                        m_partitionId,
                        SiteTracker.getHostForSite(m_siteId),
                        hostname,
                        m_context.cluster.getDeployment().get("deployment").
                        getSystemsettings().get("systemsettings").getMaxtemptablesize(),
                        m_numberOfPartitions);
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
                            m_partitionId,
                            SiteTracker.getHostForSite(m_siteId),
                            hostname,
                            m_context.cluster.getDeployment().get("deployment").
                            getSystemsettings().get("systemsettings").getMaxtemptablesize(),
                            m_backend,
                            VoltDB.instance().getConfig().m_ipcPorts.remove(0),
                            m_numberOfPartitions);
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
        Thread.currentThread().setName("Iv2ExecutionSite: " + CoreUtils.hsIdToString(m_siteId));
        initialize(m_startupConfig.m_serializedCatalog, m_startupConfig.m_txnId);
        m_startupConfig = null; // release the serializedCatalog bytes.

        try {
            while (m_shouldContinue) {
                runLoop();
            }
        }
        catch (final RuntimeException e) {
            hostLog.l7dlog(Level.ERROR, LogKeys.host_ExecutionSite_RuntimeException.name(), e);
            throw e;
        }
        catch (final InterruptedException e) {
            // acceptable - this is how site blocked on an empty scheduler terminates.
        }
        shutdown();
    }

    void runLoop() throws InterruptedException
    {
        SiteTasker task = m_scheduler.poll();
        if (task != null) {
            task.run(getSiteProcedureConnection());
        }
    }

    public void startShutdown()
    {
        m_shouldContinue = false;
    }

    void shutdown()
    {
        try {
            if (m_hsql != null) {
                m_hsql.shutdown();
            }
            if (m_ee != null) {
                m_ee.release();
            }
        } catch (InterruptedException e) {
            hostLog.warn("Interrupted shutdown execution site.", e);
        }
    }

    //
    // Legacy SiteProcedureConnection needed by ProcedureRunner
    //
    @Override
    public void registerPlanFragment(long pfId, ProcedureRunner proc)
    {
        // hostLog.warn("Sysprocs not supported in Iv2. Not loading " + proc.m_procedureName);
    }

    @Override
    public long getCorrespondingSiteId()
    {
        return m_siteId;
    }

    @Override
    public int getCorrespondingPartitionId()
    {
        return m_partitionId;
    }

    @Override
    public int getCorrespondingHostId()
    {
        return CoreUtils.getHostIdFromHSId(m_siteId);
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
            m_lastCommittedTxnId,
            readOnly ? Long.MAX_VALUE : getNextUndoToken());
    }

    @Override
    public VoltTable executePlanFragment(long planFragmentId, int inputDepId,
                                         ParameterSet parameterSet, long txnId,
                                         boolean readOnly) throws EEException
    {
        return m_ee.executePlanFragment(planFragmentId,
                                        inputDepId,
                                        parameterSet,
                                        txnId,
                                        m_lastCommittedTxnId,
                                        readOnly ? Long.MAX_VALUE : getNextUndoToken());
    }

    @Override
    public long getReplicatedDMLDivisor()
    {
        return m_numberOfPartitions;
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
        return currentTxnState.recursableRun();
    }

    @Override
    public void truncateUndoLog(boolean rollback, long token, long txnId)
    {
        if (rollback) {
            m_ee.undoUndoToken(token);
        }
        else {
            m_ee.releaseUndoToken(token);
            m_lastCommittedTxnId = txnId;
        }
    }

    @Override
    public void stashWorkUnitDependencies(Map<Integer, List<VoltTable>> dependencies)
    {
        m_ee.stashWorkUnitDependencies(dependencies);
    }
}
