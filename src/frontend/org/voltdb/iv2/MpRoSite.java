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
import java.util.Map;
import java.util.concurrent.Future;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.Pair;
import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.CatalogSpecificPlanner;
import org.voltdb.DependencyPair;
import org.voltdb.HsqlBackend;
import org.voltdb.LoadedProcedureSet;
import org.voltdb.ParameterSet;
import org.voltdb.ProcedureRunner;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.SiteSnapshotConnection;
import org.voltdb.StatsSelector;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.TableStreamType;
import org.voltdb.TheHashinator;
import org.voltdb.VoltDB;
import org.voltdb.VoltProcedure.VoltAbortException;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.dtxn.UndoAction;
import org.voltdb.exceptions.EEException;

/**
 * An implementation of Site which provides only the functionality
 * necessary to run read-only multipartition transactions.  A pool
 * of these will be used to run multiple read-only transactions
 * concurrently.
 */
public class MpRoSite implements Runnable, SiteProcedureConnection
{
    @SuppressWarnings("unused")
    private static final VoltLogger tmLog = new VoltLogger("TM");

    // Set to false trigger shutdown.
    volatile boolean m_shouldContinue = true;

    // HSId of this site's initiator.
    final long m_siteId;

    // What type of EE is controlled
    final BackendTarget m_backend;

    // Manages pending tasks.
    final SiteTaskerQueue m_scheduler;

    // Still need m_hsql here.
    HsqlBackend m_hsql;

    // Current catalog
    volatile CatalogContext m_context;

    // Currently available procedure
    volatile LoadedProcedureSet m_loadedProcedures;

    // Current topology
    int m_partitionId;

    @Override
    public long getLatestUndoToken()
    {
        throw new RuntimeException("Not needed for RO MP Site, shouldn't be here.");
    }

    // Advanced in complete transaction.
    private long m_currentTxnId = Long.MIN_VALUE;

    SiteProcedureConnection getSiteProcedureConnection()
    {
        return this;
    }

    /**
     * SystemProcedures are "friends" with ExecutionSites and granted
     * access to internal state via m_systemProcedureContext.
     *
     * The only sysproc which should run on the RO MP Site is Adhoc.  Everything
     * else will yell at you.
     */
    SystemProcedureExecutionContext m_sysprocContext = new SystemProcedureExecutionContext() {
        @Override
        public Database getDatabase() {
            throw new RuntimeException("Not needed for RO MP Site, shouldn't be here.");
        }

        @Override
        public Cluster getCluster() {
            throw new RuntimeException("Not needed for RO MP Site, shouldn't be here.");
        }

        @Override
        public long getSpHandleForSnapshotDigest() {
            throw new RuntimeException("Not needed for RO MP Site, shouldn't be here.");
        }

        @Override
        public long getSiteId() {
            throw new RuntimeException("Not needed for RO MP Site, shouldn't be here.");
        }

        @Override
        public boolean isLowestSiteId()
        {
            throw new RuntimeException("Not needed for RO MP Site, shouldn't be here.");
        }

        @Override
        public int getHostId() {
            throw new RuntimeException("Not needed for RO MP Site, shouldn't be here.");
        }

        @Override
        public int getPartitionId() {
            throw new RuntimeException("Not needed for RO MP Site, shouldn't be here.");
        }

        @Override
        public long getCatalogCRC() {
            throw new RuntimeException("Not needed for RO MP Site, shouldn't be here.");
        }

        @Override
        public byte[] getCatalogHash() {
            // AdHoc invocations need to be able to check the hash of the current catalog
            // against the hash of the catalog they were planned against.
            return m_context.getCatalogHash();
        }

        @Override
        public byte[] getDeploymentHash() {
            throw new RuntimeException("Not needed for RO MP Site, shouldn't be here.");
        }

        // Needed for Adhoc queries
        @Override
        public int getCatalogVersion() {
            return m_context.catalogVersion;
        }

        @Override
        public SiteTracker getSiteTrackerForSnapshot() {
            throw new RuntimeException("Not needed for RO MP Site, shouldn't be here.");
        }

        @Override
        public int getNumberOfPartitions() {
            throw new RuntimeException("Not needed for RO MP Site, shouldn't be here.");
        }

        @Override
        public void setNumberOfPartitions(int partitionCount) {
            throw new RuntimeException("Not needed for RO MP Site, shouldn't be here.");
        }

        @Override
        public SiteProcedureConnection getSiteProcedureConnection()
        {
            throw new RuntimeException("Not needed for RO MP Site, shouldn't be here.");
        }

        @Override
        public SiteSnapshotConnection getSiteSnapshotConnection()
        {
            throw new RuntimeException("Not needed for RO MP Site, shouldn't be here.");
        }

        @Override
        public void updateBackendLogLevels() {
            throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
        }

        @Override
        public boolean updateCatalog(String diffCmds, CatalogContext context,
                CatalogSpecificPlanner csp, boolean requiresSnapshotIsolation)
        {
            throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
        }

        @Override
        public TheHashinator getCurrentHashinator()
        {
            throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
        }

        @Override
        public void updateHashinator(TheHashinator hashinator)
        {
            throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
        }

        @Override
        public boolean activateTableStream(int tableId, TableStreamType type, boolean undo, byte[] predicates)
        {
            throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
        }

        @Override
        public Pair<Long, int[]> tableStreamSerializeMore(int tableId, TableStreamType type,
                List<DBBPool.BBContainer> outputBuffers)
        {
            throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
        }

        @Override
        public Procedure ensureDefaultProcLoaded(String procName) {
            throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
        }
    };

    /** Create a new RO MP execution site */
    public MpRoSite(
            SiteTaskerQueue scheduler,
            long siteId,
            BackendTarget backend,
            CatalogContext context,
            int partitionId)
    {
        m_siteId = siteId;
        m_context = context;
        m_partitionId = partitionId;
        m_scheduler = scheduler;
        m_backend = backend;
        m_currentTxnId = Long.MIN_VALUE;
    }

    /** Update the loaded procedures. */
    void setLoadedProcedures(LoadedProcedureSet loadedProcedure)
    {
        m_loadedProcedures = loadedProcedure;
    }

    /** Thread specific initialization */
    void initialize()
    {
        if (m_backend == BackendTarget.HSQLDB_BACKEND) {
            m_hsql = HsqlBackend.initializeHSQLBackend(m_siteId,
                                                       m_context);
        }
        else {
            m_hsql = null;
        }
    }

    @Override
    public void run()
    {
        initialize();

        try {
            while (m_shouldContinue) {
                // Normal operation blocks the site thread on the sitetasker queue.
                SiteTasker task = m_scheduler.take();
                if (task instanceof TransactionTask) {
                    m_currentTxnId = ((TransactionTask)task).getTxnId();
                }
                task.run(getSiteProcedureConnection());
            }
        }
        catch (OutOfMemoryError e)
        {
            // Even though OOM should be caught by the Throwable section below,
            // it sadly needs to be handled seperately. The goal here is to make
            // sure VoltDB crashes.
            String errmsg = "Site: " + org.voltcore.utils.CoreUtils.hsIdToString(m_siteId) +
                " ran out of Java memory. " + "This node will shut down.";
            VoltDB.crashLocalVoltDB(errmsg, true, e);
        }
        catch (Throwable t)
        {
            String errmsg = "Site: " + org.voltcore.utils.CoreUtils.hsIdToString(m_siteId) +
                " encountered an " + "unexpected error and will die, taking this VoltDB node down.";
            VoltDB.crashLocalVoltDB(errmsg, true, t);
        }
        shutdown();
    }

    /**
     * Commence the shutdown of the Site.  This will allow the run loop to escape, but
     * it will be blocked on taking from the SiteTaskerQueue.  To make shutdown actually
     * happen, the site needs to be unblocked by offering a null SiteTasker to the queue, see
     * Scheduler.m_nullTask for an example (or just use it).
     */
    public void startShutdown()
    {
        m_shouldContinue = false;
    }

    void shutdown()
    {
        if (m_hsql != null) {
            HsqlBackend.shutdownInstance();
        }
    }

    //
    // Legacy SiteProcedureConnection needed by ProcedureRunner
    //
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
    public byte[] loadTable(long txnId, long spHandle, String clusterName, String databaseName,
            String tableName, VoltTable data, boolean returnUniqueViolations, boolean shouldDRStream,
            boolean undo) throws VoltAbortException
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public byte[] loadTable(long txnId, long spHandle, int tableId, VoltTable data, boolean returnUniqueViolations,
            boolean shouldDRStream,
            boolean undo)
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public void updateBackendLogLevels()
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public Map<Integer, List<VoltTable>> recursableRun(
            TransactionState currentTxnState)
    {
        return currentTxnState.recursableRun(this);
    }

    @Override
    public void setSpHandleForSnapshotDigest(long spHandle)
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public void truncateUndoLog(boolean rollback, long beginUndoToken, long spHandle,
            List<UndoAction> undoActions)
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public void stashWorkUnitDependencies(Map<Integer, List<VoltTable>> dependencies)
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public DependencyPair executeSysProcPlanFragment(
            TransactionState txnState,
            Map<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params)
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public HsqlBackend getHsqlBackendIfExists()
    {
        return m_hsql;
    }

    @Override
    public long[] getUSOForExportTable(String signature)
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public void toggleProfiler(int toggle)
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public void tick()
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public void quiesce()
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public void exportAction(boolean syncAction,
                             long ackOffset,
                             Long sequenceNumber,
                             Integer partitionId, String tableSignature)
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public VoltTable[] getStats(StatsSelector selector, int[] locators,
                                boolean interval, Long now)
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public Future<?> doSnapshotWork()
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public void setRejoinComplete(
            JoinProducerBase.JoinCompletionAction replayComplete,
            Map<String, Map<Integer, Pair<Long, Long>>> exportSequenceNumbers,
            boolean requireExistingSequenceNumbers)
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public VoltTable[] executePlanFragments(
            int numFragmentIds,
            long[] planFragmentIds,
            long[] inputDepIds,
            Object[] parameterSets,
            String[] sqlTexts,
            long txnId,
            long spHandle,
            long uniqueId,
            boolean readOnly) throws EEException
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public ProcedureRunner getProcedureRunner(String procedureName) {
        return m_loadedProcedures.getProcByName(procedureName);
    }

    /**
     * Update the catalog.  If we're the MPI, don't bother with the EE.
     */
    public boolean updateCatalog(String diffCmds, CatalogContext context, CatalogSpecificPlanner csp,
            boolean requiresSnapshotIsolationboolean, boolean isMPI)
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public void setPerPartitionTxnIds(long[] perPartitionTxnIds, boolean skipMultipart) {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public TheHashinator getCurrentHashinator()
    {
        return null;
    }

    @Override
    public void updateHashinator(TheHashinator hashinator) {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    /**
     * For the specified list of table ids, return the number of mispartitioned rows using
     * the provided hashinator and hashinator config
     */
    @Override
    public long[] validatePartitioning(long[] tableIds, int hashinatorType, byte[] hashinatorConfig) {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public void setBatch(int batchIndex) {
        // don't need to do anything here
    }

    @Override
    public void setProcedureName(String procedureName) {
        // don't need to do anything here I think?
    }

    @Override
    public void notifyOfSnapshotNonce(String nonce, long snapshotSpHandle) {
        // TODO Auto-generated method stub

    }

    @Override
    public void applyBinaryLog(byte log[]) {
        throw new UnsupportedOperationException("RO MP Site doesn't do this, shouldn't be here");
    }
}
