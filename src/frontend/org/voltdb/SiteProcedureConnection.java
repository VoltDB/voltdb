/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

package org.voltdb;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.voltcore.utils.Pair;
import org.voltdb.VoltProcedure.VoltAbortException;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.exceptions.EEException;
import org.voltdb.iv2.JoinProducerBase;

/**
 * VoltProcedures invoke SiteProcedureConnection methods to
 * manipulate or request services from an ExecutionSite.
 */
public interface SiteProcedureConnection {

    public long getLatestUndoToken();
    public long getNextUndoToken();

    /**
     * Get the HSQL backend, if any.  Returns null if we're not configured
     * to use it
     */
    public HsqlBackend getHsqlBackendIfExists();

    /**
     * Get the catalog site id for the corresponding SiteProcedureConnection
     */
    public long getCorrespondingSiteId();

    /**
     * Get the partition id for the corresponding SiteProcedureConnection
     */
    public int getCorrespondingPartitionId();

    /**
     * Get the catalog host id for the corresponding SiteProcedureConnection
     */
    public int getCorrespondingHostId();

    /**
     * Log settings changed. Signal EE to update log level.
     */
    public void updateBackendLogLevels();

    /**
     * loadTable method used by user-facing voltLoadTable() call in ProcedureRunner
     */
    public void loadTable(
            long txnId,
            String clusterName,
            String databaseName,
            String tableName,
            VoltTable data)
    throws VoltAbortException;

    /**
     * loadTable method used internally by ExecutionSite/Site clients
     */
    public void loadTable(long txnId, int tableId, VoltTable data);

    /**
     * Execute a set of plan fragments.
     * Note: it's ok to pass null for inputDepIds if the fragments
     * have no dependencies.
     */
    public VoltTable[] executePlanFragments(
            int numFragmentIds,
            long[] planFragmentIds,
            long[] inputDepIds,
            ParameterSet[] parameterSets,
            long spHandle,
            long uniqueId,
            boolean readOnly) throws EEException;

    /**
     * Get the number of partitions so ProcedureRunner can divide
     * replicated table DML results to get the *real* number of
     * rows modified
     */
    public abstract long getReplicatedDMLDivisor();

    /**
     * For test cases that need to mimic a plan fragment being invoked
     */
    public void simulateExecutePlanFragments(long txnId, boolean readOnly);

    /**
     * Legacy recursable execution interface for MP transaction states.
     */
    public Map<Integer, List<VoltTable>> recursableRun(TransactionState currentTxnState);

    /**
     * IV2 commit / rollback interface to the EE
     */
    public void truncateUndoLog(boolean rollback, long token, long txnId, long spHandle);

    /**
     * IV2: send dependencies to the EE
     */
    public void stashWorkUnitDependencies(final Map<Integer, List<VoltTable>> dependencies);

    /**
     * IV2: run a system procedure plan fragment
     */
    public DependencyPair executeSysProcPlanFragment(
            TransactionState txnState,
            Map<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params);

    /**
     * IV2: get the procedure runner for the named procedure
     */
    public ProcedureRunner getProcedureRunner(String procedureName);

    /*
     * This isn't just a simple setter, it has behavior side effects
     * as well because it causes the Site to start replaying log data
     * if configured to do so and it will also set export sequence numbers
     */
    public void setRejoinComplete(
            JoinProducerBase.JoinCompletionAction action,
            Map<String, Map<Integer, Pair<Long, Long>>> exportSequenceNumbers);

    public long[] getUSOForExportTable(String signature);

    public void toggleProfiler(int toggle);

    public void tick();

    public void quiesce();

    public void exportAction(boolean syncAction,
                             long ackOffset,
                             Long sequenceNumber,
                             Integer partitionId,
                             String tableSignature);

    public VoltTable[] getStats(SysProcSelector selector, int[] locators,
                                boolean interval, Long now);

    // Snapshot services provided by the site
    public Future<?> doSnapshotWork(boolean ignoreQuietPeriod);
    public void setPerPartitionTxnIds(long[] perPartitionTxnIds);

    /**
     * Get the site-local fragment id for a given plan identified by 20-byte sha-1 hash
     */
    public long getFragmentIdForPlanHash(byte[] planHash);

    /**
     * Get the site-local fragment id for a given plan identified by 20-byte sha-1 hash
     * If the plan isn't known to this SPC, load it up. Otherwise addref it.
     */
    public long loadOrAddRefPlanFragment(byte[] planHash, byte[] plan);

    /**
     * Decref the plan associated with this site-local fragment id. If the refcount
     * goes to 0, the plan may be removed (depending on caching policy).
     */
    public void decrefPlanFragmentById(long fragmentId);

    /**
     * Get the full JSON plan associated with a given site-local fragment id.
     */
    public byte[] planForFragmentId(long fragmentId);
}
