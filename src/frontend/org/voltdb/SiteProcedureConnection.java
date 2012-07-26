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

import org.voltdb.VoltProcedure.VoltAbortException;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.exceptions.EEException;

/**
 * VoltProcedures invoke SiteProcedureConnection methods to
 * manipulate or request services from an ExecutionSite.
 */
public interface SiteProcedureConnection {

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

    public void loadTable(
            long txnId,
            String clusterName,
            String databaseName,
            String tableName,
            VoltTable data)
    throws VoltAbortException;

    /**
     * Get the EE's plan fragment ID for a given JSON plan.
     * May pull from cache or load on the spot.
     */
    public long loadPlanFragment(byte[] plan) throws EEException;

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
            long txnId,
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

    public Map<Integer, List<VoltTable>> recursableRun(TransactionState currentTxnState);

    /**
     * IV2 commit / rollback interface to the EE
     */
    public void truncateUndoLog(boolean rollback, long token, long txnId);

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

    public long[] getUSOForExportTable(String signature);

    public void toggleProfiler(int toggle);

    public void quiesce();

    public void exportAction(boolean syncAction,
                             int ackOffset,
                             Long sequenceNumber,
                             Integer partitionId,
                             String tableSignature);

    public VoltTable[] getStats(SysProcSelector selector, int[] locators,
                                boolean interval, Long now);
}
