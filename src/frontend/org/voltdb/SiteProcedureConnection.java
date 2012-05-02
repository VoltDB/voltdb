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

    public long getLatestUndoToken();
    public long getNextUndoToken();

    /**
     * Allow system procedures to register plan fragments to the site.
     * @param pfId
     * @param proc
     */
    public void registerPlanFragment(final long pfId, final ProcedureRunner proc);

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

    public void loadTable(
            long txnId,
            String clusterName,
            String databaseName,
            String tableName,
            VoltTable data)
    throws VoltAbortException;


    public VoltTable[] executeQueryPlanFragmentsAndGetResults(
            long[] planFragmentIds,
            int numFragmentIds,
            ParameterSet[] parameterSets,
            int numParameterSets,
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

    /**
     * Legacy recursable execution interface for MP transaction states.
     */
    public Map<Integer, List<VoltTable>> recursableRun(TransactionState currentTxnState);

    /**
     * IV2 commit / rollback interface to the EE
     */
    public void truncateUndoLog(boolean rollback, long token, long txnId);

    /**
     * IV2: Run a plan fragment
     * @param planFragmentId
     * @param inputDepId
     * @param parameterSet
     * @param txnId
     * @param readOnly
     * @return
     * @throws EEException
     */
    public VoltTable executePlanFragment(
        long planFragmentId, int inputDepId, ParameterSet parameterSet,
        long txnId, boolean readOnly) throws EEException;

}
