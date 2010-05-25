/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

package org.voltdb.jni;

import org.voltdb.*;
import org.voltdb.elt.ELTProtoMessage;
import org.voltdb.exceptions.*;
import org.voltdb.utils.DBBPool.BBContainer;

public class MockExecutionEngine extends ExecutionEngine {

    public MockExecutionEngine() {
        super(null);
    }

    @Override
    public DependencyPair executePlanFragment(final long planFragmentId, int outputDepId,
            int inputDepIdfinal, ParameterSet parameterSet, final long txnId,
            final long lastCommittedTxnId, final long undoToken) throws EEException
    {
        // TestExecutionSite uses this mock site. A plan fragment id greater than 100
        // indicates a desired rollback. Otherwise, return a made up depedendency pair.
        VoltTable vt;
        if (planFragmentId > 100) {
            System.out.println("Throwing exception for rollback.");
            throwExceptionForError(ERRORCODE_ERROR);
            // satisfy the compiler. Can't reach this point
            return null;
        }
        else {
            vt = new VoltTable(
             new VoltTable.ColumnInfo[] {
                  new VoltTable.ColumnInfo("foo", VoltType.INTEGER)
             });
            vt.addRow(new Integer(1));
            return new DependencyPair(outputDepId, vt);
        }
    }

    @Override
    protected void throwExceptionForError(int errorCode) {
        if (errorCode == ERRORCODE_ERROR) {
            throw new SQLException("66666");
        }
    }

    @Override
    public VoltTable executeCustomPlanFragment(final String plan, int outputDepId,
            int inputDepId, final long txnId, final long lastCommittedTxnId, final long undoQuantumToken)
            throws EEException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public VoltTable[] executeQueryPlanFragmentsAndGetResults(final long[] planFragmentIds, final int numFragmentIds, final ParameterSet[] parameterSets,
            final int numParameterSets, final long txnId, final long lastCommittedTxnId, final long undoToken) throws EEException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public VoltTable[] getStats(final SysProcSelector selector, final int[] locators, boolean interval, Long now) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void loadCatalog(final String serializedCatalog) throws EEException {
        // TODO Auto-generated method stub
    }

    @Override
    public void updateCatalog(final String catalogDiffs) throws EEException {
        // TODO Auto-generated method stub
    }

    @Override
    public void loadTable(final int tableId, final VoltTable table, final long txnId,
        final long lastCommittedTxnId, final long undoToken, final boolean allowELT)
    throws EEException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public void release() throws EEException {
        // TODO Auto-generated method stub
    }

    @Override
    public boolean releaseUndoToken(final long undoToken) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public VoltTable serializeTable(final int tableId) throws EEException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void tick(final long time, final long lastCommittedTxnId) {
        // TODO Auto-generated method stub
    }

    @Override
    public int toggleProfiler(final int toggle) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean undoUndoToken(final long undoToken) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean setLogLevels(final long logLevels) throws EEException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void quiesce(long lastCommittedTxnId) {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean activateCopyOnWrite(int tableId) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public int cowSerializeMore(BBContainer c, int tableId) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public ELTProtoMessage eltAction(boolean ackAction, boolean pollAction,
            boolean resetAction, long ackOffset, int partitionId, int mTableId) {
        // TODO Auto-generated method stub
        return null;
    }
}
