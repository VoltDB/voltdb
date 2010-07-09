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

import java.util.Random;

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
        VoltTable vt;
        // TestExecutionSite uses this mock site.
        //
        // For interesting things to happen, the first parameter used by the fuzz
        // tester must be a string indicating what transaction outcome should be simulated
        // by the mock EE.
        //
        // rollback_all : every execution site should throw an exception for rollback
        // rollback_random : each execution site should randomly decide to rollback
        //                   This includes the final aggregating execution site call.

        if (parameterSet.toArray().length > 0 && parameterSet.toArray()[0] instanceof String)
        {
            String txn_outcome = (String)parameterSet.toArray()[0];

            if (txn_outcome.equals("rollback_all"))
            {
                //System.out.println("Throwing MASSIVE exception for rollback.");
                throwExceptionForError(ERRORCODE_ERROR);
            }
            else if (txn_outcome.equals("rollback_random"))
            {
                Random rand = new Random(System.currentTimeMillis());
                if (rand.nextInt(100) < 20)
                {
                    //System.out.println("Throwing exception for rollback");
                    //if (planFragmentId == 1)
                    //{
                    //    System.out.println("ROLLING BACK COORDINATOR");
                    //}
                    throwExceptionForError(ERRORCODE_ERROR);
                }
            }
        }
        vt = new VoltTable(new VoltTable.ColumnInfo[] {
                  new VoltTable.ColumnInfo("foo", VoltType.INTEGER)
        });
        vt.addRow(new Integer(1));
        return new DependencyPair(outputDepId, vt);
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
    public void updateCatalog(final String catalogDiffs, int catalogVersion) throws EEException {
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
            boolean resetAction, long ackOffset, int partitionId, long mTableId) {
        // TODO Auto-generated method stub
        return null;
    }
}
