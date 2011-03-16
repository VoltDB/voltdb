/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

import java.util.ArrayList;
import java.util.Random;

import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.SysProcSelector;
import org.voltdb.TableStreamType;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.exceptions.EEException;
import org.voltdb.exceptions.SQLException;
import org.voltdb.export.ExportProtoMessage;
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
        // For interesting things to happen, the fuzz tester must provide a parameter
        // named 'txn_outcome'.  If this parameter is present, then the transaction
        // result will be determined by the parameter which follows 'txn_outcome'
        // according to:
        //
        // commit : every execution site will complete this transaction normally
        // rollback_all : every execution site should throw an exception for rollback
        // rollback_random : each execution site should randomly decide to rollback
        //                   This includes the final aggregating execution site call.

        ArrayList<Object> params = new ArrayList<Object>();

        for (Object param : parameterSet.toArray())
        {
            params.add(param);
        }

        int txn_outcome_index = params.indexOf("txn_outcome");
        if (txn_outcome_index != -1)
        {
            String txn_outcome = (String)params.get(txn_outcome_index + 1);

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
        vt.addRow(Integer.valueOf(1));
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
    public void loadCatalog(final long txnId, final String serializedCatalog) throws EEException {
        // TODO Auto-generated method stub
    }

    @Override
    public void updateCatalog(final long txnId, final String catalogDiffs) throws EEException {
        // TODO Auto-generated method stub
    }

    @Override
    public void loadTable(final int tableId, final VoltTable table, final long txnId,
        final long lastCommittedTxnId, final long undoToken)
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
    public void toggleProfiler(final int toggle) {
        // TODO Auto-generated method stub
        return;
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
    public boolean activateTableStream(int tableId, TableStreamType type) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public int tableStreamSerializeMore(BBContainer c, int tableId, TableStreamType type) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public ExportProtoMessage exportAction(boolean syncAction,
            long ackOffset, long seqNo, int partitionId, String mTableSignature) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long[] getUSOForExportTable(String tableSignature) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void processRecoveryMessage( java.nio.ByteBuffer buffer, long pointer) {
        // TODO Auto-generated method stub

    }

    @Override
    public long tableHashCode( int tableId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int hashinate(Object value, int partitionCount) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long getThreadLocalPoolAllocations() {
        // TODO Auto-generated method stub
        return 0L;
    }
}