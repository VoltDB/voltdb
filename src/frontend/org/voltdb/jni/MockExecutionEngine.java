/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

package org.voltdb.jni;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.Pair;
import org.voltdb.ParameterSet;
import org.voltdb.StatsSelector;
import org.voltdb.TableStreamType;
import org.voltdb.TheHashinator;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.exceptions.EEException;
import org.voltdb.exceptions.SQLException;

public class MockExecutionEngine extends ExecutionEngine {

    @Override
    protected void throwExceptionForError(int errorCode) {
        if (errorCode == ERRORCODE_ERROR) {
            throw new SQLException("66666");
        }
    }

    @Override
    protected VoltTable[] coreExecutePlanFragments(
            final int numFragmentIds,
            final long[] planFragmentIds,
            final long[] inputDepIds,
            final Object[] parameterSets,
            final long txnId,
            final long spHandle,
            final long lastCommittedSpHandle,
            final long uniqueId,
            final long undoToken) throws EEException
    {
        if (numFragmentIds != 1) {
            return null;
        }

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

        // de-serialize all parameter sets
        if (parameterSets[0] instanceof ByteBuffer) {
            try {
                parameterSets[0] = ParameterSet.fromByteBuffer((ByteBuffer) parameterSets[0]);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        for (Object param : ((ParameterSet) parameterSets[0]).toArray())
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
        return new VoltTable[] { vt };
    }

    @Override
    public VoltTable[] getStats(final StatsSelector selector, final int[] locators, boolean interval, Long now) {
        return null;
    }

    @Override
    public void coreLoadCatalog(final long txnId, final byte[] catalogBytes) throws EEException {
    }

    @Override
    public void coreUpdateCatalog(final long txnId, final String catalogDiffs) throws EEException {
    }

    @Override
    public byte[] loadTable(final int tableId, final VoltTable table, final long txnId,
        final long spHandle, final long lastCommittedTxnId, long uniqueId,
        boolean returnUniqueViolations, boolean shouldDRStream, long undoToken)
    throws EEException
    {
        return null;
    }

    @Override
    public void release() throws EEException {
    }

    @Override
    public boolean releaseUndoToken(final long undoToken) {
        return false;
    }

    @Override
    public VoltTable serializeTable(final int tableId) throws EEException {
        return null;
    }

    @Override
    public void tick(final long time, final long lastCommittedTxnId) {
    }

    @Override
    public void toggleProfiler(final int toggle) {
    }

    @Override
    public boolean undoUndoToken(final long undoToken) {
        return false;
    }

    @Override
    public boolean setLogLevels(final long logLevels) throws EEException {
        return false;
    }

    @Override
    public void quiesce(long lastCommittedTxnId) {
    }

    @Override
    public boolean activateTableStream(int tableId, TableStreamType type, long undoQuantumToken, byte[] predicates) {
        return false;
    }

    @Override
    public Pair<Long, int[]> tableStreamSerializeMore(int tableId, TableStreamType type,
                                                      List<BBContainer> outputBuffers) {
        return Pair.of(0l, new int[] {0});
    }

    @Override
    public void exportAction(boolean syncAction,
            long ackOffset, long seqNo, int partitionId, String mTableSignature) {
    }

    @Override
    public long[] getUSOForExportTable(String tableSignature) {
        return null;
    }

    @Override
    public void processRecoveryMessage( java.nio.ByteBuffer buffer, long pointer) {
    }

    @Override
    public long tableHashCode( int tableId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int hashinate(Object value, TheHashinator.HashinatorConfig config) {
        return 0;
    }

    @Override
    public void updateHashinator(TheHashinator.HashinatorConfig config) {
    }

    @Override
    public long applyBinaryLog(ByteBuffer log, long txnId, long spHandle, long lastCommittedSpHandle, long uniqueId,
                               int remoteClusterId, long undoToken) throws EEException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getThreadLocalPoolAllocations() {
        return 0L;
    }

    @Override
    public byte[] executeTask(TaskType taskType, ByteBuffer task) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuffer getParamBufferForExecuteTask(int requiredCapacity) {
        throw new UnsupportedOperationException();
    }
}
