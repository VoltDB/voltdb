/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb_testprocs.regressionsuites.fallbackbuffers;

import java.nio.ByteBuffer;

import org.voltdb.BackendTarget;
import org.voltdb.SQLStmt;
import org.voltdb.VoltDB;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

/**
 * This stored procedure accesses multiple sets of rows in separate results based on the
 * a different modulo value for each SQL statement. Smaller modulo's will return more rows
 * while large modulo's will return fewer rows. This procedure will verify that the returned
 * result is correct, is not overwritten by subsequent requests, and uses the EE supplied
 * direct buffers under the correct circumstances to reduce heap allocations and buffer
 * copies.
 */
public class SPMultiStatementQuery extends VoltProcedure {

    public final SQLStmt query = new SQLStmt("select * from P1 where MOD(id, ?) = 0 ORDER BY id");

    static final boolean USING_JNI = VoltDB.instance().getBackendTargetType() == BackendTarget.NATIVE_EE_JNI;
    static final int SHARED_BUFFER_SIZE = 10 * 1024 * 1024;
    private boolean isTrue(int value) {
        return value == 0 ? false: true;
    }

    private void checkBuffer(VoltTable t, int modVal, boolean isDirect, boolean isFinal) {
        // If not using JNI, the buffer will never be direct
        isDirect &= USING_JNI;
        t.resetRowPosition();
        t.advanceRow();
        // Verify that the first row in the table has the expected modulo value
        if (t.getLong("id") != modVal) {
            throw new VoltAbortException("The expected row " + modVal + " but found row " +
                    t.getLong("id") + " as first row in the returned table");
        }
        ByteBuffer buf = t.getBuffer();
        if (isFinal) {
            if (isDirect != buf.isDirect()) {
                throw new VoltAbortException("The final buffer should be direct");
            }
        }
        else if (buf.capacity() > SHARED_BUFFER_SIZE) {
            if (buf.isDirect()) {
                throw new VoltAbortException("Fallback buffers should be copied into Heap Buffers");
            }
        }
        else {
            if (isDirect != buf.isDirect()) {
                throw new VoltAbortException("Unexpected result buffer");
            }
        }
    }

    // use a partition key here to put all data insert into one partition
    public VoltTable[] run(int partitionKey,
            int returnBuffer, int checkMiddle, int useFinal,
            int firstMod, int secondMod, int thirdMod) {
        VoltTable[] result = null;

        // Perform first read
        voltQueueSQL(query, firstMod);
        VoltTable[] t1 = voltExecuteSQL();

        VoltTable[] t2 = null;
        // perform middle (what would be a second, third, etc batch)
        if (isTrue(checkMiddle)) {
            voltQueueSQL(query, secondMod);
            t2 = voltExecuteSQL();
        }

        // perform last read either with and without the final flag set
        voltQueueSQL(query, thirdMod);
        VoltTable[] t3;
        if (isTrue(useFinal)) {
            t3 = voltExecuteSQL(true);
            checkBuffer(t3[0], thirdMod, true, true);
        }
        else {
            t3 = voltExecuteSQL();
            checkBuffer(t3[0], thirdMod, false, false);
        }

        // Verify that the middle buffer is correct and has not been written over by the last batch
        if (isTrue(checkMiddle)) {
            checkBuffer(t2[0], secondMod, false, false);
        }

        // Verify that the first buffer is correct and has not been written over by subsequent batches
        checkBuffer(t1[0], firstMod, true, false);

        if (returnBuffer == 1) {
            result = t1;
        }
        else if (returnBuffer == 2){
            result = t2;
        }
        else {
            result = t3;
        }
        return result;
    }
}
