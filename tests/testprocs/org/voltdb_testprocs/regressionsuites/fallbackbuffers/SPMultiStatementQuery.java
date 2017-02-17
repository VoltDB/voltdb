/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltDB;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertFalse;


@ProcInfo
(
    singlePartition = true,
    partitionInfo = "P1.NUM: 0"
)

public class SPMultiStatementQuery extends VoltProcedure {

    public final SQLStmt query = new SQLStmt("select * from P1 where MOD(id, ?) = 0");

    static final int SHARED_BUFFER_SIZE = 10 * 1024 * 1024;
    private boolean isTrue(int value) {
        return value == 0 ? false: true;
    }

    private void checkBuffer(ByteBuffer buf, boolean mayBeDirect, boolean isFinal) {
        if (isFinal) {
            if (!buf.isDirect()) {
                throw new VoltAbortException("The final buffer should be direct");
            }
        }
        else if (buf.capacity() > SHARED_BUFFER_SIZE) {
            if (buf.isDirect()) {
                throw new VoltAbortException("Fallback buffers should be copied into Heap Buffers");
            }
        }
        else {
            if (mayBeDirect != buf.isDirect()) {
                throw new VoltAbortException("Unexpected result buffer");
            }
        }
    }

    // use a partition key here to put all data insert into one partition
    public VoltTable[] run(int partitionKey,
            int returnBuffer, int checkMiddle, int useFinal,
            int firstMod, int secondMod, int thirdMod) {
        VoltTable[] result = null;

        voltQueueSQL(query, firstMod);
        VoltTable[] t1 = voltExecuteSQL();
        checkBuffer(t1[0].getBuffer(), true, false);

        VoltTable[] t2 = null;
        if (isTrue(checkMiddle)) {
            voltQueueSQL(query, secondMod);
            t2 = voltExecuteSQL();
            checkBuffer(t2[0].getBuffer(), false, false);
        }

        voltQueueSQL(query, thirdMod);
        VoltTable[] t3;
        if (isTrue(useFinal)) {
            t3 = voltExecuteSQL(true);
            checkBuffer(t3[0].getBuffer(), true, true);
        }
        else {
            t3 = voltExecuteSQL();
            checkBuffer(t3[0].getBuffer(), false, false);
        }

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
