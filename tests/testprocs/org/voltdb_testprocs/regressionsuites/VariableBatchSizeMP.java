/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltdb_testprocs.regressionsuites;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;

public class VariableBatchSizeMP extends VoltProcedure {

    public static final int P_READ = 1;
    public static final int R_READ = 2;
    public static final int P_WRITE = 4;
    public static final int R_WRITE = 8;
    public static final int CONSTRAINT_FAIL = 16;

    final SQLStmt pRead  = new SQLStmt("select * from P1 order by id limit 1;");
    final SQLStmt rRead  = new SQLStmt("select * from R1 order by id limit 1;");
    final SQLStmt pWrite = new SQLStmt("update P1 set id = 1 where id = 1;");
    final SQLStmt rWrite = new SQLStmt("update R1 set id = 1 where id = 1;");
    final SQLStmt constraintFail = new SQLStmt("insert into P1 values (1);");

    void queueOp(int value, boolean singlePartition) {
        switch (value) {
        case P_READ:
            voltQueueSQL(pRead, EXPECT_SCALAR_MATCH(1));
            break;
        case R_READ:
            voltQueueSQL(rRead, EXPECT_SCALAR_MATCH(1));
            break;
        case P_WRITE:
            voltQueueSQL(pWrite, EXPECT_SCALAR_MATCH(1));
            break;
        case R_WRITE:
            if (!singlePartition) voltQueueSQL(rWrite, EXPECT_SCALAR_MATCH(1));
            else voltQueueSQL(pWrite, EXPECT_SCALAR_MATCH(1));
            break;
        case CONSTRAINT_FAIL:
            voltQueueSQL(constraintFail);
            break;
        default:
            throw new VoltAbortException();
        }
    }

    public long run(long partitionParam, int[] opsForBatch1, int[] opsForBatch2) {
        ProcInfo pi = getClass().getAnnotation(ProcInfo.class);
        boolean singlePartition = (pi != null) && pi.singlePartition();

        // ensure the state is right
        voltQueueSQL(rRead, EXPECT_SCALAR_MATCH(1));
        voltQueueSQL(pRead, EXPECT_SCALAR_MATCH(1));
        voltExecuteSQL();

        // batch 1
        for (int op : opsForBatch1) {
            queueOp(op, singlePartition);
        }
        voltExecuteSQL();

        // batch 2
        for (int op : opsForBatch2) {
            queueOp(op, singlePartition);
        }
        voltExecuteSQL();

        return 0;
    }
}
