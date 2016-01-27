/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

package org.voltdb_testprocs.regressionsuites.rollbackprocs;

import org.voltdb.*;

@ProcInfo (
    partitionInfo = "NEW_ORDER.NO_W_ID: 0",
    singlePartition = true
)
public class SinglePartitionConstraintError extends VoltProcedure {

    public final SQLStmt insert = new SQLStmt("INSERT INTO NEW_ORDER VALUES (?, ?, ?);");

    public VoltTable[] run(byte w_id) {
        voltQueueSQL(insert, w_id, w_id, w_id);
        long tuplesChanged1 = voltExecuteSQL()[0].asScalarLong();

        // this second insert should fail
        voltQueueSQL(insert, w_id, w_id, w_id);
        long tuplesChanged2 = voltExecuteSQL()[0].asScalarLong();

        VoltTable[] retval = new VoltTable[2];
        retval[0] = new VoltTable(new VoltTable.ColumnInfo("TxnId", VoltType.BIGINT));
        retval[0].addRow(tuplesChanged1);
        retval[1] = new VoltTable(new VoltTable.ColumnInfo("TxnId", VoltType.BIGINT));
        retval[1].addRow(tuplesChanged2);
        return retval;
    }
}
