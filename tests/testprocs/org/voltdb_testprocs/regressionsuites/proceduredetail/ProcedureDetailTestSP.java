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

package org.voltdb_testprocs.regressionsuites.proceduredetail;

import org.voltdb.exceptions.SQLException;
import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

/* This Java stored procedure is used to test the PROCEDUREDETAIL selector in @Statistics.
 * It will queue batches based on the parameters you gave to test the behavior of PROCEDUREDETAIL
 * under different scenarios. */
@ProcInfo(
    partitionInfo = "ENG11890.a: 0",
    singlePartition = true
)
public class ProcedureDetailTestSP extends VoltProcedure {

    public final SQLStmt anInsert = new SQLStmt("INSERT INTO ENG11890 VALUES (?, ?);");
    public final SQLStmt anUpdate = new SQLStmt("UPDATE ENG11890 SET b = ? WHERE a = ?;");
    public final SQLStmt aDelete = new SQLStmt("DELETE FROM ENG11890 WHERE a = ?;");
    public final SQLStmt aSelect = new SQLStmt("SELECT * FROM ENG11890 WHERE a = ? ORDER BY a;");

    public VoltTable[] run(int id, String arg) throws VoltAbortException {
        /* Parse the options in a simple way:
         *
         * rw: option to queue a batch that has both read and write operations.
         * This is particularly useful to test multi-partition stored procedure details.
         * For multi-partition stored procedures, there are different code paths for
         * homogeneous batches (pure read or pure write) and
         * heterogeneous batches (has both read and write). */
        boolean hasReadWrite = arg.contains("rw");
        // err: option to queue a batch that has a query that can cause an error.
        boolean hasError = arg.contains("err");
        /* 2batch: option to issue two batches in the stored procedure.
         * if the "err" option is enabled, the failing statement will be queued in the
         * first batch and the exception will be caught and extinguished.
         * So you will see in the procedure detail statistics that the procedure
         * succeeded but one of the statements has the failure count = 1 :) */
        boolean twoBatches = arg.contains("2batch");

        // Start to queue the first batch:
        voltQueueSQL(anInsert, id, String.valueOf(id));
        voltQueueSQL(anUpdate, String.valueOf(id + 1), id);
        if (hasReadWrite) {
            voltQueueSQL(aSelect, id);
        }
        if (hasError) {
            voltQueueSQL(anInsert, id, "012345678910"); // overflow
        }
        voltQueueSQL(aDelete, id);
        VoltTable[] result = null;
        try {
            result = voltExecuteSQL(! twoBatches);
        }
        catch (SQLException ex) {
            if (twoBatches) {
                System.out.println("Caught exception:\n" + ex.getMessage());
                System.out.print("This procedure is configured to execute another batch, ");
                System.out.println("so this exception is extinguished.");
            }
            else {
                throw ex;
            }
        }

        if (! twoBatches) {
            return result;
        }
        // Start to queue the second batch, if asked:
        voltQueueSQL(anInsert, id, String.valueOf(id));
        voltQueueSQL(anUpdate, String.valueOf(id + 1), id);
        if (hasReadWrite) {
            voltQueueSQL(aSelect, id);
        }
        voltQueueSQL(aDelete, id);
        result = voltExecuteSQL(true);
        return result;
    }
}
