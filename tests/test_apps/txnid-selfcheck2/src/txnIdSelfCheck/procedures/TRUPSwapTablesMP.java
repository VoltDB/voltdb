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

package txnIdSelfCheck.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltProcedure.VoltAbortException;

public class TRUPSwapTablesMP extends VoltProcedure {
    final SQLStmt count_tru = new SQLStmt("select count(*) from trup;");
    final SQLStmt count_swap = new SQLStmt("select count(*) from swapp;");
    final SQLStmt scancount_tru = new SQLStmt("select count(*) from trup where p >= 0;");
    final SQLStmt scancount_swap = new SQLStmt("select count(*) from swapp where p >= 0;");
    // TODO: uncomment this, after SWAP TABLES actually works
    //final SQLStmt swap = new SQLStmt("swap tables trup swapp;");

    // TODO: remove these, after SWAP TABLES actually works
    final SQLStmt swap0 = new SQLStmt("truncate table tempp;");
    final SQLStmt swap1 = new SQLStmt("insert into tempp select * from trup;");
    final SQLStmt swap2 = new SQLStmt("truncate table trup;");
    final SQLStmt swap3 = new SQLStmt("insert into trup select * from swapp;");
    final SQLStmt swap4 = new SQLStmt("truncate table swapp;");
    final SQLStmt swap5 = new SQLStmt("insert into swapp select * from tempp;");
    final SQLStmt swap6 = new SQLStmt("select count(*) from tempp;");

    public VoltTable[] run(long p, byte shouldRollback) {

        // Execute the SWAP TABLES query, with SELECT COUNT(*) queries before and after
        voltQueueSQL(count_tru);
        voltQueueSQL(count_swap);
        voltQueueSQL(scancount_tru);
        voltQueueSQL(scancount_swap);
        // TODO: uncomment this, after SWAP TABLES actually works
        //voltQueueSQL(swap);

        // TODO: remove these, after SWAP TABLES actually works
        voltQueueSQL(swap6);
        voltQueueSQL(swap0);
        voltQueueSQL(swap6);
        voltQueueSQL(swap1);
        voltQueueSQL(swap2);
        voltQueueSQL(swap3);
        voltQueueSQL(swap4);
        voltQueueSQL(swap5);
        voltQueueSQL(swap6);
        voltQueueSQL(swap0);
        voltQueueSQL(swap6);

        voltQueueSQL(count_tru);
        voltQueueSQL(count_swap);
        voltQueueSQL(scancount_tru);
        voltQueueSQL(scancount_swap);
        VoltTable[] results = voltExecuteSQL(true);

        // Check that the total (opt) counts of the two tables have been swapped
        VoltTable data = results[0];
        VoltTableRow row = data.fetchRow(0);
        long truCountBefore = row.getLong(0);
        data = results[1];
        row = data.fetchRow(0);
        long swapCountBefore = row.getLong(0);
        data = results[15];  // TODO: [5], not [11], after SWAP TABLES actually works
        row = data.fetchRow(0);
        long truCountAfter = row.getLong(0);
        data = results[16];  // TODO: [6], not [12], after SWAP TABLES actually works
        row = data.fetchRow(0);
        long swapCountAfter = row.getLong(0);
        if (truCountBefore != swapCountAfter || swapCountBefore != truCountAfter) {
            throw new VoltAbortException("after swap (opt) counts (" + truCountAfter + "," + swapCountAfter
                    + ") do not match those before (" + truCountBefore + "," + swapCountBefore + ")");
        }

        // Check that the partial (scan) counts of the two tables have been swapped
        data = results[2];
        row = data.fetchRow(0);
        long truScanCountBefore = row.getLong(0);
        data = results[3];
        row = data.fetchRow(0);
        long swapScanCountBefore = row.getLong(0);
        data = results[17];  // TODO: [7], not [13], after SWAP TABLES actually works
        row = data.fetchRow(0);
        long truScanCountAfter = row.getLong(0);
        data = results[18];  // TODO: [8], not [14], after SWAP TABLES actually works
        row = data.fetchRow(0);
        long swapScanCountAfter = row.getLong(0);
        if (truScanCountBefore != swapScanCountAfter || swapScanCountBefore != truScanCountAfter) {
            throw new VoltAbortException("after swap (scan) counts (" + truScanCountAfter + "," + swapScanCountAfter
                    + ") do not match those before (" + truScanCountBefore + "," + swapScanCountBefore + ")");
        }

        // TODO: temp debug print:
        data = results[4];
        row = data.fetchRow(0);
        long countTemp0 = row.getLong(0);
        data = results[6];
        row = data.fetchRow(0);
        long countTemp1 = row.getLong(0);
        data = results[12];
        row = data.fetchRow(0);
        long countTemp2 = row.getLong(0);
        data = results[14];
        row = data.fetchRow(0);
        long countTemp3 = row.getLong(0);
        System.out.println("In TRUPSwapTablesMP: shouldRollback: " + shouldRollback
                + "\n  truCountBefore: " + truCountBefore + " swapCountBefore: " + swapCountBefore
                + "; truCountAfter: " + truCountAfter + " swapCountAfter: " + swapCountAfter
                + "\n  truScanCountBefore: " + truScanCountBefore + " swapScanCountBefore: " + swapScanCountBefore
                + "; truScanCountAfter: " + truScanCountAfter + " swapScanCountAfter: " + swapScanCountAfter
                + "\n  countTemp0: " + countTemp0 + " countTemp1: " + countTemp1
                + "; countTemp2: " + countTemp2 + " countTemp3: " + countTemp3);

        if (shouldRollback != 0) {
            throw new VoltAbortException("EXPECTED ROLLBACK");
        }
        return results;
    }
}
