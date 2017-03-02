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

public class TRURSwapTables extends VoltProcedure {
    final SQLStmt count_tru = new SQLStmt("select count(*) from trur;");
    final SQLStmt count_swap = new SQLStmt("select count(*) from swapr;");
    final SQLStmt scancount_tru = new SQLStmt("select count(*) from trur where p >= 0;");
    final SQLStmt scancount_swap = new SQLStmt("select count(*) from swapr where p >= 0;");
    final SQLStmt swap = new SQLStmt("swap tables trur swapr;");

    public VoltTable[] run(long p, byte shouldRollback) {

        // Execute the SWAP TABLES query, with SELECT COUNT(*) queries before and after
        voltQueueSQL(count_tru);
        voltQueueSQL(count_swap);
        voltQueueSQL(scancount_tru);
        voltQueueSQL(scancount_swap);
        voltQueueSQL(swap);
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
        data = results[5];
        row = data.fetchRow(0);
        long truCountAfter = row.getLong(0);
        data = results[6];
        row = data.fetchRow(0);
        long swapCountAfter = row.getLong(0);
        if (truCountBefore != swapCountAfter || swapCountBefore != truCountAfter) {
            throw new VoltAbortException("after swap (opt) counts (" + truCountAfter + "," + swapCountAfter
                    + ") do not match reverse of those before (" + truCountBefore + "," + swapCountBefore + ")");
        }

        // Check that the partial (scan) counts of the two tables have been swapped
        data = results[2];
        row = data.fetchRow(0);
        long truScanCountBefore = row.getLong(0);
        data = results[3];
        row = data.fetchRow(0);
        long swapScanCountBefore = row.getLong(0);
        data = results[7];
        row = data.fetchRow(0);
        long truScanCountAfter = row.getLong(0);
        data = results[8];
        row = data.fetchRow(0);
        long swapScanCountAfter = row.getLong(0);
        if (truScanCountBefore != swapScanCountAfter || swapScanCountBefore != truScanCountAfter) {
            throw new VoltAbortException("after swap (scan) counts (" + truScanCountAfter + "," + swapScanCountAfter
                    + ") do not match reverse of those before (" + truScanCountBefore + "," + swapScanCountBefore + ")");
        }

        if (shouldRollback != 0) {
            throw new VoltAbortException("EXPECTED ROLLBACK");
        }
        return results;
    }
}
