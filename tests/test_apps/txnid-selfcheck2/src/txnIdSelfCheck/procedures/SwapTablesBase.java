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

package txnIdSelfCheck.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltProcedure.VoltAbortException;

/**
 * Note: this Stored Procedure base class, and its sub-classes, are not useful
 * for testing the @SwapTables system stored procedure being added to VoltDB
 * V7.1; they were written when we planned to add a DML statement of the form
 * "SWAP TABLE T1 WITH T2" - SQL statementes of that type would have been called
 * within these stored procedures. They are retained here in case we ever do
 * support that DML version of Swap Tables, but they are currently not called.
 */
public abstract class SwapTablesBase extends VoltProcedure {
    protected final SQLStmt count_tru;
    protected final SQLStmt count_swap;
    protected final SQLStmt scancount_tru;
    protected final SQLStmt scancount_swap;
//    protected final SQLStmt swap;

    // TODO: when and if the "SWAP TABLE T1 WITH T2" DML statement (not to be
    // confused with the @SwapTables system stored procedure) is on master,
    // change this to false - or remove it and related code completely;
    // meanwhile, the test code below can itself be tested, with USE_FAKE_SWAP
    // set to true.
    private final boolean USE_FAKE_SWAP = true;

    // TODO: remove these, when/if the "SWAP TABLE T1 WITH T2" DML statement is on master
    protected final SQLStmt swap0;
    protected final SQLStmt swap1;
    protected final SQLStmt swap2;
    protected final SQLStmt swap3;
    protected final SQLStmt swap4;
    protected final SQLStmt swap5;
    protected final SQLStmt swap6;

    // Default Constructor: cannot use
    private SwapTablesBase () {
        this.count_tru = null;
        this.count_swap = null;
        this.scancount_tru = null;
        this.scancount_swap = null;
//        this.swap = null;

        // TODO: remove these, when/if the "SWAP TABLE T1 WITH T2" DML statement
        // is on master; and uncomment the line above
        this.swap0 = null;
        this.swap1 = null;
        this.swap2 = null;
        this.swap3 = null;
        this.swap4 = null;
        this.swap5 = null;
        this.swap6 = null;
    }

    // Constructor to actually use
    public SwapTablesBase (String count_tru, String count_swap,
            String scancount_tru, String scancount_swap, // String swap,
            String swap0, String swap1, String swap2,
            String swap3, String swap4, String swap5, String swap6) {
        this.count_tru = new SQLStmt(count_tru);
        this.count_swap = new SQLStmt(count_swap);
        this.scancount_tru = new SQLStmt(scancount_tru);
        this.scancount_swap = new SQLStmt(scancount_swap);
//        this.swap = new SQLStmt(swap);

        // TODO: remove these, when/if the "SWAP TABLE T1 WITH T2" DML statement
        // is on master; and uncomment the line above
        this.swap0 = new SQLStmt(swap0);
        this.swap1 = new SQLStmt(swap1);
        this.swap2 = new SQLStmt(swap2);
        this.swap3 = new SQLStmt(swap3);
        this.swap4 = new SQLStmt(swap4);
        this.swap5 = new SQLStmt(swap5);
        this.swap6 = new SQLStmt(swap6);
    }

    public VoltTable[] run(long p, byte shouldRollback, String storedProcName) {

        // TODO: call this temporary method, so that the test code can be tested
        // and used before the SWAP TABLES (as ad hoc DML) feature is complete
        if (USE_FAKE_SWAP) {
            return runFakeSwap(p, shouldRollback, storedProcName);
        }

        // Execute the SWAP TABLES query, with SELECT COUNT(*) queries before and after
        voltQueueSQL(count_tru);
        voltQueueSQL(count_swap);
        voltQueueSQL(scancount_tru);
        voltQueueSQL(scancount_swap);
//        voltQueueSQL(swap);
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
            throw new VoltAbortException("EXPECTED SWAP ROLLBACK");
        }
        return results;
    }

    // TODO: this is a temporary method, so that the test code can be tested
    // and used before the SWAP TABLES (as ad hoc DML) feature is complete.
    private VoltTable[] runFakeSwap(long p, byte shouldRollback, String storedProcName) {

        // Execute the SWAP TABLES query, with SELECT COUNT(*) queries before and after
        voltQueueSQL(count_tru);
        voltQueueSQL(count_swap);
        voltQueueSQL(scancount_tru);
        voltQueueSQL(scancount_swap);

        // "Fake" swap, using a temp table:
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
        data = results[15];
        row = data.fetchRow(0);
        long truCountAfter = row.getLong(0);
        data = results[16];
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
        data = results[17];
        row = data.fetchRow(0);
        long truScanCountAfter = row.getLong(0);
        data = results[18];
        row = data.fetchRow(0);
        long swapScanCountAfter = row.getLong(0);
        if (truScanCountBefore != swapScanCountAfter || swapScanCountBefore != truScanCountAfter) {
            throw new VoltAbortException("after swap (scan) counts (" + truScanCountAfter + "," + swapScanCountAfter
                    + ") do not match reverse of those before (" + truScanCountBefore + "," + swapScanCountBefore + ")");
        }

        // TODO: temporary debug print:
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
        System.out.println("In "+storedProcName+": shouldRollback: " + shouldRollback
                + "\n  truCountBefore: " + truCountBefore + " swapCountBefore: " + swapCountBefore
                + "; truCountAfter: " + truCountAfter + " swapCountAfter: " + swapCountAfter
                + "\n  truScanCountBefore: " + truScanCountBefore + " swapScanCountBefore: " + swapScanCountBefore
                + "; truScanCountAfter: " + truScanCountAfter + " swapScanCountAfter: " + swapScanCountAfter
                + "\n  countTemp0: " + countTemp0 + " countTemp1: " + countTemp1
                + "; countTemp2: " + countTemp2 + " countTemp3: " + countTemp3);

        if (shouldRollback != 0) {
            throw new VoltAbortException("EXPECTED SWAP ROLLBACK");
        }
        return results;
    }

}
