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

package txnIdSelfCheck.procedures;

import java.util.Random;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class updateReplicated extends VoltProcedure
{
    // Different reasons for abort
    public static enum AbortStatus {NORMAL, OUT_OF_ORDER};

    // Delete everything in the replicated table
    public final SQLStmt deleteStmt = new SQLStmt("DELETE FROM replicated;");

    // Insert into the replicated table
    public final SQLStmt insertStmt = new SQLStmt("INSERT INTO replicated VALUES (?, ?, ?, ?);");

    // Update the counter
    public final SQLStmt updateStmt = new SQLStmt("UPDATE replicated SET cnt = cnt + 1 WHERE txnid = ?;");

    // Get top 10 from the replicated table
    public final SQLStmt selectStmt = new SQLStmt("SELECT * FROM replicated ORDER BY txnid LIMIT 10;");

    public VoltTable run(long rid) {
        final long txnId = this.getVoltPrivateRealTransactionIdDontUseMe();
        final long uniqueId = getUniqueId();
        final Random rand = getSeededRandomNumberGenerator();

        voltQueueSQL(selectStmt);
        voltQueueSQL(deleteStmt, EXPECT_SCALAR_LONG);
        VoltTable previousResult = voltExecuteSQL()[0];

        // some percent of this will rollback
        if (rand.nextDouble() < 0.1) {
            setAppStatusCode((byte) AbortStatus.NORMAL.ordinal());
            throw new VoltAbortException("Normal abort");
        }

        // rid in the table should be smaller than the current rid
        if (previousResult.advanceRow() && previousResult.getLong("rid") >= rid) {
            setAppStatusCode((byte) AbortStatus.OUT_OF_ORDER.ordinal());
            throw new VoltAbortException("updateReplicated may be executed out of order, " +
                                         "previous rid " + previousResult.getLong("rid") +
                                         " >= current rid " + rid);
        }

        voltQueueSQL(insertStmt, EXPECT_SCALAR_MATCH(1), txnId, uniqueId, rid, previousResult.getLong("cnt"));
        voltExecuteSQL();

        voltQueueSQL(updateStmt, EXPECT_SCALAR_MATCH(1), txnId);
        voltQueueSQL(selectStmt);
        VoltTable[] results = voltExecuteSQL(true);

        // Return last result
        return results[results.length - 1];
    }
}
