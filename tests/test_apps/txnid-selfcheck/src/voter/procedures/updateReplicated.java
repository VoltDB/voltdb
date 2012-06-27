/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

package voter.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

@ProcInfo (
    singlePartition = false
)
public class updateReplicated extends VoltProcedure
{
    // Delete everything in the replicated table
    public final SQLStmt deleteStmt = new SQLStmt("DELETE FROM replicated;");

    // Insert into the replicated table
    public final SQLStmt insertStmt = new SQLStmt("INSERT INTO replicated VALUES (?);");

    // Get top 10 from the replicated table
    public final SQLStmt selectStmt = new SQLStmt("SELECT * FROM replicated ORDER BY id LIMIT 10;");

    public VoltTable run() {
        voltQueueSQL(selectStmt);
        voltQueueSQL(deleteStmt, EXPECT_SCALAR_LONG);

        // initialize the data using the txnId as a base
        long txnId = this.getTransactionId();
        voltQueueSQL(insertStmt, EXPECT_SCALAR_MATCH(1), txnId);
        voltQueueSQL(selectStmt);
        VoltTable[] results = voltExecuteSQL(true);

        // txnId in the table should be smaller than the current txnId
        if (results[0].asScalarLong() >= txnId) {
            throw new VoltAbortException("updateReplicated executed out of order");
        }

        // Return last result
        return results[results.length - 1];
    }
}