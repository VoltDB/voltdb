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

@ProcInfo (
    singlePartition = false
)
public class Initialize extends VoltProcedure
{
    // Check if the database has already been initialized
    public final SQLStmt checkStmt = new SQLStmt("SELECT COUNT(*) FROM replicated;");

    // Insert into the replicated table
    public final SQLStmt insertStmt = new SQLStmt("INSERT INTO replicated VALUES (?);");

    public long run() {
        voltQueueSQL(checkStmt, EXPECT_SCALAR_LONG);
        long currentCount = voltExecuteSQL()[0].asScalarLong();

        // if the data is initialized, return the current count
        if (currentCount != 0)
            return currentCount;

        // initialize the data using the txnId as a base
        long base = this.getTransactionId();
        voltQueueSQL(insertStmt, EXPECT_SCALAR_MATCH(1), base);
        voltExecuteSQL(true);

        // return the number of rows added
        return 1;
    }
}