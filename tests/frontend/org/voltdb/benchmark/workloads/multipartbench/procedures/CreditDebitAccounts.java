/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

package org.voltdb.benchmark.workloads.multipartbench.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

@ProcInfo
(
    singlePartition = false
)
public class CreditDebitAccounts extends VoltProcedure
{
    public final SQLStmt creditAcct =
        new SQLStmt("UPDATE FAKE_ACCOUNTS SET BALANCE = BALANCE + ? WHERE ID = ?;");

    public final SQLStmt debitAcct =
        new SQLStmt("UPDATE FAKE_ACCOUNTS SET BALANCE = BALANCE - ? WHERE ID = ?;");

    public long run(int id1, int id2) throws VoltAbortException
    {
        // compute some random stuff
        //int id1 = getSeededRandomNumberGenerator().nextInt(100000);
        //int id2 = getSeededRandomNumberGenerator().nextInt(100000);

        // Add a SQL statement to the execution queue.
        voltQueueSQL(creditAcct, 1, id1);
        voltQueueSQL(debitAcct, 1, id2);

        // Run all queued queries.
        // Passing true parameter since this is the last voltExecuteSQL for this procedure.
        VoltTable[] retval = voltExecuteSQL(true);

        // Ensure there is one table as expected
        assert(retval.length == 2);
        // Use a convenience method to get one
        long modifiedTuples = retval[0].asScalarLong();
        // Check that one tuple was modified
        assert(modifiedTuples == 1);

        // This will be converted into an array of VoltTable for the client.
        // It will contain one table, with one column and one row.
        return modifiedTuples;
    }
}
