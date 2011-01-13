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
package org.voltdb.benchmark.workloads.procedures;

import org.voltdb.*;

@ProcInfo
(
    partitionInfo = "ALL_TYPES.SHORT_ITEM: 0",
    singlePartition = true
)
public class Delete extends VoltProcedure
{
    public final SQLStmt deleteItem =
      new SQLStmt("DELETE FROM ALL_TYPES " +
                  "WHERE SHORT_ITEM = ?");

    public long run(short shortItem)
        throws VoltAbortException
    {
        // Add a SQL statement to the execution queue.
        voltQueueSQL(deleteItem, shortItem);

        // Run all queued queries.
        // Passing true parameter since this is the last voltExecuteSQL for this procedure.
        VoltTable[] retval = voltExecuteSQL(true);

        // Ensure there is one table as expected
        assert(retval.length == 1);
        // Use a convenience method to get one
        long modifiedTuples = retval[0].asScalarLong();
        // Check that one tuple was modified
        assert(modifiedTuples == 1);

        // This will be converted into an array of VoltTable for the client.
        // It will contain one table, with one column and one row.
        return modifiedTuples;
    }
}
