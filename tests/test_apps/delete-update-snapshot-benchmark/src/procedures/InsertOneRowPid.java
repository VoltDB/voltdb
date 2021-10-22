/* This file is part of VoltDB.
 * Copyright (C) 2021 VoltDB Inc.
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

package procedures;

import client.benchmark.DUSBenchmark;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;

import java.util.Arrays;
import java.util.List;


/** Partitioned version of InsertOneRow */
public class InsertOneRowPid extends InsertOneRow {

    // The run() method, as required for each VoltProcedure
    public VoltTable[] run(long id, long blockId, String tableName,
            String[] columnNames, String[] columnValues)
            throws VoltAbortException
    {
        // Check for a non-partitioned table, which is not allowed here
        if (tableName == null || !DUSBenchmark.PARTITIONED_TABLES.contains(tableName.toUpperCase())) {
            throw new VoltAbortException("Illegal table name ("+tableName+") for InsertOneRowPid.");
        }

        // Determine which SQLStmt to use
        SQLStmt sqlStatement = getInsertStatement(tableName);

        // Get the query args, as an Object array
        Object[] args = getInsertArgs(id, blockId, columnNames, columnValues);

        // Queue the query
        voltQueueSQL(sqlStatement, args);

        // Execute the query
        VoltTable[] vt = voltExecuteSQL(true);

        return vt;
    }

}
