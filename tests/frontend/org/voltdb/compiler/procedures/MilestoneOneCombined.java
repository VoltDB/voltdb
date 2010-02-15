/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

package org.voltdb.compiler.procedures;

import org.voltdb.*;

@ProcInfo (
    //partitionInfo = "WAREHOUSE.W_ID: 0",
    //singlePartition = true
)
public class MilestoneOneCombined extends VoltProcedure {

    public final SQLStmt
    sqlInsert = new SQLStmt("INSERT INTO WAREHOUSE VALUES (?, ?);");

    public final SQLStmt
    sqlSelect = new SQLStmt("SELECT W_NAME FROM WAREHOUSE WHERE W_ID = ?;");

    public VoltTable[] run(long id, String name)
    throws VoltAbortException {
        voltQueueSQL(sqlInsert, id, name);
        VoltTable[] insertResults = voltExecuteSQL();
        if (insertResults.length != 1)
            throw new VoltAbortException("Insert seemed to fail (1).");
        long rowsUpdated = insertResults[0].asScalarLong();
        if (rowsUpdated != 1)
            throw new VoltAbortException("Insert seemed to fail (2).");

        voltQueueSQL(sqlSelect, id);
        return voltExecuteSQL();
    }
}
