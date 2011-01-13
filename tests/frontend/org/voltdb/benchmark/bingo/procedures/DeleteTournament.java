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

package org.voltdb.benchmark.bingo.procedures;

import org.voltdb.*;

@ProcInfo(
    partitionInfo = "T.T_ID: 0",
    singlePartition = true
)
public class DeleteTournament extends VoltProcedure {

    public final SQLStmt deleteT =
            new SQLStmt("DELETE FROM T WHERE T_ID = ?");

    public final SQLStmt deleteB =
            new SQLStmt("DELETE FROM B WHERE T_ID = ?");

    public final SQLStmt deleteR =
            new SQLStmt("DELETE FROM R WHERE T_ID = ?");

    /* create a new round in R and update the last value for all boards in this T */
    public VoltTable[] run(int t_id) throws VoltAbortException {
        voltQueueSQL(deleteT, t_id);
        voltQueueSQL(deleteB, t_id);
        voltQueueSQL(deleteR, t_id);
        return voltExecuteSQL();
    }
}
