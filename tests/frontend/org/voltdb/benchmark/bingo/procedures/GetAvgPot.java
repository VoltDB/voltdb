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

package org.voltdb.benchmark.bingo.procedures;

import org.voltdb.*;

@ProcInfo (
    singlePartition = false
)

public class GetAvgPot extends VoltProcedure {

    public final SQLStmt getAvgPotValue = new SQLStmt("SELECT AVG(R_POT) FROM " +
            "R WHERE R.T_ID = ? OR " +
            "R.T_ID = ? OR " +
            "R.T_ID = ? OR " +
            "R.T_ID = ? OR " +
            "R.T_ID = ? OR " +
            "R.T_ID = ? OR " +
            "R.T_ID = ? OR " +
            "R.T_ID = ? OR " +
            "R.T_ID = ? OR " +
            "R.T_ID = ?");

    public VoltTable[] run(long tournament_ids[]) throws VoltAbortException {
        voltQueueSQL(getAvgPotValue,
                tournament_ids[0],
                tournament_ids[1],
                tournament_ids[2],
                tournament_ids[3],
                tournament_ids[4],
                tournament_ids[5],
                tournament_ids[6],
                tournament_ids[7],
                tournament_ids[8],
                 tournament_ids[9]);
        return voltExecuteSQL();
    }
}
