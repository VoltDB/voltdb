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

@ProcInfo (
    partitionInfo = "T.T_ID: 0",
    singlePartition = true
)
public class PlayRound extends VoltProcedure {

    public final SQLStmt getPotValue = new SQLStmt("SELECT MAX(R_POT) FROM R WHERE T_ID = ?");
    public final SQLStmt insertR =
            new SQLStmt("INSERT INTO R VALUES (?, ?, ?, ?)");

    public final SQLStmt updateB =
            new SQLStmt("UPDATE B SET LAST_VALUE = ? WHERE T_ID = ?");

    public final SQLStmt getAggPotValueForTourney = new SQLStmt(
            "SELECT AVG(R_POT), MIN(R_POT), MAX(R_POT), COUNT(R_POT) FROM R WHERE T_ID = ?");

    public final SQLStmt doJoin = new SQLStmt(
            "SELECT B.T_ID, B.B_ID, R.R_ID, R.R_POT FROM B, R WHERE B.B_ID = 0 AND B.T_ID = ? AND R.T_ID = ? AND R.T_ID = B.T_ID");
    private final java.util.Random r = new java.util.Random();

    /* create a new round in R and update the last value for all boards in this T */
    public VoltTable[] run(int t_id, int r_id, String val) throws VoltAbortException {

        voltQueueSQL(getPotValue, t_id);
        VoltTable potValue[] = voltExecuteSQL();

        long pot = 0;
        if (potValue[0].advanceRow()) {
            pot = potValue[0].getLong(0);
            if (potValue[0].wasNull()) {
                pot = 0;
            }
        }

        voltQueueSQL(insertR, t_id, r_id, pot + r.nextInt(10), val);
        voltQueueSQL(updateB, val, t_id);
        voltExecuteSQL();

        voltQueueSQL(getAggPotValueForTourney, t_id);
        voltQueueSQL(doJoin, t_id, t_id);
        VoltTable results[] = voltExecuteSQL();
        return results;
    }
}
