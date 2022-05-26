/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

package callcenter;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.types.TimestampType;

public abstract class BeginOrEndCallBase extends VoltProcedure {
    // STDDEV SQL
    final SQLStmt findTodaysStddevStatsForAgent = new SQLStmt(
            "SELECT curdate, n, sumk, qk " +
            "  FROM stddevbyagent " +
            "  WHERE curdate = TRUNCATE(DAY, ?) AND agent_id = ?;");

    final SQLStmt upsertTodaysStddevStatsForAgent = new SQLStmt(
            "UPSERT INTO stddevbyagent " +
            "         (agent_id,          curdate, n, sumk, qk, stddev) " +
            "  VALUES (       ?, TRUNCATE(DAY, ?), ?,    ?,  ?,      ?);");

    void computeRunningStdDev(int agent_id, TimestampType end_ts, long durationms) {
        voltQueueSQL(findTodaysStddevStatsForAgent, EXPECT_ZERO_OR_ONE_ROW, end_ts, agent_id);
        VoltTable stddevTable = voltExecuteSQL()[0];

        long nprev = 0, n = 0;
        long sumprev = 0, sum = 0;
        double qprev = 0, q = 0;
        double avgprev = 0, avg = 0;
        double stddev = 0;

        if (stddevTable.getRowCount() == 1) {
            VoltTableRow stddevRow = stddevTable.fetchRow(0);
            nprev = stddevRow.getLong("n");
            sumprev = stddevRow.getLong("sumk");
            qprev = stddevRow.getDouble("qk");
        }

        n = nprev + 1;
        sum = sumprev + durationms;
        avgprev = nprev > 0 ? (sumprev / (double) nprev) : 0;
        avg = sum / (double) n;

        q = qprev + (durationms - avgprev) * (durationms - avg);
        stddev = Math.sqrt(q / n);

        // really basic validity checks that the math hasn't corrupted something
        if (!Double.isFinite(q)) { throw new VoltAbortException("q is not finite"); }
        if (!Double.isFinite(avg)) { throw new VoltAbortException("avg is not finite"); }
        if (!Double.isFinite(stddev)) { throw new VoltAbortException("stddev is not finite"); }

        voltQueueSQL(upsertTodaysStddevStatsForAgent, EXPECT_SCALAR_MATCH(1),
                agent_id, end_ts, n, sum, q, stddev);
    }
}
