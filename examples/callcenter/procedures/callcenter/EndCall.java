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
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.types.TimestampType;

/**
 * <p>Step 1. Insert given tuple.
 * Step 2. If the table (at this partition) is larger than than maxTotalRows,
 * delete tuples from oldest to newest until it's the right size, or until
 * maxRowsToDeletePerProc tuples have been deleted.</p>
 *
 * <p>This procedure basically combines TIMEDATA.insert with DeleteOldestToTarget.</p>
 *
 * <p>One important thing to consider when developing procedures like this is
 * that they be deterministic. This procedure may be applied simultaniously on
 * many replica partitions, but since it will deterministically delete the same
 * tuples if the database contents are identical, then it will be fine. Note:
 * this is why VoltDB doesn't allow LIMIT in delete operators. All DML must
 * be deterministic.</p>
 *
 * <p>Note, there is a lot of redundant code/comments among the stored procedures
 * in this example app. That's intentional to make each stand alone and be easier
 * to follow. A production app might offer less choice or just reuse more code.</p>
 *
 */
public class EndCall extends BeginOrEndCallBase {

    final SQLStmt findCompletedCall = new SQLStmt(
            "SELECT * FROM completedcalls WHERE call_id = ? AND agent_id = ? AND phone_no = ?;");

    final SQLStmt findOpenCall = new SQLStmt(
            "SELECT * FROM opencalls WHERE call_id = ? AND agent_id = ? AND phone_no = ?;");

    final SQLStmt upsertOpenCall = new SQLStmt(
            "UPSERT INTO opencalls " +
            "         (call_id, agent_id, phone_no, end_ts) " +
            "  VALUES (      ?,        ?,        ?,      ?);");

    final SQLStmt insertCompletedCall = new SQLStmt(
            "INSERT INTO completedcalls " +
            "         (call_id, agent_id, phone_no, start_ts, end_ts, duration) " +
            "  VALUES (      ?,        ?,        ?,        ?,      ?,        ?);");

    final SQLStmt deleteOpenCall = new SQLStmt(
            "DELETE FROM opencalls WHERE call_id = ? AND agent_id = ? AND phone_no = ?;");

    /**
     * Procedure main logic.
     *
     * @param uuid Column value for tuple insertion and partitioning key for this procedure.
     * @param val Column value for tuple insertion.
     * @param update_ts Column value for tuple insertion.
     * @param maxTotalRows The desired number of rows per partition.
     * @param targetMaxRowsToDelete The upper limit on the number of rows to delete per transaction.
     * @return The number of deleted rows.
     * @throws VoltAbortException on bad input.
     */
    public long run(int agent_id, String phone_no, long call_id, TimestampType end_ts) {
        voltQueueSQL(findOpenCall, EXPECT_ZERO_OR_ONE_ROW, call_id, agent_id, phone_no);
        voltQueueSQL(findCompletedCall, EXPECT_ZERO_OR_ONE_ROW, call_id, agent_id, phone_no);
        VoltTable[] results = voltExecuteSQL();

        boolean completedCall = results[1].getRowCount() > 0;
        if (completedCall) {
            return -1;
        }

        VoltTable openRowTable = results[0];
        if (openRowTable.getRowCount() > 0) {
            VoltTableRow existingCall = openRowTable.fetchRow(0);

            // check if this is the second begin we've seen for this open call
            existingCall.getTimestampAsTimestamp("end_ts");
            if (existingCall.wasNull() == false) {
                return -1;
            }

            // check if this completes the call
            TimestampType start_ts = existingCall.getTimestampAsTimestamp("start_ts");
            if (existingCall.wasNull() == false) {

                int durationms = (int) ((end_ts.getTime() - start_ts.getTime()) / 1000);

                // update per-day running stddev calculation
                computeRunningStdDev(agent_id, end_ts, durationms);

                // completes the call
                voltQueueSQL(deleteOpenCall, EXPECT_SCALAR_MATCH(1),
                        call_id, agent_id, phone_no);
                voltQueueSQL(insertCompletedCall, EXPECT_SCALAR_MATCH(1),
                        call_id, agent_id, phone_no, start_ts, end_ts, durationms);

                voltExecuteSQL(true);
                return 0;
            }
        }

        voltQueueSQL(upsertOpenCall, EXPECT_SCALAR_MATCH(1), call_id, agent_id, phone_no, end_ts);
        voltExecuteSQL(true);
        return 0;
    }
}
