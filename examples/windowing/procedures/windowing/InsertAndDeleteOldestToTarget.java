/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package windowing;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
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
public class InsertAndDeleteOldestToTarget extends VoltProcedure {

    final SQLStmt insert = new SQLStmt(
            "INSERT INTO timedata (uuid, val, update_ts) VALUES (?, ?, ?);");

    public final SQLStmt countRows = new SQLStmt(
            "SELECT COUNT(*) FROM timedata;");

    public final SQLStmt getNthOldestTimestamp = new SQLStmt(
            "SELECT update_ts FROM timedata ORDER BY update_ts ASC OFFSET ? LIMIT 1;");

    // Note this is identical in effect/performance to:
    //   TRUNCATE TABLE timedata;
    public final SQLStmt deleteAll = new SQLStmt(
            "DELETE FROM timedata;");

    public final SQLStmt deleteOlderThanDate = new SQLStmt(
            "DELETE FROM timedata WHERE update_ts <= ?;");

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
    public long run(String uuid, long val, TimestampType update_ts, long maxTotalRows, long targetMaxRowsToDelete) {
        if (targetMaxRowsToDelete <= 0) {
            throw new VoltAbortException("maxRowsToDeletePerProc must be > 0.");
        }
        if (maxTotalRows < 0) {
            throw new VoltAbortException("maxTotalRows must be >= 0.");
        }

        // This line inserts the row.
        voltQueueSQL(insert, EXPECT_SCALAR_MATCH(1), uuid, val, update_ts);
        // In the same round trip to the storage engine, count the rows.
        voltQueueSQL(countRows, EXPECT_SCALAR_LONG);
        // Can assume insert worked because of EXPECT_SCALAR_MATCH(1)
        // Note that the index into the set of results tables below is the second table.
        long count = voltExecuteSQL()[1].asScalarLong();

        // If partition is smaller than desired, return
        if (count < maxTotalRows) {
            return 0;
        }

        // If asked to remove all rows, go ahead
        if ((maxTotalRows == 0) && (count < targetMaxRowsToDelete)) {
            voltQueueSQL(deleteAll, EXPECT_SCALAR_MATCH(count));
            voltExecuteSQL(true);
            return count;
        }

        // Figure out how many rows to try to delete.
        long rowsToConsider = Math.min(count - maxTotalRows, targetMaxRowsToDelete);

        // Find the timestamp of the row at position N in the sorted order, where N is the chunk size
        voltQueueSQL(getNthOldestTimestamp, EXPECT_SCALAR, rowsToConsider);
        TimestampType newestToDiscard = voltExecuteSQL()[0].fetchRow(0).getTimestampAsTimestamp(0);

        // Delete all rows >= the timestamp found in the previous statement.
        // This will delete AT LEAST N rows, but since timestamps may be non-unique,
        //  it might delete more than N. In the worst case, it could delete all rows
        //  if every row has an identical timestamp value. It is guaranteed to make
        //  progress. If we used strictly less than, it might not make progress.
        // This is why the max rows to delete number is a target, not always a perfect max.
        voltQueueSQL(deleteOlderThanDate, EXPECT_SCALAR_LONG, newestToDiscard);
        long deletedCount = voltExecuteSQL(true)[0].asScalarLong();

        // Return the number of rows deleted.
        return deletedCount;
    }
}
