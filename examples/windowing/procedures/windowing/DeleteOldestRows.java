/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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


//
// Accepts a vote, enforcing business logic: make sure the vote is for a valid
// contestant and that the voter (phone number of the caller) is not above the
// number of allowed votes.
//

package windowing;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.types.TimestampType;

public class DeleteOldestRows extends VoltProcedure {

    public final SQLStmt countRows = new SQLStmt(
            "SELECT COUNT(*) FROM timedata;");

    public final SQLStmt getNthOldestTimestamp = new SQLStmt(
            "SELECT update_ts FROM timedata ORDER BY update_ts ASC OFFSET ? LIMIT 1;");

    public final SQLStmt deleteAll = new SQLStmt(
            "DELETE FROM timedata;");

    public final SQLStmt deleteOlderThanDate = new SQLStmt(
            "DELETE FROM timedata WHERE update_ts <= ?;");

    public long run(String partitionValue, long maxRowsToDeletePerProc) {
        if (maxRowsToDeletePerProc <= 0) {
            throw new VoltAbortException("maxRowsToDeletePerProc must be > 0.");
        }

        voltQueueSQL(countRows, EXPECT_SCALAR_LONG);
        long count = voltExecuteSQL()[0].asScalarLong();

        if (count <= maxRowsToDeletePerProc) {
            voltQueueSQL(deleteAll, EXPECT_SCALAR_MATCH(count));
            voltExecuteSQL(true);
            return count;
        }
        else {
            voltQueueSQL(getNthOldestTimestamp, EXPECT_SCALAR, maxRowsToDeletePerProc);
            TimestampType newestToDiscard = voltExecuteSQL()[0].fetchRow(0).getTimestampAsTimestamp(0);

            voltQueueSQL(deleteOlderThanDate, EXPECT_SCALAR_LONG, newestToDiscard);
            long deletedCount = voltExecuteSQL(true)[0].asScalarLong();

            return deletedCount;
        }

    }
}
