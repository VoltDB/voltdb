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


//
// Accepts a vote, enforcing business logic: make sure the vote is for a valid
// contestant and that the voter (phone number of the caller) is not above the
// number of allowed votes.
//

package LiveRejoinConsistency.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;

@ProcInfo (
        partitionInfo = "counters_ptn.id:0",
        singlePartition = true
        )
public class getNextFromPtn extends VoltProcedure {

    // potential return codes
    public static final long ERR_INVALID_COUNTER = 0;

    // get Counter
    public final SQLStmt getCounterStmt = new SQLStmt(
            "SELECT counter FROM counters_ptn WHERE id = ? order by id;");
    // update Counter
    public final SQLStmt updateCounterStmt = new SQLStmt(
            "UPDATE counters_ptn SET counter = ? WHERE id = ? and counter = ?;");

    public long run(int id, long inc) {

        voltQueueSQL(getCounterStmt, EXPECT_ZERO_OR_ONE_ROW, id);
        VoltTable validation[] = voltExecuteSQL();

        if (validation[0].getRowCount() != 1) {
            return ERR_INVALID_COUNTER;
        }

        VoltTableRow row = validation[0].fetchRow(0);

        // what happens when this overflows?
        long count = row.getLong(0)+inc;

        voltQueueSQL(updateCounterStmt, EXPECT_ONE_ROW, count, id, row.getLong(0));
        VoltTable result[] = voltExecuteSQL(true);

        // return the updated value
        return count;
    }
}
