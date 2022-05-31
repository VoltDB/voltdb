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

package txnIdSelfCheck.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;

public class Summarize extends VoltProcedure {

    final SQLStmt dateSqlP = new SQLStmt("select ts from partitioned order by ts desc limit 1;");
    final SQLStmt dateSqlR = new SQLStmt("select ts from replicated order by ts desc limit 1;");
    final SQLStmt countSqlP = new SQLStmt("select cnt from partitioned where cid = ? order by cnt desc limit 1;");
    final SQLStmt countSqlR = new SQLStmt("select cnt from replicated where cid = ? order by cnt desc limit 1;");

    public long countForCid(int cid) {
        voltQueueSQL(countSqlP, EXPECT_ZERO_OR_ONE_ROW, cid);
        voltQueueSQL(countSqlR, EXPECT_ZERO_OR_ONE_ROW, cid);
        VoltTable[] results = voltExecuteSQL();

        long count1 = results[0].getRowCount() > 0 ?
                results[0].asScalarLong() : 0;
        long count2 = results[1].getRowCount() > 0 ?
                results[1].asScalarLong() : 0;

        if ((count1 > 0) && (count2 > 0)) {
            if (count1 != count2) {
                throw new VoltAbortException(
                        "hybrid updates left partitioned and replicated tables in a different state.");
            }
        }

        return Math.max(count1, count2);
    }

    public VoltTable run() {
        voltQueueSQL(dateSqlP, EXPECT_ZERO_OR_ONE_ROW);
        voltQueueSQL(dateSqlR, EXPECT_ZERO_OR_ONE_ROW);
        VoltTable[] results = voltExecuteSQL();

        long latest1 = results[0].advanceRow() ?
                results[0].getTimestampAsLong(0) : 0;
        long latest2 = results[1].advanceRow() ?
                results[1].getTimestampAsLong(0) : 0;

        long latest = Math.max(latest1, latest2);

        long sum = 0;
        for (int i = 0; i <= Byte.MAX_VALUE; i++) {
            sum += countForCid(i);
        }

        VoltTable t = new VoltTable(
                new ColumnInfo("ts", VoltType.TIMESTAMP),
                new ColumnInfo("count", VoltType.BIGINT));

        t.addRow(latest, sum);
        return t;
    }
}
