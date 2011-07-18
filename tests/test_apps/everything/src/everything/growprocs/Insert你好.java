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

package everything.growprocs;

import java.util.Random;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.types.TimestampType;

@ProcInfo(
    partitionInfo = "main.pval: 0",
    singlePartition = true
)
public class Insert你好 extends VoltProcedure {

    public final SQLStmt findLargestID =
            new SQLStmt("select ival from main order by ival desc limit 1;");
    public final SQLStmt insert =
            new SQLStmt("insert into main values (?, ?, ?, ?)");

    public VoltTable run(byte pval你好, int numRows, String prefix) {
        // limited by batch size
        assert(numRows < 1000);

        // create a table to return inserted rows
        VoltTable retval = new VoltTable(
                new VoltTable.ColumnInfo("pval", VoltType.TINYINT),
                new VoltTable.ColumnInfo("ival", VoltType.BIGINT),
                new VoltTable.ColumnInfo("ts", VoltType.TIMESTAMP),
                new VoltTable.ColumnInfo("text1", VoltType.STRING));

        // use deterministic time
        TimestampType now = new TimestampType(getTransactionTime());

        // get the largest
        voltQueueSQL(findLargestID);
        VoltTable t = voltExecuteSQL()[0];
        long maxIval = 0;
        if (t.getRowCount() != 0)
            maxIval = t.asScalarLong();

        // use deterministic random numbers
        Random r = getSeededRandomNumberGenerator();

        for (int i = 0; i < numRows; ++i) {
            long rando = r.nextLong();
            // about 1/5 of the time, deterministically abort the insert
            if ((rando % (numRows * 5)) == 0) {
                throw new VoltAbortException("Hopefully deterministically aborting every few inserts. 你好.");
            }

            String randomStringData = prefix + String.valueOf(rando);
            voltQueueSQL(insert, pval你好 , maxIval + 1 + i, now, randomStringData);
            retval.addRow(pval你好 , maxIval + 1 + i, now, randomStringData);
        }

        VoltTable[] results = voltExecuteSQL();
        assert(results.length == numRows);
        for (VoltTable rt : results) {
            assert(rt.asScalarLong() == 1);
        }

        return retval;
    }
}
