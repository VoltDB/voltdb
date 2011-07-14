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

package everything.shrinkprocs;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.types.TimestampType;

@ProcInfo(
    partitionInfo = "main.pval: 0",
    singlePartition = true
)
public class Nibble你好 extends VoltProcedure {

    public final SQLStmt count = new SQLStmt(
            "select count(*) from main;");
    public final SQLStmt countByDate = new SQLStmt(
            "select rowtime from main order by rowtime asc limit ?");
    public final SQLStmt delete = new SQLStmt(
            "delete from main where rowtime <= ?");

    public VoltTable[] run(byte pval你好, int rowsToDelete) {
        voltQueueSQL(count);
        long tableSize = voltExecuteSQL()[0].asScalarLong();

        if (rowsToDelete > tableSize)
            rowsToDelete = (int) tableSize;

        if (rowsToDelete == 0)
            return null;

        voltQueueSQL(countByDate, rowsToDelete);
        VoltTable rt1 = voltExecuteSQL()[0];
        assert(rt1.getRowCount() == rowsToDelete);

        VoltTableRow row = rt1.fetchRow(rt1.getRowCount() - 1);
        TimestampType splitTime = row.getTimestampAsTimestamp(0);

        voltQueueSQL(delete, splitTime);
        VoltTable rt2 = voltExecuteSQL()[0];

        return new VoltTable[] { rt1, rt2 };
    }
}