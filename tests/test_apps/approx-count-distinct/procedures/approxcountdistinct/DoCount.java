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
package approxcountdistinct;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

public class DoCount extends VoltProcedure {

    final SQLStmt countApprox = new SQLStmt("select approx_count_distinct(value) from data;");
    final SQLStmt countExact = new SQLStmt("select count(distinct value) from data;");

    public VoltTable run() {

        for (int i = 0; i < 10; ++i) {
            voltQueueSQL(countApprox);
        }
        voltExecuteSQL(); // warm up

        for (int i = 0; i < 10; ++i) {
            voltQueueSQL(countApprox);
        }
        long startTime = System.currentTimeMillis();
        VoltTable vt = voltExecuteSQL()[0];
        long approxMillis = System.currentTimeMillis() - startTime;
        vt.advanceRow();
        long approxAnswer = vt.getLong(0);

        for (int i = 0; i < 10; ++i) {
            voltQueueSQL(countExact);
        }
        voltExecuteSQL(); // warm up

        for (int i = 0; i < 10; ++i) {
            voltQueueSQL(countExact);
        }

        startTime = System.currentTimeMillis();
        vt = voltExecuteSQL()[0];
        long exactMillis = System.currentTimeMillis() - startTime;
        vt.advanceRow();
        long exactAnswer = vt.getLong(0);

        VoltTable.ColumnInfo[] cols = new VoltTable.ColumnInfo[] {
                new VoltTable.ColumnInfo("approx answer", VoltType.BIGINT),
                new VoltTable.ColumnInfo("approx elapsed millis", VoltType.FLOAT),
                new VoltTable.ColumnInfo("exact answer", VoltType.BIGINT),
                new VoltTable.ColumnInfo("exact elapsed millis", VoltType.FLOAT)
        };

        VoltTable retTable = new VoltTable(cols);
        retTable.addRow(approxAnswer, approxMillis / 10.0, exactAnswer, exactMillis / 10.0);

        return retTable;
    }
}
