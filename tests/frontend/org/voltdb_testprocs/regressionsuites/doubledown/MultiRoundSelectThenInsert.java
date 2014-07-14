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

package org.voltdb_testprocs.regressionsuites.doubledown;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

@ProcInfo(singlePartition = false)

public class MultiRoundSelectThenInsert extends VoltProcedure
{
    public final SQLStmt selectAll = new SQLStmt("SELECT B1, 'x' || A2 FROM P1;");
    public final SQLStmt insertNext = new SQLStmt("INSERT INTO P1 (B1, A2) VALUES( ?, ? );");

    public long run(int iterations) {
        long added = 0;
        while (iterations-- > 0) {
            voltQueueSQL(selectAll);
            VoltTable result = voltExecuteSQL()[0];
            while (result.advanceRow()) {
                long b1 = result.getLong(0);
                String a2 = result.getString(1);
                voltQueueSQL(insertNext, b1, a2);
            }
            VoltTable[] results = voltExecuteSQL(iterations <= 0);
            for (VoltTable result2 : results) {
                added += result2.asScalarLong();
            }
        }
        return added;
    }
}
