/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

package org.voltdb_testprocs.regressionsuites.matviewprocs;

import org.voltdb.*;

@ProcInfo (
    partitionInfo = "OVERFLOWTEST.COL_1: 1",
    singlePartition = true
)
public class OverflowTest extends VoltProcedure {

    public final SQLStmt insert = new SQLStmt("INSERT INTO OVERFLOWTEST VALUES (?, 0, ?, ?);");

    public final SQLStmt select = new SQLStmt("SELECT * FROM V_OVERFLOWTEST;");

    public VoltTable[] run(int action, int col1Val, long invocationIndex) {
        switch (action) {
        case 0:
            voltQueueSQL( insert, invocationIndex, 0, Long.MAX_VALUE - 10);
            break;
        case 1:
            voltQueueSQL( select);
            break;
        case 2:
            voltQueueSQL( insert, invocationIndex, Integer.MAX_VALUE, 1);
            break;
        }
        return voltExecuteSQL();
    }
}
