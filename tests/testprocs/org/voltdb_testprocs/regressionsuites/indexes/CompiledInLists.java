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

package org.voltdb_testprocs.regressionsuites.indexes;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

@ProcInfo (
    singlePartition = false
)

/**
 * Test that selecting using a MultiMap multi column integer index works with GTE. This
 * means a where clause where the key is greater then or equal to the provided key.
 */
public class CompiledInLists extends VoltProcedure {

    public final SQLStmt P3with5DESCs =
        new SQLStmt("select * from P3 T where T.DESC IN (?, ?, ?, ?, ?)" +
                    " and T.NUM IN (100, 200, 300, 400, 500)");

    public final SQLStmt R3with5DESCs =
        new SQLStmt("select * from R3 T where T.DESC IN (?, ?, ?, ?, ?)" +
                    " and T.NUM IN (100, 200, 300, 400, 500)");

    public final SQLStmt P3withDESCs =
            new SQLStmt("select * from P3 T where T.DESC IN ?" +
                        " and T.NUM IN (100, 200, 300, 400, 500)");


    public final SQLStmt P3with5NUMs =
        new SQLStmt("select * from P3 T where T.DESC IN ('a', 'b', 'c', 'g', " +
                    "'this here is a longish string to force a permanent object allocation'" +
                    ")" +
                    " and T.NUM IN (?, ?, ?, ?, ?)");

    public final SQLStmt R3with5NUMs =
        new SQLStmt("select * from R3 T where T.DESC IN ('a', 'b', 'c', 'g', " +
                    "'this here is a longish string to force a permanent object allocation'" +
                    ")" +
                    " and T.NUM IN (?, ?, ?, ?, ?)");

    public final SQLStmt P3withNUMs =
            new SQLStmt("select * from P3 T where T.DESC IN ('a', 'b', 'c', 'g', " +
                        "'this here is a longish string to force a permanent object allocation'" +
                        ")" +
                        " and T.NUM IN ?");


    public VoltTable[] run(String[] fiveDESCs, int[] fiveNUMs, int goEasyOnHSQL)
    {
        voltQueueSQL(P3with5DESCs, fiveDESCs[0], fiveDESCs[1], fiveDESCs[2], fiveDESCs[3], fiveDESCs[4]);
        voltQueueSQL(R3with5DESCs, fiveDESCs[0], fiveDESCs[1], fiveDESCs[2], fiveDESCs[3], fiveDESCs[4]);
        // Without teaching the HSQL backend the "IN ?" trick, we need to simulate this part of the test
        // using an effectively equivalent statement to get a consistent result.
        if (goEasyOnHSQL != 0) {
            voltQueueSQL(P3with5DESCs, fiveDESCs[0], fiveDESCs[1], fiveDESCs[2], fiveDESCs[3], fiveDESCs[4]);
        }
        else {
            voltQueueSQL(P3withDESCs, (Object)fiveDESCs);
        }
        voltQueueSQL(P3with5NUMs, fiveNUMs[0], fiveNUMs[1], fiveNUMs[2], fiveNUMs[3], fiveNUMs[4]);
        voltQueueSQL(R3with5NUMs, fiveNUMs[0], fiveNUMs[1], fiveNUMs[2], fiveNUMs[3], fiveNUMs[4]);
        // Without teaching the HSQL backend the "IN ?" trick, we need to simulate this part of the test
        // using an effectively equivalent statement to get a consistent result.
        if (goEasyOnHSQL != 0) {
            voltQueueSQL(P3with5NUMs, fiveNUMs[0], fiveNUMs[1], fiveNUMs[2], fiveNUMs[3], fiveNUMs[4]);
        }
        else {
            voltQueueSQL(P3withNUMs, fiveNUMs);
        }
        return voltExecuteSQL(true);
    }
}
