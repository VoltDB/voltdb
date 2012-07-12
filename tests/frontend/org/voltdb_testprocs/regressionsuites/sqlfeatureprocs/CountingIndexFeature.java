/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

package org.voltdb_testprocs.regressionsuites.sqlfeatureprocs;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

@ProcInfo (
    singlePartition = true,
    partitionInfo = "T1.ID: 0"
)
public class CountingIndexFeature extends VoltProcedure {

    //public final SQLStmt countstar1 = new SQLStmt("SELECT COUNT(*) FROM T1 WHERE POINTS > ?");
    public final SQLStmt countstar2 = new SQLStmt("SELECT COUNT(*) FROM T1 WHERE POINTS > ? AND POINTS < ?");
    //public final SQLStmt countstar3 = new SQLStmt("SELECT COUNT(*) FROM T2 WHERE POINTS > ? AND ID = 2");
    //public final SQLStmt countstar4 = new SQLStmt("SELECT COUNT(*) FROM T3 WHERE POINTS > ? AND POINTS < ?");
    //public final SQLStmt countstar5 = new SQLStmt("SELECT COUNT(*) FROM T3 WHERE POINTS > ? AND NAME = ?");

    public VoltTable[] run(int p1, int p2) {
        //voltQueueSQL(countstar1, p1);
        voltQueueSQL(countstar2, p1, p2);
        return voltExecuteSQL();
    }

}
