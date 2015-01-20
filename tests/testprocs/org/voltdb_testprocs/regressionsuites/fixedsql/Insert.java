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

package org.voltdb_testprocs.regressionsuites.fixedsql;

import org.voltdb.*;

@ProcInfo (
    singlePartition = false
)
public class Insert extends VoltProcedure {

    public final SQLStmt i_p1 = new SQLStmt
      ("INSERT INTO P1 VALUES (?, ?, ?, ?);");

    public final SQLStmt i_r1 = new SQLStmt
        ("INSERT INTO R1 VALUES (?, ?, ?, ?);");

    public final SQLStmt i_p2 = new SQLStmt
    ("INSERT INTO P2 VALUES (?, ?, ?, ?);");

    public final SQLStmt i_r2 = new SQLStmt
      ("INSERT INTO R2 VALUES (?, ?, ?, ?);");

    public VoltTable[] run(String tablename, long id, String desc, long num,
                    double ratio)
    {
        if (tablename.equals("P1"))
        {
            voltQueueSQL(i_p1, (int)id, desc, (int)num, ratio);
        }
        else if (tablename.equals("R1"))
        {
            voltQueueSQL(i_r1, (int)id, desc, (int)num, ratio);
        }
        else if (tablename.equals("P2"))
        {
            voltQueueSQL(i_p2, (int)id, desc, (int)num, ratio);
        }
        else if (tablename.equals("R2"))
        {
            voltQueueSQL(i_r2, (int)id, desc, (int)num, ratio);
        }


        return voltExecuteSQL();
    }
}
