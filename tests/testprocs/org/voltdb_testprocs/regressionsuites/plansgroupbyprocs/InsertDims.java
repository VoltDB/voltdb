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

package org.voltdb_testprocs.regressionsuites.plansgroupbyprocs;

import org.voltdb.*;

public class InsertDims extends VoltProcedure {

    public final SQLStmt loadD1 = new SQLStmt
     ("INSERT INTO D1 VALUES (?, ?); ");

    public final SQLStmt loadD2 = new SQLStmt
    ("INSERT INTO D2 VALUES (?, ?); ");

    public final SQLStmt loadD3 = new SQLStmt
    ("INSERT INTO D3 VALUES (?, ?); ");

    public VoltTable[] run() {

        // d1 had pkeys 0-10
        for (int i=0; i < 10; ++i) {
            voltQueueSQL(loadD1, i, "D1_" + String.valueOf(i));
        }
        voltExecuteSQL();


        // d2 has pkeys 0 to 50
        for (int i=0; i < 50; ++i) {
            voltQueueSQL(loadD2, i, "D2_" + String.valueOf(i));
        }
        voltExecuteSQL();


        // d3 has pkeys 0 to 100
        for (int i=0; i < 100; ++i) {
            voltQueueSQL(loadD3, i, "D3_" + String.valueOf(i));
        }
        voltExecuteSQL(true);

        VoltTable[] vta = new VoltTable[1];
        vta[0] = new VoltTable(new VoltTable.ColumnInfo("RETVAL", VoltType.INTEGER));
        vta[0].addRow(1);
        return vta;
    }

}
