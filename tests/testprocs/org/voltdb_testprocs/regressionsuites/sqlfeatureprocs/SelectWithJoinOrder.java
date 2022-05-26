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

package org.voltdb_testprocs.regressionsuites.sqlfeatureprocs;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class SelectWithJoinOrder extends VoltProcedure {

    public final SQLStmt selectWithRightJoinOrder = new SQLStmt("SELECT * FROM WIDE, T1, T2 WHERE T2.ID = T1.ID", "T1,T2,WIDE");

    public final SQLStmt selectWithWrongJoinOrder = new SQLStmt("SELECT * FROM WIDE, T1, T2 WHERE T2.ID = T1.ID", "WIDE,T1,T2");

    public VoltTable[] run(int order) {
        if (order == 0) {
            voltQueueSQL(selectWithRightJoinOrder);
        } else {
            voltQueueSQL(selectWithWrongJoinOrder);
        }
        return voltExecuteSQL();
    }

}
