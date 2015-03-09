/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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
/* Copyright (C) 2008
 * Michael McCanna
 * Massachusetts Institute of Technology
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
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.benchmark.tpcc.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

//Notes on Stored Procedure:
//return VoltTable[] has 1 element:
//1) stock_count, represented as a 1x1 table representing a Long.

@ProcInfo (
    partitionInfo = "WAREHOUSE.W_ID: 0",
    singlePartition = true
)
public class slev extends VoltProcedure {

    public final SQLStmt GetOId = new SQLStmt("SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?;");

    public final SQLStmt GetStockCount = new SQLStmt(
        "SELECT COUNT(DISTINCT(OL_I_ID)) FROM ORDER_LINE, STOCK " +
        "WHERE OL_W_ID = ? AND " +
        "OL_D_ID = ? AND " +
        "OL_O_ID < ? AND " +
        "OL_O_ID >= ? AND " +
        "S_W_ID = OL_W_ID AND " +
        "S_I_ID = OL_I_ID AND " +
        "S_QUANTITY < ?;");

    public VoltTable[] run(short w_id, byte d_id, int threshold) {

        voltQueueSQL(GetOId, w_id, d_id);
        final VoltTable result = voltExecuteSQL()[0];

//      // handle the case where the first part fails (un-elegantly)
//      long o_id;
//      if (result.getRowCount() > 0)
//          o_id = result.asScalarLong();
//      else
//          o_id = 0;
        final long o_id = result.asScalarLong(); //if invalid (i.e. no matching o_id), we expect a fail here.

        voltQueueSQL(GetStockCount, w_id, d_id, o_id, o_id - 20, threshold);
        //return assumes that o_id is a temporary variable, and that stock_count is a necessarily returned variable.
        return voltExecuteSQL();
    }
}
