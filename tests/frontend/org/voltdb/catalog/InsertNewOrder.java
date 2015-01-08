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

package org.voltdb.catalog;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;

/**
 * This class is the same as org.voltdb.benchmark.tpcc.procedures.InsertHistory
 * except for this comment, the package name and some dummy code in the middle.
 * The purpose is to test transitioning from the official InsertHistory to this
 * one using a catalog change.
 *
 */
@ProcInfo (
    partitionInfo = "HISTORY.H_W_ID: 4",
    singlePartition = true
)
public class InsertNewOrder extends VoltProcedure {

    public final SQLStmt insert = new SQLStmt("INSERT INTO HISTORY VALUES (?, ?, ?, ?, ?, ?, ?, ?);");

    public long run(long h_c_id, long h_c_d_id, long h_c_w_id, long h_d_id, short h_w_id,
            long h_date, double h_amount, String h_data) {

        // some dummy code
        @SuppressWarnings("unused")
        int x = 5;

        voltQueueSQL(insert, h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data);
        voltExecuteSQL();

        return 1776;
    }
}
