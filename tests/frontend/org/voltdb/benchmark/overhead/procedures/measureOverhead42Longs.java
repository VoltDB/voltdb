/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

package org.voltdb.benchmark.overhead.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

@ProcInfo (
    partitionInfo = "NEWORDER.NO_O_ID: 0",
    singlePartition = true
)
public class measureOverhead42Longs extends VoltProcedure {

    public final SQLStmt getID = new SQLStmt("SELECT NO_O_ITEM FROM NEWORDER WHERE NO_O_ID = ?;");

    public VoltTable[] run(int no_o_id, long arg2, long arg3, long arg4,long arg5,
                           long arg6, long arg7, long arg8, long arg9, long arg10,
                           long arg11, long arg12, long arg13, long arg14, long arg15,
                           long arg16, long arg17, long arg18, long arg19, long arg20,
                           long arg21, long arg22, long arg23, long arg24, long arg25,
                           long arg26, long arg27, long arg28, long arg29, long arg30,
                           long arg31, long arg32, long arg33, long arg34, long arg35,
                           long arg36, long arg37, long arg38, long arg39, long arg40,
                           long arg41, long arg42
                           ) {

        voltQueueSQL(getID, no_o_id);
        return voltExecuteSQL(true);
    }
}
