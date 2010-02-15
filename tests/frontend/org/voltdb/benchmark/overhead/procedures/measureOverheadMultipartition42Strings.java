/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

import org.voltdb.*;

/** This procedure is (needlessly) multipartition, used to test efficiency
    of multipartition initiation. */

@ProcInfo (
    singlePartition = false
)
public class measureOverheadMultipartition42Strings extends VoltProcedure {

    public final SQLStmt getID = new SQLStmt("SELECT NO_O_ITEM FROM NEWORDER WHERE NO_O_ID = ?;");

    public VoltTable[] run(long no_o_id, String arg2, String arg3, String arg4,String arg5,
            String arg6, String arg7, String arg8, String arg9, String arg10,
            String arg11, String arg12, String arg13, String arg14, String arg15,
            String arg16, String arg17, String arg18, String arg19, String arg20,
            String arg21, String arg22, String arg23, String arg24, String arg25,
            String arg26, String arg27, String arg28, String arg29, String arg30,
            String arg31, String arg32, String arg33, String arg34, String arg35,
            String arg36, String arg37, String arg38, String arg39, String arg40,
            String arg41, String arg42) {

        voltQueueSQL(getID, no_o_id);
        return voltExecuteSQL(true);
    }
}
