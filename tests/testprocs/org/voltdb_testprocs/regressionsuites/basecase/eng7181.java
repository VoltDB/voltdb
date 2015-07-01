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

package org.voltdb_testprocs.regressionsuites.basecase;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;

@ProcInfo(
           singlePartition = false)

public class eng7181 extends VoltProcedure
{
    public final SQLStmt insertR1 =
        new SQLStmt("insert into r1 values (?, ?, ?, ?);");
    public final SQLStmt countR1 =
        new SQLStmt("select count(*) from r1;");
    public final SQLStmt ConstraintViolationUpdate =
        new SQLStmt("update r1 set b1 = 1;");

    public Long run(int val, int die)
    {
        voltQueueSQL(countR1);
        voltQueueSQL(insertR1, val, val, val, Integer.toHexString(val));
        if (die == 1) {
            voltQueueSQL(insertR1, val, val, val, Integer.toHexString(val));
        }
        Long result = voltExecuteSQL(true)[0].asScalarLong();
        return result;
    }
}
