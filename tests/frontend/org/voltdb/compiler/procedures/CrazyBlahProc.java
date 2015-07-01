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

package org.voltdb.compiler.procedures;

import java.math.BigDecimal;

import org.voltdb.ProcInfo;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.types.TimestampType;

@ProcInfo (
    partitionInfo = "blah.ival: 0",
    singlePartition = true
)
public class CrazyBlahProc extends VoltProcedure {

    public VoltTable[] run(long ival, short ival2, double[] dvals, VoltTable tval, BigDecimal bd, BigDecimal[] decvals, TimestampType timeval) {
        VoltTable[] retval = new VoltTable[4];

        retval[0] = new VoltTable(new VoltTable.ColumnInfo("foo", VoltType.BIGINT));
        retval[0].addRow(1);

        retval[1] = new VoltTable(new VoltTable.ColumnInfo("foo", VoltType.STRING));
        // this ought to work
        retval[1].addRow("你好");

        retval[2] = new VoltTable(new VoltTable.ColumnInfo("foo", VoltType.BIGINT));
        retval[3] = new VoltTable(new VoltTable.ColumnInfo("foo", VoltType.TIMESTAMP));
        retval[3].addRow(timeval);
        return retval;
    }
}
