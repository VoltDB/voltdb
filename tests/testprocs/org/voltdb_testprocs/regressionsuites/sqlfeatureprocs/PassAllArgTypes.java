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

package org.voltdb_testprocs.regressionsuites.sqlfeatureprocs;

import java.io.UnsupportedEncodingException;

import org.voltdb.ProcInfo;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.types.TimestampType;

@ProcInfo (
    partitionInfo = "ORDER_LINE.OL_W_ID: 0",
    singlePartition = true
)
public class PassAllArgTypes extends VoltProcedure {

    public final static long MILLISECONDS_SINCE_EPOCH_TEST_VALUE = 1001001001001L;

    public VoltTable[] run(byte b, byte bArray[], short s, short sArray[],
            int i, int iArray[], long l, long lArray[], String str, byte bString[],
            TimestampType tst, java.util.Date utild, java.sql.Date sqld, java.sql.Timestamp ts) throws VoltAbortException {
        if (b != 100) throw new VoltAbortException();
        if (bArray[0] != 100 || bArray[1] != 101 || bArray[2] != 102) throw new VoltAbortException();
        if (s != 32000) throw new VoltAbortException();
        if (sArray[0] != 32000 || sArray[1] != 32001 || sArray[2] != 32002) throw new VoltAbortException();
        if (i != 2147483640) throw new VoltAbortException();
        if (iArray[0] != 2147483640 || iArray[1] != 2147483641 || iArray[2] != 2147483642)
            throw new VoltAbortException();
        if (l != Long.MAX_VALUE - 10) throw new VoltAbortException();
        if (lArray[0] != Long.MAX_VALUE - 10 || lArray[1] != Long.MAX_VALUE - 9 || lArray[2] != Long.MAX_VALUE - 8)
            throw new VoltAbortException();
        if (!str.equals("foo")) throw new VoltAbortException();
        try {
            if (!(new String(bString, "UTF-8").equals("bar"))) throw new VoltAbortException();
        } catch (UnsupportedEncodingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        if (!tst.equals(new TimestampType(MILLISECONDS_SINCE_EPOCH_TEST_VALUE*1000))) {
            throw new VoltAbortException();
        }
        if (!utild.equals(new java.util.Date(MILLISECONDS_SINCE_EPOCH_TEST_VALUE))) {
            throw new VoltAbortException();
        }
        if (!sqld.equals(new java.sql.Date(MILLISECONDS_SINCE_EPOCH_TEST_VALUE))) {
            throw new VoltAbortException();
        }
        if (!ts.equals(new java.sql.Timestamp(MILLISECONDS_SINCE_EPOCH_TEST_VALUE))) {
            throw new VoltAbortException();
        }
        VoltTable result0 = new VoltTable(
                new VoltTable.ColumnInfo("b", VoltType.typeFromClass(byte.class)),
                new VoltTable.ColumnInfo("bArray", VoltType.typeFromClass(byte[].class)),
                new VoltTable.ColumnInfo("s", VoltType.typeFromClass(short.class)),
                // new VoltTable.ColumnInfo("sArray", VoltType.typeFromClass(short[].class)),
                new VoltTable.ColumnInfo("i", VoltType.typeFromClass(int.class)),
                // new VoltTable.ColumnInfo("iArray", VoltType.typeFromClass(int[].class)),
                new VoltTable.ColumnInfo("l", VoltType.typeFromClass(long.class)),
                // new VoltTable.ColumnInfo("lArray", VoltType.typeFromClass(long[].class)),
                new VoltTable.ColumnInfo("str", VoltType.typeFromClass(String.class)),
                new VoltTable.ColumnInfo("bString", VoltType.typeFromClass(byte[].class)),
                new VoltTable.ColumnInfo("tst", VoltType.typeFromClass(TimestampType.class)),
                new VoltTable.ColumnInfo("utild", VoltType.typeFromClass(java.util.Date.class)),
                new VoltTable.ColumnInfo("sqld", VoltType.typeFromClass(java.sql.Date.class)),
                new VoltTable.ColumnInfo("ts", VoltType.typeFromClass(java.sql.Timestamp.class))
                );
        result0.addRow(b, bArray, s, /*sArray,*/ i, /*iArray,*/ l, /*lArray,*/ str, bString, tst, utild, sqld, ts);
        VoltTable result1 = new VoltTable(new VoltTable.ColumnInfo("sArray", VoltType.typeFromClass(short.class)));
        for (int j = 0; j < sArray.length; j++) {
            result1.addRow(sArray[j]);
        }
        VoltTable result2 = new VoltTable(new VoltTable.ColumnInfo("iArray", VoltType.typeFromClass(int.class)));
        for (int j = 0; j < iArray.length; j++) {
            result2.addRow(iArray[j]);
        }
        VoltTable result3 = new VoltTable(new VoltTable.ColumnInfo("lArray", VoltType.typeFromClass(long.class)));
        for (int j = 0; j < lArray.length; j++) {
            result3.addRow(lArray[j]);
        }

    return new VoltTable[]{ result0, result1, result2, result3 };
    }
}
