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

package org.voltdb_testprocs.regressionsuites.fixedsql;

import java.math.BigDecimal;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class InPrimitiveArrays extends VoltProcedure {

    public final SQLStmt aBYTESelect = new SQLStmt("SELECT * FROM ENG_12105 WHERE VARBIN IN ?;");
    public final SQLStmt aSHORTSelect = new SQLStmt("SELECT * FROM ENG_12105 WHERE SMALL IN ?;");
    public final SQLStmt aINTSelect = new SQLStmt("SELECT * FROM ENG_12105 WHERE ID IN ?;");
    public final SQLStmt aLNGSelect = new SQLStmt("SELECT * FROM ENG_12105 WHERE BIG IN ?;");
    public final SQLStmt aDBLSelect = new SQLStmt("SELECT * FROM ENG_12105 WHERE NUM IN ?;");
    public final SQLStmt aBIGDSelect = new SQLStmt("SELECT * FROM ENG_12105 WHERE DEC IN ?;");
    public final SQLStmt aSTRSelect = new SQLStmt("SELECT * FROM ENG_12105 WHERE VCHAR IN ?;");
    public final SQLStmt anInsert = new SQLStmt("INSERT INTO ENG_12105 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");

    public VoltTable[] run(String inpType, byte[][] byteArr, short[] shortArr, int[] intArr,
            long[] lngArr, double[] dblArr, BigDecimal[] bigdArr, String[] strArr, byte[] insByteArr) {

        switch (inpType) {
            case "BYTES":
                voltQueueSQL(aBYTESelect, (Object)byteArr);
                break;
            case "SHORTS":
                voltQueueSQL(aSHORTSelect, shortArr);
                break;
            case "INTS":
                voltQueueSQL(aINTSelect, intArr);
                break;
            case "LNGS":
                voltQueueSQL(aLNGSelect, lngArr);
                break;
            case "DBLS":
                voltQueueSQL(aDBLSelect, dblArr);
                break;
            case "BIGDS":
                voltQueueSQL(aBIGDSelect, (Object)bigdArr);
                break;
            case "STRS":
                voltQueueSQL(aSTRSelect, (Object)strArr);
                break;
            case "LNGINT":
                voltQueueSQL(aINTSelect, lngArr);
                break;
            case "LNGDBL":
                voltQueueSQL(aDBLSelect, lngArr);
                break;
            case "LNGBIGD":
                voltQueueSQL(aBIGDSelect, lngArr);
                break;
            case "INSBYTES":
                voltQueueSQL(anInsert, 1, null, null, null, null, null, null, null, null, null, null, insByteArr);
                break;
        }

        return voltExecuteSQL();
    }
}
