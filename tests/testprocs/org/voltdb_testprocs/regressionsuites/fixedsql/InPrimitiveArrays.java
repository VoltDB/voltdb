/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

import org.voltdb.*;
import java.math.BigDecimal;
//import org.apache.commons.lang.ArrayUtils;

@ProcInfo(singlePartition = false)
public class InPrimitiveArrays extends VoltProcedure {

    public final SQLStmt aSHORTSelect = new SQLStmt("SELECT * FROM ENG_12105 WHERE SMALL IN ?;");
    public final SQLStmt aINTSelect = new SQLStmt("SELECT * FROM ENG_12105 WHERE ID IN ?;");
    public final SQLStmt aLNGSelect = new SQLStmt("SELECT * FROM ENG_12105 WHERE BIG IN ?;");
    public final SQLStmt aDBLSelect = new SQLStmt("SELECT * FROM ENG_12105 WHERE NUM IN ?;");
    public final SQLStmt aBIGDSelect = new SQLStmt("SELECT * FROM ENG_12105 WHERE DEC IN ?;");

    public VoltTable[] run(String inpType, String[] inpArr) {

        if (inpType.equals("SHORTS")) {

            short[] shortArr = new short[inpArr.length];
            int i = 0;
            for (String s : inpArr) {
                shortArr[i++] = Short.parseShort(s);
            }
            voltQueueSQL(aSHORTSelect, shortArr);

        } else if (inpType.equals("INTS")) {

            int[] intArr = new int[inpArr.length];
            int i = 0;
            for (String s : inpArr) {
                intArr[i++] = Integer.parseInt(s);
            }
            voltQueueSQL(aINTSelect, intArr);

        } else if (inpType.equals("LNGS")) {

            long[] lngArr = new long[inpArr.length];
            int i = 0;
            for (String s : inpArr) {
                lngArr[i++] = Long.parseLong(s);
            }
            voltQueueSQL(aLNGSelect, lngArr);

        } else if (inpType.equals("DBLS")) {

            double[] dblArr = new double[inpArr.length];
            int i = 0;
            for (String s : inpArr) {
                dblArr[i++] = Double.parseDouble(s);
            }
            voltQueueSQL(aDBLSelect, dblArr);

        } else if (inpType.equals("BIGDS")) {

            BigDecimal[] bigdArr = new BigDecimal[inpArr.length];
            int i = 0;
            for (String s : inpArr) {
                bigdArr[i++] = new BigDecimal(s);
            }
            voltQueueSQL(aBIGDSelect, bigdArr);

        }

        return voltExecuteSQL();
    }
}
