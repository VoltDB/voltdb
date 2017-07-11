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
    public final SQLStmt aSTRSelect = new SQLStmt("SELECT * FROM ENG_12105 WHERE VCHAR IN ?;");

    public VoltTable[] run(String inpType, short[] shortArr, int[] intArr, long[] lngArr,
              double[] dblArr, BigDecimal[] bigdArr, String[] strArr) {

        if (inpType.equals("SHORTS")) {
            voltQueueSQL(aSHORTSelect, shortArr);
        } else if (inpType.equals("INTS")) {
            voltQueueSQL(aINTSelect, intArr);
        } else if (inpType.equals("LNGS")) {
            voltQueueSQL(aLNGSelect, lngArr);
        } else if (inpType.equals("DBLS")) {
            voltQueueSQL(aDBLSelect, dblArr);
        } else if (inpType.equals("BIGDS")) {
            voltQueueSQL(aBIGDSelect, bigdArr);
        } else if (inpType.equals("STRS")) {
            voltQueueSQL(aSTRSelect, strArr);
        }

        return voltExecuteSQL();
    }
}
