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

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class BoxedByteArrays extends VoltProcedure {
    public final SQLStmt anInsert = new SQLStmt
            ("INSERT INTO ENG_539 VALUES (?, ?, ?);");

    public final SQLStmt aBIGSelect = new SQLStmt
            ("SELECT * FROM ENG_539 WHERE BIG IN ?;");

    public final SQLStmt aINTSelect = new SQLStmt
            ("SELECT * FROM ENG_539 WHERE ID IN ?;");

    public final SQLStmt aBYTESelect = new SQLStmt
            ("SELECT * FROM ENG_539 WHERE VARBIN IN ?;");

    // Byte[] is not currently being accepted as an input array type for Java SP
    public VoltTable[] run(String inpType, Integer id, byte[] varbin,
            byte[][] varbinArr, Long[] lngArr, Integer[] intArr, String inpString) {

        switch(inpType) {
            case "VARBIN":
                voltQueueSQL(anInsert, id, varbin, null);
                break;
            case "STR":
                voltQueueSQL(anInsert, id, inpString, null);
                break;
            case "DSTR":
                voltQueueSQL(anInsert, id, null, inpString);
                break;
            case "BIGD":
                voltQueueSQL(anInsert, id, null, id);
                break;
            case "LNGARR":
                voltQueueSQL(aBIGSelect, (Object)lngArr);
                break;
            case "INTARR":
                voltQueueSQL(aINTSelect, (Object)intArr);
                break;
            case "SEL_VARBIN":
                voltQueueSQL(aBYTESelect, (Object)varbinArr);
                break;
        }

        return voltExecuteSQL();
    }
}
