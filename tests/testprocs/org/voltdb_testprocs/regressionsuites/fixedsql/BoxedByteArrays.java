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

@ProcInfo (
    singlePartition = false
)
public class BoxedByteArrays extends VoltProcedure {

//    CREATE TABLE ENG_539 (
//            ID      INTEGER NOT NULL,
//            VARBIN  VARBINARY(3),
//            BIG     BIGINT,
//            PRIMARY KEY (ID)
//          );

    public final SQLStmt anInsert = new SQLStmt
            ("INSERT INTO ENG_539 VALUES (?, ?, ?);");

    public final SQLStmt aBIGSelect = new SQLStmt
            ("SELECT * FROM ENG_539 WHERE BIG IN ?;");

    public final SQLStmt aINTSelect = new SQLStmt
            ("SELECT * FROM ENG_539 WHERE ID IN ?;");

    public final SQLStmt aBYTESelect = new SQLStmt
            ("SELECT * FROM ENG_539 WHERE VARBIN IN ?;");

    public VoltTable[] run(String inpType, Integer id, byte[] varbin,
            byte[][] varbinArr, Long[] lngArr, Integer[] intArr, String inpString) {

        if (inpType.equals("VARBIN")) {
            voltQueueSQL(anInsert, id, varbin, null);
        } else if (inpType.equals("STR")) {
            voltQueueSQL(anInsert, id, inpString, null);
        } else if (inpType.equals("DSTR")) {
            voltQueueSQL(anInsert, id, null, inpString);
        } else if (inpType.equals("BIGD")) {
            voltQueueSQL(anInsert, id, null, id);
        } else if (inpType.equals("LNGARR")) {
            voltQueueSQL(aBIGSelect, lngArr);
        }  else if (inpType.equals("INTARR")) {
            voltQueueSQL(aINTSelect, intArr);
        }
        else if (inpType.equals("SEL_VARBIN")) {
            voltQueueSQL(aBYTESelect, varbinArr);
        }
//        else if (inpType.equals("SEL_STR")) {
//            voltQueueSQL(aSelect, strArr);
//        }
        return voltExecuteSQL();
    }
}
