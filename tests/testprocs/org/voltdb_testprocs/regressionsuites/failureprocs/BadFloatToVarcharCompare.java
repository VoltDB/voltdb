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

package org.voltdb_testprocs.regressionsuites.failureprocs;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;

public class BadFloatToVarcharCompare extends VoltProcedure {

    public final SQLStmt insertSQL =
        new SQLStmt("insert into BAD_COMPARES (ID, FLOATVAL) values (?, ?);");

    public final SQLStmt selectSQL =
        new SQLStmt("select ID from BAD_COMPARES where FLOATVAL = ?;");

    public long run(int id) throws VoltAbortException {
        long numRows = 0;

        voltQueueSQL(insertSQL, 1, 1.1);
        voltQueueSQL(insertSQL, 2, 2.2);
        voltQueueSQL(insertSQL, 3, 3.3);

        voltExecuteSQL();

        // comparing FLOATVAL to string causes ENG-800
        voltQueueSQL(selectSQL, "onepointone");

        VoltTable results1[] = voltExecuteSQL(true);

        if (results1[0].getRowCount() == 1)
        {
            VoltTableRow row1 = results1[0].fetchRow(0);
            numRows = row1.getLong(0);
        }

        return numRows;
    }
}
