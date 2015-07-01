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

package org.voltdb_testprocs.regressionsuites.sqltypesprocs;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

@ProcInfo (
    partitionInfo = "NO_NULLS.PKEY: 1",
    singlePartition = true
)
public class Select extends VoltProcedure {

    public final SQLStmt i_no_nulls = new SQLStmt
    ("SELECT * FROM NO_NULLS WHERE PKEY = ?;");

    public final SQLStmt i_allow_nulls = new SQLStmt
    ("SELECT * FROM ALLOW_NULLS WHERE PKEY = ?;");

    public final SQLStmt i_with_defaults = new SQLStmt
    ("SELECT * FROM WITH_DEFAULTS WHERE PKEY = ?;");

    public final SQLStmt i_with_null_defaults = new SQLStmt
    ("SELECT * FROM WITH_NULL_DEFAULTS WHERE PKEY = ?;");

    public final SQLStmt i_expressions_with_nulls = new SQLStmt
    ("SELECT * FROM EXPRESSIONS_WITH_NULLS WHERE PKEY = ?;");

    public final SQLStmt i_expressions_no_nulls = new SQLStmt
    ("SELECT * FROM EXPRESSIONS_NO_NULLS WHERE PKEY = ?;");

    public final SQLStmt i_jumbo_row = new SQLStmt
    ("SELECT * FROM JUMBO_ROW WHERE PKEY = ?;");

    public VoltTable[] run(String tablename, int pkey) {

        if (tablename.equals("NO_NULLS")) {
            voltQueueSQL(i_no_nulls, pkey);
        }
        else if (tablename.equals("ALLOW_NULLS")) {
            voltQueueSQL(i_allow_nulls, pkey);
        }
        else if (tablename.equals("WITH_DEFAULTS")) {
            voltQueueSQL(i_with_defaults, pkey);
        }
        else if (tablename.equals("WITH_NULL_DEFAULTS")) {
            voltQueueSQL(i_with_null_defaults, pkey);
        }
        else if (tablename.equals("EXPRESSIONS_WITH_NULLS")) {
            voltQueueSQL(i_expressions_with_nulls, pkey);
        }
        else if (tablename.equals("EXPRESSIONS_NO_NULLS")) {
            voltQueueSQL(i_expressions_no_nulls, pkey);
        } else if (tablename.equals("JUMBO_ROW")) {
            voltQueueSQL(i_jumbo_row, 0);
        }

        return voltExecuteSQL();
    }

}
