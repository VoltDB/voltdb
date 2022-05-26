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

package org.voltdb_testprocs.regressionsuites.exportprocs;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class ExportInsertFromTableSelectMP extends VoltProcedure {

    // This statement currently fails to compile. So it is commented out for now. See ENG-16980
    //private final SQLStmt i_insert_select_repl = new SQLStmt
    //("INSERT INTO S_ALLOW_NULLS_REPL SELECT * FROM NO_NULLS_REPL;");

    private final SQLStmt i_insert_select_part = new SQLStmt
    ("INSERT INTO S_ALLOW_NULLS SELECT * FROM NO_NULLS_REPL;");

    public VoltTable[] run(
            String targetStream,
            int pkey
            )
    {
        if (targetStream.equals("S_ALLOW_NULLS_REPL")) {
            //voltQueueSQL(i_insert_select_repl);
        }
        else if (targetStream.equals("S_ALLOW_NULLS")) {
            voltQueueSQL(i_insert_select_part);
        }
        else {
            throw new RuntimeException("Don't call this.");
        }
        return voltExecuteSQL();
    }
}
