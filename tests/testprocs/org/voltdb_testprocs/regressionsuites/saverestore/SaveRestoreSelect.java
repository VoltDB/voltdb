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

package org.voltdb_testprocs.regressionsuites.saverestore;

import org.voltdb.*;

@ProcInfo (
    singlePartition = false
)
public class SaveRestoreSelect extends VoltProcedure {

    public final SQLStmt selectAllPartitioned =
        new SQLStmt("SELECT * FROM PARTITION_TESTER ORDER BY PT_ID ASC;");

    public final SQLStmt selectAllReplicated =
        new SQLStmt("SELECT * FROM REPLICATED_TESTER ORDER BY RT_ID ASC;");

    public final SQLStmt selectAllBecomesMaterialized =
        new SQLStmt("SELECT * FROM BECOMES_MATERIALIZED ORDER BY PT_INTVAL ASC;");

    public final SQLStmt selectAllChangeColumns =
        new SQLStmt("SELECT * FROM CHANGE_COLUMNS ORDER BY ID ASC;");

    public final SQLStmt selectAllChangeTypes =
        new SQLStmt("SELECT * FROM CHANGE_TYPES ORDER BY ID ASC;");

    public VoltTable[] run(String tableName) {
        if (tableName.equals("PARTITION_TESTER"))
        {
            voltQueueSQL(selectAllPartitioned);
        }
        else if (tableName.equals("REPLICATED_TESTER"))
        {
            voltQueueSQL(selectAllReplicated);
        }
        else if (tableName.equals("BECOMES_MATERIALIZED"))
        {
            voltQueueSQL(selectAllBecomesMaterialized);
        }
        else if (tableName.equals("CHANGE_COLUMNS"))
        {
            voltQueueSQL(selectAllChangeColumns);
        }
        else if (tableName.equals("CHANGE_TYPES"))
        {
            voltQueueSQL(selectAllChangeTypes);
        }
        VoltTable[] results = voltExecuteSQL();

        return results;
    }
}
