/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
 * terms and conditions:
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
/* Copyright (C) 2008
 * Michael McCanna
 * Massachusetts Institute of Technology
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
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.benchmark.tpcc.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class FragmentUpdateTestProcedure extends VoltProcedure {

    // Note: all of these SQL statements are accessed via reflection in the test:
    //   TestFragmentProgressUpdate
    // So please don't remove them or change their names.

    public final SQLStmt warehouse_select = new SQLStmt("SELECT * FROM WAREHOUSE;");
    public final SQLStmt warehouse_del_half = new SQLStmt("DELETE FROM WAREHOUSE WHERE W_ID > 5000;");
    public final SQLStmt warehouse_join = new SQLStmt("SELECT W1.W_TAX + W1.W_YTD FROM WAREHOUSE W1, WAREHOUSE W2"
            + " WHERE W1.W_ID = W2.W_ID AND W1.W_ID > -1 AND W1.W_TAX > W2.W_TAX ORDER BY W2.W_ID;");

    // This query doesn't produce a very meaningful result,
    // but should take a decent amount of time to run with just a few rows in ITEM.
    public final SQLStmt item_crazy_join = new SQLStmt("SELECT COUNT(*) FROM ITEM i1, ITEM i2, ITEM i3");

    // Not meaningful, but should take long.
    public final SQLStmt item_big_del = new SQLStmt("DELETE FROM ITEM WHERE I_NAME <> 'NULL_NULL';");

    // Just a quick query
    public final SQLStmt quick_query = new SQLStmt("SELECT W_ID FROM WAREHOUSE LIMIT 1");

    public VoltTable[] run() {
        voltQueueSQL(warehouse_select);
        voltQueueSQL(warehouse_del_half);
        voltQueueSQL(warehouse_join);
        return voltExecuteSQL();
    }
}
