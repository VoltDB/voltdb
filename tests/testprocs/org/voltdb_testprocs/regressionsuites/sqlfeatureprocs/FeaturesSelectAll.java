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

package org.voltdb_testprocs.regressionsuites.sqlfeatureprocs;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

/*
 * NOTE:
 * Select all won't work without distribution. For now it really does
 * select all from the site partitioned to wid 0.
 */

public class FeaturesSelectAll extends VoltProcedure {

    public final SQLStmt item = new SQLStmt("SELECT * FROM ITEM;");

    public final SQLStmt new_order = new SQLStmt("SELECT * FROM NEW_ORDER;");

    public final SQLStmt order_line = new SQLStmt("SELECT * FROM ORDER_LINE;");

    public final SQLStmt fivek_string = new SQLStmt("SELECT * FROM FIVEK_STRING;");

    public final SQLStmt fivek_string_with_index = new SQLStmt("SELECT * FROM FIVEK_STRING_WITH_INDEX;");

    public VoltTable[] run() {
        voltQueueSQL(item);
        voltQueueSQL(new_order);
        voltQueueSQL(order_line);
        voltQueueSQL(fivek_string);
        voltQueueSQL(fivek_string_with_index);
        return voltExecuteSQL();
    }
}
