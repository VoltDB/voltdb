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

/*
 * NOTE:
 * Select all won't work without distribution. For now it really does
 * select all from the site partitioned to wid 0.
 */
public class SelectAll extends VoltProcedure {

    public final SQLStmt warehouse = new SQLStmt("SELECT * FROM WAREHOUSE ORDER BY W_ID;");

    public final SQLStmt district = new SQLStmt("SELECT * FROM DISTRICT ORDER BY D_ID;");

    public final SQLStmt item = new SQLStmt("SELECT * FROM ITEM ORDER BY I_ID;");

    public final SQLStmt customer = new SQLStmt("SELECT * FROM CUSTOMER ORDER BY C_ID;");

    public final SQLStmt history = new SQLStmt("SELECT * FROM HISTORY;");

    public final SQLStmt stock = new SQLStmt("SELECT * FROM STOCK ORDER BY S_W_ID, S_I_ID;");

    public final SQLStmt orders = new SQLStmt("SELECT * FROM ORDERS ORDER BY O_ID;");

    public final SQLStmt new_order = new SQLStmt("SELECT * FROM NEW_ORDER ORDER BY NO_D_ID, NO_W_ID, NO_O_ID;");

    public final SQLStmt order_line = new SQLStmt("SELECT * FROM ORDER_LINE;");

    public VoltTable[] run() {
        voltQueueSQL(warehouse);
        voltQueueSQL(district);
        voltQueueSQL(item);
        voltQueueSQL(customer);
        voltQueueSQL(history);
        voltQueueSQL(stock);
        voltQueueSQL(orders);
        voltQueueSQL(new_order);
        voltQueueSQL(order_line);
        return voltExecuteSQL();
    }
}
