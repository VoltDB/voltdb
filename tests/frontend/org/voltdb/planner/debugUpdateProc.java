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

package org.voltdb.planner;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class debugUpdateProc extends VoltProcedure {

    //public final SQLStmt getNewOrder = new SQLStmt("SELECT NO_O_ID FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ? AND NO_O_ID > -1 LIMIT 1;");

    /*public final SQLStmt foo = new SQLStmt(
            "SELECT OL_I_ID FROM ORDER_LINE, STOCK " +
            "WHERE S_W_ID = ? AND " +
            "S_I_ID = OL_I_ID AND " +
            "S_QUANTITY < ?;");*/

    public final SQLStmt GetStockCount = new SQLStmt(
            "SELECT COUNT(DISTINCT(OL_I_ID)) FROM ORDER_LINE, STOCK " +
            "WHERE OL_W_ID = ? AND " +
            "OL_D_ID = ? AND " +
            "OL_O_ID < ? AND " +
            "OL_O_ID >= ? AND " +
            "S_W_ID = ? AND " +
            "S_I_ID = OL_I_ID AND " +
            "S_QUANTITY < ?;");

    /*public final SQLStmt test = new SQLStmt(
            "SELECT S_W_ID FROM STOCK " +
            "WHERE S_W_ID = ? AND " +
            "S_QUANTITY < ?;");*/

    public VoltTable[] run(long zip) throws VoltAbortException {

        return null;
    }
}
