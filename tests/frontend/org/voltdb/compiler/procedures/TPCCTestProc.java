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

package org.voltdb.compiler.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

@ProcInfo (
    singlePartition = false
)
public class TPCCTestProc extends VoltProcedure {

    // starting with whitespace and containing newlines on purpose
    public final String
    neword000 = " SELECT C_DISCOUNT, C_LAST, \nC_CREDIT, W_TAX FROM CUSTOMER, WAREHOUSE WHERE W_ID = ? AND C_W_ID = W_ID AND C_D_ID = ? AND C_ID = ?;";

    public final String
    neword001 = "SELECT D_NEXT_O_ID, D_TAX FROM DISTRICT WHERE D_ID = ? AND D_W_ID = ?;";

    public final String
    neword002 = "UPDATE DISTRICT SET D_NEXT_O_ID = ? + 1 WHERE D_ID = ? AND D_W_ID = ?;";

    public final String
    neword003 = "INSERT INTO ORDERS (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL) VALUES (?, ?, ?, ?, ?, ?, ?);";

    public final String
    neword004 = "INSERT INTO NEW_ORDER (NO_O_ID, NO_D_ID, NO_W_ID) VALUES (?, ?, ?);";

    public final String
    neword005 = "SELECT I_PRICE, I_NAME , I_DATA FROM ITEM WHERE I_ID = ?;";

    public final String
    neword006 = "SELECT S_QUANTITY, S_DATA, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10 FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?;";

    public final String
    neword007 = "UPDATE stock SET s_quantity = ? WHERE s_i_id = ? AND s_w_id = ?;";

    public final String
    neword008 = "INSERT INTO ORDER_LINE (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);";

    public VoltTable[] run(long id)
    throws VoltAbortException {
        return voltExecuteSQL();
    }
}
