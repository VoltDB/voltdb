/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
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

package com.procedures;

import org.voltdb.*;

//Notes on Stored Procedure:
//return VoltTables has 2 elements:
//1) Set of Non-Repeating Data, represented as a 1x10 table representing w_id, d_id, c_id, c_first, c_middle, c_last, c_balance, o_id, o_entry_d, o_carrier_id.
//2) Set of Repeating data, represented as a Nx5 table representing several order-lines each with fields ol_supply_w_id, ol_i_id, ol_quantity, ol_amount, ol_delivery_d.
//See TPC-C (Revision 5.9) Section 2.6.3.4 for more

@ProcInfo (
    partitionInfo = "WAREHOUSE.W_ID: 0",
    singlePartition = true
)
public class ostatByCustomerId extends VoltProcedure {
    // Parameters: w_id, d_id, c_id
    public final SQLStmt getCustomerByCustomerId = new SQLStmt("SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?;");

    // Parameters: w_id, d_id, c_id
    public final SQLStmt getLastOrder = new SQLStmt("SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM ORDERS WHERE O_W_ID = ? AND O_D_ID = ? AND O_C_ID = ? ORDER BY O_ID DESC LIMIT 1");
    private int O_ID_IDX = 0;

    // Parameters: w_id, d_id, o_id
    public final SQLStmt getOrderLines = new SQLStmt("SELECT OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM ORDER_LINE WHERE OL_W_ID = ? AND OL_O_ID = ? AND OL_D_ID = ?");
    //gets returned directly, as, since all data must be returned as VoltTables, this the most useful form the data can be presented in.

    public VoltTable[] getOrderStatus(short w_id, byte d_id, int c_id, VoltTable customer) {
        voltQueueSQL(getLastOrder, w_id, d_id, c_id);
        final VoltTable order = voltExecuteSQL()[0];

        final long o_id = order.fetchRow(0).getLong(O_ID_IDX);
        voltQueueSQL(getOrderLines, w_id, o_id, d_id);
        final VoltTable orderLines = voltExecuteSQL()[0];

        return new VoltTable[]{customer, order, orderLines};
    }

    public VoltTable[] run(short w_id, byte d_id, int c_id) {
        voltQueueSQL(getCustomerByCustomerId, w_id, d_id, c_id);
        final VoltTable customer = voltExecuteSQL()[0];
        return getOrderStatus(w_id, d_id, c_id, customer);
    }
}
