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
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.benchmark.tpcc.Constants;
import org.voltdb.types.TimestampType;

//Notes on Stored Procedure:
//Need to add error handling to catch invalid items, and still return needed values.

public class neworder extends VoltProcedure {
    private final VoltTable item_data_template = new VoltTable(
            new VoltTable.ColumnInfo("i_name", VoltType.STRING),
            new VoltTable.ColumnInfo("s_quantity", VoltType.INTEGER),
            new VoltTable.ColumnInfo("brand_generic", VoltType.STRING),
            new VoltTable.ColumnInfo("i_price", VoltType.FLOAT),
            new VoltTable.ColumnInfo("ol_amount", VoltType.FLOAT)
    );
    private final VoltTable misc_template = new VoltTable(
            new VoltTable.ColumnInfo("w_tax", VoltType.FLOAT),
            new VoltTable.ColumnInfo("d_tax", VoltType.FLOAT),
            new VoltTable.ColumnInfo("o_id", VoltType.INTEGER),
            new VoltTable.ColumnInfo("total", VoltType.FLOAT)
    );

    public final SQLStmt getWarehouseTaxRate =
        new SQLStmt("SELECT W_TAX FROM WAREHOUSE WHERE W_ID = ?;"); //w_id

    public final SQLStmt getDistrict =
        new SQLStmt("SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_ID = ? AND D_W_ID = ?;"); //d_id, w_id

    public final SQLStmt incrementNextOrderId =
        new SQLStmt("UPDATE DISTRICT SET D_NEXT_O_ID = ? WHERE D_ID = ? AND D_W_ID = ?;"); //d_next_o_id, d_id, w_id

    public final SQLStmt getCustomer =
        new SQLStmt("SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?;"); //w_id, d_id, c_id

    public final SQLStmt createOrder =
        new SQLStmt("INSERT INTO ORDERS (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL) VALUES (?, ?, ?, ?, ?, ?, ?, ?);"); //d_next_o_id, d_id, w_id, c_id, timestamp, o_carrier_id, o_ol_cnt, o_all_local

    public final SQLStmt createNewOrder =
        new SQLStmt("INSERT INTO NEW_ORDER (NO_O_ID, NO_D_ID, NO_W_ID) VALUES (?, ?, ?);"); //o_id, d_id, w_id

    public final SQLStmt getItemInfo =
        new SQLStmt("SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = ?;"); //ol_i_id

    public final SQLStmt getStockInfo01 = new SQLStmt("SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_01 FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?;"); //ol_i_id, ol_supply_w_id
    public final SQLStmt getStockInfo02 = new SQLStmt("SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_02 FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?;"); //ol_i_id, ol_supply_w_id
    public final SQLStmt getStockInfo03 = new SQLStmt("SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_03 FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?;"); //ol_i_id, ol_supply_w_id
    public final SQLStmt getStockInfo04 = new SQLStmt("SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_04 FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?;"); //ol_i_id, ol_supply_w_id
    public final SQLStmt getStockInfo05 = new SQLStmt("SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_05 FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?;"); //ol_i_id, ol_supply_w_id
    public final SQLStmt getStockInfo06 = new SQLStmt("SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_06 FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?;"); //ol_i_id, ol_supply_w_id
    public final SQLStmt getStockInfo07 = new SQLStmt("SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_07 FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?;"); //ol_i_id, ol_supply_w_id
    public final SQLStmt getStockInfo08 = new SQLStmt("SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_08 FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?;"); //ol_i_id, ol_supply_w_id
    public final SQLStmt getStockInfo09 = new SQLStmt("SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_09 FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?;"); //ol_i_id, ol_supply_w_id
    public final SQLStmt getStockInfo10 = new SQLStmt("SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_10 FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?;"); //ol_i_id, ol_supply_w_id

    public final SQLStmt[] getStockInfo = {
            getStockInfo01,
            getStockInfo02,
            getStockInfo03,
            getStockInfo04,
            getStockInfo05,
            getStockInfo06,
            getStockInfo07,
            getStockInfo08,
            getStockInfo09,
            getStockInfo10,
    };

    public final SQLStmt updateStock = new SQLStmt("UPDATE STOCK SET S_QUANTITY = ?, S_YTD = ?, S_ORDER_CNT = ?, S_REMOTE_CNT = ? WHERE S_I_ID = ? AND S_W_ID = ?;"); //s_quantity, s_order_cnt, s_remote_cnt, ol_i_id, ol_supply_w_id

    public final SQLStmt createOrderLine = new SQLStmt("INSERT INTO ORDER_LINE (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"); //o_id, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info

    private int indexOf(byte[] array, byte[] subarray) {
        for (int i = 0; i <= array.length - subarray.length; ++i) {
            boolean match = true;
            for (int j = 0; j < subarray.length; ++j) {
                if (array[i + j] != subarray[j]) {
                    match = false;
                    break;
                }
            }
            if (match) return i;
        }

        return -1;
    }

    public VoltTable[] run(short w_id, byte d_id, int c_id, TimestampType timestamp, int[] item_id, short[] supware, int[] quantity) throws VoltAbortException {
        assert item_id.length > 0;
        assert item_id.length == supware.length;
        assert item_id.length == quantity.length;

        // CHEAT: Validate all items to see if we will need to abort.
        // Also determine if this is an all local order or not
        final VoltTableRow[] items = new VoltTableRow[item_id.length];
        boolean isAllLocal = true;
        for (int i = 0; i < item_id.length; ++i) {
            isAllLocal = isAllLocal && supware[i] == w_id;
            voltQueueSQL(getItemInfo, item_id[i]);
        }
        voltQueueSQL(getWarehouseTaxRate, w_id);
        voltQueueSQL(getDistrict, d_id, w_id);
        voltQueueSQL(getCustomer, w_id, d_id, c_id);

        final VoltTable[] itemresults = voltExecuteSQL();
        assert itemresults.length == item_id.length + 3;
        for (int i = 0; i < item_id.length; ++i) {
            if (itemresults[i].getRowCount() == 0) {
                // note that this will happen with 1% of transactions on purpose.
                // TPCC defines 1% of neworder gives a wrong itemid, causing rollback.
                throw new VoltAbortException(Constants.INVALID_ITEM_MESSAGE);
            }
            assert itemresults[i].getRowCount() == 1;
            items[i] = itemresults[i].fetchRow(0);
        }

        //final VoltTable[] backgroundInfo = executeSQL();
        VoltTable customer = itemresults[item_id.length + 2];

        final double w_tax = itemresults[item_id.length].fetchRow(0).getDouble(0);
        final int D_TAX_COL = 0, D_NEXT_O_ID = 1;
        final int C_DISCOUNT = 0;
        final VoltTableRow tempRow = itemresults[item_id.length + 1].fetchRow(0);
        final double d_tax = tempRow.getDouble(D_TAX_COL);
        final double c_discount = itemresults[item_id.length + 2].fetchRow(0).getDouble(C_DISCOUNT);
        final long d_next_o_id = tempRow.getLong(D_NEXT_O_ID);
        final long ol_cnt = item_id.length;
        final long all_local = isAllLocal ? 1 : 0;

        voltQueueSQL(incrementNextOrderId, d_next_o_id + 1, d_id, w_id);
        voltQueueSQL(createOrder, d_next_o_id, d_id, w_id, c_id, timestamp,
                Constants.NULL_CARRIER_ID, ol_cnt, all_local);
        voltQueueSQL(createNewOrder, d_next_o_id, d_id, w_id);
        voltExecuteSQL();

        // values the client is missing: i_name, s_quantity, brand_generic, i_price, ol_amount
        final VoltTable item_data = item_data_template.clone(2048);

        double total = 0;
        for (int i = 0; i < item_id.length; ++i) {
            final long ol_supply_w_id = supware[i];
            final long ol_i_id = item_id[i];

            // One getStockInfo SQL statement for each district
            voltQueueSQL(getStockInfo[d_id-1], ol_i_id, ol_supply_w_id);
        }
        final VoltTable[] stockresults = voltExecuteSQL();
        assert stockresults.length == item_id.length;

        for (int i = 0; i < item_id.length; ++i) {
            final long ol_number = i + 1;
            final long ol_supply_w_id = supware[i];
            final long ol_i_id = item_id[i];
            final long ol_quantity = quantity[i];

            assert stockresults[i].getRowCount() == 1 : "Cannot find stock info for item; should not happen with valid database";
            final VoltTableRow itemInfo = items[i];
            final VoltTableRow stockInfo = stockresults[i].fetchRow(0);

            final int I_PRICE = 0, I_NAME = 1, I_DATA = 2;
            final byte[] i_name = itemInfo.getStringAsBytes(I_NAME);
            final byte[] i_data = itemInfo.getStringAsBytes(I_DATA);
            final double i_price = itemInfo.getDouble(I_PRICE);

            final int S_QUANTITY = 0, S_DATA = 1, S_YTD = 2, S_ORDER_CNT = 3, S_REMOTE_CNT = 4, S_DIST_XX = 5;
            long s_quantity = stockInfo.getLong(S_QUANTITY);
            long s_ytd = stockInfo.getLong(S_YTD);
            long s_order_cnt = stockInfo.getLong(S_ORDER_CNT);
            long s_remote_cnt = stockInfo.getLong(S_REMOTE_CNT);
            final byte[] s_data = stockInfo.getStringAsBytes(S_DATA);
            // Fetches data from the s_dist_[d_id] column
            final byte[] s_dist_xx = stockInfo.getStringAsBytes(S_DIST_XX);

            // Update stock
            s_ytd += ol_quantity;
            if (s_quantity >= ol_quantity + 10) {
                s_quantity = s_quantity - ol_quantity;
            } else {
                s_quantity = s_quantity + 91 - ol_quantity;
            }
            s_order_cnt++;
            if (ol_supply_w_id != w_id) s_remote_cnt++;
            // TODO(evanj): Faster to do s_ytd and s_order_cnt increment in SQL?
            // Saves fetching those columns the first time
            voltQueueSQL(updateStock, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, ol_i_id, ol_supply_w_id );

            byte[] brand_generic;
            if (indexOf(i_data, Constants.ORIGINAL_BYTES) != -1
                    && indexOf(s_data, Constants.ORIGINAL_BYTES) != -1) {
                brand_generic = new byte[]{ 'B' };
            } else {
                brand_generic = new byte[]{ 'G' };
            }

            //Transaction profile states to use "ol_quantity * i_price"
            final double ol_amount = ol_quantity * i_price;
            total += ol_amount;

            voltQueueSQL(createOrderLine, d_next_o_id, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id, timestamp, ol_quantity, ol_amount, s_dist_xx);

            // Add the info to be returned
            item_data.addRow(i_name, s_quantity, brand_generic, i_price, ol_amount);
        }
        voltExecuteSQL();

        // Adjust the total for the discount
        total *= (1 - c_discount) * (1 + w_tax + d_tax);

        // pack up values the client is missing (see TPC-C 2.4.3.5)
        final VoltTable misc = misc_template.clone(256);
        misc.addRow(w_tax, d_tax, d_next_o_id, total);

        return new VoltTable[] { customer, misc, item_data };
    }
}
