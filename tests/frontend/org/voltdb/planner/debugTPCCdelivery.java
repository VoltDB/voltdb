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

import java.util.Date;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.benchmark.tpcc.Constants;

public class debugTPCCdelivery extends VoltProcedure {

    private final VoltTable result_template = new VoltTable(
            new VoltTable.ColumnInfo("d_id", VoltType.BIGINT),
            new VoltTable.ColumnInfo("o_id", VoltType.BIGINT)
    );

    final long W_ID = 3L;
    final long D_ID = 7L;
    final long O_ID = 9L;
    final long C_ID = 42L;
    final long I_ID = 12345L;

    public final SQLStmt getWarehouses = new SQLStmt("SELECT * FROM WAREHOUSE;");
    public final SQLStmt getDistricts = new SQLStmt("SELECT * FROM DISTRICT;");
    public final SQLStmt getItems = new SQLStmt("SELECT * FROM ITEM;");
    public final SQLStmt getCustomers = new SQLStmt("SELECT * FROM CUSTOMER;");
    public final SQLStmt getHistorys = new SQLStmt("SELECT * FROM HISTORY;");
    public final SQLStmt getStocks = new SQLStmt("SELECT * FROM STOCK;");
    public final SQLStmt getOrders = new SQLStmt("SELECT * FROM ORDERS;");
    public final SQLStmt getNewOrders = new SQLStmt("SELECT * FROM NEW_ORDER;");
    public final SQLStmt getOrderlines = new SQLStmt("SELECT * FROM ORDER_LINE;");

    public final SQLStmt insertWarehouse = new SQLStmt("INSERT INTO WAREHOUSE VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);");
    public final SQLStmt insertDistrict = new SQLStmt("INSERT INTO DISTRICT VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");
    public final SQLStmt insertCustomer = new SQLStmt("INSERT INTO CUSTOMER VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");
    public final SQLStmt insertOrders = new SQLStmt("INSERT INTO ORDERS VALUES (?, ?, ?, ?, ?, ?, ?, ?);");
    public final SQLStmt insertOrderLine = new SQLStmt("INSERT INTO ORDER_LINE VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");
    public final SQLStmt insertNewOrder = new SQLStmt("INSERT INTO NEW_ORDER VALUES (?, ?, ?);");

    // delivery procs
    public final SQLStmt getNewOrder = new SQLStmt("SELECT NO_O_ID FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ? AND NO_O_ID > -1 LIMIT 1;");
    public final SQLStmt deleteNewOrder = new SQLStmt("DELETE FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ? AND NO_O_ID = ?;");
    public final SQLStmt getCId = new SQLStmt("SELECT O_C_ID FROM ORDERS WHERE O_ID = ? AND O_D_ID = ? AND O_W_ID = ?;");
    public final SQLStmt updateOrders = new SQLStmt("UPDATE ORDERS SET O_CARRIER_ID = ? WHERE O_ID = ? AND O_D_ID = ? AND O_W_ID = ?;");
    public final SQLStmt updateOrderLine = new SQLStmt("UPDATE ORDER_LINE SET OL_DELIVERY_D = ? WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?;");
    public final SQLStmt sumOLAmount = new SQLStmt("SELECT SUM(OL_AMOUNT) FROM ORDER_LINE WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?;");
    public final SQLStmt updateCustomer = new SQLStmt("UPDATE CUSTOMER SET C_BALANCE = C_BALANCE + ? WHERE C_ID = ? AND C_D_ID = ? AND C_W_ID = ?;");

    public VoltTable[] run(long zip) throws VoltAbortException {
        Date timestamp = new Date();
        long o_carrier_id = 10L;

        voltQueueSQL(insertDistrict, 7L, 3L, "A District", "Street Addy", "meh", "westerfield", "BA", "99999", .0825, 15241.45, 21L);
        // check that a district was inserted
        long results = voltExecuteSQL()[0].asScalarLong();
        assert(results == 1);

        voltQueueSQL(insertWarehouse, 3L, "EZ Street WHouse", "Headquarters", "77 Mass. Ave.", "Cambridge", "AZ", "12938", .1234, 18837.57);
        // check that a warehouse was inserted
        results = voltExecuteSQL()[0].asScalarLong();
        assert(results == 1);

        voltQueueSQL(insertCustomer, 5L, D_ID, W_ID, "We", "Represent", "Customer", "Random Department", "Place2", "BiggerPlace",
                "AL", "13908", "(913) 909 - 0928", new Date(), "GC", 19298943.12, .13, 15.75, 18832.45, 45L, 15L, "Some History");
        // check that a customer was inserted
        results = voltExecuteSQL()[0].asScalarLong();
        assert(results == 1);

        final long O_OL_CNT = 1;
        voltQueueSQL(insertOrders, O_ID, D_ID, W_ID, 5L, new Date(), 10L, O_OL_CNT, 1L);
        // check that an orders was inserted
        results = voltExecuteSQL()[0].asScalarLong();
        assert(results == 1);

        voltQueueSQL(insertOrderLine, O_ID, D_ID, W_ID, 1L, I_ID, W_ID, null, 1L, 1.0, "ol_dist_info");
        // check that an orderline was inserted
        results = voltExecuteSQL()[0].asScalarLong();
        assert(results == 1);

        voltQueueSQL(insertOrderLine, O_ID, D_ID, W_ID, 2L, I_ID + 1, W_ID, null, 1L, 1.0, "ol_dist_info");
        // check that an orderline was inserted
        results = voltExecuteSQL()[0].asScalarLong();
        assert(results == 1);

        voltQueueSQL(insertNewOrder, O_ID, D_ID, W_ID);
        // check that an orders was inserted
        results = voltExecuteSQL()[0].asScalarLong();
        assert(results == 1);

        for (long d_id  = 1; d_id <= Constants.DISTRICTS_PER_WAREHOUSE; ++d_id) {
            voltQueueSQL(getNewOrder, d_id, W_ID);
        }
        final VoltTable[] neworderresults = voltExecuteSQL();
        assert neworderresults.length == Constants.DISTRICTS_PER_WAREHOUSE;
        final Long[] no_o_ids = new Long[Constants.DISTRICTS_PER_WAREHOUSE];
        int valid_neworders = 0;
        int[] result_offsets = new int[Constants.DISTRICTS_PER_WAREHOUSE];
        for (long d_id  = 1; d_id <= Constants.DISTRICTS_PER_WAREHOUSE; ++d_id) {
            final VoltTable newOrder = neworderresults[(int) d_id - 1];

            if (newOrder.getRowCount() == 0) {
                // No orders for this district: skip it. Note: This must be reported if > 1%
                result_offsets[(int) d_id - 1] = -1;
                continue;
            }
            assert newOrder.getRowCount() == 1;

            result_offsets[(int) d_id - 1] = valid_neworders * 2;
            ++valid_neworders;
            final Long no_o_id = newOrder.asScalarLong();
            no_o_ids[(int) d_id - 1] = no_o_id;

            voltQueueSQL(getOrderlines);
            VoltTable olResult = voltExecuteSQL()[0];
            try {
                System.out.println("All Orderlines: " + olResult.toString());
            } catch (Exception e) {
                e.printStackTrace();
            }

            voltQueueSQL(sumOLAmount, no_o_id, d_id, W_ID);
            VoltTable sumResult = voltExecuteSQL()[0];
            try {
                System.out.println("Sum Result: " + sumResult.toString());
            } catch (Exception e) {
                e.printStackTrace();
            }

            voltQueueSQL(getCId, no_o_id, d_id, W_ID);
            voltQueueSQL(sumOLAmount, no_o_id, d_id, W_ID);
        }
        final VoltTable[] otherresults = voltExecuteSQL();
        assert otherresults.length == valid_neworders * 2;

        for (long d_id  = 1; d_id <= Constants.DISTRICTS_PER_WAREHOUSE; ++d_id) {
            final VoltTable newOrder = neworderresults[(int) d_id - 1];

            if (newOrder.getRowCount() == 0) {
                // No orders for this district: skip it. Note: This must be reported if > 1%
                continue;
            }
            assert newOrder.getRowCount() == 1;

            final Long no_o_id = newOrder.asScalarLong();
            no_o_ids[(int) d_id - 1] = no_o_id;
            voltQueueSQL(deleteNewOrder, d_id, W_ID, no_o_id);
            voltQueueSQL(updateOrders, o_carrier_id, no_o_id, d_id, W_ID);
            voltQueueSQL(updateOrderLine, timestamp, no_o_id, d_id, W_ID);
        }
        voltExecuteSQL();

        // these must be logged in the "result file" according to TPC-C 2.7.2.2 (page 39)
        // We remove the queued time, completed time, w_id, and o_carrier_id: the client can figure
        // them out
        final VoltTable result = result_template.clone(8192);
        for (long d_id  = 1; d_id <= Constants.DISTRICTS_PER_WAREHOUSE; ++d_id) {
            int resultoffset = result_offsets[(int) d_id - 1];

            if (resultoffset < 0) {
                continue;
            }
            assert otherresults[resultoffset + 0].getRowCount() == 1;
            assert otherresults[resultoffset + 1].getRowCount() == 1;
            final long c_id = (otherresults[resultoffset + 0].asScalarLong());
            final VoltTableRow row = otherresults[resultoffset + 1].fetchRow(0);
            final double ol_total = row.getDouble(0);
            final boolean ol_total_wasnull = row.wasNull();

            // If there are no order lines, SUM returns null. There should always be order lines.
            if (ol_total_wasnull) {
                throw new VoltAbortException(
                        "ol_total is NULL: there are no order lines. This should not happen");
            }
            assert ol_total > 0.0;

            voltQueueSQL(updateCustomer, ol_total, c_id, d_id, W_ID);

            final Long no_o_id = no_o_ids[(int) d_id - 1];
            result.addRow(d_id, no_o_id);
        }
        voltExecuteSQL();

        VoltTableRow r = result.fetchRow(0);
        assert(D_ID == r.getLong(0));
        assert(O_ID == r.getLong(1));

        return null;
    }
}
