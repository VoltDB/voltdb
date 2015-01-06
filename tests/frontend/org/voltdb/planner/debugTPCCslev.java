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

package org.voltdb.planner;

import java.util.Date;

import org.voltdb.*;

@ProcInfo (
    partitionInfo = "WAREHOUSE.W_ID: 0",
    singlePartition = true
)
public class debugTPCCslev extends VoltProcedure {

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
    public final SQLStmt insertStock = new SQLStmt("INSERT INTO STOCK VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");

    // ostat by id
    public final SQLStmt GetOId = new SQLStmt("SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?;");
    public final SQLStmt GetStockCount = new SQLStmt(
        "SELECT COUNT(DISTINCT(OL_I_ID)) FROM ORDER_LINE, STOCK " +
        "WHERE OL_W_ID = ? AND " +
        "OL_D_ID = ? AND " +
        "OL_O_ID < ? AND " +
        "OL_O_ID >= ? AND " +
        "S_W_ID = ? AND " +
        "S_I_ID = OL_I_ID AND " +
        "S_QUANTITY < ?;");

    public VoltTable[] run(long zip) throws VoltAbortException {

        voltQueueSQL(insertDistrict, D_ID, W_ID, "A District", "Street Addy", "meh", "westerfield", "BA", "99999", .0825, 15241.45, 21L);
        // check that a district was inserted
        long resultsl = voltExecuteSQL()[0].asScalarLong();
        assert(resultsl == 1);

        System.out.println("Running GetOId"); System.out.flush();
        voltQueueSQL(GetOId, W_ID, D_ID);
        VoltTable result = voltExecuteSQL()[0];

        long o_id = result.asScalarLong(); //if invalid (i.e. no matching o_id), we expect a fail here.

        System.out.println("Running GetStockCount"); System.out.flush();
        voltQueueSQL(GetStockCount, W_ID, W_ID, o_id, o_id - 20, W_ID, 1L);
        //return assumes that o_id is a temporary variable, and that stock_count is a necessarily returned variable.
        result = voltExecuteSQL()[0];

        long stockCount = result.asScalarLong();
        // check count was 0 (should be, for we have empty stock and order-line
        // tables.
        assert(0L == stockCount);

        // Now we repeat the same thing, but adding a valid order-line.
        // long ol_o_id, long ol_d_id, long ol_w_id, long ol_number, long
        // ol_i_id, long ol_supply_w_id, Date ol_delivery_d, long ol_quantity,
        // double ol_amount, String ol_dist_info
        Date timestamp = new Date();

        voltQueueSQL(insertOrderLine, 4L, 7L, 3L, 1L, 4L, 3L, timestamp, 45L, 152.15, "blah blah blah");
        // check that a orderline was inserted
        resultsl = voltExecuteSQL()[0].asScalarLong();
        assert(resultsl == 1);

        System.out.println("Running GetOId"); System.out.flush();
        voltQueueSQL(GetOId, W_ID, D_ID);
        result = voltExecuteSQL()[0];

        o_id = result.asScalarLong(); //if invalid (i.e. no matching o_id), we expect a fail here.

        System.out.println("Running GetStockCount"); System.out.flush();
        voltQueueSQL(GetStockCount, W_ID, W_ID, o_id, o_id - 20, W_ID, 1L);
        //return assumes that o_id is a temporary variable, and that stock_count is a necessarily returned variable.
        result = voltExecuteSQL()[0];

        stockCount = result.asScalarLong();
        // check count was 0 (should be, for we have empty stock and order-line
        // tables.
        assert(0L == stockCount);

        timestamp = new Date();
        voltQueueSQL(insertStock, 4L, 3L, 45L,
                "INFO", "INFO", "INFO", "INFO", "INFO", "INFO", "INFO", "INFO",
                "INFO", "INFO", 5582L, 152L, 32L, "DATA");
        // check that a stock was inserted
        resultsl = voltExecuteSQL()[0].asScalarLong();
        assert(resultsl == 1);

        System.out.println("Running GetOId"); System.out.flush();
        voltQueueSQL(GetOId, W_ID, D_ID);
        result = voltExecuteSQL()[0];

        o_id = result.asScalarLong(); //if invalid (i.e. no matching o_id), we expect a fail here.

        System.out.println("Running GetStockCount"); System.out.flush();
        voltQueueSQL(GetStockCount, W_ID, W_ID, o_id, o_id - 20, W_ID, 5000L);
        //return assumes that o_id is a temporary variable, and that stock_count is a necessarily returned variable.
        result = voltExecuteSQL()[0];

        stockCount = result.asScalarLong();
        // check count was 0 (should be, for we have empty stock and order-line
        // tables.
        assert(1L == stockCount);


        // On more test: this test that Distinct is working properly.
        voltQueueSQL(insertOrderLine, 5L, 7L,
                3L, 1L, 5L, 3L, timestamp, 45L, 152.15, "blah blah blah");
        // check that a orderline was inserted
        resultsl = voltExecuteSQL()[0].asScalarLong();
        assert(resultsl == 1);

        voltQueueSQL(insertOrderLine, 6L, 7L,
                3L, 1L, 4L, 3L, timestamp, 45L, 152.15, "blah blah blah");
        // check that a orderline was inserted
        resultsl = voltExecuteSQL()[0].asScalarLong();
        assert(resultsl == 1);

        voltQueueSQL(insertStock, "InsertStock", 5L, 3L, 45L,
                "INFO", "INFO", "INFO", "INFO", "INFO", "INFO", "INFO", "INFO",
                "INFO", "INFO", 5582L, 152L, 32L, "DATA");
        // check that a stock was inserted
        resultsl = voltExecuteSQL()[0].asScalarLong();
        assert(resultsl == 1);

        System.out.println("Running GetOId"); System.out.flush();
        voltQueueSQL(GetOId, W_ID, D_ID);
        result = voltExecuteSQL()[0];

        o_id = result.asScalarLong(); //if invalid (i.e. no matching o_id), we expect a fail here.

        System.out.println("Running GetStockCount"); System.out.flush();
        voltQueueSQL(GetStockCount, W_ID, W_ID, o_id, o_id - 20, W_ID, 5000L);
        //return assumes that o_id is a temporary variable, and that stock_count is a necessarily returned variable.
        result = voltExecuteSQL()[0];

        stockCount = result.asScalarLong();
        // check count was 0 (should be, for we have empty stock and order-line
        // tables.
        assert(2L == stockCount);

        return null;
    }
}
