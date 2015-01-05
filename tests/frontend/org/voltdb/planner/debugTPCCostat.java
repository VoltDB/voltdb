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
public class debugTPCCostat extends VoltProcedure {
    private final VoltTable result_template = new VoltTable(
            new VoltTable.ColumnInfo("C_ID", VoltType.BIGINT),
            new VoltTable.ColumnInfo("C_FIRST", VoltType.STRING),
            new VoltTable.ColumnInfo("C_MIDDLE", VoltType.STRING),
            new VoltTable.ColumnInfo("C_LAST", VoltType.STRING),
            new VoltTable.ColumnInfo("C_BALANCE", VoltType.FLOAT)
    );

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

    // ostat by id
    public final SQLStmt getCustomerByCustomerId = new SQLStmt("SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?;");
    public final SQLStmt getLastOrder = new SQLStmt("SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM ORDERS WHERE O_W_ID = ? AND O_D_ID = ? AND O_C_ID = ? ORDER BY O_ID DESC LIMIT 1");
    private int O_ID_IDX = 0;
    public final SQLStmt getOrderLines = new SQLStmt("SELECT OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM ORDER_LINE WHERE OL_W_ID = ? AND OL_O_ID = ? AND OL_D_ID = ?");
    public final SQLStmt getCustomersByLastName = new SQLStmt("SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST;");
    private final int C_ID_IDX = 0;

    public VoltTable[] run(long zip) throws VoltAbortException {
        // create a District, Warehouse, and 4 Customers (multiple, since we
        // want to test for correct behavior of paymentByCustomerName.

        voltQueueSQL(insertDistrict, 7L, 3L, "A District", "Street Addy", "meh", "westerfield", "BA", "99999", .0825, 15241.45, 21L);
        // check that a district was inserted
        long results = voltExecuteSQL()[0].asScalarLong();
        assert(results == 1);

        voltQueueSQL(insertWarehouse, 3L, "EZ Street WHouse", "Headquarters", "77 Mass. Ave.", "Cambridge", "AZ", "12938", .1234, 18837.57);
        // check that a warehouse was inserted
        results = voltExecuteSQL()[0].asScalarLong();
        assert(results == 1);

        voltQueueSQL(insertCustomer, 5L, 7L, 3L, "We", "Represent", "Customer", "Random Department", "Place2", "BiggerPlace",
                "AL", "13908", "(913) 909 - 0928", new Date(), "GC", 19298943.12, .13, 15.75, 18832.45, 45L, 15L, "Some History");
        // check that a customer was inserted
        results = voltExecuteSQL()[0].asScalarLong();
        assert(results == 1);

        voltQueueSQL(insertOrders, 9L, 7L, 3L, 5L, new Date(), 10L, 5L, 6L);
        // check that an orders was inserted
        results = voltExecuteSQL()[0].asScalarLong();
        assert(results == 1);

        /*queueQuery(getCustomerByCustomerId, 3L, 7L, 5L);
        final VoltTable customer = executeQueries()[0];*/

        voltQueueSQL(getCustomersByLastName, 3L, 7L, "Customer");
        VoltTable customers = voltExecuteSQL()[0];

        // Get the midpoint customer's id
        final int namecnt = customers.getRowCount();
        final int index = (namecnt-1)/2;
        final VoltTableRow customer = customers.fetchRow(index);
        final long c_id = customer.getLong(C_ID_IDX);

        // Build an VoltTable with a single customer row
        final VoltTable customerResultTable = result_template.clone(8192);
        customerResultTable.addRow(c_id, customer.getStringAsBytes(1), customer.getStringAsBytes(2),
                customer.getStringAsBytes(3), customer.getDouble(4));

        voltQueueSQL(getLastOrder, 3L, 7L, 5L);
        final VoltTable order = voltExecuteSQL()[0];

        final long o_id = order.fetchRow(0).getLong(O_ID_IDX);
        voltQueueSQL(getOrderLines, 3L, 7L, o_id);
        final VoltTable orderLines = voltExecuteSQL()[0];

        try {
            System.out.println("CUSTOMER: " + customerResultTable.toString());
            System.out.println("ORDER: " + order.toString());
            System.out.println("ORDERLINES: " + orderLines.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }
}
