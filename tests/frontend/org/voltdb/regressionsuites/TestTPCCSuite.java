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

package org.voltdb.regressionsuites;

import java.io.IOException;

import org.voltdb.BackendTarget;
import org.voltdb.TPCDataPrinter;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.benchmark.tpcc.Constants;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.benchmark.tpcc.procedures.ByteBuilder;
import org.voltdb.benchmark.tpcc.procedures.slev;
import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;
import org.voltdb.types.TimestampType;

import junit.framework.Test;

/**
 * Run each of the main procedures from TPC-C one or more times
 * on a small known set of data and check the results of the calls
 * with varying degrees of strictness. If this runs, it's very
 * likely the TPC-C benchmark will run, but no guarantees as the data
 * used here is very small.
 *
 */
public class TestTPCCSuite extends RegressionSuite {

    // constants used int the benchmark
    static final short W_ID = 3;
    static final short W2_ID = 4;
    static final byte D_ID = 7;
    static final byte D2_ID = 8;
    static final int O_ID = 9;
    static final int C_ID = 42;
    static final int I_ID = 12345;

    /**
     * Supplemental classes needed by TPC-C procs.
     */
    public static final Class<?>[] SUPPLEMENTALS = {
        ByteBuilder.class, Constants.class };

    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestTPCCSuite(String name) {
        super(name);
    }

    public void testInsertsAndSelects() throws IOException {
        Client client = getClient();

        try {
            VoltTable[] results = null;
            TimestampType timestamp = new TimestampType();

            results = client.callProcedure("InsertWarehouse", 8L, "EZ Street WHouse", "Headquarters", "77 Mass. Ave.", "Cambridge", "AZ", "12938", .1234, 18837.57).getResults();
            assertTrue(results[0].asScalarLong() == 1L);

            results = client.callProcedure("InsertDistrict", 7L, 3L, "A District", "Street Addy", "meh", "westerfield", "BA", "99999", .0825, 15241.45, 21L).getResults();
            assertTrue(results[0].asScalarLong() == 1L);

            results = client.callProcedure("InsertItem", 5L, 21L, "An Item", 7.33, "Some Data").getResults();
            assertTrue(results[0].asScalarLong() == 1L);

            results = client.callProcedure("InsertCustomer", 2L, 7L, 8L, "I", "Is", "Name", "Place", "Place2", "BiggerPlace", "AL", "91083", "(913) 909 - 0928", new TimestampType(), "GC", 19298943.12, .13, 15.75, 18832.45, 45L, 15L, "Some History").getResults();
            assertTrue(results[0].asScalarLong() == 1L);

            results = client.callProcedure("InsertHistory", 13L, 2L, 7L, 5L, 6L, timestamp, 23.334, "Some History").getResults();
            assertTrue(results[0].asScalarLong() == 1L);

            results = client.callProcedure("InsertStock", 5L, 3L, 45L, "INFO", "INFO", "INFO", "INFO", "INFO", "INFO", "INFO", "INFO", "INFO", "INFO", 5582L, 152L, 32L, "DATA").getResults();
            assertTrue(results[0].asScalarLong() == 1L);

            results = client.callProcedure("InsertOrders", 2L, 7L, 5L, 6L, timestamp, 2L, 7L, 5L).getResults();
            assertTrue(results[0].asScalarLong() == 1L);

            results = client.callProcedure("InsertNewOrder", 7L, 5L, 6L).getResults();
            assertTrue(results[0].asScalarLong() == 1L);

            results = client.callProcedure("InsertOrderLine", 6L, 7L, 3L, 1L, 4L, 3L, timestamp, 45L, 152.15, "blah blah blah").getResults();
            assertTrue(results[0].asScalarLong() == 1L);

            TPCDataPrinter.printAllData(client);

        } catch (ProcCallException e1) {
            e1.printStackTrace();
            assertTrue(false);
        } catch (IOException e1) {
            e1.printStackTrace();
            assertTrue(false);
        }
    }

    public void testSLEV() throws IOException, ProcCallException {
        Client client = getClient();

        // call the insertDistrict procedure so we have a valid o_id
        // long d_id, long d_w_id, String d_name, String d_street_1, String
        // d_street_2, String d_city, String d_state, String d_zip, double
        // d_tax, double d_ytd, long d_next_o_id
        VoltTable[] idresults = client.callProcedure("InsertDistrict", 7L, 3L,
                "A District", "Street Addy", "meh", "westerfield", "BA",
                "99999", .0825, 15241.45, 21L).getResults();
        // check that a district was inserted
        assertEquals(1L, idresults[0].asScalarLong());

        // This tests case of stock level transaction being called when there
        // are no items with stock and no
        // long w_id, long d_id, long threshold
        VoltTable[] results = client.callProcedure(Constants.STOCK_LEVEL, (byte)3, (byte)7, 1).getResults();
        // check one table was returned
        assertEquals(1, results.length);
        // check one tuple was modified
        VoltTable result = results[0];
        assertNotNull(result);
        long stockCount = result.asScalarLong();
        // check count was 0 (should be, for we have empty stock and order-line
        // tables.
        assertEquals(0L, stockCount);

        // Now we repeat the same thing, but adding a valid order-line.
        // long ol_o_id, long ol_d_id, long ol_w_id, long ol_number, long
        // ol_i_id, long ol_supply_w_id, Date ol_delivery_d, long ol_quantity,
        // double ol_amount, String ol_dist_info
        TimestampType timestamp = new TimestampType();
        VoltTable[] olresults = client.callProcedure("InsertOrderLine", 4L, 7L,
                3L, 1L, 4L, 3L, timestamp, 45L, 152.15, "blah blah blah").getResults();
        assertEquals(1L, olresults[0].asScalarLong());

        try {
            // We expect this to fail because the stock table is empty.
            results = client.callProcedure(Constants.STOCK_LEVEL, (byte)3, (byte)7, 1L).getResults();

            //
            // If this is true SLEV procedure, then we want to check that we got nothing
            //
            if (Constants.STOCK_LEVEL.equals(slev.class.getSimpleName())) {
                // check one table was returned
                assertEquals(1, results.length);
                // check one tuple was modified
                result = results[0];
                assertNotNull(result);
                stockCount = result.asScalarLong();
                // check count was 0 (should be, for we have an empty stock table.
                assertEquals(0L, stockCount);
            //
            // Otherwise it's the "hand-crafted" SLEV, which should return an error
            //
            } else {
                fail("expected exception");
            }
        } catch (ProcCallException e) {}

        // Now we repeat the same thing, but adding a valid stock.
        // long pkey, long s_i_id, long s_w_id, long s_quantity, String
        // s_dist_01, String s_dist_02, String s_dist_03, String s_dist_04,
        // String s_dist_05, String s_dist_06, String s_dist_07, String
        // s_dist_08, String s_dist_09, String s_dist_10, long s_ytd, double
        // s_order_cnt, double s_remote_cnt, String s_data
        timestamp = new TimestampType();
        VoltTable[] isresults = client.callProcedure("InsertStock", 4L, 3L, 45L,
                "INFO", "INFO", "INFO", "INFO", "INFO", "INFO", "INFO", "INFO",
                "INFO", "INFO", 5582L, 152L, 32L, "DATA").getResults();
        assertEquals(1L, isresults[0].asScalarLong());

        results = client.callProcedure(Constants.STOCK_LEVEL, (byte)3, (byte)7, 5000).getResults();
        // check one table was returned
        assertEquals(1, results.length);
        // check one tuple was modified
        result = results[0];
        assertNotNull(result);
        stockCount = result.asScalarLong();
        // check count is 1
        assertEquals(1L, stockCount);

        // On more test: this test that Distinct is working properly.
        VoltTable[] ol2results = client.callProcedure("InsertOrderLine", 5L, 7L,
                3L, 1L, 5L, 3L, timestamp, 45L, 152.15, "blah blah blah").getResults();
        assertEquals(1L, ol2results[0].asScalarLong());

        VoltTable[] ol3results = client.callProcedure("InsertOrderLine", 6L, 7L,
                3L, 1L, 4L, 3L, timestamp, 45L, 152.15, "blah blah blah").getResults();
        assertEquals(1L, ol3results[0].asScalarLong());

        VoltTable[] is2results = client.callProcedure("InsertStock", 5L, 3L, 45L,
                "INFO", "INFO", "INFO", "INFO", "INFO", "INFO", "INFO", "INFO",
                "INFO", "INFO", 5582L, 152L, 32L, "DATA").getResults();
        assertEquals(1L, is2results[0].asScalarLong());

        results = client.callProcedure(Constants.STOCK_LEVEL, (byte)3, (byte)7, 5000).getResults();
        // check one table was returned
        assertEquals(1, results.length);
        // check one tuple was modified
        result = results[0];
        assertNotNull(result);
        stockCount = result.asScalarLong();
        // check count is 2, (not 3 or 1).
        assertEquals(2L, stockCount);
    }

    public void testNEWORDER() throws IOException, ProcCallException {
        Client client = getClient();

        final double W_TAX = 0.1234;
        // long w_id, String w_name, String w_street_1, String w_street_2,
        // String w_city, String w_zip, double w_tax, long w_ytd
        VoltTable warehouse = client.callProcedure("InsertWarehouse", W_ID,
                "EZ Street WHouse", "Headquarters", "77 Mass. Ave.",
                "Cambridge", "AZ", "12938", W_TAX, 18837.57).getResults()[0];
        // check for successful insertion.
        assertEquals(1L, warehouse.asScalarLong());

        final double D_TAX = 0.0825;
        final int D_NEXT_O_ID = 21;
        // long d_id, long d_w_id, String d_name, String d_street_1, String
        // d_street_2, String d_city, String d_state, String d_zip, double
        // d_tax, double d_ytd, long d_next_o_id
        VoltTable district = client.callProcedure("InsertDistrict", D_ID, W_ID,
                "A District", "Street Addy", "meh", "westerfield", "BA",
                "99999", D_TAX, 15241.45, D_NEXT_O_ID).getResults()[0];
        // check that a district was inserted
        assertEquals(1L, district.asScalarLong());

        final double C_DISCOUNT = 0.13;
        // long c_id, long c_d_id, long c_w_id, String c_first, String c_middle,
        // String c_last, String c_street_1, String c_street_2, String d_city,
        // String d_state, String d_zip, String c_phone, Date c_since, String
        // c_credit, double c_credit_lim, double c_discount, double c_balance,
        // double c_ytd_payment, double c_payment_cnt, double c_delivery_cnt,
        // String c_data
        VoltTable customer = client.callProcedure("InsertCustomer", C_ID, D_ID,
                W_ID, "I", "Is", "Name", "Place", "Place2", "BiggerPlace",
                "AL", "91083", "(913) 909 - 0928", new TimestampType(), "GC",
                19298943.12, C_DISCOUNT, 15.75, 18832.45, 45L, 15L,
                "Some History").getResults()[0];
        // check for successful insertion.
        assertEquals(1L, customer.asScalarLong());

        final int[] s_quantities = { 45, 85, 15 };
        final long INITIAL_S_YTD = 5582L;
        final long INITIAL_S_ORDER_CNT = 152L;
        // long pkey, long s_i_id, long s_w_id, long s_quantity, String
        // s_dist_01, String s_dist_02, String s_dist_03, String s_dist_04,
        // String s_dist_05, String s_dist_06, String s_dist_07, String
        // s_dist_08, String s_dist_09, String s_dist_10, long s_ytd, long
        // s_order_cnt, long s_remote_cnt, String s_data
        VoltTable stock1 = client.callProcedure("InsertStock", 4L, W_ID,
                s_quantities[0], "INFO", "INFO", "INFO", "INFO", "INFO",
                "INFO", "INFO", "INFO", "INFO", "INFO", INITIAL_S_YTD, INITIAL_S_ORDER_CNT, 32L,
                "DATA").getResults()[0];
        VoltTable stock2 = client.callProcedure("InsertStock", 5L, W_ID,
                s_quantities[1], "INFO", "INFO", "INFO", "INFO", "INFO",
                "INFO", "INFO", "INFO", "INFO", "INFO", INITIAL_S_YTD+10, INITIAL_S_ORDER_CNT+10,
                32L, "foo" + Constants.ORIGINAL_STRING + "bar").getResults()[0];
        VoltTable stock3 = client.callProcedure("InsertStock", 6L, W_ID,
                s_quantities[2], "INFO", "INFO", "INFO", "INFO", "INFO",
                "INFO", "INFO", "INFO", "INFO", "INFO", INITIAL_S_YTD+20, INITIAL_S_ORDER_CNT+20,
                32L, "DATA").getResults()[0];

        final double PRICE = 2341.23;
        // long i_id, long i_im_id, String i_name, double i_price, String i_data
        VoltTable item1 = client.callProcedure("InsertItem", 4L, 4L, "ITEM1",
                PRICE, Constants.ORIGINAL_STRING).getResults()[0];
        VoltTable item2 = client.callProcedure("InsertItem", 5L, 5L, "ITEM2",
                PRICE, Constants.ORIGINAL_STRING).getResults()[0];
        VoltTable item3 = client.callProcedure("InsertItem", 6L, 6L, "ITEM3",
                PRICE, Constants.ORIGINAL_STRING).getResults()[0];
        // check the inserts went through.
        assertEquals(1L, stock1.asScalarLong());
        assertEquals(1L, stock2.asScalarLong());
        assertEquals(1L, stock3.asScalarLong());
        assertEquals(1L, item1.asScalarLong());
        assertEquals(1L, item2.asScalarLong());
        assertEquals(1L, item3.asScalarLong());

        // call the neworder transaction:
        // if(ol_supply_w_id != w_id) all_local = 0;
        // test all_local behavior, first, then remote warehouse situation.

        // long w_id, long d_id, long c_id, long ol_cnt, long all_local, long[]
        // item_id, long[] supware, long[] quantity
        int[] items = { 4, 5, 6 };
        short[] warehouses = { W_ID, W_ID, W_ID };
        int[] quantities = { 3, 5, 1 };

        TPCDataPrinter.printAllData(client);
        TimestampType timestamp = new TimestampType();
        VoltTable[] neworder = client.callProcedure("neworder", W_ID, D_ID, C_ID,
                timestamp, items, warehouses, quantities).getResults();

        // Now to check returns are correct. We assume that inserts and such
        // within the actual transaction went through since it didn't rollback
        // and error out.
        VoltTableRow customerData = neworder[0].fetchRow(0);
        VoltTableRow miscData = neworder[1].fetchRow(0);
        assertEquals("Name", customerData.getString("C_LAST"));
        assertEquals("GC", customerData.getString("C_CREDIT"));
        assertEquals(.13, customerData.getDouble("C_DISCOUNT"));
        assertEquals(W_TAX, miscData.getDouble("w_tax"));
        assertEquals(D_TAX, miscData.getDouble("d_tax"));
        assertEquals(21L, miscData.getLong("o_id"));
        final double AMOUNT = PRICE * (3 + 5 + 1) * (1 - C_DISCOUNT) * (1 + D_TAX + W_TAX);
        assertEquals(AMOUNT, miscData.getDouble("total"), 0.001);

        // Check each item
        VoltTable itemResults = neworder[2];
        assertEquals(quantities.length, itemResults.getRowCount());
        for (int i = 0; i < itemResults.getRowCount(); ++i) {
            VoltTableRow itemRow = itemResults.fetchRow(i);
            assertEquals("ITEM" + (i + 1), itemRow.getString("i_name"));
            //~ assertEquals(quantities[i], itemRow.getLong("));
            long expected = s_quantities[i] - quantities[i];
            if (expected < 10) expected += 91;
            assertEquals(expected, itemRow.getLong("s_quantity"));
            if (i == 1) {
                assertEquals("B", itemRow.getString("brand_generic"));
            } else {
                assertEquals("G", itemRow.getString("brand_generic"));
            }
            assertEquals(PRICE, itemRow.getDouble("i_price"));
            assertEquals(PRICE * quantities[i], itemRow.getDouble("ol_amount"));
        }

        // verify that stock was updated correctly
        VoltTable[] allTables = client.callProcedure("SelectAll").getResults();
        VoltTable stock = allTables[TPCDataPrinter.nameMap.get("STOCK")];
        for (int i = 0; i < stock.getRowCount(); ++i) {
            VoltTableRow stockRow = stock.fetchRow(i);
            assertEquals(INITIAL_S_YTD + i*10 + quantities[i], stockRow.getLong("S_YTD"));
            assertEquals(INITIAL_S_ORDER_CNT + i*10 + 1, stockRow.getLong("S_ORDER_CNT"));
        }

        // New order with a missing item
        items = new int[] { Constants.NUM_ITEMS + 1 };
        warehouses = new short[] { W_ID };
        quantities = new int[] { 42 };
        try {
            client.callProcedure("neworder", W_ID, D_ID, C_ID, timestamp,
                    items, warehouses, quantities);
        } catch (ProcCallException e) {
            assertTrue(e.getMessage().indexOf(Constants.INVALID_ITEM_MESSAGE) > 0);
        }

        // Verify that we only inserted one new order
        allTables = client.callProcedure("SelectAll").getResults();
        TPCDataPrinter.nameMap.get("ORDERS");
        // only 1 order, from the first new order call
        district = allTables[TPCDataPrinter.nameMap.get("DISTRICT")];
        assertEquals(1, district.getRowCount());
        assertEquals(D_NEXT_O_ID + 1, district.fetchRow(0).getLong(
                "D_NEXT_O_ID"));
        // TODO(evanj): Verify that everything else is updated correctly
    }

    public void testPAYMENT() throws IOException, ProcCallException {
        Client client = getClient();

        // create a District, Warehouse, and 4 Customers (multiple, since we
        // want to test for correct behavior of paymentByCustomerName.
        // long d_id, long d_w_id, String d_name, String d_street_1, String
        // d_street_2, String d_city, String d_state, String d_zip, double
        // d_tax, double d_ytd, long d_next_o_id
        final double initialYTD = 15241.45;
        VoltTable district = client.callProcedure("InsertDistrict", D_ID, W_ID,
                "A District", "Street Addy", "meh", "westerfield", "BA",
                "99999", .0825, initialYTD, 21L).getResults()[0];
        // check that a district was inserted
        assertEquals(1L, district.asScalarLong());

        // long w_id, String w_name, String w_street_1, String w_street_2,
        // String w_city, String w_zip, double w_tax, long w_ytd
        VoltTable warehouse = client.callProcedure("InsertWarehouse", W_ID,
                "EZ Street WHouse", "Headquarters", "77 Mass. Ave.",
                "Cambridge", "AZ", "12938", .1234, initialYTD).getResults()[0];
        // check for successful insertion.
        assertEquals(1L, warehouse.asScalarLong());

        // long c_id, long c_d_id, long c_w_id, String c_first, String c_middle,
        // String c_last, String c_street_1, String c_street_2, String d_city,
        // String d_state, String d_zip, String c_phone, Date c_since, String
        // c_credit, double c_credit_lim, double c_discount, double c_balance,
        // double c_ytd_payment, double c_payment_cnt, double c_delivery_cnt,
        // String c_data
        final double initialBalance = 15.75;
        VoltTable customer1 = client.callProcedure("InsertCustomer", C_ID, D_ID,
                W_ID, "I", "Be", "lastname", "Place", "Place2", "BiggerPlace",
                "AL", "91083", "(193) 099 - 9082", new TimestampType(), "BC",
                19298943.12, .13, initialBalance, initialYTD, 0L, 15L,
                "Some History").getResults()[0];
        // check for successful insertion.
        assertEquals(1L, customer1.asScalarLong());

        VoltTable customer2 = client.callProcedure("InsertCustomer", C_ID + 1,
                D_ID, W_ID, "We", "R", "Customer", "Random Department",
                "Place2", "BiggerPlace", "AL", "13908", "(913) 909 - 0928",
                new TimestampType(), "GC", 19298943.12, .13, initialBalance, initialYTD,
                1L, 15L, "Some History").getResults()[0];
        // check for successful insertion.
        assertEquals(1L, customer2.asScalarLong());

        VoltTable customer3 = client.callProcedure("InsertCustomer", C_ID + 2,
                D_ID, W_ID, "Who", "Is", "Customer", "Receiving",
                "450 Mass F.X.", "BiggerPlace", "CI", "91083",
                "(541) 931 - 0928", new TimestampType(), "GC", 19899324.21, .13,
                initialBalance, initialYTD, 2L, 15L, "Some History").getResults()[0];
        // check for successful insertion.
        assertEquals(1L, customer3.asScalarLong());

        VoltTable customer4 = client.callProcedure("InsertCustomer", C_ID + 3,
                D_ID, W_ID, "ICanBe", "", "Customer", "street", "place",
                "BiggerPlace", "MA", "91083", "(913) 909 - 0928", new TimestampType(),
                "GC", 19298943.12, .13, initialBalance, initialYTD, 3L, 15L,
                "Some History").getResults()[0];
        // check for successful insertion.
        assertEquals(1L, customer4.asScalarLong());


        TPCDataPrinter.printAllData(client);


        // long d_id, long w_id, double h_amount, String c_last, long c_w_id,
        // long c_d_id
        final double paymentAmount = 500.25;
        VoltTable[] results = client.callProcedure("paymentByCustomerName", W_ID,
                D_ID, paymentAmount, W_ID, D_ID, "Customer", new TimestampType()).getResults();
        assertEquals(3, results.length);
        // check that the middle "Customer" was returned
        assertEquals(C_ID + 1, results[2].fetchRow(0).getLong("c_id"));
        assertEquals("", results[2].fetchRow(0).getString("c_data"));

        // Verify that both warehouse, district and customer were updated
        // correctly
        VoltTable[] allTables = client.callProcedure("SelectAll").getResults();
        warehouse = allTables[TPCDataPrinter.nameMap.get("WAREHOUSE")];
        assertEquals(1, warehouse.getRowCount());
        assertEquals(initialYTD + paymentAmount, warehouse.fetchRow(0)
                .getDouble("W_YTD"));
        district = allTables[TPCDataPrinter.nameMap.get("DISTRICT")];
        assertEquals(1, district.getRowCount());
        assertEquals(initialYTD + paymentAmount, district.fetchRow(0)
                .getDouble("D_YTD"));
        customer1 = allTables[TPCDataPrinter.nameMap.get("CUSTOMER")];
        assertEquals(4, customer1.getRowCount());
        assertEquals(C_ID + 1, customer1.fetchRow(1).getLong("C_ID"));
        assertEquals(initialBalance - paymentAmount, customer1.fetchRow(1)
                .getDouble("C_BALANCE"));
        assertEquals(initialYTD + paymentAmount, customer1.fetchRow(1)
                .getDouble("C_YTD_PAYMENT"));
        assertEquals(2, customer1.fetchRow(1).getLong("C_PAYMENT_CNT"));

        // long d_id, long w_id, double h_amount, String c_last, long c_w_id,
        // long c_d_id
        results = client.callProcedure("paymentByCustomerId", W_ID, D_ID,
                paymentAmount, W_ID, D_ID, C_ID, new TimestampType()).getResults();
        // Also tests badcredit case.
        assertEquals(3, results.length);
        assertEquals(C_ID, results[2].fetchRow(0).getLong("c_id"));
        // bad credit: insert history into c_data
        String data = results[2].fetchRow(0).getString("c_data");
        assertTrue(data.startsWith(new Long(C_ID).toString()));

        // Verify that both warehouse and district's ytd values were incremented
        // correctly
        allTables = client.callProcedure("SelectAll").getResults();
        warehouse = allTables[TPCDataPrinter.nameMap.get("WAREHOUSE")];
        assertEquals(1, warehouse.getRowCount());
        assertEquals(initialYTD + paymentAmount * 2, warehouse.fetchRow(0)
                .getDouble("W_YTD"));
        district = allTables[TPCDataPrinter.nameMap.get("DISTRICT")];
        assertEquals(1, district.getRowCount());
        assertEquals(initialYTD + paymentAmount * 2, district.fetchRow(0)
                .getDouble("D_YTD"));
        customer1 = allTables[TPCDataPrinter.nameMap.get("CUSTOMER")];
        assertEquals(4, customer1.getRowCount());
        assertEquals(C_ID, customer1.fetchRow(0).getLong("C_ID"));
        assertEquals(initialBalance - paymentAmount, customer1.fetchRow(1)
                .getDouble("C_BALANCE"));
        assertEquals(initialYTD + paymentAmount, customer1.fetchRow(1)
                .getDouble("C_YTD_PAYMENT"));
        assertEquals(1, customer1.fetchRow(0).getLong("C_PAYMENT_CNT"));
    }

    public void testPAYMENTMultiPartition() throws IOException, ProcCallException {
        Client client = getClient();

        // create 2 Districts, 2 Warehouses, and 2 Customers on each Warehouse/District
        // long d_id, long d_w_id, String d_name, String d_street_1, String
        // d_street_2, String d_city, String d_state, String d_zip, double
        // d_tax, double d_ytd, long d_next_o_id
        final double initialYTD = 15241.45;
        VoltTable district1 = client.callProcedure("InsertDistrict", D_ID, W_ID,
                "A District", "Street Addy", "meh", "westerfield", "BA",
                "99999", .0825, initialYTD, 21L).getResults()[0];
        // check that a district was inserted
        assertEquals(1L, district1.asScalarLong());
        VoltTable district2 = client.callProcedure("InsertDistrict", D2_ID, W2_ID,
                "fdsdfaaaaa", "fsdfsdfasas", "fda", "asdasfddsds", "MA",
                "99999", .0825, initialYTD, 21L).getResults()[0];
        assertEquals(1L, district2.asScalarLong());

        // long w_id, String w_name, String w_street_1, String w_street_2,
        // String w_city, String w_zip, double w_tax, long w_ytd
        VoltTable warehouse1 = client.callProcedure("InsertWarehouse", W_ID,
                "EZ Street WHouse", "Headquarters", "77 Mass. Ave.",
                "Cambridge", "AZ", "12938", .1234, initialYTD).getResults()[0];
        // check for successful insertion.
        assertEquals(1L, warehouse1.asScalarLong());
        VoltTable warehouse2 = client.callProcedure("InsertWarehouse", W2_ID,
                "easdsdfsdfddfdsd", "asfadasffass", "fdswwwwaafff",
                "Cambridge", "AZ", "12938", .1234, initialYTD).getResults()[0];
        assertEquals(1L, warehouse2.asScalarLong());

        // customer 1 and 2 are in district1, 3 and 4 are in district2

        // long c_id, long c_d_id, long c_w_id, String c_first, String c_middle,
        // String c_last, String c_street_1, String c_street_2, String d_city,
        // String d_state, String d_zip, String c_phone, Date c_since, String
        // c_credit, double c_credit_lim, double c_discount, double c_balance,
        // double c_ytd_payment, double c_payment_cnt, double c_delivery_cnt,
        // String c_data
        final double initialBalance = 15.75;
        VoltTable customer1 = client.callProcedure("InsertCustomer", C_ID, D_ID,
                W_ID, "I", "Be", "lastname", "Place", "Place2", "BiggerPlace",
                "AL", "91083", "(193) 099 - 9082", new TimestampType(), "BC",
                19298943.12, .13, initialBalance, initialYTD, 0L, 15L,
                "Some History").getResults()[0];
        // check for successful insertion.
        assertEquals(1L, customer1.asScalarLong());
        client.callProcedure("InsertCustomerName", C_ID, D_ID, W_ID, "I", "lastname");

        VoltTable customer2 = client.callProcedure("InsertCustomer", C_ID + 1,
                D_ID, W_ID, "We", "R", "Customer", "Random Department",
                "Place2", "BiggerPlace", "AL", "13908", "(913) 909 - 0928",
                new TimestampType(), "GC", 19298943.12, .13, initialBalance, initialYTD,
                1L, 15L, "Some History").getResults()[0];
        // check for successful insertion.
        assertEquals(1L, customer2.asScalarLong());
        client.callProcedure("InsertCustomerName", C_ID + 1, D_ID, W_ID, "We", "Customer");

        VoltTable customer3 = client.callProcedure("InsertCustomer", C_ID + 2,
                D2_ID, W2_ID, "Who", "Is", "Customer", "Receiving",
                "450 Mass F.X.", "BiggerPlace", "CI", "91083",
                "(541) 931 - 0928", new TimestampType(), "GC", 19899324.21, .13,
                initialBalance, initialYTD, 2L, 15L, "Some History").getResults()[0];
        // check for successful insertion.
        assertEquals(1L, customer3.asScalarLong());
        client.callProcedure("InsertCustomerName", C_ID + 2, D2_ID, W2_ID, "Who", "Customer");

        VoltTable customer4 = client.callProcedure("InsertCustomer", C_ID + 3,
                D2_ID, W2_ID, "ICanBe", "", "Customer", "street", "place",
                "BiggerPlace", "MA", "91083", "(913) 909 - 0928", new TimestampType(),
                "GC", 19298943.12, .13, initialBalance, initialYTD, 3L, 15L,
                "Some History").getResults()[0];
        // check for successful insertion.
        assertEquals(1L, customer4.asScalarLong());
        client.callProcedure("InsertCustomerName", C_ID + 3, D2_ID, W2_ID, "ICanBe", "Customer");

        final double paymentAmount = 500.25;
        // long d_id, long w_id, double h_amount, String c_last, long c_w_id,
        // long c_d_id
        // w_id =Warehouse2 but c_w_id=warehouse1 !
        VoltTable[] results = client.callProcedure("paymentByCustomerName", W2_ID,
                D2_ID, paymentAmount, W_ID, D_ID, "Customer", new TimestampType()).getResults();
        assertEquals(3, results.length);
        assertTrue(results[0].getRowCount() > 0);
        // only customer2 should be returned as this is a query on warehouse1
        assertEquals(C_ID + 1, results[2].fetchRow(0).getLong("c_id"));
        assertEquals("", results[2].fetchRow(0).getString("c_data"));

 /* TODO : SelectAll doesn't work for multi-partition. how to check results?
        // Verify that both warehouse, district and customer were updated
        // correctly
        VoltTable[] allTables = client.callProcedure("SelectAll");
        warehouse = allTables[TPCDataPrinter.nameMap.get("WAREHOUSE")];
        assertEquals(1, warehouse.getRowCount());
        assertEquals(initialYTD + paymentAmount, warehouse.fetchRow(0)
                .getDouble("W_YTD"));
        district = allTables[TPCDataPrinter.nameMap.get("DISTRICT")];
        assertEquals(1, district.getRowCount());
        assertEquals(initialYTD + paymentAmount, district.fetchRow(0)
                .getDouble("D_YTD"));
        customer1 = allTables[TPCDataPrinter.nameMap.get("CUSTOMER")];
        assertEquals(4, customer1.getRowCount());
        assertEquals(C_ID + 1, customer1.fetchRow(1).getLong("C_ID"));
        assertEquals(initialBalance - paymentAmount, customer1.fetchRow(1)
                .getDouble("C_BALANCE"));
        assertEquals(initialYTD + paymentAmount, customer1.fetchRow(1)
                .getDouble("C_YTD_PAYMENT"));
        assertEquals(2, customer1.fetchRow(1).getLong("C_PAYMENT_CNT"));
        */

        // long d_id, long w_id, double h_amount, String c_last, long c_w_id,
        // long c_d_id
        // w_id =Warehouse2 but c_w_id=warehouse1 !
        results = client.callProcedure("paymentByCustomerId", W2_ID, D2_ID,
                paymentAmount, W_ID, D_ID, C_ID, new TimestampType()).getResults();
        // Also tests badcredit case.
        assertEquals(3, results.length);
        assertEquals(C_ID, results[2].fetchRow(0).getLong("c_id"));
        // bad credit: insert history into c_data
        String data = results[2].fetchRow(0).getString("c_data");
        assertTrue(data.startsWith(new Long(C_ID).toString()));

        /* TODO : SelectAll doesn't work for multi-partition. how to check results?
        // Verify that both warehouse and district's ytd values were incremented
        // correctly
        allTables = client.callProcedure("SelectAll");
        warehouse = allTables[TPCDataPrinter.nameMap.get("WAREHOUSE")];
        assertEquals(1, warehouse.getRowCount());
        assertEquals(initialYTD + paymentAmount * 2, warehouse.fetchRow(0)
                .getDouble("W_YTD"));
        district = allTables[TPCDataPrinter.nameMap.get("DISTRICT")];
        assertEquals(1, district.getRowCount());
        assertEquals(initialYTD + paymentAmount * 2, district.fetchRow(0)
                .getDouble("D_YTD"));
        customer1 = allTables[TPCDataPrinter.nameMap.get("CUSTOMER")];
        assertEquals(4, customer1.getRowCount());
        assertEquals(C_ID, customer1.fetchRow(0).getLong("C_ID"));
        assertEquals(initialBalance - paymentAmount, customer1.fetchRow(1)
                .getDouble("C_BALANCE"));
        assertEquals(initialYTD + paymentAmount, customer1.fetchRow(1)
                .getDouble("C_YTD_PAYMENT"));
        assertEquals(1, customer1.fetchRow(0).getLong("C_PAYMENT_CNT"));
        */
    }

    public void testOSTAT() throws IOException, ProcCallException {
        Client client = getClient();

        // create a District, Warehouse, and 4 Customers (multiple, since we
        // want to test for correct behavior of paymentByCustomerName.
        // long d_id, long d_w_id, String d_name, String d_street_1, String
        // d_street_2, String d_city, String d_state, String d_zip, double
        // d_tax, double d_ytd, long d_next_o_id
        VoltTable[] idresults = client.callProcedure("InsertDistrict", 7L, 3L,
                "A District", "Street Addy", "meh", "westerfield", "BA",
                "99999", .0825, 15241.45, 21L).getResults();
        // check that a district was inserted
        assertEquals(1L, idresults[0].asScalarLong());

        // long w_id, String w_name, String w_street_1, String w_street_2,
        // String w_city, String w_zip, double w_tax, long w_ytd
        VoltTable warehouse = client.callProcedure("InsertWarehouse", 3L,
                "EZ Street WHouse", "Headquarters", "77 Mass. Ave.",
                "Cambridge", "AZ", "12938", .1234, 18837.57).getResults()[0];
        // check for successful insertion.
        assertEquals(1L, warehouse.asScalarLong());

        VoltTable customer = client.callProcedure("InsertCustomer", 5L, 7L, 3L,
                "We", "R", "Customer", "Random Department", "Place2",
                "BiggerPlace", "AL", "13908", "(913) 909 - 0928", new TimestampType(),
                "GC", 19298943.12, .13, 15.75, 18832.45, 45L, 15L,
                "Some History").getResults()[0];
        // check for successful insertion.
        assertEquals(1L, customer.asScalarLong());

        VoltTable orders = client.callProcedure("InsertOrders", 9L, 7L, 3L, 5L,
                new TimestampType(), 10L, 5L, 6L).getResults()[0];
        // check for successful insertion.
        assertEquals(1L, orders.asScalarLong());

        TPCDataPrinter.printAllData(client);

        VoltTable[] results = client.callProcedure("ostatByCustomerName", (byte)3, (byte)7,
                "Customer").getResults();
        assertEquals(3, results.length);

        results = client.callProcedure("ostatByCustomerId", (byte)3, (byte)7, 5).getResults();
        assertEquals(3, results.length);
    }

    public void testDELIVERY() throws IOException, ProcCallException {
        Client client = getClient();

        // create a District, Warehouse, and 4 Customers (multiple, since we
        // want to test for correct behavior of paymentByCustomerName.
        // long d_id, long d_w_id, String d_name, String d_street_1, String
        // d_street_2, String d_city, String d_state, String d_zip, double
        // d_tax, double d_ytd, long d_next_o_id
        VoltTable[] idresults = client.callProcedure("InsertDistrict", D_ID,
                W_ID, "A District", "Street Addy", "meh", "westerfield", "BA",
                "99999", .0825, 15241.45, 21L).getResults();
        // check that a district was inserted
        assertEquals(1L, idresults[0].asScalarLong());

        // long w_id, String w_name, String w_street_1, String w_street_2,
        // String w_city, String w_zip, double w_tax, long w_ytd
        VoltTable warehouse = client.callProcedure("InsertWarehouse", W_ID,
                "EZ Street WHouse", "Headquarters", "77 Mass. Ave.",
                "Cambridge", "AZ", "12938", .1234, 18837.57).getResults()[0];
        // check for successful insertion.
        assertEquals(1L, warehouse.asScalarLong());

        VoltTable customer = client.callProcedure("InsertCustomer", 5L, D_ID,
                W_ID, "We", "R", "Customer", "Random Department",
                "Place2", "BiggerPlace", "AL", "13908", "(913) 909 - 0928",
                new TimestampType(), "GC", 19298943.12, .13, 15.75, 18832.45, 45L, 15L,
                "Some History").getResults()[0];
        // check for successful insertion.
        assertEquals(1L, customer.asScalarLong());

        final long O_OL_CNT = 1;
        VoltTable orders = client.callProcedure("InsertOrders", O_ID, D_ID, W_ID,
                5L, new TimestampType(), 10L, O_OL_CNT, 1L).getResults()[0];
        // check for successful insertion.
        assertEquals(1L, orders.asScalarLong());

        // Insert an order line for this order
        VoltTable line = client.callProcedure("InsertOrderLine",
                O_ID, D_ID, W_ID, 1L, I_ID, W_ID, new TimestampType(), 1L,
                1.0, "ol_dist_info").getResults()[0];
        assertEquals(1L, line.asScalarLong());

        VoltTable newOrder = client.callProcedure("InsertNewOrder", O_ID, D_ID,
                W_ID).getResults()[0];
        // check for successful insertion.
        assertEquals(1L, newOrder.asScalarLong());

        System.out.println("DATA before DELIVERY transaction");
        TPCDataPrinter.printAllData(client);

        VoltTable[] results = client.callProcedure("delivery", W_ID, 10,
                new TimestampType()).getResults();

        System.out.println("DATA after DELIVERY transaction");
        TPCDataPrinter.printAllData(client);

        assertEquals(1, results.length);
        assertEquals(1, results[0].getRowCount());
        VoltTableRow r = results[0].fetchRow(0);
        assertEquals(D_ID, r.getLong(0));
        assertEquals(O_ID, r.getLong(1));
    }

    /**
     * Build a list of the tests that will be run when TestTPCCSuite gets run by JUnit.
     * Use helper classes that are part of the RegressionSuite framework.
     * This particular class runs all tests on the JNI and HSQL backends.
     *
     * @return The TestSuite containing all the tests to be run.
     */
    static public Test suite() {
        VoltServerConfig config = null;
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestTPCCSuite.class);

        // build up a project builder for the TPC-C app
        TPCCProjectBuilder project = new TPCCProjectBuilder();
        //project.setBackendTarget(BackendTarget.NATIVE_EE_IPC);
        project.addDefaultSchema();
        project.addDefaultProcedures();
        project.addDefaultPartitioning();
        project.addSupplementalClasses(SUPPLEMENTALS);

        /////////////////////////////////////////////////////////////
        // CONFIG #1: 1 Local Site/Partition running on JNI backend
        /////////////////////////////////////////////////////////////
        config = new LocalCluster("tpcc.jar", 3, 1, 0, BackendTarget.NATIVE_EE_JNI);
        //config = new LocalSingleProcessServer("tpcc.jar", 1, BackendTarget.NATIVE_EE_IPC);
        boolean success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        // no cluster tests as this is primarily a SQL correctness test

        return builder;
    }

}
