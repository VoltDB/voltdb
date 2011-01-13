/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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
 * Evan Jones
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

package org.voltdb.benchmark.tpcc;

import org.voltdb.benchmark.*;
import org.voltdb.benchmark.tpcc.TPCCClient;
import org.voltdb.client.MockVoltClient;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

import java.io.IOException;
import junit.framework.TestCase;

public class ClientTest extends TestCase {
    private static long WAREHOUSES = 5;

    MockVoltClient mockClient;
    MockRandomGenerator generator;
    TPCCClient client;

    public void setUp() {
        mockClient = new MockVoltClient();
        generator = new MockRandomGenerator();
        client = new TPCCClient(mockClient, generator, new Clock.Mock(),
                ScaleParameters.makeDefault((int) WAREHOUSES));
        mockClient.nextResult = new VoltTable[0];
        mockClient.resetAfterCall = false;
    }

    private static final int SMALL_ITEMS = 50;
    private static final int SMALL_DISTRICTS = 2;
    private static final int SMALL_CUSTOMERS = 20;
    private void makeSmallWarehouseClient() {
        client = new TPCCClient(mockClient, generator, new Clock.Mock(),
                new ScaleParameters(SMALL_ITEMS, 1, SMALL_DISTRICTS, SMALL_CUSTOMERS, 0));
    }

    public void testStockLevel() throws IOException {
        VoltTable t = new VoltTable(new VoltTable.ColumnInfo("foo", VoltType.BIGINT));
        t.addRow(0);
        mockClient.nextResult = new VoltTable[]{ t };
        client.m_tpccSim.doStockLevel();
        assertEquals(Constants.STOCK_LEVEL, mockClient.calledName);
        assertEquals(3, mockClient.calledParameters.length);
        assertEquals(1L, mockClient.calledParameters[0]);  // w_id
        assertEquals(1L, mockClient.calledParameters[1]);  // d_id
        // threshold
        assertEquals((long) Constants.MIN_STOCK_LEVEL_THRESHOLD, mockClient.calledParameters[2]);

        generator.minimum = false;
        client.m_tpccSim.doStockLevel();
        assertEquals(Constants.STOCK_LEVEL, mockClient.calledName);
        assertEquals(3, mockClient.calledParameters.length);
        //assertEquals(WAREHOUSES, mockClient.calledParameters[0]);
        assertEquals((long) Constants.DISTRICTS_PER_WAREHOUSE, mockClient.calledParameters[1]);
        assertEquals((long) Constants.MAX_STOCK_LEVEL_THRESHOLD, mockClient.calledParameters[2]);
    }

    public void testOrderStatus() throws IOException {
        client.m_tpccSim.doOrderStatus();
        assertEquals("ostatByCustomerName", mockClient.calledName);
        assertEquals(3, mockClient.calledParameters.length);
        //assertEquals(1L, mockClient.calledParameters[0]);  // w_id
        assertEquals(1L, mockClient.calledParameters[1]);  // d_id
        assertEquals(generator.makeLastName(0), mockClient.calledParameters[2]);  // c_last

        generator.minimum = false;
        client.m_tpccSim.doOrderStatus();
        assertEquals("ostatByCustomerId", mockClient.calledName);
        assertEquals(3, mockClient.calledParameters.length);
        //assertEquals(WAREHOUSES, mockClient.calledParameters[0]);  // w_id
        assertEquals((long) Constants.DISTRICTS_PER_WAREHOUSE, mockClient.calledParameters[1]);
        assertEquals(72L, mockClient.calledParameters[2]);  // c_id
    }

    public void testDelivery() throws IOException {
        VoltTable orders = new VoltTable(
                new VoltTable.ColumnInfo("", VoltType.BIGINT)
        );
        for (int i = 0; i < Constants.DISTRICTS_PER_WAREHOUSE; ++i) {
            orders.addRow((long) i);
        }
        mockClient.nextResult = new VoltTable[]{ orders };
        client.m_tpccSim.doDelivery();
        assertEquals("delivery", mockClient.calledName);
        assertEquals(3, mockClient.calledParameters.length);
        //assertEquals(1L, mockClient.calledParameters[0]);
        assertEquals((long) Constants.MIN_CARRIER_ID, mockClient.calledParameters[1]);
        assertEquals(Clock.Mock.NOW, mockClient.calledParameters[2]);

        generator.minimum = false;
        client.m_tpccSim.doDelivery();
        assertEquals("delivery", mockClient.calledName);
        assertEquals(3, mockClient.calledParameters.length);
        //assertEquals(WAREHOUSES, mockClient.calledParameters[0]);
        assertEquals((long) Constants.MAX_CARRIER_ID, mockClient.calledParameters[1]);
        assertEquals(Clock.Mock.NOW, mockClient.calledParameters[2]);
    }

    public void testPayment() throws IOException {
        client.m_tpccSim.doPayment();
        assertTrue("paymentByCustomerName".equals(mockClient.calledName)
                || "paymentByCustomerNameC".equals(mockClient.calledName)
                || "paymentByCustomerNameW".equals(mockClient.calledName));
        assertEquals(7, mockClient.calledParameters.length);
        //assertEquals(1L, mockClient.calledParameters[0]);  // w_id
        assertEquals(1L, mockClient.calledParameters[1]);  // d_id
        assertEquals(Constants.MIN_PAYMENT, mockClient.calledParameters[2]);  // h_amount
        //assertEquals(1L, mockClient.calledParameters[3]);  // c_w_id
        assertEquals(1L, mockClient.calledParameters[4]);  // c_d_id
        assertEquals("BARBARBAR", mockClient.calledParameters[5]);  // c_last
        assertEquals(Clock.Mock.NOW, mockClient.calledParameters[6]);  // now

        generator.minimum = false;
        client.m_tpccSim.doPayment();
        assertTrue("paymentByCustomerId".equals(mockClient.calledName)
                || "paymentByCustomerIdC".equals(mockClient.calledName)
                || "paymentByCustomerIdW".equals(mockClient.calledName));
        assertEquals(7, mockClient.calledParameters.length);
        //assertEquals(WAREHOUSES, mockClient.calledParameters[0]);  // w_id
        assertEquals((long) Constants.DISTRICTS_PER_WAREHOUSE, mockClient.calledParameters[1]);
        assertEquals(Constants.MAX_PAYMENT, mockClient.calledParameters[2]);  // h_amount
        //assertEquals(WAREHOUSES-1, mockClient.calledParameters[3]);  // c_w_id
        assertEquals((long) Constants.DISTRICTS_PER_WAREHOUSE, mockClient.calledParameters[4]);
        assertEquals(72L, mockClient.calledParameters[5]);  // c_id
        assertEquals(Clock.Mock.NOW, mockClient.calledParameters[6]);  // now
    }

    public void testPaymentSmallWarehouse() throws IOException {
        makeSmallWarehouseClient();
        client.m_tpccSim.doPayment();
        assertEquals(1L, mockClient.calledParameters[0]);  // w_id
        assertEquals(1L, mockClient.calledParameters[1]);  // d_id
        assertEquals(1L, mockClient.calledParameters[3]);  // c_w_id
        assertEquals(1L, mockClient.calledParameters[4]);  // c_d_id
        assertEquals("BARBARBAR", mockClient.calledParameters[5]);  // c_last

        // With > 1 warehouse, this would select a remote customer
        generator.minimum = false;
        client.m_tpccSim.doPayment();
        assertEquals(1L, mockClient.calledParameters[0]);  // w_id
        assertEquals((long) SMALL_DISTRICTS, mockClient.calledParameters[1]);
        assertEquals(1L, mockClient.calledParameters[3]);  // c_w_id
        assertEquals((long) SMALL_DISTRICTS, mockClient.calledParameters[4]);
        assertEquals(4L, mockClient.calledParameters[5]);  // c_id
    }

    public void testNewOrder() throws IOException {
        // Minimum = rollback
        mockClient.abortMessage = Constants.INVALID_ITEM_MESSAGE;
        client.m_tpccSim.doNewOrder();
        assertEquals("neworder", mockClient.calledName);
        assertEquals(7, mockClient.calledParameters.length);
        assertEquals(1L, mockClient.calledParameters[0]);  // w_id
        assertEquals(1L, mockClient.calledParameters[1]);  // d_id
        assertEquals(2L, mockClient.calledParameters[2]);  // c_id
        assertEquals(Clock.Mock.NOW, mockClient.calledParameters[3]);  // timestamp

        long[] item_id = (long[]) mockClient.calledParameters[4];
        assertEquals(Constants.MIN_OL_CNT, item_id.length);
        long[] supply_w_id = (long[]) mockClient.calledParameters[5];
        assertEquals(Constants.MIN_OL_CNT, supply_w_id.length);
        long[] quantity = (long[]) mockClient.calledParameters[6];
        assertEquals(Constants.MIN_OL_CNT, quantity.length);

        for (int i = 0; i < item_id.length; ++i) {
            if (i +1 == item_id.length) {
                assertEquals((long) Constants.NUM_ITEMS + 1, item_id[i]);
            } else {
                assertEquals(2L, item_id[i]);
            }
            // TODO : now supply_w_id is same as w_id because
            // neworder doesn't support remote for now. will revert to this check in future.
            // assertEquals(2L, supply_w_id[i]);
            assertEquals(1L, supply_w_id[i]);
            assertEquals(1L, quantity[i]);
        }

        generator.minimum = false;
        client.m_tpccSim.doNewOrder();
        assertEquals("neworder", mockClient.calledName);
        assertEquals(7, mockClient.calledParameters.length);
        //assertEquals(WAREHOUSES, mockClient.calledParameters[0]);  // w_id
        assertEquals((long) Constants.DISTRICTS_PER_WAREHOUSE, mockClient.calledParameters[1]);
        assertEquals(72L, mockClient.calledParameters[2]);  // c_id
        assertEquals(Clock.Mock.NOW, mockClient.calledParameters[3]);  // timestamp

        item_id = (long[]) mockClient.calledParameters[4];
        assertEquals(Constants.MAX_OL_CNT, item_id.length);
        supply_w_id = (long[]) mockClient.calledParameters[5];
        assertEquals(Constants.MAX_OL_CNT, supply_w_id.length);
        quantity = (long[]) mockClient.calledParameters[6];
        assertEquals(Constants.MAX_OL_CNT, quantity.length);

        for (int i = 0; i < item_id.length; ++i) {
            assertEquals(6496L, item_id[i]);
            //assertEquals(WAREHOUSES, supply_w_id[i]);
            assertEquals((long) Constants.MAX_OL_QUANTITY, quantity[i]);
        }
    }

    public void testNewOrderSmallWarehouse() throws IOException {
        // Minimum = rollback
        mockClient.abortMessage = Constants.INVALID_ITEM_MESSAGE;
        makeSmallWarehouseClient();
        client.m_tpccSim.doNewOrder();

        long[] supply_w_id = (long[]) mockClient.calledParameters[5];
        for (int i = 0; i < supply_w_id.length; ++i) {
            // Normally this would be a "remote" item
            //assertEquals(1L, supply_w_id[i]);
        }

        generator.minimum = false;
        client.m_tpccSim.doNewOrder();
        assertEquals(4L, mockClient.calledParameters[2]);  // c_id
        long[] item_id = (long[]) mockClient.calledParameters[4];
        for (int i = 0; i < item_id.length; ++i) {
            assertEquals(42L, item_id[i]);
        }
    }
}
