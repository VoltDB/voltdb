/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

import java.util.List;

import junit.framework.TestCase;
import org.voltdb.benchmark.tpcc.TPCCClient;
import org.voltdb.plannodes.*;


public class TestPlansTPCC extends TestCase {

    private PlannerTestAideDeCamp aide;

    /** A helper here where the junit test can assert on success */
    private AbstractPlanNode compile(String sql, int paramCount) {
        List<AbstractPlanNode> pn = null;
        try {
            pn =  aide.compile(sql, paramCount);
        }
        catch (NullPointerException ex) {
            ex.printStackTrace();
            fail();
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail();
        }
        assertTrue(pn != null);
        assertFalse(pn.isEmpty());
        assertTrue(pn.get(0) != null);
        return pn.get(0);
    }


    @Override
    protected void setUp() throws Exception {
        aide = new PlannerTestAideDeCamp(TPCCClient.class.getResource("tpcc-ddl.sql"), "testplanstpcc");
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        aide.tearDown();
    }

    public void testInserts() {
        AbstractPlanNode node = null;
        node = compile("INSERT INTO WAREHOUSE VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);", 9);
        assertTrue(node != null);
        node = compile("INSERT INTO DISTRICT VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", 11);
        assertTrue(node != null);
        node = compile("INSERT INTO ITEM VALUES (?, ?, ?, ?, ?);", 5);
        assertTrue(node != null);
        node = compile("INSERT INTO CUSTOMER VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", 21);
        assertTrue(node != null);
        node = compile("INSERT INTO CUSTOMER_NAME VALUES (?, ?, ?, ?, ?);", 5);
        assertTrue(node != null);
        node = compile("INSERT INTO HISTORY VALUES (?, ?, ?, ?, ?, ?, ?, ?);", 8);
        assertTrue(node != null);
        node = compile("INSERT INTO STOCK VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", 17);
        assertTrue(node != null);
        node = compile("INSERT INTO ORDERS VALUES (?, ?, ?, ?, ?, ?, ?, ?);", 8);
        assertTrue(node != null);
        node = compile("INSERT INTO NEW_ORDER VALUES (?, ?, ?);", 3);
        assertTrue(node != null);
        node = compile("INSERT INTO ORDER_LINE VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", 10);
        assertTrue(node != null);
    }

    public void testSelectAllSQL() {
        AbstractPlanNode node = null;
        node = compile("SELECT * FROM WAREHOUSE;", 0);
        assertTrue(node != null);
    }

    public void testNewOrderSQL() {
        AbstractPlanNode node = null;
        node = compile("SELECT W_TAX FROM WAREHOUSE WHERE W_ID = ?;", 1);
        assertTrue(node != null);
        node = compile("SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_ID = ? AND D_W_ID = ?;", 2);
        assertTrue(node != null);
        node = compile("SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?;", 3);
        assertTrue(node != null);
        node = compile("SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = ?;", 1);
        assertTrue(node != null);
        node = compile("SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_01 FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?;", 2);
        assertTrue(node != null);
        node = compile("UPDATE DISTRICT SET D_NEXT_O_ID = ? WHERE D_ID = ? AND D_W_ID = ?;", 3);
        assertTrue(node != null);
        node = compile("UPDATE STOCK SET S_QUANTITY = ?, S_YTD = ?, S_ORDER_CNT = ?, S_REMOTE_CNT = ? WHERE S_I_ID = ? AND S_W_ID = ?;", 3);
        assertTrue(node != null);
    }

    public void testSlevSQL() {
        AbstractPlanNode node = null;
        node = compile("SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?;", 2);
        assertTrue(node != null);
        node = compile("SELECT COUNT(DISTINCT(OL_I_ID)) FROM ORDER_LINE, STOCK " +
                       "WHERE OL_W_ID = ? AND " +
                       "OL_D_ID = ? AND " +
                       "OL_O_ID < ? AND " +
                       "OL_O_ID >= ? AND " +
                       "S_W_ID = ? AND " +
                       "S_I_ID = OL_I_ID AND " +
                       "S_QUANTITY < ?;", 6);
        assertTrue(node != null);
    }

    public void testDeliverySQL() {
        AbstractPlanNode node = null;
        node = compile("SELECT NO_O_ID FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ? AND NO_O_ID > -1 LIMIT 1;", 2);
        assertTrue(node != null);
        node = compile("DELETE FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ? AND NO_O_ID = ?;", 3);
        assertTrue(node != null);
        node = compile("SELECT O_C_ID FROM ORDERS WHERE O_ID = ? AND O_D_ID = ? AND O_W_ID = ?;", 3);
        assertTrue(node != null);
        node = compile("UPDATE ORDERS SET O_CARRIER_ID = ? WHERE O_ID = ? AND O_D_ID = ? AND O_W_ID = ?;", 4);
        assertTrue(node != null);
        node = compile("UPDATE ORDER_LINE SET OL_DELIVERY_D = ? WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?;", 4);
        assertTrue(node != null);
        node = compile("SELECT SUM(OL_AMOUNT) FROM ORDER_LINE WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?;", 3);
        assertTrue(node != null);
        node = compile("UPDATE CUSTOMER SET C_BALANCE = C_BALANCE + ? WHERE C_ID = ? AND C_D_ID = ? AND C_W_ID = ?;", 4);
        assertTrue(node != null);
    }

    public void testOStatSQL() {
        AbstractPlanNode node = null;
        node = compile("SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?;", 2);
        assertTrue(node != null);
        node = compile("SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM ORDERS WHERE O_W_ID = ? AND O_D_ID = ? AND O_C_ID = ? ORDER BY O_ID DESC LIMIT 1", 3);
        assertTrue(node != null);
        node = compile("SELECT OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM ORDER_LINE WHERE OL_W_ID = ? AND OL_O_ID = ? AND OL_D_ID = ?", 3);
        assertTrue(node != null);
        node = compile("SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST;", 4);
        assertTrue(node != null);
    }

    public void testPaymentSQL() {
        AbstractPlanNode node = null;
        node = compile("SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER WHERE C_ID = ? AND C_D_ID = ? AND C_W_ID = ?;", 3);
        assertTrue(node != null);
        node = compile("SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP FROM WAREHOUSE WHERE W_ID = ?;", 1);
        assertTrue(node != null);
        node = compile("UPDATE WAREHOUSE SET W_YTD = W_YTD + ? WHERE W_ID = ?;", 2);
        assertTrue(node != null);
        node = compile("SELECT D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?;", 2);
        assertTrue(node != null);
        node = compile("UPDATE DISTRICT SET D_YTD = D_YTD + ? WHERE D_W_ID = ? AND D_ID = ?;", 3);
        assertTrue(node != null);
        node = compile("UPDATE CUSTOMER SET C_BALANCE = ?, C_YTD_PAYMENT = ?, C_PAYMENT_CNT = ?, C_DATA = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?;", 7);
        assertTrue(node != null);
        node = compile("UPDATE CUSTOMER SET C_BALANCE = ?, C_YTD_PAYMENT = ?, C_PAYMENT_CNT = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?;", 6);
        assertTrue(node != null);
        node = compile("SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER WHERE C_LAST = ? AND C_D_ID = ? AND C_W_ID = ? ORDER BY C_FIRST;", 3);
        //node = compile("delete from NEW_ORDER where NO_O_ID = 1 and NO_D_ID = 1 and NO_W_ID = 1 and NO_W_ID = 3;", 0);
        assertTrue(node != null);
    }
}
