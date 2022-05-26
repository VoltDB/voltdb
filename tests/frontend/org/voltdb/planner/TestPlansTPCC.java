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

import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;

public class TestPlansTPCC extends PlannerTestCase {

    @Override
    protected void setUp() throws Exception {
        boolean planForSinglePartitionFalse = false;
        setupSchema(TPCCProjectBuilder.class.getResource("tpcc-ddl.sql"), "testplanstpcc", planForSinglePartitionFalse);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testInserts() {
        compile("INSERT INTO WAREHOUSE VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);");
        compile("INSERT INTO DISTRICT VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");
        compile("INSERT INTO ITEM VALUES (?, ?, ?, ?, ?);");
        compile("INSERT INTO CUSTOMER VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");
        compile("INSERT INTO CUSTOMER_NAME VALUES (?, ?, ?, ?, ?);");
        compile("INSERT INTO HISTORY VALUES (?, ?, ?, ?, ?, ?, ?, ?);");
        compile("INSERT INTO STOCK VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");
        compile("INSERT INTO ORDERS VALUES (?, ?, ?, ?, ?, ?, ?, ?);");
        compile("INSERT INTO NEW_ORDER VALUES (?, ?, ?);");
        compile("INSERT INTO ORDER_LINE VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");
    }

    public void testSelectAllSQL() {
        compile("SELECT * FROM WAREHOUSE;");
    }

    public void testNewOrderSQL() {
        compile("SELECT W_TAX FROM WAREHOUSE WHERE W_ID = ?;");
        compile("SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_ID = ? AND D_W_ID = ?;");
        compile("SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?;");
        compile("SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = ?;");
        compile("SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_01 FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?;");
        compile("UPDATE DISTRICT SET D_NEXT_O_ID = ? WHERE D_ID = ? AND D_W_ID = ?;");
        compile("UPDATE STOCK SET S_QUANTITY = ?, S_YTD = ?, S_ORDER_CNT = ?, S_REMOTE_CNT = ? WHERE S_I_ID = ? AND S_W_ID = ?;");
    }

    public void testSlevSQL() {
        compile("SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?;");
        compile("SELECT COUNT(DISTINCT(OL_I_ID)) FROM ORDER_LINE, STOCK " +
                       "WHERE OL_W_ID = ? AND " +
                       "OL_D_ID = ? AND " +
                       "OL_O_ID < ? AND " +
                       "OL_O_ID >= ? AND " +
                       "S_W_ID = ? AND " +
                       "S_I_ID = OL_I_ID AND " +
                       "S_QUANTITY < ?;");
    }

    public void testDeliverySQL() {
        compile("SELECT NO_O_ID FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ? AND NO_O_ID > -1 LIMIT 1;");
        compile("DELETE FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ? AND NO_O_ID = ?;");
        compile("SELECT O_C_ID FROM ORDERS WHERE O_ID = ? AND O_D_ID = ? AND O_W_ID = ?;");
        compile("UPDATE ORDERS SET O_CARRIER_ID = ? WHERE O_ID = ? AND O_D_ID = ? AND O_W_ID = ?;");
        compile("UPDATE ORDER_LINE SET OL_DELIVERY_D = ? WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?;");
        compile("SELECT SUM(OL_AMOUNT) FROM ORDER_LINE WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?;");
        compile("UPDATE CUSTOMER SET C_BALANCE = C_BALANCE + ? WHERE C_ID = ? AND C_D_ID = ? AND C_W_ID = ?;");
    }

    public void testOStatSQL() {
        compile("SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?;");
        compile("SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM ORDERS WHERE O_W_ID = ? AND O_D_ID = ? AND O_C_ID = ? ORDER BY O_ID DESC LIMIT 1");
        compile("SELECT OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM ORDER_LINE WHERE OL_W_ID = ? AND OL_O_ID = ? AND OL_D_ID = ?");
        compile("SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST;");
    }

    public void testPaymentSQL() {
        compile("SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER WHERE C_ID = ? AND C_D_ID = ? AND C_W_ID = ?;");
        compile("SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP FROM WAREHOUSE WHERE W_ID = ?;");
        compile("UPDATE WAREHOUSE SET W_YTD = W_YTD + ? WHERE W_ID = ?;");
        compile("SELECT D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?;");
        compile("UPDATE DISTRICT SET D_YTD = D_YTD + ? WHERE D_W_ID = ? AND D_ID = ?;");
        compile("UPDATE CUSTOMER SET C_BALANCE = ?, C_YTD_PAYMENT = ?, C_PAYMENT_CNT = ?, C_DATA = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?;");
        compile("UPDATE CUSTOMER SET C_BALANCE = ?, C_YTD_PAYMENT = ?, C_PAYMENT_CNT = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?;");
        compile("SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER WHERE C_LAST = ? AND C_D_ID = ? AND C_W_ID = ? ORDER BY C_FIRST;");
        //compile("delete from NEW_ORDER where NO_O_ID = 1 and NO_D_ID = 1 and NO_W_ID = 1 and NO_W_ID = 3;");
    }
}
