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

package com;

import com.procedures.*;

/** Holds TPC-C constants.  */
public final class Constants {
    private Constants() { assert false; }

    // 2 digits after the decimal point for money types
    public static final int MONEY_DECIMALS = 2;

    // Item constants
    public static final int NUM_ITEMS = 100000;
    public static final int MIN_IM = 1;
    public static final int MAX_IM = 10000;
    public static final double MIN_PRICE = 1.00;
    public static final double MAX_PRICE = 100.00;
    public static final int MIN_I_NAME = 14;
    public static final int MAX_I_NAME = 24;
    public static final int MIN_I_DATA = 26;
    public static final int MAX_I_DATA = 50;

    // Warehouse constants
    public static final double MIN_TAX = 0;
    public static final double MAX_TAX = 0.2000;
    public static final int TAX_DECIMALS = 4;
    public static final double INITIAL_W_YTD = 300000.00;
    public static final int MIN_NAME = 6;
    public static final int MAX_NAME = 10;
    public static final int MIN_STREET = 10;
    public static final int MAX_STREET = 20;
    public static final int MIN_CITY = 10;
    public static final int MAX_CITY = 20;
    public static final int STATE = 2;
    public static final int ZIP_LENGTH = 9;
    public static final String ZIP_SUFFIX = "11111";

    // Stock constants
    public static final int MIN_QUANTITY = 10;
    public static final int MAX_QUANTITY = 100;
    public static final int DIST = 24;
    public static final int STOCK_PER_WAREHOUSE = 100000;

    // District constants
    public static final int DISTRICTS_PER_WAREHOUSE = 10;
    public static final double INITIAL_D_YTD = 30000.00;  // different from Warehouse
    public static final int INITIAL_NEXT_O_ID = 3001;

    // Customer constants
    public static final int CUSTOMERS_PER_DISTRICT = 3000;
    public static final double INITIAL_CREDIT_LIM = 50000.00;
    public static final double MIN_DISCOUNT = 0.0000;
    public static final double MAX_DISCOUNT = 0.5000;
    public static final int DISCOUNT_DECIMALS = 4;
    public static final double INITIAL_BALANCE = -10.00;
    public static final double INITIAL_YTD_PAYMENT = 10.00;
    public static final int INITIAL_PAYMENT_CNT = 1;
    public static final int INITIAL_DELIVERY_CNT = 0;
    public static final int MIN_FIRST = 6;
    public static final int MAX_FIRST = 10;
    public static final String MIDDLE = "OE";
    public static final int PHONE = 16;
    public static final int MIN_C_DATA = 300;
    public static final int MAX_C_DATA = 500;
    public static final String GOOD_CREDIT = "GC";
    public static final String BAD_CREDIT = "BC";
    public static final byte[] BAD_CREDIT_BYTES = BAD_CREDIT.getBytes();

    // Order constants
    public static final int MIN_CARRIER_ID = 1;
    public static final int MAX_CARRIER_ID = 10;
    // HACK: This is not strictly correct, but it works
    public static final long NULL_CARRIER_ID = 0L;
    // o_id < than this value, carrier != null, >= -> carrier == null
    public static final int NULL_CARRIER_LOWER_BOUND = 2101;
    public static final int MIN_OL_CNT = 5;
    public static final int MAX_OL_CNT = 15;
    public static final int INITIAL_ALL_LOCAL = 1;
    public static final int INITIAL_ORDERS_PER_DISTRICT = 3000;
    // Used to generate new order transactions
    public static final int MAX_OL_QUANTITY = 10;

    // Order line constants
    public static final int INITIAL_QUANTITY = 5;
    public static final double MIN_AMOUNT = 0.01;

    // History constants
    public static final int MIN_DATA = 12;
    public static final int MAX_DATA = 24;
    public static final double INITIAL_AMOUNT = 10.00f;

    // New order constants
    public static final int INITIAL_NEW_ORDERS_PER_DISTRICT = 900;

    // TPC-C 2.4.3.4 (page 31) says this must be displayed when new order rolls back.
    public static final String INVALID_ITEM_MESSAGE = "Item number is not valid";

    // Used to generate stock level transactions
    public static final int MIN_STOCK_LEVEL_THRESHOLD = 10;
    public static final int MAX_STOCK_LEVEL_THRESHOLD = 20;

    // Used to generate payment transactions
    public static final double MIN_PAYMENT = 1.0;
    public static final double MAX_PAYMENT = 5000.0;

    // Indicates "brand" items and stock in i_data and s_data.
    public static final String ORIGINAL_STRING = "ORIGINAL";
    public static final byte[] ORIGINAL_BYTES = ORIGINAL_STRING.getBytes();

    // It turns out that getSimpleName is slow: it calls getEnclosingClass and other crap. These
    // constants mean that if a name changes, the compile breaks, while not wasting time looking up
    // the names.
    public static final String INSERT_ITEM = "InsertItem";
    public static final String INSERT_STOCK = "InsertStock";
    public static final String INSERT_WAREHOUSE = "InsertWarehouse";
    public static final String INSERT_DISTRICT = "InsertDistrict";
    public static final String INSERT_CUSTOMER = "InsertCustomer";
    public static final String INSERT_CUSTOMER_NAME = "InsertCustomerName";
    public static final String INSERT_ORDERS = "InsertOrders";
    public static final String INSERT_ORDER_LINE = "InsertOrderLine";
    public static final String INSERT_NEW_ORDER = "InsertNewOrder";
    public static final String INSERT_HISTORY = "InsertHistory";
    public static final String LOAD_WAREHOUSE = LoadWarehouse.class.getSimpleName();
    public static final String LOAD_WAREHOUSE_REPLICATED = LoadWarehouseReplicated.class.getSimpleName();

    public static final String ORDER_STATUS_BY_NAME = ostatByCustomerName.class.getSimpleName();
    public static final String ORDER_STATUS_BY_ID = ostatByCustomerId.class.getSimpleName();
    public static final String DELIVERY = delivery.class.getSimpleName();
    public static final String PAYMENT_BY_NAME = paymentByCustomerName.class.getSimpleName();
    public static final String PAYMENT_BY_NAME_C = paymentByCustomerNameC.class.getSimpleName();
    public static final String PAYMENT_BY_NAME_W = paymentByCustomerNameW.class.getSimpleName();
    public static final String PAYMENT_BY_ID = paymentByCustomerId.class.getSimpleName();
    public static final String PAYMENT_BY_ID_C = paymentByCustomerIdC.class.getSimpleName();
    public static final String PAYMENT_BY_ID_W = paymentByCustomerIdW.class.getSimpleName();
    public static final String NEWORDER = neworder.class.getSimpleName();
    public static final String STOCK_LEVEL = slev.class.getSimpleName();
    //public static final String STOCK_LEVEL = slevIterative.class.getSimpleName();

    //public static final String MEASUREOVERHEAD = measureOverhead.class.getSimpleName();
    public static final String RESET_WAREHOUSE = ResetWarehouse.class.getSimpleName();
    public static final String[] TRANS_PROCS =
        {STOCK_LEVEL, NEWORDER, PAYMENT_BY_NAME, PAYMENT_BY_NAME_C,
         PAYMENT_BY_ID, PAYMENT_BY_ID_C,
         DELIVERY, ORDER_STATUS_BY_NAME, ORDER_STATUS_BY_ID};
}
