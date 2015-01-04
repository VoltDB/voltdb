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
package org.voltdb.benchmark.tpcc.procedures;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.benchmark.tpcc.Constants;
import org.voltdb.types.TimestampType;

//Notes on Stored Procedure:
//Does Transaction in slightly different order than Profile specifies--gets the customer first.
//return VoltTable[] has N element(s):
//1) var_name, represented as a NxN table representing typeFOOBAR.

@ProcInfo (
    //partitionInfo = "WAREHOUSE.W_ID: 0",
    singlePartition = false
)
public class paymentByCustomerName extends VoltProcedure {

    final int misc_expected_string_len = 32 + 2 + 32 + 32 + 32 + 32 + 2 + 9 + 32 + 2 + 500;

    final VoltTable misc_template = new VoltTable(
            new VoltTable.ColumnInfo("c_id", VoltType.INTEGER),
            new VoltTable.ColumnInfo("c_first", VoltType.STRING),
            new VoltTable.ColumnInfo("c_middle", VoltType.STRING),
            new VoltTable.ColumnInfo("c_last", VoltType.STRING),
            new VoltTable.ColumnInfo("c_street_1", VoltType.STRING),
            new VoltTable.ColumnInfo("c_street_2", VoltType.STRING),
            new VoltTable.ColumnInfo("c_city", VoltType.STRING),
            new VoltTable.ColumnInfo("c_state", VoltType.STRING),
            new VoltTable.ColumnInfo("c_zip", VoltType.STRING),
            new VoltTable.ColumnInfo("c_phone", VoltType.STRING),
            new VoltTable.ColumnInfo("c_since", VoltType.TIMESTAMP),
            new VoltTable.ColumnInfo("c_credit", VoltType.STRING),
            new VoltTable.ColumnInfo("c_credit_lim", VoltType.FLOAT),
            new VoltTable.ColumnInfo("c_discount", VoltType.FLOAT),
            new VoltTable.ColumnInfo("c_balance", VoltType.FLOAT),
            new VoltTable.ColumnInfo("c_data", VoltType.STRING)
    );

    private final int C_ID_IDX = 0;
    private final int C_FIRST_IDX = 1;
    private final int C_MIDDLE_IDX = 2;
    private final int C_LAST_IDX = 3;
    private final int C_STREET_1_IDX = 4;
    private final int C_STREET_2_IDX = 5;
    private final int C_CITY_IDX = 6;
    private final int C_STATE_IDX = 7;
    private final int C_ZIP_IDX = 8;
    private final int C_PHONE_IDX = 9;
    private final int C_SINCE_IDX = 10;
    private final int C_CREDIT_IDX = 11;
    private final int C_CREDIT_LIM_IDX = 12;
    private final int C_DISCOUNT_IDX = 13;
    private final int C_BALANCE_IDX = 14;
    private final int C_YTD_PAYMENT_IDX = 15;
    private final int C_PAYMENT_CNT_IDX = 16;
    private final int C_DATA_IDX = 17;

    public final SQLStmt getWarehouse = new SQLStmt("SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP FROM WAREHOUSE WHERE W_ID = ?;"); //w_id
    private final int W_NAME_IDX = 0;

    public final SQLStmt updateWarehouseBalance = new SQLStmt("UPDATE WAREHOUSE SET W_YTD = W_YTD + ? WHERE W_ID = ?;"); //h_amount, w_id

    public final SQLStmt getDistrict = new SQLStmt("SELECT D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?;"); //w_id, d_id
    private final int D_NAME_IDX = 0;

    //Does this work?
    //h_amount, d_w_id, d_id
    public final SQLStmt updateDistrictBalance = new SQLStmt("UPDATE DISTRICT SET D_YTD = D_YTD + ? WHERE D_W_ID = ? AND D_ID = ?;");

    public final SQLStmt updateBCCustomer = new SQLStmt("UPDATE CUSTOMER SET C_BALANCE = ?, C_YTD_PAYMENT = ?, C_PAYMENT_CNT = ?, C_DATA = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?;"); //c_balance, c_ytd_payment, c_payment_cnt, c_data, c_w_id, c_d_id, c_id

    public final SQLStmt updateGCCustomer = new SQLStmt("UPDATE CUSTOMER SET C_BALANCE = ?, C_YTD_PAYMENT = ?, C_PAYMENT_CNT = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?;"); //c_balance, c_ytd_payment, c_payment_cnt, c_w_id, c_d_id, c_id

    public final SQLStmt getCustomersByLastName = new SQLStmt("SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER WHERE C_LAST = ? AND C_D_ID = ? AND C_W_ID = ? ORDER BY C_FIRST;");// c_last, d_id, w_id

    public final SQLStmt insertHistory = new SQLStmt("INSERT INTO HISTORY VALUES (?, ?, ?, ?, ?, ?, ?, ?);");

    public VoltTable[] processPayment(short w_id, byte d_id, short c_w_id, byte c_d_id, int c_id, double h_amount, VoltTableRow customer, TimestampType timestamp) {
        voltQueueSQL(getWarehouse, w_id);
        voltQueueSQL(getDistrict, w_id, d_id);
        final VoltTable[] results = voltExecuteSQL();
        final VoltTable warehouse = results[0];
        final VoltTable district = results[1];

        voltQueueSQL(updateWarehouseBalance, h_amount, w_id);
        voltQueueSQL(updateDistrictBalance, h_amount, w_id, d_id);
        voltExecuteSQL();

        //do stuff to extract district and warehouse info.

        //customer info
        final byte[] c_first = customer.getStringAsBytes(C_FIRST_IDX);
        final byte[] c_middle = customer.getStringAsBytes(C_MIDDLE_IDX);
        final byte[] c_last = customer.getStringAsBytes(C_LAST_IDX);
        final byte[] c_street_1 = customer.getStringAsBytes(C_STREET_1_IDX);
        final byte[] c_street_2 = customer.getStringAsBytes(C_STREET_2_IDX);
        final byte[] c_city = customer.getStringAsBytes(C_CITY_IDX);
        final byte[] c_state = customer.getStringAsBytes(C_STATE_IDX);
        final byte[] c_zip = customer.getStringAsBytes(C_ZIP_IDX);
        final byte[] c_phone = customer.getStringAsBytes(C_PHONE_IDX);
        final TimestampType c_since = customer.getTimestampAsTimestamp(C_SINCE_IDX);
        final byte[] c_credit = customer.getStringAsBytes(C_CREDIT_IDX);
        final double c_credit_lim = customer.getDouble(C_CREDIT_LIM_IDX);
        final double c_discount = customer.getDouble(C_DISCOUNT_IDX);
        final double c_balance = customer.getDouble(C_BALANCE_IDX) - h_amount;
        final double c_ytd_payment = customer.getDouble(C_YTD_PAYMENT_IDX) + h_amount;
        final int c_payment_cnt = (int)customer.getLong(C_PAYMENT_CNT_IDX) + 1;
        byte[] c_data;
        if (Arrays.equals(c_credit, Constants.BAD_CREDIT_BYTES)) {
            c_data = customer.getStringAsBytes(C_DATA_IDX);
            byte[] newData = (c_id + " " + c_d_id + " " + c_w_id + " " + d_id + " " + w_id  + " " + h_amount + "|").getBytes();

            int newLength = newData.length + c_data.length;
            if (newLength > Constants.MAX_C_DATA) {
                newLength = Constants.MAX_C_DATA;
            }
            ByteBuilder builder = new ByteBuilder(newLength);

            int minLength = newLength;
            if (newData.length < minLength) minLength = newData.length;
            builder.append(newData, 0, minLength);

            int remaining = newLength - minLength;
            builder.append(c_data, 0, remaining);
            c_data = builder.array();
            voltQueueSQL(updateBCCustomer, c_balance, c_ytd_payment, c_payment_cnt, c_data, c_w_id, c_d_id, c_id);
        }
        else{
            c_data = new byte[0];
            voltQueueSQL(updateGCCustomer, c_balance, c_ytd_payment, c_payment_cnt, c_w_id, c_d_id, c_id);
        }

        // Concatenate w_name, four spaces, d_name
        byte[] w_name = warehouse.fetchRow(0).getStringAsBytes(W_NAME_IDX);
        final byte[] FOUR_SPACES = { ' ', ' ', ' ', ' ' };
        byte[] d_name = district.fetchRow(0).getStringAsBytes(D_NAME_IDX);
        ByteBuilder builder = new ByteBuilder(w_name.length + FOUR_SPACES.length + d_name.length);
        builder.append(w_name);
        builder.append(FOUR_SPACES);
        builder.append(d_name);
        byte[] h_data = builder.array();

        // Create the history record
        voltQueueSQL(insertHistory, c_id, c_d_id, c_w_id, d_id, w_id, timestamp, h_amount, h_data);
        voltExecuteSQL();

        // TPC-C 2.5.3.3: Must display the following fields:
        // W_ID, D_ID, C_ID, C_D_ID, C_W_ID, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP,
        // D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1,
        // C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM,
        // C_DISCOUNT, C_BALANCE, the first 200 characters of C_DATA (only if C_CREDIT = "BC"),
        // H_AMOUNT, and H_DATE.

        // Return the entire warehouse and district tuples. The client provided:
        // w_id, d_id, c_d_id, c_w_id, h_amount, h_data.
        // Build a table for the rest
        final VoltTable misc = misc_template.clone(1024);
        misc.addRow(c_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip,
                c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_data);

        // Hand back all the warehouse, district, and customer data
        return new VoltTable[]{warehouse, district, misc};
    }

    public VoltTable[] run(short w_id, byte d_id, double h_amount, short c_w_id, byte c_d_id, byte[] c_last, TimestampType timestamp) throws VoltAbortException {
        voltQueueSQL(getCustomersByLastName, c_last, c_d_id, c_w_id);
        final VoltTable customers = voltExecuteSQL()[0];
        final int namecnt = customers.getRowCount();
        if (namecnt == 0) {
            String c_last_str = "";
            try { c_last_str = new String(c_last, "UTF-8"); }
            catch (UnsupportedEncodingException e) {}
            throw new VoltAbortException("no customers with last name: " + c_last_str + " in warehouse: "
                    + c_w_id + " and in district " + c_d_id);
        }

        final int index = (namecnt-1)/2;
        final VoltTableRow customer = customers.fetchRow(index);
        final int c_id = (int)customer.getLong(C_ID_IDX);
        return processPayment(w_id, d_id, c_w_id, c_d_id, c_id, h_amount, customer, timestamp);
    }
}
