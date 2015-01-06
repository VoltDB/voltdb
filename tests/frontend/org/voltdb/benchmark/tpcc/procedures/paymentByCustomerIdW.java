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

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;


/**
 * Multi-partition version of {@link paymentByCustomerId} split for
 * Warehouse/District related queries. See deps.pdf for dependencies between the queries
 * in original paymentByCustomerId.
 */
@ProcInfo (
    partitionInfo = "WAREHOUSE.W_ID: 0", //partition on W_ID
    singlePartition = true
)
public class paymentByCustomerIdW extends VoltProcedure {

    public final SQLStmt getWarehouse = new SQLStmt("SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP FROM WAREHOUSE WHERE W_ID = ?;"); //w_id
    private final int W_NAME_IDX = 0;

    //Does this work?
    public final SQLStmt updateWarehouseBalance = new SQLStmt("UPDATE WAREHOUSE SET W_YTD = W_YTD + ? WHERE W_ID = ?;"); //h_amount, w_id

    public final SQLStmt getDistrict = new SQLStmt("SELECT D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?;"); //w_id, d_id
    private final int D_NAME_IDX = 0;

    //Does this work?
    //h_amount, d_w_id, d_id
    public final SQLStmt updateDistrictBalance = new SQLStmt("UPDATE DISTRICT SET D_YTD = D_YTD + ? WHERE D_W_ID = ? AND D_ID = ?;");

    public final SQLStmt insertHistory = new SQLStmt("INSERT INTO HISTORY VALUES (?, ?, ?, ?, ?, ?, ?, ?);");

    public VoltTable[] processPayment(short w_id, byte d_id, double h_amount, short c_w_id, byte c_d_id, int c_id, TimestampType timestamp) {

        voltQueueSQL(getWarehouse, w_id);
        voltQueueSQL(getDistrict, w_id, d_id);
        final VoltTable[] results = voltExecuteSQL();
        final VoltTable warehouse = results[0];
        final VoltTable district = results[1];

        voltQueueSQL(updateWarehouseBalance, h_amount, w_id);
        voltQueueSQL(updateDistrictBalance, h_amount, w_id, d_id);
        voltExecuteSQL();

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

        return new VoltTable[]{warehouse, district};
    }

    public VoltTable[] run(short w_id, byte d_id, double h_amount, short c_w_id, byte c_d_id, int c_id, TimestampType timestamp) {
        return processPayment(w_id, d_id, h_amount, c_w_id, c_d_id, c_id, timestamp);
    }
}
