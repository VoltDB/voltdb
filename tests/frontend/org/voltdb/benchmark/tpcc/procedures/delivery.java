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
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.benchmark.tpcc.Constants;
import org.voltdb.types.TimestampType;

@ProcInfo (
    partitionInfo = "WAREHOUSE.W_ID: 0",
    singlePartition = true
)
public class delivery extends VoltProcedure {
    private final VoltTable result_template = new VoltTable(
        new VoltTable.ColumnInfo("d_id", VoltType.TINYINT),
        new VoltTable.ColumnInfo("o_id", VoltType.INTEGER)
    );

    //BLOCK: Replaces use of cursor with a limited select.
    public final SQLStmt getNewOrder =
        new SQLStmt("SELECT NO_O_ID FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ? AND NO_O_ID > -1 LIMIT 1;");

    public final SQLStmt deleteNewOrder =
        new SQLStmt("DELETE FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ? AND NO_O_ID = ?;"); // d_id, w_id, no_o_id
    //END OF BLOCK

    public final SQLStmt getCId =
        new SQLStmt("SELECT O_C_ID FROM ORDERS WHERE O_ID = ? AND O_D_ID = ? AND O_W_ID = ?;"); //no_o_id, d_id, w_id
    //into c_id

    public final SQLStmt updateOrders =
        new SQLStmt("UPDATE ORDERS SET O_CARRIER_ID = ? WHERE O_ID = ? AND O_D_ID = ? AND O_W_ID = ?;"); //o_carrier_id, no_o_id, d_id, w_id

    public final SQLStmt updateOrderLine =
        new SQLStmt("UPDATE ORDER_LINE SET OL_DELIVERY_D = ? WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?;"); //timestamp, no_o_id, d_id, w_id

    //is ol_amount a column? assuming so (and that this sum() is being supported)
    public final SQLStmt sumOLAmount =
        new SQLStmt("SELECT SUM(OL_AMOUNT) FROM ORDER_LINE WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?;");//no_o_id, d_id, w_id
    //into ol_total

    public final SQLStmt updateCustomer =
        new SQLStmt("UPDATE CUSTOMER SET C_BALANCE = C_BALANCE + ? WHERE C_ID = ? AND C_D_ID = ? AND C_W_ID = ?;"); //ol_total, c_id, d_id, w_id

    public VoltTable run(short w_id, int o_carrier_id, TimestampType timestamp) throws VoltAbortException {
        for (long d_id  = 1; d_id <= Constants.DISTRICTS_PER_WAREHOUSE; ++d_id) {
            voltQueueSQL(getNewOrder, d_id, w_id);
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
            voltQueueSQL(getCId, no_o_id, d_id, w_id);
            voltQueueSQL(sumOLAmount, no_o_id, d_id, w_id);
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
            voltQueueSQL(deleteNewOrder, d_id, w_id, no_o_id);
            voltQueueSQL(updateOrders, o_carrier_id, no_o_id, d_id, w_id);
            voltQueueSQL(updateOrderLine, timestamp, no_o_id, d_id, w_id);
        }
        voltExecuteSQL();

        // these must be logged in the "result file" according to TPC-C 2.7.2.2 (page 39)
        // We remove the queued time, completed time, w_id, and o_carrier_id: the client can figure
        // them out
        final VoltTable result = result_template.clone(1024);
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

            voltQueueSQL(updateCustomer, ol_total, c_id, d_id, w_id);

            final Long no_o_id = no_o_ids[(int) d_id - 1];
            result.addRow(d_id, no_o_id);
        }
        voltExecuteSQL();

        return result;
    }
}
