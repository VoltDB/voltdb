/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
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

package org.voltdb.benchmark.tpcc.procedures;


import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class ResetWarehouse extends VoltProcedure {
    // w_id, last valid o_id
    public final SQLStmt cleanOrders =
        new SQLStmt("DELETE FROM ORDERS WHERE O_W_ID = ? AND O_ID > ?;");

    // w_id, last valid o_id
    public final SQLStmt cleanOrderLines =
        new SQLStmt("DELETE FROM ORDER_LINE WHERE OL_W_ID = ? AND OL_O_ID > ?;");

    public final SQLStmt cleanNewOrder =
        new SQLStmt("DELETE FROM NEW_ORDER WHERE NO_W_ID = ?;");

    // TODO(evanj): This erases the entire history table. This isn't 100%
    // correct, since this isn't "warehouse specific", but it does the job.
    // Since we never read it, it probably doesn't matter.
    public final SQLStmt cleanHistory =
        new SQLStmt("DELETE FROM HISTORY;");

    public final SQLStmt insertNewOrder =
        new SQLStmt("INSERT INTO NEW_ORDER VALUES (?, ?, ?);");

    public VoltTable[] run(short w_id, long districtsPerWarehouse, long customersPerDistrict, long newOrdersPerDistrict) {
        voltQueueSQL(cleanOrders, w_id, customersPerDistrict);
        voltQueueSQL(cleanOrderLines, w_id, customersPerDistrict);
        voltQueueSQL(cleanNewOrder, w_id);
        voltQueueSQL(cleanHistory);
        voltExecuteSQL();

        // Recreate the new orders table
        for (long d_id = 1; d_id <= districtsPerWarehouse; ++d_id) {
            for (long o_id = customersPerDistrict - newOrdersPerDistrict + 1;
                    o_id <= customersPerDistrict; ++o_id) {
                voltQueueSQL(insertNewOrder, o_id, d_id, w_id);
                voltExecuteSQL();
            }
        }

        return null;
    }
}
