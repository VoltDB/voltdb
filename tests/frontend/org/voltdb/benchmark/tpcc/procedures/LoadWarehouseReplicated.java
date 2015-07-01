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

package org.voltdb.benchmark.tpcc.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

@ProcInfo (
    partitionInfo = "WAREHOUSE.W_ID: 0",
    singlePartition = true
)
/**
 * Loads initial data into replicated TPCC tables.
 */
public class LoadWarehouseReplicated extends VoltProcedure {

    public final SQLStmt checkItemExists = new SQLStmt("SELECT * FROM ITEM LIMIT 1");

    public VoltTable[] run(short w_id, VoltTable items, VoltTable customerNames)
    throws VoltAbortException {
        if (items != null) {
            // check if we've already set up this partition
            voltQueueSQL(checkItemExists);
            VoltTable item = voltExecuteSQL()[0];
            if (item.getRowCount() > 0)
                return null;

            // now we know the partition is not loaded yet
            voltLoadTable("cluster", "database", "ITEM", items, false, false);
        }
        voltLoadTable("cluster", "database", "CUSTOMER_NAME", customerNames, false, false);
        return null;
    }

}
