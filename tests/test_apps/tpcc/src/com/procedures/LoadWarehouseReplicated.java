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

package com.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

/**
 * Loads initial data into replicated TPCC tables.
 */
public class LoadWarehouseReplicated extends VoltProcedure {

    public final static SQLStmt insertIntoItem = new SQLStmt("INSERT INTO ITEM VALUES (?, ?, ?, ?, ?);");
    public final static SQLStmt insertIntoCustomerName = new SQLStmt("INSERT INTO CUSTOMER_NAME VALUES (?, ?, ?, ?, ?);");

    @SuppressWarnings("deprecation")
    public VoltTable[] run(VoltTable items, VoltTable customerNames)
    throws VoltAbortException {

        if (items != null) {
            loadTable(items, insertIntoItem);
        }

        if (customerNames != null) {
            loadTable(customerNames, insertIntoCustomerName);
        }

        return null;
    }

    private void loadTable(VoltTable sourceTable, SQLStmt stmt) {
        final int BATCH_SIZE = 100;

        int i = 0;
        while (sourceTable.advanceRow()) {
            ++i;
            voltQueueSQL(stmt,
                    sourceTable.get(0, sourceTable.getColumnType(0)),
                    sourceTable.get(1, sourceTable.getColumnType(1)),
                    sourceTable.get(2, sourceTable.getColumnType(2)),
                    sourceTable.get(3, sourceTable.getColumnType(3)),
                    sourceTable.get(4, sourceTable.getColumnType(4)));
            if (i % BATCH_SIZE == 0) {
                voltExecuteSQL();
            }
        }

        if (i % BATCH_SIZE != 0) {
            voltExecuteSQL();
        }
    }

}
