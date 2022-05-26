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
package com.procedures;

import org.voltdb.DeprecatedProcedureAPIAccess;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

/**
 * Loads initial data into TPCC tables.
 */
public class LoadWarehouse extends VoltProcedure {

    public static final SQLStmt writeStmt = new SQLStmt("INSERT INTO WAREHOUSE VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);");

    @SuppressWarnings("deprecation")
    public VoltTable[] run(short w_id, VoltTable warehouses, VoltTable districts, VoltTable customers,
        VoltTable stocks, VoltTable orders, VoltTable neworders, VoltTable orderLines, VoltTable histories)
    throws VoltAbortException {
        DeprecatedProcedureAPIAccess.voltLoadTable(this, "WAREHOUSE", warehouses);
        DeprecatedProcedureAPIAccess.voltLoadTable(this, "DISTRICT", districts);
        DeprecatedProcedureAPIAccess.voltLoadTable(this, "CUSTOMER", customers);
        DeprecatedProcedureAPIAccess.voltLoadTable(this, "STOCK", stocks);
        DeprecatedProcedureAPIAccess.voltLoadTable(this, "ORDERS", orders);
        DeprecatedProcedureAPIAccess.voltLoadTable(this, "NEW_ORDER", neworders);
        DeprecatedProcedureAPIAccess.voltLoadTable(this, "ORDER_LINE", orderLines);
        DeprecatedProcedureAPIAccess.voltLoadTable(this, "HISTORY", histories);
        return null;
    }
}
