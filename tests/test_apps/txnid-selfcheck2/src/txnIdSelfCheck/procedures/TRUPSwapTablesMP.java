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

package txnIdSelfCheck.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltProcedure.VoltAbortException;

/**
 * Note: this Stored Procedure class, and its base class (and "siblings"), are
 * not useful for testing the @SwapTables system stored procedure being added
 * to VoltDB V7.1; they were written when we planned to add a DML statement of
 * the form "SWAP TABLE T1 WITH T2" - SQL statementes of that type would have
 * been called within these stored procedures. They are retained here in case
 * we ever do support that DML version of Swap Tables, but they are currently
 * not called.
 */
public class TRUPSwapTablesMP extends SwapTablesBase {

    // Default Constructor
    public TRUPSwapTablesMP () {
        super("select count(*) from trup;",
                "select count(*) from swapp;",
                "select count(*) from trup where p >= 0;",
                "select count(*) from swapp where p >= 0;",
//                "swap table trup with swapp;",

                // TODO: remove these, when/if the "SWAP TABLE T1 WITH T2" DML statement
                // is on master; and uncomment the line above
                "truncate table tempp;",
                "insert into tempp select * from trup;",
                "truncate table trup;",
                "insert into trup select * from swapp;",
                "truncate table swapp;",
                "insert into swapp select * from tempp;",
                "select count(*) from tempr;"
                );
    }

    public VoltTable[] run(long p, byte shouldRollback) {
        return super.run(p, shouldRollback, "TRUPSwapTablesMP");
    }

}
