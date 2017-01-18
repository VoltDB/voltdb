/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

public class TRUPSwapTablesSP extends SwapTablesBase {

    // Default Constructor
    public TRUPSwapTablesSP () {
        super("select count(*) from trup;",
                "select count(*) from swapp;",
                "select count(*) from trup where p >= 0;",
                "select count(*) from swapp where p >= 0;",
//                "swap tables trup swapp;",
                // TODO: remove these, after SWAP TABLES is on master (as ad hoc DML)
                "truncate table tempp;",
                "insert into tempp select * from trup;",
                "truncate table trup;",
                "insert into trup select * from swapp;",
                "truncate table swapp;",
                "insert into swapp select * from tempp;",
                "select count(*) from tempr;");
    }

    public VoltTable[] run(long p, byte shouldRollback) {
        return super.run(p, shouldRollback, "TRUPSwapTablesSP");
    }

}
