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

package txnIdSelfCheck.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltProcedure.VoltAbortException;

public class TRURTruncateTable extends VoltProcedure {
    final SQLStmt count = new SQLStmt("select count(*) from trur;");
    final SQLStmt scancount = new SQLStmt("select count(*) from trur where p >= 0;");
    final SQLStmt truncate = new SQLStmt("truncate table trur;");

    public VoltTable[] run(long p, byte shouldRollback) {
        voltQueueSQL(truncate);
        voltQueueSQL(count);
        voltQueueSQL(scancount);
        VoltTable[] results = voltExecuteSQL(true);
        VoltTable data = results[1];
        VoltTableRow row = data.fetchRow(0);
        long optCount = row.getLong(0);
        if (optCount != 0) {
            throw new VoltAbortException("after truncate (opt) count not zero");
        }
        data = results[2];
        row = data.fetchRow(0);
        long scanCount = row.getLong(0);
        if (scanCount != 0) {
            throw new VoltAbortException("after truncate (scan) count not zero");
        }
        if (shouldRollback != 0) {
            throw new VoltAbortException("EXPECTED ROLLBACK");
        }
        return results;
    }
}
