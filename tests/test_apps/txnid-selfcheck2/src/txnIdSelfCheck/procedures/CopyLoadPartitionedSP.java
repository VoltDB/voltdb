/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

public class CopyLoadPartitionedSP extends VoltProcedure {

    private final SQLStmt selectStmt = new SQLStmt("SELECT cid,txnid,rowid from loadp WHERE cid=? ORDER BY cid;");
    private final SQLStmt insertStmt = new SQLStmt("INSERT INTO  cploadp VALUES (?, ?, ?);");

    public VoltTable[] run(long cid) {
        // Get row for cid and copy to new table.
        voltQueueSQL(selectStmt, cid);
        VoltTable[] results = voltExecuteSQL();
        if (results == null || results.length != 1) {
            throw new VoltAbortException("Failed to find cid that should exist.");
        }
        VoltTable data = results[0];
        if (data == null) {
            throw new VoltAbortException("Failed to find cid that should exist.");
        }
        data.advanceRow();
        long rcid = data.getLong(0);
        long txnid = data.getLong(1);
        long rowid = data.getLong(2);
        voltQueueSQL(insertStmt, rcid, txnid, rowid);
        return voltExecuteSQL();
    }
}
