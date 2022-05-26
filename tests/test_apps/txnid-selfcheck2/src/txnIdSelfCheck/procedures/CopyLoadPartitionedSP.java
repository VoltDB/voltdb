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
import org.voltdb.VoltTable;

public class CopyLoadPartitionedSP extends CopyLoadPartitionedBase {

    private final SQLStmt selectStmt = new SQLStmt("SELECT cid,txnid,rowid from loadp WHERE cid=? ORDER BY cid LIMIT 1;");
    private final SQLStmt insertStmt = new SQLStmt("INSERT INTO  cploadp (cid, txnid, rowid) VALUES (?, ?, ?);");
    private final SQLStmt insertIntoStmt = new SQLStmt("INSERT INTO cploadp SELECT cid,txnid,rowid FROM loadp WHERE cid=? ORDER BY cid;");

    public VoltTable[] run(long cid, int useSelect, VoltTable vtable) {
        if (useSelect == 0)
            return doWork(selectStmt, insertStmt, cid, vtable);
        return doWork(selectStmt, insertIntoStmt, cid, vtable);
    }
}
