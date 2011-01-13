/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

// 1a. Insert into TABLE1
// 1b. Update TABLE2
// 1c. Update TABLE3
// 1d. Execute 1a - 1c
// 2a. If update (1b) failed, insert into TABLE2
// 2b. If update (1c) failed, insert into TABLE3
// 2c. Execute (2a) and/or (2b) if either is needed

package org.voltdb.benchmark.appupdate.procs;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

@ProcInfo(
        partitionInfo = "A.PID: 0",
        singlePartition = true
)

public class InsertA extends VoltProcedure {

    public final SQLStmt select
        = new SQLStmt("SELECT PAYLOAD from FK WHERE FK.I=?");

    public final SQLStmt insert
        = new SQLStmt("INSERT INTO A VALUES (?, ?, ?)"); // pid, int, payload

    public long run(int pid, int key) {
        // read the payload
        voltQueueSQL(select, key);
        VoltTable[] vt = voltExecuteSQL();
        vt[0].advanceRow();
        String payload= (String)(vt[0].get(0, VoltType.STRING));

        // insert the new row
        voltQueueSQL(insert, pid, key, payload);
        voltExecuteSQL();
        return 0;
    }
}
