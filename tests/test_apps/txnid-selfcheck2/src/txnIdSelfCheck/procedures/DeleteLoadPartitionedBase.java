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

public class DeleteLoadPartitionedBase extends VoltProcedure {

    public long doWork(SQLStmt delete, SQLStmt deletecp, long cid) {

        voltQueueSQL(delete, cid);
        VoltTable[] results = voltExecuteSQL();
        long r = results[0].asScalarLong();
        if (r != 1) {
            throw new VoltAbortException("Failed to delete cid that should exist: deleted=" + r + " cid=" + cid);
        }
        voltQueueSQL(deletecp, cid);
        results = voltExecuteSQL();
        r = results[0].asScalarLong();
        if (r != 1) {
            throw new VoltAbortException("Failed to delete cpcid that should exist: deleted=" + r + " cpcid=" + cid);
        }
        return 2;
    }

    public long run() {
        return 0; // never called in base procedure
    }

}
