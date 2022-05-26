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

public class DeleteLoadPartitionedBase extends DeleteOnlyLoadBase {

    public long doWork(SQLStmt select, SQLStmt delete, SQLStmt selectcp, SQLStmt deletecp, long cid, VoltTable vtable) {

        // "base" table
        VoltTable[] results = doWork(select, delete, cid, vtable);
        long del = results[0].asScalarLong();
        if (del != 1)
            throw new VoltAbortException("base table incorrect number of deleted rows=" + del + " for cid=" + cid);

        // "copied" table
        results = doWork(selectcp, deletecp, cid, vtable);
        long delcp = results[0].asScalarLong();
        if (delcp != 1)
            throw new VoltAbortException("cpy table incorrect number of deleted rows=" + delcp + " for cid=" + cid);

        // assemble the return code as a 2 bit bitmap
        return (del>0?2:0) + (delcp>0?1:0);
    }

    public long run() {
        return 0; // never called in base procedure
    }

}
