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

package org.voltdb_testprocs.regressionsuites.catchexceptions;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;

public class MPCatchRethrowOnPartitionTable extends VoltProcedure {

    public final SQLStmt insertP1 = new SQLStmt("insert into P1 (ID, ratio) values (?, ?)");

    private boolean isTrue(int value) {
        return value == 0 ? false: true;
    }

    // use a partition key here to put all data insert into one partition
    public long run(int tryCatchContains1Batch) {
        int result = 0;

        voltQueueSQL(insertP1, 0, 0.1);
        voltExecuteSQL();

        try {
            voltQueueSQL(insertP1, 1, 1.1);
            if (! isTrue(tryCatchContains1Batch)) {
                voltExecuteSQL();
            }
            // second insert will violate the primary key constraint
            voltQueueSQL(insertP1, 1, 1.2);
            voltExecuteSQL();
        } catch (Exception e) {
            result = -1;
            throw new VoltAbortException("User's MP constraint error message");
        }

        voltQueueSQL(insertP1, 500, 500.1);
        voltExecuteSQL();

        return result;
    }
}
