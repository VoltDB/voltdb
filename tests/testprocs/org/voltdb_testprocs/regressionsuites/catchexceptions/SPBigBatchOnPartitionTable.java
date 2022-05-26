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

public class SPBigBatchOnPartitionTable extends VoltProcedure {

    public final SQLStmt insertP1 = new SQLStmt("insert into P1 (ID, ratio) values (?, ?)");

    private boolean isTrue(int value) {
        return value == 0 ? false: true;
    }

    // use a partition key here to put all data insert into one partition
    public long run(int partitionKey, int hasPreviousBatch, int duplicatedID,
            int hasFollowingBatch, int followingBatchHasException) {
        int result = 0;

        if (isTrue(hasPreviousBatch)) {
            voltQueueSQL(insertP1, 0, 0.1);
            voltExecuteSQL();
        }

        // VoltDB break large batch with 200 units
        // 300 here will be be broke into two batches at least
        try {
            for (int i = 1; i <= 300; i++) {
                voltQueueSQL(insertP1, i, 10.1);

                if (i == duplicatedID) {
                    voltQueueSQL(insertP1, i, 10.2);
                }
            }
            voltExecuteSQL();
        } catch (Exception e) {
            result = -1;
        }

        if (isTrue(hasFollowingBatch)) {
            voltQueueSQL(insertP1, 500, 500.1);
            if (isTrue(followingBatchHasException)) {
                voltQueueSQL(insertP1, 500, 500.2);
            }
            voltExecuteSQL();
        }

        return result;
    }
}
