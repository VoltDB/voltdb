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

package org.voltdb_testprocs.regressionsuites.fallbackbuffers;

import java.util.Arrays;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;

public class SPPopulatePartitionTable extends VoltProcedure {

    static {
        final int ARRAY_SIZE = 1048576;
        char[] genArray = new char[ARRAY_SIZE];
        Arrays.fill(genArray, '1');
        generatedStr = new String(genArray);
    }

    private static final String generatedStr;

    public final SQLStmt insertP1 = new SQLStmt("insert into P1 (ID, str) values (?, ?)");

    // use a partition key here to put all data insert into one partition
    public long run(int partitionKey, int rowCount) {
        int result = 0;

        // VoltDB break large batch with 200 units
        // 300 here will be be broke into two batches at least
        try {
            for (int i = 1; i <= rowCount; i++) {
                voltQueueSQL(insertP1, i, generatedStr);
            }
            voltExecuteSQL();
        } catch (Exception e) {
            result = -1;
        }

        return result;
    }
}
