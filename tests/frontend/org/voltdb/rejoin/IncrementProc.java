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

package org.voltdb.rejoin;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

/**
 * Used to test pause-less rejoin under load.
 * If the data diverges, this should crash VoltDB.
 */
public class IncrementProc extends VoltProcedure {
    public final static byte USER_ABORT = 66;

    public final SQLStmt read = new SQLStmt(
            "select ival from blah_replicated order by ival limit 1;");
    public final SQLStmt write = new SQLStmt(
            "update blah_replicated set ival = ? where ival = ?;");

    public VoltTable[] run() {
        // batch one does a read
        voltQueueSQL(read, EXPECT_SCALAR_LONG);
        long current = voltExecuteSQL()[0].asScalarLong();

        // batch 2 does a write based on the read
        voltQueueSQL(write, EXPECT_SCALAR_MATCH(1), current + 1, current);
        voltExecuteSQL();

        if (getSeededRandomNumberGenerator().nextBoolean()) {
            setAppStatusCode(USER_ABORT);
            throw new VoltAbortException("Decided to roll this baby back y'all!");
        }

        // batch 2 just does a read
        voltQueueSQL(read, EXPECT_SCALAR_MATCH(current + 1));
        voltExecuteSQL();

        // batch three does a write and a read together
        voltQueueSQL(read, EXPECT_SCALAR_MATCH(current + 1));
        voltQueueSQL(write, EXPECT_SCALAR_MATCH(1), current + 2, current + 1);
        VoltTable[] results = voltExecuteSQL();

        return results;
    }
}
