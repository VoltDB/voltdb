/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

package org.voltdb.benchmark.tpcc.procedures;

import org.voltdb.*;

@ProcInfo (
    singlePartition = false
)
public class UpdateNewOrder extends VoltProcedure {

    public final SQLStmt update = new SQLStmt("UPDATE NEW_ORDER SET NO_D_ID = 10 WHERE NO_O_ID = ?;");

    public VoltTable[] run(long no_o_id, long alwaysFail)
    throws VoltAbortException {

        if (alwaysFail == 0) {
            throw new VoltAbortException("Intentional failure for testing.");
        }

        voltQueueSQL(update, no_o_id);
        VoltTable[] retval = voltExecuteSQL();

        // Should always succeed as called in testing. If you change this
        // assumption, update the TestMultiPartitionSQL update tests.

        assert(retval.length == 1);
        assert(retval[0].asScalarLong() == 1);
        return retval;
    }
}
