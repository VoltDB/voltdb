/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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


// Results stored procedure
//
//   Returns results of vote.

package com.procedures;

import org.voltdb.*;

@ProcInfo(
    singlePartition = false
)

public class Results extends VoltProcedure {
    // get the results

    public final SQLStmt getResults = new SQLStmt("select a.contestant_name c1, " +
                                                  "       sum(b.num_votes) c2 " +
                                                  "from v_votes_by_contestant_number b, " +
                                                  "     contestants a " +
                                                  "where a.contestant_number = b.contestant_number " +
                                                  "group by a.contestant_name " +
                                                  "order by a.contestant_name;");

    public VoltTable[] run() {
        voltQueueSQL(getResults);
        return voltExecuteSQL(true);
    }
}
