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

//
// Returns the top 10 area codes for a given contestant.
//
package com.procedures;

import org.voltdb.*;

@ProcInfo
(
  singlePartition = false
)

public class ContestantResultsByAreaCode extends VoltProcedure
{
    public final SQLStmt resultStmt = new SQLStmt( "   SELECT TOP 10 area_code"
                                                 + "        , SUM(num_votes) AS total_votes"
                                                 + "     FROM v_votes_by_contestant_number_area_code"
                                                 + "    WHERE contestant_number = ?"
                                                 + " GROUP BY area_code"
                                                 + " ORDER BY total_votes DESC;");
    public VoltTable[] run(int contestantNumber)
    {
        voltQueueSQL(resultStmt,contestantNumber);
        return voltExecuteSQL(true);
    }
}
