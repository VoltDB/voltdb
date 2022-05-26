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

//
// Returns the results of the votes.
//

package voter.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class Results extends VoltProcedure
{
    // Gets the results
    public final SQLStmt resultStmt = new SQLStmt( "   SELECT a.contestant_name   AS contestant_name"
                                                 + "        , a.contestant_number AS contestant_number"
                                                 + "        , SUM(b.num_votes)    AS total_votes"
                                                 + "     FROM v_votes_by_contestant_number_state AS b"
                                                 + "        , contestants AS a"
                                                 + "    WHERE a.contestant_number = b.contestant_number"
                                                 + " GROUP BY a.contestant_name"
                                                 + "        , a.contestant_number"
                                                 + " ORDER BY total_votes DESC;" );
    public VoltTable[] run() {
        voltQueueSQL(resultStmt);
        return voltExecuteSQL(true);
    }
}
