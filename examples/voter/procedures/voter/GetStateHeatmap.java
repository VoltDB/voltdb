/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
// Returns the heat map data (winning contestant by state) for display on nthe Live Statistics dashboard.
//

package voter;

import java.util.ArrayList;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

/**
 * This procedure returns two VoltTables as results.  The first is a list of states,
 * the contestant numbers of the winner in that state and the total number of votes for that
 * contestant in that state.  The second is a table of contestants and all votes
 * for that contestant globally.
 */
public class GetStateHeatmap extends VoltProcedure {

    public final SQLStmt stateHeatMap = new SQLStmt(
                                                    "  SELECT state"                                                     +
                                                    "       , contestant_number"                                         +
                                                    "       , num_votes"                                                 +
                                                    "    FROM ( SELECT state"                                            +
                                                    "                , contestant_number"                                +
                                                    "                , num_votes"                                        +
                                                    "                , RANK() OVER ( PARTITION by state "                +
                                                    "                                ORDER BY num_votes DESC ) AS vrank" +
                                                    "             FROM v_votes_by_contestant_number_state ) AS sub"      +
                                                    "   WHERE sub.vrank = 1;");
    public final SQLStmt contestantTotals = new SQLStmt("   SELECT contestant_number"                                    +
                                                        "        , SUM(num_votes)"                                       +
                                                        "     FROM v_votes_by_contestant_number_state"                   +
                                                        " GROUP BY contestant_number;");

    public VoltTable[] run() {
        voltQueueSQL(stateHeatMap);
        voltQueueSQL(contestantTotals);
        VoltTable[] result = voltExecuteSQL();
        return result;
    }
}
