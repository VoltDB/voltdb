/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

public class GetStateHeatmap extends VoltProcedure {

    public final SQLStmt resultStmt = new SQLStmt(
            "SELECT contestant_number, state, num_votes " +
            "FROM v_votes_by_contestant_number_state " +
            "ORDER BY 2 ASC, 3 DESC, 1 ASC;");

    static class Result {
        public final String state;
        public final int contestantNumber;
        public final long votes;
        public final byte isWinning;

        public Result(String state, int contestantNumber, long votes, byte isWinning) {
            this.state = state;
            this.contestantNumber = contestantNumber;
            this.votes = votes;
            this.isWinning = isWinning;
        }
    }

    public VoltTable run() {
        ArrayList<Result> results = new ArrayList<Result>();
        voltQueueSQL(resultStmt);
        VoltTable summary = voltExecuteSQL()[0];
        String state = "";

        while(summary.advanceRow()) {
            if (!summary.getString(1).equals(state)) {
                state = summary.getString(1);
                results.add(new Result(state, (int)summary.getLong(0), summary.getLong(2), (byte)1));
            }
            else {
                results.add(new Result(state, (int)summary.getLong(0), summary.getLong(2), (byte)0));
            }
        }

        Object[] resultArray = results.toArray();
        VoltTable result = new VoltTable(
                new VoltTable.ColumnInfo("state",VoltType.STRING),
                new VoltTable.ColumnInfo("contestant_number",VoltType.INTEGER),
                new VoltTable.ColumnInfo("num_votes",VoltType.BIGINT),
                new VoltTable.ColumnInfo("is_winning",VoltType.TINYINT));

        for(int i=0;i<resultArray.length;i++) {
            result.addRow(new Object[] {
                    ((Result)resultArray[i]).state,
                    ((Result)resultArray[i]).contestantNumber,
                    ((Result)resultArray[i]).votes,
                    ((Result)resultArray[i]).isWinning });
        }
        return result;
    }
}
