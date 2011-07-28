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


// Results stored procedure
//
//   Returns results of vote.

package com.procedures;

import org.voltdb.*;
import java.util.Comparator;
import java.util.Arrays;
import java.util.ArrayList;

@ProcInfo(
    singlePartition = false
)

public class ContestantWinningStates extends VoltProcedure {
    // get the results

    public final SQLStmt getStateSummaryByContestant = new SQLStmt( "select contestant_number, state, sum(num_votes) AS num_votes from v_votes_by_contestant_number_state group by contestant_number, state order by 2 asc, 3 desc;");

    static class Result
    {
        public final String State;
        public final long Votes;
        public Result(String state, long votes)
        {
            this.State = state;
            this.Votes = votes;
        }
    }
    static class OrderByVotesDesc implements Comparator<Result>
    {
        public int compare(Result a, Result b)
        {
            long numVotesA = a.Votes;
            long numVotesB = b.Votes;
            if (numVotesA > numVotesB)
                return -1;
            else if (numVotesA < numVotesB)
                return 1;
            else
                return 0;
        }
    }

    public VoltTable[] run(int contestantNumber, int max)
    {
        ArrayList<Result> results = new ArrayList<Result>();
        voltQueueSQL(getStateSummaryByContestant);
        VoltTable summary = voltExecuteSQL()[0];
        String state = "";
        while(summary.advanceRow())
        {
            if (!summary.getString(1).equals(state))
            {
                state = summary.getString(1);
                if (summary.getLong(0) == contestantNumber)
                    results.add(new Result(state, summary.getLong(2)));
            }
        }
        Result[] resultArray = (Result[])results.toArray();
        Arrays.sort(resultArray, new OrderByVotesDesc());
        VoltTable result = new VoltTable(new VoltTable.ColumnInfo("state",VoltType.STRING), new VoltTable.ColumnInfo("num_votes",VoltType.BIGINT));
        for(int i=0;i<Math.min(resultArray.length,max);i++)
            result.addRow(new Object[] { ((Result)resultArray[i]).State, ((Result)resultArray[i]).Votes });
        return new VoltTable[] { result };
    }
}
