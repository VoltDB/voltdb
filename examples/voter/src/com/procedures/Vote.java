/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
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


// Vote stored procedure
//
//   Accept a vote.  Make sure it is for a valid contestant and that the voter is not above the number of allowed votes.

package com.procedures;

import org.voltdb.*;

@ProcInfo(
    partitionInfo = "VOTES.PHONE_NUMBER: 0",
    singlePartition = true
)

public class Vote extends VoltProcedure {
    // check if the vote is for a valid contestant
    public final SQLStmt checkContestant = new SQLStmt("select contestant_number from contestants where contestant_number = ?;");

    // check if the voter has exceeded their allowed number of votes
    public final SQLStmt checkVoter = new SQLStmt("select num_votes from v_votes_by_phone_number where phone_number = ?;");

    // record the vote
    public final SQLStmt insertVote = new SQLStmt("insert into votes (phone_number, contestant_number) values (?, ?);");

    public VoltTable[] run(
            long phoneNumber,
            byte contestantNumber,
            long maxVotesPerPhoneNumber
    ) {
        boolean validVoter = false;
        boolean validContestant = false;
        int returnValue = 0;

        voltQueueSQL(checkContestant, contestantNumber);
        voltQueueSQL(checkVoter, phoneNumber);
        VoltTable resultsCheck[] = voltExecuteSQL();

        if (resultsCheck[0].getRowCount() > 0) {
            // valid contestant
            validContestant = true;

            if (resultsCheck[1].getRowCount() == 0) {
                // phone number has not yet voted
                validVoter = true;
            } else if (resultsCheck[1].fetchRow(0).getLong(0) < maxVotesPerPhoneNumber) {
                // phone number still has votes
                validVoter = true;
            }
        }

        if (validContestant && validVoter) {
            voltQueueSQL(insertVote, phoneNumber, contestantNumber);
            voltExecuteSQL();
            returnValue = 0;
        } else if (!validContestant) {
            returnValue = 1;
        } else {
            returnValue = 2;
        }

        // return a 1 row 2 column VoltTable
        //   column return_value : 0 = successful vote
        //                         1 = invalid contestant number
        //                         2 = voter over vote limit
        VoltTable vtLoad = new VoltTable(new VoltTable.ColumnInfo("return_value",VoltType.INTEGER));
        Object row[] = new Object[1];
        row[0] = returnValue;
        vtLoad.addRow(row);

        final VoltTable[] vtReturn = {vtLoad};

        return vtReturn;
    }
}
