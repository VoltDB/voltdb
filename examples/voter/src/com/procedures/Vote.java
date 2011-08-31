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
// Accepts a vote, enforcing business logic: make sure the vote is for a valid
// contestant and that the voter (phone number of the caller) is not above the
// number of allowed votes.
//
package com.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

@ProcInfo
(
  partitionInfo = "votes.phone_number:0"
, singlePartition = true
)

public class Vote extends VoltProcedure
{
    // Checks if the vote is for a valid contestant
    public final SQLStmt checkContestantStmt = new SQLStmt("SELECT contestant_number FROM contestants WHERE contestant_number = ?;");

    // Checks if the voter has exceeded their allowed number of votes
    public final SQLStmt checkVoterStmt = new SQLStmt("SELECT num_votes FROM v_votes_by_phone_number WHERE phone_number = ?;");

    // Retrieves the state for a given area code
    public final SQLStmt getStateStmt = new SQLStmt("SELECT state FROM area_code_state WHERE area_code = ?;");

    // Records a vote
    public final SQLStmt insertVoteStmt = new SQLStmt("INSERT INTO votes (phone_number, area_code, state, contestant_number) VALUES (?, ?, ?, ?);");

    public VoltTable[] run(long phoneNumber, int contestantNumber, long maxVotesPerPhoneNumber)
    {
        boolean validVoter = false;
        boolean validContestant = false;
        int returnValue = 0;
        short areaCode = (short)(phoneNumber / 10000000l);
        voltQueueSQL(checkContestantStmt, contestantNumber);
        voltQueueSQL(checkVoterStmt, phoneNumber);
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

            // Some sample client libraries use the legacy random phone generation that mostly
            // created invalid phone numbers. Until refactoring, re-assign all such votes to
            // the "EU" fake state (those votes will not appear on the Live Statistics dashboard,
            // but are tracked as legitimate instead of invalid, as old client would mostly get
            // it wrong and see all their transactions rejected).
            String state = "EU";
            voltQueueSQL(getStateStmt, areaCode);
            VoltTable r1 = voltExecuteSQL()[0];
            if (r1.getRowCount() > 0)
                state = r1.fetchRow(0).getString(0);
            voltQueueSQL(insertVoteStmt, phoneNumber, areaCode, state, contestantNumber);
            voltExecuteSQL(true);
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
