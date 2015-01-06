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
// Accepts a vote, enforcing business logic: make sure the vote is for a valid
// contestant and that the voter (phone number of the caller) is not above the
// number of allowed votes.
//

package voter.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltTable.ColumnInfo;

@ProcInfo (
    partitionInfo = "votes.phone_number:0",
    singlePartition = true
)
public class Vote extends VoltProcedure {

    // potential return codes
    public static final long VOTE_SUCCESSFUL = 0;
    public static final long ERR_INVALID_CONTESTANT = 1;
    public static final long ERR_VOTER_OVER_VOTE_LIMIT = 2;

    // Checks if the vote is for a valid contestant
    public final SQLStmt checkContestantStmt = new SQLStmt(
            "SELECT contestant_number FROM contestants WHERE contestant_number = ?;");

    // Checks if the voter has exceeded their allowed number of votes
    public final SQLStmt checkVoterStmt = new SQLStmt(
            "SELECT num_votes FROM v_votes_by_phone_number WHERE phone_number = ?;");

    // Checks an area code to retrieve the corresponding state
    public final SQLStmt checkStateStmt = new SQLStmt(
            "SELECT state FROM area_code_state WHERE area_code = ?;");

    // Records a vote
    public final SQLStmt insertVoteStmt = new SQLStmt(
            "INSERT INTO votes (phone_number, state, contestant_number) VALUES (?, ?, ?);");

    // Get rejected votes count
    public final SQLStmt checkRejectedVotesStmt = new SQLStmt(
            "SELECT rejected_votes FROM rejected_votes_by_phone_number WHERE phone_number = ?;");

    // Increment the rejected votes count
    public final SQLStmt insertRejectedVotesStmt = new SQLStmt(
            "INSERT INTO rejected_votes_by_phone_number " +
            "(phone_number, rejected_votes)" +
            "VALUES (?, ?);");

    // Increment the rejected votes count
    public final SQLStmt incrementRejectedVotesStmt = new SQLStmt(
            "UPDATE rejected_votes_by_phone_number " +
            "SET rejected_votes = rejected_votes + 1 " +
            "WHERE phone_number = ?;");

    public VoltTable[] run(long phoneNumber, int contestantNumber, long maxVotesPerPhoneNumber) {
        VoltTable result = new VoltTable(new ColumnInfo("STATUS", VoltType.BIGINT),
                                         new ColumnInfo("REJECTED", VoltType.BIGINT));

        // Queue up validation statements
        voltQueueSQL(checkContestantStmt, EXPECT_ZERO_OR_ONE_ROW, contestantNumber);
        voltQueueSQL(checkVoterStmt, EXPECT_ZERO_OR_ONE_ROW, phoneNumber);
        voltQueueSQL(checkStateStmt, EXPECT_ZERO_OR_ONE_ROW, (short)(phoneNumber / 10000000l));
        voltQueueSQL(checkRejectedVotesStmt, EXPECT_ZERO_OR_ONE_ROW, phoneNumber);
        VoltTable validation[] = voltExecuteSQL();

        if (validation[0].getRowCount() == 0) {
            result.addRow(ERR_INVALID_CONTESTANT, -1);
            return new VoltTable[] {result};
        }

        // Get rejected votes
        long rejectedVotes = 1;
        if (validation[3].getRowCount() == 1) {
            rejectedVotes = validation[3].asScalarLong() + 1;
        }

        if ((validation[1].getRowCount() == 1) &&
                (validation[1].asScalarLong() >= maxVotesPerPhoneNumber)) {
            if (validation[3].getRowCount() == 1) {
                // Increment count
                voltQueueSQL(incrementRejectedVotesStmt, phoneNumber);
            } else {
                // insert
                voltQueueSQL(insertRejectedVotesStmt, phoneNumber, 1);
            }
            voltExecuteSQL();

            result.addRow(ERR_VOTER_OVER_VOTE_LIMIT, rejectedVotes);
            return new VoltTable[] {result};
        }

        // Some sample client libraries use the legacy random phone generation that mostly
        // created invalid phone numbers. Until refactoring, re-assign all such votes to
        // the "XX" fake state (those votes will not appear on the Live Statistics dashboard,
        // but are tracked as legitimate instead of invalid, as old clients would mostly get
        // it wrong and see all their transactions rejected).
        final String state = (validation[2].getRowCount() > 0) ? validation[2].fetchRow(0).getString(0) : "XX";

        // Post the vote
        voltQueueSQL(insertVoteStmt, EXPECT_SCALAR_MATCH(1), phoneNumber, state, contestantNumber);
        voltExecuteSQL(true);

        // Set the return value to 0: successful vote
        result.addRow(VOTE_SUCCESSFUL, rejectedVotes);
        return new VoltTable[] {result};
    }
}
