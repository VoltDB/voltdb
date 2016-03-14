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

package fantasysports;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class UpsertUserScores extends VoltProcedure {

    public final SQLStmt upsertScores = new SQLStmt(
        "UPSERT INTO user_contest_score (contest_id, user_id, score) "+
        "SELECT c.contest_id, r.user_id, SUM(p.score) "+
        "FROM "+
        "  user_contest_roster r "+
        "INNER JOIN nfl_contest_large c ON r.contest_id = c.contest_id "+
        "INNER JOIN nfl_player_game_score p ON r.player_id = p.player_id AND c.game_id = p.game_id "+
        "WHERE "+
        " r.contest_id = ? "+
        "GROUP BY c.contest_id, r.user_id "+
        "ORDER BY 1,2"+
        ";");

    public final SQLStmt selectScores = new SQLStmt(
        "SELECT user_id, score "+
        "FROM user_contest_score "+
        "WHERE contest_id = ? "+
        "ORDER BY score;");

    public VoltTable[] run(int partitionVal, int contest) throws VoltAbortException {
        voltQueueSQL(upsertScores, contest);
        voltQueueSQL(selectScores, contest);
        return voltExecuteSQL(true);
    }
}
