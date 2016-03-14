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

public class SelectAllScoresInPartition extends VoltProcedure {

    public final SQLStmt selectScores = new SQLStmt(
        "SELECT c.contest_id, r.user_id, SUM(pgs.score) "+
        "FROM "+
        "  nfl_player_game_score pgs, "+
        "  nfl_contest_large c,"+
        "  user_contest_roster r "+
        "WHERE "+
        " r.contest_id = c.contest_id AND "+
        " r.player_id = pgs.player_id AND "+
        " c.game_id = pgs.game_id "+
        "GROUP BY c.contest_id, r.user_id "+
        "ORDER BY 1,2,3"+
        ";");



    public VoltTable[] run(int partitionVal) throws VoltAbortException {

    voltQueueSQL(selectScores);
        return voltExecuteSQL(true);
    }
}
