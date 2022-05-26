/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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
package server;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class GetNextBestAction extends VoltProcedure {

    // @formatter:off

    public static final SQLStmt getUser = new SQLStmt(
            "SELECT * FROM users WHERE username = ?;");

    public static final SQLStmt reportHit = new SQLStmt(
            "INSERT INTO user_hits (username \n"
                    + ",category \n"
                    + ",cookieid \n"
                    + ",accountid \n"
                    + ",url)"
                    + "VALUES"
                    + "(?,?,?,?,?);");

    public static final SQLStmt updateUser = new SQLStmt(
            "UPDATE users SET next_best_action = ? WHERE username = ?;");

    // @formatter:on

    public VoltTable[] run(String username, long cookieid , long accountid, String url) throws VoltAbortException {


        // See if we know about this user and transaction...
        voltQueueSQL(getUser, EXPECT_ONE_ROW, username);

        VoltTable user = voltExecuteSQL()[0];
        user.advanceRow();

        String nextBestAction = user.getString("next_best_action");

        if (nextBestAction.equals("BAA")) {
            nextBestAction = "BOO";
        } else {
            nextBestAction = "BAA";
        }

        voltQueueSQL(updateUser, nextBestAction,username );
        voltQueueSQL(reportHit,username,nextBestAction,cookieid,accountid,url );    // FIXME - no accountid
        voltExecuteSQL();

        voltQueueSQL(getUser, EXPECT_ONE_ROW, username);

        return voltExecuteSQL(true);
    }
}
