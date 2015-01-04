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

package jsonsessions;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class Login extends VoltProcedure {

    // potential return codes (must be synced with client code)
    public static final long LOGIN_SUCCESSFUL = 0;
    public static final long LOGIN_FAIL = 1;
    public static final long LOGIN_TIMEOUT = 2;

    // Checks if the vote is for a valid user
    public final SQLStmt findUserStmt = new SQLStmt(
            "SELECT username FROM user_session_table WHERE username = ?;");

    // Update the last accessed time for a user
    public final SQLStmt updateUserStmt = new SQLStmt(
            "UPDATE user_session_table SET last_accessed = ? WHERE username = ?;");

    // Add a logged in user
    public final SQLStmt insertUserStmt = new SQLStmt(
            "INSERT INTO user_session_table (username, password, global_session_id, last_accessed, json_data) VALUES (?, ?, ?, ?, ?);");

    public long run(String username, String password, String json) {
        // See if the user is already logged in.
        voltQueueSQL(findUserStmt, username);
        VoltTable[] selectResults = voltExecuteSQL();
        if (selectResults[0].advanceRow() == true) {
            // Update the last time this account was accessed (for timeout purposes)
            voltQueueSQL(updateUserStmt, this.getTransactionTime(), username);
        } else {
            // Do the login, create an entry in the session table.  Use VoltDB's getUniqueID() api to create a deterministic unique ID for the login.
            voltQueueSQL(insertUserStmt, username, password, Long.toString(getUniqueId()), this.getTransactionTime(), json);
        }

        voltExecuteSQL();

        return LOGIN_SUCCESSFUL;
    }
}
