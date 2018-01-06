/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

package org.voltdb_testprocs.regressionsuites.sqltypesprocs;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import static org.voltdb.VoltProcedure.EXPECT_SCALAR_MATCH;
import static org.voltdb.VoltProcedure.EXPECT_ZERO_OR_ONE_ROW;
import org.voltdb.VoltTable;

//1 - paused
//0 - resumed
public class PauseResumeExport extends VoltProcedure {

    public static final SQLStmt checkStateStmt = new SQLStmt(
            "SELECT status FROM PAUSE_RESUME WHERE STATUS_ID = ?;");
    public static final SQLStmt insertStmt = new SQLStmt(
            "INSERT INTO PAUSE_RESUME (status, status_id) VALUES (?, ?);");
    public static final SQLStmt updateStmt = new SQLStmt(
            "UPDATE PAUSE_RESUME set status=? where status_id=1;");

    public long run(long flag) {
        voltQueueSQL(checkStateStmt, EXPECT_ZERO_OR_ONE_ROW, 1);
        VoltTable validation[] = voltExecuteSQL();

        if (validation[0].getRowCount() == 0) {
            voltQueueSQL(insertStmt, EXPECT_SCALAR_MATCH(1), flag, 1);
            voltExecuteSQL();
        } else {
            voltQueueSQL(updateStmt, EXPECT_SCALAR_MATCH(1), flag);
            voltExecuteSQL();
        }
        return flag;
    }
}
