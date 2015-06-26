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

package org.voltdb_testprocs.regressionsuites.matviewprocs;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

@ProcInfo (
    singlePartition = false
)
public class TruncateMatViewDataMP extends VoltProcedure {
    /* TODO: Migrate this use TRUNCATE table as soon as it is supported */
    public final SQLStmt truncatebase1 = new SQLStmt("DELETE FROM PEOPLE;");
    public final SQLStmt truncatebase2 = new SQLStmt("DELETE FROM THINGS;");
    public final SQLStmt truncatebase3 = new SQLStmt("DELETE FROM OVERFLOWTEST;");
    public final SQLStmt truncatebase4 = new SQLStmt("DELETE FROM ENG798;");
    public final SQLStmt truncatebase5 = new SQLStmt("DELETE FROM CONTEST;");
    public final SQLStmt truncatebase6 = new SQLStmt("DELETE FROM DEPT_PEOPLE;");

    public final SQLStmt validatebase1 = new SQLStmt("SELECT COUNT(*) FROM PEOPLE;");
    public final SQLStmt validatebase2 = new SQLStmt("SELECT COUNT(*) FROM THINGS;");
    public final SQLStmt validatebase3 = new SQLStmt("SELECT COUNT(*) FROM OVERFLOWTEST;");
    public final SQLStmt validatebase4 = new SQLStmt("SELECT COUNT(*) FROM ENG798;");
    public final SQLStmt validatebase5 = new SQLStmt("SELECT COUNT(*) FROM CONTEST;");
    public final SQLStmt validatebase6 = new SQLStmt("SELECT COUNT(*) FROM DEPT_PEOPLE;");

    public final SQLStmt validateview1 = new SQLStmt("SELECT COUNT(*) FROM MATPEOPLE;");
    public final SQLStmt validateview2 = new SQLStmt("SELECT COUNT(*) FROM MATTHINGS;");
    public final SQLStmt validateview3 = new SQLStmt("SELECT COUNT(*) FROM V_OVERFLOWTEST;");
    public final SQLStmt validateview4 = new SQLStmt("SELECT COUNT(*) FROM V_ENG798;");
    public final SQLStmt validateview5 = new SQLStmt("SELECT COUNT(*) FROM V_RUNNING_TEAM;");
    public final SQLStmt validateview6 = new SQLStmt("SELECT COUNT(*) FROM V_TEAM_MEMBERSHIP;");
    public final SQLStmt validateview7 = new SQLStmt("SELECT COUNT(*) FROM V_TEAM_TIMES;");
    public final SQLStmt validateview8 = new SQLStmt("SELECT COUNT(*) FROM MATPEOPLE2;");
    public final SQLStmt validateview9 = new SQLStmt("SELECT COUNT(*) FROM MATPEOPLE3;");
    public final SQLStmt validateview10 = new SQLStmt("SELECT COUNT(*) FROM DEPT_AGE_MATVIEW;");
    public final SQLStmt validateview11 = new SQLStmt("SELECT COUNT(*) FROM DEPT_AGE_FILTER_MATVIEW;");


    public VoltTable[] run() {
        VoltTable[] result;
        voltQueueSQL(truncatebase1); // ("DELETE FROM PEOPLE;");
        voltQueueSQL(truncatebase2); // ("DELETE FROM THINGS;");
        voltQueueSQL(truncatebase3); // ("DELETE FROM OVERFLOWTEST;");
        voltQueueSQL(truncatebase4); // ("DELETE FROM ENG798;");
        voltQueueSQL(truncatebase5); // ("DELETE FROM CONTEST;");
        voltQueueSQL(truncatebase6); // ("DELETE FROM DEPT_PEOPLE;");
        result = voltExecuteSQL();
        /*
        for (VoltTable deleted : result) {
            System.out.println("DEBUG Deleted: " + deleted.asScalarLong());
        }
        */
        voltQueueSQL(validatebase1); // ("SELECT COUNT(*) FROM PEOPLE;");
        voltQueueSQL(validatebase2); // ("SELECT COUNT(*) FROM THINGS;");
        voltQueueSQL(validatebase3); // ("SELECT COUNT(*) FROM OVERFLOWTEST;");
        voltQueueSQL(validatebase4); // ("SELECT COUNT(*) FROM ENG798;");
        voltQueueSQL(validatebase5); // ("SELECT COUNT(*) FROM contest;");
        voltQueueSQL(validatebase6); // ("SELECT COUNT(*) FROM DEPT_PEOPLE;");
        voltQueueSQL(validateview1); // ("SELECT COUNT(*) FROM MATPEOPLE;");
        voltQueueSQL(validateview2); // ("SELECT COUNT(*) FROM MATTHINGS;");
        voltQueueSQL(validateview3); // ("SELECT COUNT(*) FROM V_OVERFLOWTEST;");
        voltQueueSQL(validateview4); // ("SELECT COUNT(*) FROM V_ENG798;");
        voltQueueSQL(validateview5); // ("SELECT COUNT(*) FROM V_RUNNING_TEAM;");
        voltQueueSQL(validateview6); // ("SELECT COUNT(*) FROM V_TEAM_MEMBERSHIP;");
        voltQueueSQL(validateview7); // ("SELECT COUNT(*) FROM V_TEAM_TIMES;");
        voltQueueSQL(validateview8); // ("SELECT COUNT(*) FROM MATPEOPLE2;");
        voltQueueSQL(validateview9); // ("SELECT COUNT(*) FROM MATPEOPLE3;");
        voltQueueSQL(validateview10); // ("SELECT COUNT(*) FROM DEPT_AGE_MATVIEW;");
        voltQueueSQL(validateview11); // ("SELECT COUNT(*) FROM DEPT_AGE_FILTER_MATVIEW;");
        result = voltExecuteSQL(true);
        /*
        for (VoltTable deleted : result) {
            System.out.println("DEBUG Validated deletion: " + deleted.asScalarLong());
        }
        */
        return result;
    }
}
