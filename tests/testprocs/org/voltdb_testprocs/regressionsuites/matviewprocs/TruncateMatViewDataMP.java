/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class TruncateMatViewDataMP extends VoltProcedure {
    public final SQLStmt truncatebase1 = new SQLStmt("TRUNCATE TABLE PEOPLE;");
    public final SQLStmt truncatebase2 = new SQLStmt("TRUNCATE TABLE THINGS;");
    public final SQLStmt truncatebase3 = new SQLStmt("TRUNCATE TABLE OVERFLOWTEST;");
    public final SQLStmt truncatebase4 = new SQLStmt("TRUNCATE TABLE ENG798;");
    public final SQLStmt truncatebase5 = new SQLStmt("TRUNCATE TABLE CONTEST;");
    public final SQLStmt truncatebase6 = new SQLStmt("TRUNCATE TABLE DEPT_PEOPLE;");
    public final SQLStmt truncatebase7 = new SQLStmt("TRUNCATE TABLE ENG6511;");
    public final SQLStmt truncatebase8 = new SQLStmt("TRUNCATE TABLE CUSTOMERS;");
    public final SQLStmt truncatebase9 = new SQLStmt("TRUNCATE TABLE ORDERS;");
    public final SQLStmt truncatebase10 = new SQLStmt("TRUNCATE TABLE ORDERITEMS;");
    public final SQLStmt truncatebase11 = new SQLStmt("TRUNCATE TABLE PRODUCTS;");

    public final SQLStmt validatebase1 = new SQLStmt("SELECT COUNT(*) FROM PEOPLE;");
    public final SQLStmt validatebase2 = new SQLStmt("SELECT COUNT(*) FROM THINGS;");
    public final SQLStmt validatebase3 = new SQLStmt("SELECT COUNT(*) FROM OVERFLOWTEST;");
    public final SQLStmt validatebase4 = new SQLStmt("SELECT COUNT(*) FROM ENG798;");
    public final SQLStmt validatebase5 = new SQLStmt("SELECT COUNT(*) FROM CONTEST;");
    public final SQLStmt validatebase6 = new SQLStmt("SELECT COUNT(*) FROM DEPT_PEOPLE;");
    public final SQLStmt validatebase7 = new SQLStmt("SELECT COUNT(*) FROM ENG6511;");
    public final SQLStmt validatebase8 = new SQLStmt("SELECT COUNT(*) FROM CUSTOMERS;");
    public final SQLStmt validatebase9 = new SQLStmt("SELECT COUNT(*) FROM ORDERS;");
    public final SQLStmt validatebase10 = new SQLStmt("SELECT COUNT(*) FROM ORDERITEMS;");
    public final SQLStmt validatebase11 = new SQLStmt("SELECT COUNT(*) FROM PRODUCTS;");

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
    public final SQLStmt validateview12 = new SQLStmt("SELECT COUNT(*) FROM VENG6511;");
    public final SQLStmt validateview13 = new SQLStmt("SELECT COUNT(*) FROM VENG6511expL;");
    public final SQLStmt validateview14 = new SQLStmt("SELECT COUNT(*) FROM VENG6511expR;");
    public final SQLStmt validateview15 = new SQLStmt("SELECT COUNT(*) FROM VENG6511expLR;");
    public final SQLStmt validateview16 = new SQLStmt("SELECT COUNT(*) FROM VENG6511C;");
    public final SQLStmt validateview17 = new SQLStmt("SELECT COUNT(*) FROM ORDER_COUNT_NOPCOL;");
    // ORDER_COUNT_GLOBAL is a view without group by column.
    // If the source tables are successfully truncated, it will have one row with value 0.
    public final SQLStmt validateview18 = new SQLStmt("SELECT CNT FROM ORDER_COUNT_GLOBAL;");
    public final SQLStmt validateview19 = new SQLStmt("SELECT COUNT(*) FROM ORDER_DETAIL_NOPCOL;");
    public final SQLStmt validateview20 = new SQLStmt("SELECT COUNT(*) FROM ORDER_DETAIL_WITHPCOL;");
    public final SQLStmt validateview21 = new SQLStmt("SELECT COUNT(*) FROM ORDER2016;");
    // The following four views are all views without group-by columns (ENG-7872).
    public final SQLStmt validateview22 = new SQLStmt("SELECT * FROM MATPEOPLE_COUNT;");
    public final SQLStmt validateview23 = new SQLStmt("SELECT * FROM MATPEOPLE_CONDITIONAL_COUNT;");
    public final SQLStmt validateview24 = new SQLStmt("SELECT NUM FROM MATPEOPLE_CONDITIONAL_COUNT_SUM;");
    public final SQLStmt validateview25 = new SQLStmt("SELECT NUM FROM MATPEOPLE_CONDITIONAL_COUNT_MIN_MAX;");


    public VoltTable[] run() {
        VoltTable[] result;
        voltQueueSQL(truncatebase1); // ("TRUNCATE TABLE PEOPLE;");
        voltQueueSQL(truncatebase2); // ("TRUNCATE TABLE THINGS;");
        voltQueueSQL(truncatebase3); // ("TRUNCATE TABLE OVERFLOWTEST;");
        voltQueueSQL(truncatebase4); // ("TRUNCATE TABLE ENG798;");
        voltQueueSQL(truncatebase5); // ("TRUNCATE TABLE CONTEST;");
        voltQueueSQL(truncatebase6); // ("TRUNCATE TABLE DEPT_PEOPLE;");
        voltQueueSQL(truncatebase7); // ("TRUNCATE TABLE ENG6511;");
        voltQueueSQL(truncatebase8); // ("TRUNCATE TABLE CUSTOMERS;");
        voltQueueSQL(truncatebase9); // ("TRUNCATE TABLE ORDERS;");
        voltQueueSQL(truncatebase10); // ("TRUNCATE TABLE ORDERITEMS;");
        voltQueueSQL(truncatebase11); // ("TRUNCATE TABLE PRODUCTS;");
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
        voltQueueSQL(validatebase7); // ("SELECT COUNT(*) FROM ENG6511;");
        voltQueueSQL(validatebase8); // ("SELECT COUNT(*) FROM CUSTOMERS;");
        voltQueueSQL(validatebase9); // ("SELECT COUNT(*) FROM ORDERS;");
        voltQueueSQL(validatebase10); // ("SELECT COUNT(*) FROM ORDERITEMS;");
        voltQueueSQL(validatebase11); // ("SELECT COUNT(*) FROM PRODUCTS;");

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
        voltQueueSQL(validateview12); // ("SELECT COUNT(*) FROM VENG6511;");
        voltQueueSQL(validateview13); // ("SELECT COUNT(*) FROM VENG6511expL;");
        voltQueueSQL(validateview14); // ("SELECT COUNT(*) FROM VENG6511expR;");
        voltQueueSQL(validateview15); // ("SELECT COUNT(*) FROM VENG6511expLR;");
        voltQueueSQL(validateview16); // ("SELECT COUNT(*) FROM VENG6511C;");
        voltQueueSQL(validateview17); // ("SELECT COUNT(*) FROM ORDER_COUNT_NOPCOL;");
        voltQueueSQL(validateview18); // ("SELECT CNT FROM ORDER_COUNT_GLOBAL;");
        voltQueueSQL(validateview19); // ("SELECT COUNT(*) FROM ORDER_DETAIL_NOPCOL;");
        voltQueueSQL(validateview20); // ("SELECT COUNT(*) FROM ORDER_DETAIL_WITHPCOL;");
        voltQueueSQL(validateview21); // ("SELECT COUNT(*) FROM ORDER2016;");
        voltQueueSQL(validateview22); // ("SELECT * FROM MATPEOPLE_COUNT;");
        voltQueueSQL(validateview23); // ("SELECT * FROM MATPEOPLE_CONDITIONAL_COUNT;");
        voltQueueSQL(validateview24); // ("SELECT NUM FROM MATPEOPLE_CONDITIONAL_COUNT_SUM;");
        voltQueueSQL(validateview25); // ("SELECT NUM FROM MATPEOPLE_CONDITIONAL_COUNT_MIN_MAX;");
        result = voltExecuteSQL(true);
        int ii = 0;
        for (VoltTable undeleted : result) {
            ++ii;
            try {
                long found = undeleted.asScalarLong();
                if (found != 0) {
                    System.out.println("DEBUG: In TruncateMatViewDataMP.java," +
                            " validated truncate statements with check  " + ii +
                            " and got: " + found + " undeleted tuples.");
                }
            }
            catch (Exception exc) {
                System.out.println("DEBUG: In TruncateMatViewDataMP.java, " +
                        "validated truncate statements with check " + ii +
                        " and got: " + exc);
            }
        }
        return result;
    }
}
