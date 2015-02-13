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

package org.voltdb_testprocs.regressionsuites.failureprocs;

import org.voltdb.Expectation;
import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;

@ProcInfo (
    partitionInfo = "BLAH.IVAL: 0",
    singlePartition = true
)
public class CrushExpectations extends VoltProcedure {

    public static final SQLStmt insert = new SQLStmt("insert into blah values (?, ?)");
    public static final SQLStmt queryZeroRows = new SQLStmt("select * from blah where ival = -1");
    public static final SQLStmt queryOneRow = new SQLStmt("select * from blah where ival = 0");
    public static final SQLStmt queryTwoRows = new SQLStmt("select * from blah");
    public static final SQLStmt queryScalarString = new SQLStmt("select sval from blah where ival = 99");
    public static final SQLStmt queryScalarLong = new SQLStmt("select ival from blah where ival = 99");

    boolean runSingleFailure(SQLStmt stmt, Expectation expectation) {
        try {
            voltQueueSQL(stmt, expectation);
            voltExecuteSQL();
            return false;
        }
        catch (VoltAbortException e) {
            System.out.println(e.getMessage());
            return true;
        }
    }

    public long run(long i) {
        // insert sample data
        voltQueueSQL(insert, EXPECT_SCALAR_MATCH(1), 0, "hello"); // try with expectation
        voltQueueSQL(insert, 99, "goodbye");                      // try without
        voltExecuteSQL();

        // try successful expectations in individual batches
        voltQueueSQL(queryZeroRows, EXPECT_EMPTY);
        voltExecuteSQL();
        voltQueueSQL(queryZeroRows, EXPECT_ZERO_OR_ONE_ROW);
        voltExecuteSQL();
        voltQueueSQL(queryOneRow, EXPECT_ONE_ROW);
        voltExecuteSQL();
        voltQueueSQL(queryOneRow, EXPECT_NON_EMPTY);
        voltExecuteSQL();
        voltQueueSQL(queryOneRow, EXPECT_ZERO_OR_ONE_ROW);
        voltExecuteSQL();
        voltQueueSQL(queryTwoRows, EXPECT_NON_EMPTY);
        voltExecuteSQL();
        voltQueueSQL(queryScalarString, EXPECT_SCALAR);
        voltExecuteSQL();
        voltQueueSQL(queryScalarLong, EXPECT_SCALAR);
        voltExecuteSQL();
        voltQueueSQL(queryScalarLong, EXPECT_SCALAR_LONG);
        voltExecuteSQL();
        voltQueueSQL(queryScalarLong, EXPECT_SCALAR_MATCH(99));
        voltExecuteSQL();

        // try failures, one at a time
        if (!runSingleFailure(queryZeroRows, EXPECT_ONE_ROW)) return -1;
        if (!runSingleFailure(queryZeroRows, EXPECT_SCALAR)) return -2;
        if (!runSingleFailure(queryZeroRows, EXPECT_NON_EMPTY)) return -3;
        if (!runSingleFailure(queryOneRow, EXPECT_EMPTY)) return -4;
        if (!runSingleFailure(queryOneRow, EXPECT_SCALAR)) return -5;
        if (!runSingleFailure(queryOneRow, EXPECT_SCALAR_MATCH(5))) return -6;
        if (!runSingleFailure(queryTwoRows, EXPECT_SCALAR)) return -7;
        if (!runSingleFailure(queryTwoRows, EXPECT_ZERO_OR_ONE_ROW)) return -8;
        if (!runSingleFailure(queryTwoRows, EXPECT_SCALAR_LONG)) return -9;
        if (!runSingleFailure(queryScalarString, EXPECT_SCALAR_LONG)) return -10;
        if (!runSingleFailure(queryScalarString, EXPECT_SCALAR_MATCH(7))) return -11;
        if (!runSingleFailure(queryScalarLong, EXPECT_SCALAR_MATCH(15))) return -12;
        if (!runSingleFailure(queryScalarLong, EXPECT_EMPTY)) return -13;

        // try in a bigger batch success
        voltQueueSQL(queryScalarString, EXPECT_SCALAR);
        voltQueueSQL(queryScalarLong, EXPECT_SCALAR);
        voltQueueSQL(queryScalarLong); // HOLE!
        voltQueueSQL(queryScalarLong, EXPECT_SCALAR_MATCH(99));
        try {
            voltExecuteSQL();
        }
        catch (VoltAbortException e) {
            e.printStackTrace(); // easier to debug, but don't expect to hit this
        }

        // try in a bigger batch failure
        voltQueueSQL(queryScalarString, EXPECT_SCALAR);
        voltQueueSQL(queryScalarLong, EXPECT_EMPTY);  // THIS ONE IS THE FAIL
        voltQueueSQL(queryScalarLong); // HOLE!
        voltQueueSQL(queryScalarLong, EXPECT_SCALAR_MATCH(99));
        try {
            voltExecuteSQL();
            return -14;
        }
        catch (VoltAbortException e) {
            // success case
        }

        // zero is a successful return
        return 0;
    }

}
