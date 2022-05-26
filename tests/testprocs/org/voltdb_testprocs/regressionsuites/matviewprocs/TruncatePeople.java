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
import org.voltdb.VoltType;

public class TruncatePeople extends VoltProcedure {

    public final SQLStmt captureview1 = new SQLStmt("SELECT * FROM MATPEOPLE ORDER BY AGE, PARTITION;");
    public final SQLStmt captureview2 = new SQLStmt("SELECT * FROM MATPEOPLE2 ORDER BY AGE, PARTITION;");

    public final SQLStmt clearcache1 = new SQLStmt("TRUNCATE TABLE WAS_PEOPLE;");

    public final SQLStmt cachebase1 = new SQLStmt("INSERT INTO WAS_PEOPLE SELECT * FROM PEOPLE ORDER BY ID, PARTITION;");

    public final SQLStmt truncatebase0 = new SQLStmt("TRUNCATE TABLE PEOPLE;");

    public final SQLStmt validatebase0 = new SQLStmt("SELECT COUNT(*) FROM PEOPLE;");

    public final SQLStmt renewbase0 = new SQLStmt("INSERT INTO PEOPLE SELECT * FROM WAS_PEOPLE ORDER BY ID, PARTITION;");

    public VoltTable run(int rollback, int repeats, int restores) {
        try {
            //* enable to debug */  System.out.println("IN TruncatePeople." + rollback + "." + repeats + "." + restores);
            voltQueueSQL(captureview1);
            voltQueueSQL(captureview2);
            VoltTable[] beforeViews = voltExecuteSQL();
            throwVoltAbortExceptionIf("Initial empty view is not a representative test.",
                    ! beforeViews[0].advanceRow(), null, beforeViews[0]);
            //* enable to debug */  System.out.println("IN TruncatePeople captured");

            voltQueueSQL(clearcache1);
            voltQueueSQL(cachebase1);
            voltExecuteSQL();
            //* enable to debug */  System.out.println("IN TruncatePeople cached");

            VoltTable[] afterViews = null;

            int leaveTruncated = repeats - restores;
            for (int ii = 0; ii < repeats; ++ii) {
                voltQueueSQL(truncatebase0); // ("TRUNCATE TABLE PEOPLE;");
                voltQueueSQL(validatebase0); // ("SELECT COUNT(*) FROM PEOPLE;");
                VoltTable validated = voltExecuteSQL()[1];

                throwVoltAbortExceptionIf(
                        "Rolling back unexpectedly after truncate misbehavior.",
                        ( ! validated.advanceRow()) || validated.getLong(0) != 0,
                        null, null);

                // Optionally leave the table truncated between early truncates,
                // but always restore the rows towards the end of the iterations.
                if (leaveTruncated-- <= 0) {
                    voltQueueSQL(renewbase0); // ("INSERT INTO PEOPLE ...");
                    voltExecuteSQL();

                    voltQueueSQL(captureview1);
                    voltQueueSQL(captureview2);
                    afterViews = voltExecuteSQL();
                    //* enable to debug */  System.out.println("IN TruncatePeople captured expecting renewed");
                    validateSame(beforeViews[0], afterViews[0]);
                    validateSame(beforeViews[1], afterViews[1]);
                    //* enable to debug */  System.out.println("IN TruncatePeople confirmed renewed");
                }
                else {
                    voltQueueSQL(captureview1);
                    voltQueueSQL(captureview2);
                    afterViews = voltExecuteSQL();
                    //* enable to debug */  System.out.println("IN TruncatePeople captured expecting empty");
                    validatePurged(afterViews);
                    //* enable to debug */  System.out.println("IN TruncatePeople confirmed empty");
                }
            }

            throwVoltAbortExceptionIf("Rolling back as requested.",
                    rollback != 0, null, null);

            //* enable to debug */  System.out.println("IN TruncatePeople exiting normally");
            return afterViews[0];
        }
        catch(Throwable t) {
            //* enable to debug */  System.out.println("IN TruncatePeople exiting abnormally");
            throw t;
        }
    }

    private void validatePurged(VoltTable[] afterViews) {
        for (VoltTable viewContent : afterViews) {
            throwVoltAbortExceptionIf("view should reflect 0 rows",
                    viewContent.advanceRow(), null, viewContent);
        }
    }

    private void validateSame(VoltTable expectedRows, VoltTable actualRows) {
        throwVoltAbortExceptionIf("column count mismatch.  Expected: " + expectedRows.getColumnCount() +
                " actual: " + actualRows.getColumnCount(),
                expectedRows.getColumnCount() != actualRows.getColumnCount(), expectedRows, actualRows);

        throwVoltAbortExceptionIf("row count mismatch.  Expected: " + expectedRows.getRowCount() +
                " actual: " + actualRows.getRowCount(),
                expectedRows.getRowCount() != actualRows.getRowCount(), expectedRows, actualRows);

        int ii = 0;
        while (expectedRows.advanceToRow(ii)) {
            throwVoltAbortExceptionIf("too few actual rows; expected more than " + (ii + 1),
                    ! actualRows.advanceToRow(ii), expectedRows, actualRows);

            for (int j = 0; j < actualRows.getColumnCount(); j++) {
                String columnName = actualRows.getColumnName(j);
                String colPrefix = "row " + ii + ": column: " + columnName + ": ";

                VoltType actualType = actualRows.getColumnType(j);
                VoltType expectedType = expectedRows.getColumnType(j);
                throwVoltAbortExceptionIf(colPrefix + "type mismatch",
                        expectedType != actualType, expectedRows, actualRows);

                Object expectedObj = expectedRows.get(j,  expectedType);
                Object actualObj = actualRows.get(j,  actualType);
                boolean expectedNull = expectedRows.wasNull();
                boolean actualNull = actualRows.wasNull();
                throwVoltAbortExceptionIf(colPrefix + "null/not null mismatch",
                        expectedNull != actualNull, expectedRows, actualRows);

                if (expectedNull) {
                    continue;
                }
                if (expectedType == VoltType.FLOAT) {
                    //TODO: inject epsilon comparison logic here rather
                    // than assuming that FLOAT is a don't care case.
                    continue;
                }
                String message = colPrefix + "values not equal: expected: " + expectedObj + ", actual: " + actualObj;
                throwVoltAbortExceptionIf(message,
                        ! expectedObj.toString().equals(actualObj.toString()), expectedRows, actualRows);
            }
            ii++;
        }
    }

    private void throwVoltAbortExceptionIf(String label, boolean condition,
            VoltTable expectedRows, VoltTable actualRows) {
        if (condition) {
            System.out.println("IN TruncatePeople throwing: " + label);
            if (expectedRows != null) {
                System.out.println("EXPECTED: " + expectedRows);
            }
            if (actualRows != null) {
                System.out.println("  ACTUAL: " + actualRows);
            }
            throw new VoltAbortException(label);
        }
    }
}
