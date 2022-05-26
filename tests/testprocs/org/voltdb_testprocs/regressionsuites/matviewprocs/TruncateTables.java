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

public class TruncateTables extends VoltProcedure {

    public final SQLStmt captureview1 = new SQLStmt("SELECT * FROM ORDER_COUNT_GLOBAL;");
    public final SQLStmt captureview2 = new SQLStmt("SELECT * FROM ORDER_COUNT_NOPCOL ORDER BY NAME;");
    public final SQLStmt captureview3 = new SQLStmt("SELECT * FROM ORDER_DETAIL_NOPCOL ORDER BY NAME;");
    public final SQLStmt captureview4 = new SQLStmt("SELECT * FROM ORDER2016 ORDER BY NAME;");

    public final SQLStmt validateview1 = new SQLStmt("SELECT CNT FROM ORDER_COUNT_GLOBAL;");
    public final SQLStmt validateview2 = new SQLStmt("SELECT COUNT(*) FROM ORDER_COUNT_NOPCOL;");
    public final SQLStmt validateview3 = new SQLStmt("SELECT COUNT(*) FROM ORDER_DETAIL_NOPCOL;");
    public final SQLStmt validateview4 = new SQLStmt("SELECT COUNT(*) FROM ORDER2016;");

    public final SQLStmt clearcache1 = new SQLStmt("TRUNCATE TABLE WAS_CUSTOMERS;");
    public final SQLStmt clearcache2 = new SQLStmt("TRUNCATE TABLE WAS_ORDERS;");
    public final SQLStmt clearcache3 = new SQLStmt("TRUNCATE TABLE WAS_ORDERITEMS;");
    public final SQLStmt clearcache4 = new SQLStmt("TRUNCATE TABLE WAS_PRODUCTS;");

    public final SQLStmt cachebase1 = new SQLStmt("INSERT INTO WAS_CUSTOMERS  SELECT * FROM CUSTOMERS  ORDER BY CUSTOMER_ID;");
    public final SQLStmt cachebase2 = new SQLStmt("INSERT INTO WAS_ORDERS     SELECT * FROM ORDERS     ORDER BY ORDER_ID;");
    public final SQLStmt cachebase3 = new SQLStmt("INSERT INTO WAS_ORDERITEMS SELECT * FROM ORDERITEMS ORDER BY ORDER_ID, PID;");
    public final SQLStmt cachebase4 = new SQLStmt("INSERT INTO WAS_PRODUCTS   SELECT * FROM PRODUCTS   ORDER BY PID;");

    public final SQLStmt truncatebase1 = new SQLStmt("TRUNCATE TABLE CUSTOMERS;");
    public final SQLStmt truncatebase2 = new SQLStmt("TRUNCATE TABLE ORDERS;");
    public final SQLStmt truncatebase3 = new SQLStmt("TRUNCATE TABLE ORDERITEMS;");
    public final SQLStmt truncatebase4 = new SQLStmt("TRUNCATE TABLE PRODUCTS;");

    public final SQLStmt validatebase1 = new SQLStmt("SELECT COUNT(*) FROM CUSTOMERS;");
    public final SQLStmt validatebase2 = new SQLStmt("SELECT COUNT(*) FROM ORDERS;");
    public final SQLStmt validatebase3 = new SQLStmt("SELECT COUNT(*) FROM ORDERITEMS;");
    public final SQLStmt validatebase4 = new SQLStmt("SELECT COUNT(*) FROM PRODUCTS;");

    public final SQLStmt renewbase1 = new SQLStmt("INSERT INTO CUSTOMERS  SELECT * FROM WAS_CUSTOMERS  ORDER BY CUSTOMER_ID;");
    public final SQLStmt renewbase2 = new SQLStmt("INSERT INTO ORDERS     SELECT * FROM WAS_ORDERS     ORDER BY ORDER_ID;");
    public final SQLStmt renewbase3 = new SQLStmt("INSERT INTO ORDERITEMS SELECT * FROM WAS_ORDERITEMS ORDER BY ORDER_ID, PID;");
    public final SQLStmt renewbase4 = new SQLStmt("INSERT INTO PRODUCTS   SELECT * FROM WAS_PRODUCTS   ORDER BY PID;");

    public VoltTable run(int rollback,
            int truncateTable1,
            int truncateTable2,
            int truncateTable3,
            int truncateTable4) {
        String diff = null;
        try {
            System.out.println("Running TruncateTables." + rollback + "." +
                    truncateTable1 + "." + truncateTable2 + "." +
                    truncateTable3 + "." + truncateTable4);
            int truncationCount = 0;

            VoltTable[] beforeViews = executeViewScans();

            if (truncateTable1 != 0) {
                ++truncationCount;
                voltQueueSQL(clearcache1);
                voltQueueSQL(cachebase1);
                voltQueueSQL(truncatebase1); // ("TRUNCATE TABLE CUSTOMERS;");
            }
            if (truncateTable2 != 0) {
                ++truncationCount;
                voltQueueSQL(clearcache2);
                voltQueueSQL(cachebase2);
                voltQueueSQL(truncatebase2); // ("TRUNCATE TABLE ORDERS;");
            }
            if (truncateTable3 != 0) {
                ++truncationCount;
                voltQueueSQL(clearcache3);
                voltQueueSQL(cachebase3);
                voltQueueSQL(truncatebase3); // ("TRUNCATE TABLE ORDERITEMS;");
            }
            if (truncateTable4 != 0) {
                ++truncationCount;
                voltQueueSQL(clearcache4);
                voltQueueSQL(cachebase4);
                voltQueueSQL(truncatebase4); // ("TRUNCATE TABLE PRODUCTS;");
            }

            if (truncationCount == 0) {
                if (rollback != 0) {
                    throw new VoltAbortException("Rolling back as requested.");
                }
                // There's nothing to do. The queue is empty.
                return wrapResult(""); // success
            }

            voltExecuteSQL();

            executeTruncationChecks(
                    truncateTable1,
                    truncateTable2,
                    truncateTable3,
                    truncateTable4);

            executeRenewBases(
                    truncateTable1,
                    truncateTable2,
                    truncateTable3,
                    truncateTable4);

            VoltTable[] afterViews = executeViewScans();
            int ii = 0;
            for (VoltTable afterView : afterViews){
                diff = compareTables("View " + (ii + 1) + " after " + truncationCount +
                        " truncate(s) and restore(s), ",
                        beforeViews[ii], afterView, 0.01);
                ++ii;
            }
        }
        catch (Throwable t) {
            System.out.println("TruncateTables abnormal exit after: " + t);
            throw t;
        }
        if (rollback != 0) {
            System.out.println("Rolling back as requested.");
            throw new VoltAbortException("Rolling back as requested.");
        }
        VoltTable result = wrapResult(diff);
        System.out.println("TruncateTables normal exit.");
        return result;
    }

    private VoltTable[] executeViewScans() {
        voltQueueSQL(captureview1);
        voltQueueSQL(captureview2);
        voltQueueSQL(captureview3);
        voltQueueSQL(captureview4);
        return voltExecuteSQL();
    }

    private VoltTable wrapResult(String diff) {
        VoltTable result = new VoltTable(
                new VoltTable.ColumnInfo("DIFF", VoltType.STRING));
        result.addRow(diff);
        return result;
    }

    private void executeTruncationChecks(
                int truncateTable1,
                int truncateTable2,
                int truncateTable3,
                int truncateTable4) {
        voltQueueSQL(validateview1);
        voltQueueSQL(validateview2);
        voltQueueSQL(validateview3);
        voltQueueSQL(validateview4);

        if (truncateTable1 != 0) {
            voltQueueSQL(validatebase1); // ("SELECT COUNT(*) FROM CUSTOMERS;");
        }
        if (truncateTable2 != 0) {
            voltQueueSQL(validatebase2); // ("SELECT COUNT(*) FROM ORDERS;");
        }
        if (truncateTable3 != 0) {
            voltQueueSQL(validatebase3); // ("SELECT COUNT(*) FROM ORDERITEMS;");
        }
        if (truncateTable4 != 0) {
            voltQueueSQL(validatebase4); // ("SELECT COUNT(*) FROM PRODUCTS;");
        }
        VoltTable[] empties = voltExecuteSQL();

        int jj = 1;
        for (VoltTable cleared : empties) {
            if (cleared.asScalarLong() != 0) {
                System.out.println(
                        "DEBUG Truncate failed to delete all rows of table/view " + jj +
                        " of " + empties.length + ", leaving: " + cleared.asScalarLong());
            }
            ++jj;
        }
    }

    private void executeRenewBases(
            int truncateTable1,
            int truncateTable2,
            int truncateTable3,
            int truncateTable4) {
        if (truncateTable1 != 0) {
            voltQueueSQL(renewbase1); // ("INSERT INTO CUSTOMERS;");
        }
        if (truncateTable2 != 0) {
            voltQueueSQL(renewbase2); // ("INSERT INTO ORDERS;");
        }
        if (truncateTable3 != 0) {
            voltQueueSQL(renewbase3); // ("INSERT INTO ORDERITEMS;");
        }
        if (truncateTable4 != 0) {
            voltQueueSQL(renewbase4); // ("INSERT INTO PRODUCTS;");
        }
        voltExecuteSQL();
    }

    private static final long TOO_MUCH_INFO = 100;

    private String compareTables(String prefix,
            VoltTable expectedRows, VoltTable actualRows, Double epsilon) {
        if (expectedRows.getColumnCount() != actualRows.getColumnCount()) {
            return prefix + "column count mismatch." +
                    " Expected: " + expectedRows.getColumnCount() + " actual: " + actualRows.getColumnCount();
        }
        if (expectedRows.getRowCount() != actualRows.getRowCount()) {
            long expRowCount = expectedRows.getRowCount();
            long actRowCount = actualRows.getRowCount();
            if (expRowCount + actRowCount < TOO_MUCH_INFO) {
                System.out.println("Expected: " + expectedRows);
                System.out.println("Actual:   " + actualRows);
            }
            else {
                System.out.println("Expected: " + expRowCount + " rows");
                System.out.println("Actual:   " + actRowCount + " rows");
            }
            return prefix + "row count mismatch.  Expected: " + expectedRows.getRowCount() + " actual: " + actualRows.getRowCount();
        }
        int ii = 1;
        while (expectedRows.advanceRow()) {
            if (! actualRows.advanceRow()) {
                return prefix + "too few actual rows; expected more than " + ii;
            }
            for (int j = 0; j < actualRows.getColumnCount(); j++) {
                String columnName = actualRows.getColumnName(j);
                String colPrefix = prefix + "row " + ii + ": column: " + columnName + ": ";
                VoltType actualType = actualRows.getColumnType(j);
                VoltType expectedType = expectedRows.getColumnType(j);
                if (expectedType != actualType) {
                    return colPrefix + "type mismatch: expected:" +
                            expectedType + " got: " + actualType;
                }
                Object expectedObj = expectedRows.get(j, expectedType);
                Object actualObj = actualRows.get(j, actualType);
                if (expectedRows.wasNull()) {
                    if (actualRows.wasNull()) {
                        continue;
                    }
                    return colPrefix + "expected null, got non null value: " + actualObj;
                }
                else {
                    if (actualRows.wasNull()) {
                        return colPrefix + "expected the value " + expectedObj +
                            ", got a null value.";
                    }
                    String message = colPrefix + "values not equal: expected: " +
                        expectedObj + ", actual: " + actualObj;
                    if (expectedType == VoltType.FLOAT) {
                        if (epsilon != null) {
                            if (((Double)expectedObj).doubleValue() + epsilon <
                                    ((Double)actualObj).doubleValue() ||
                               (((Double)actualObj).doubleValue() + epsilon <
                                       ((Double)expectedObj).doubleValue())) {
                                return message;
                            }
                            continue;
                        }
                        // With no epsilon provided, fall through to take
                        // a chance on an exact value match, but helpfully
                        // annotate any false positive that results.
                        message += ". NOTE: You may want to pass a" +
                                " non-null epsilon value >= " +
                                Math.abs((Double)expectedObj - (Double)actualObj) +
                                " to the table comparison test " +
                                " if nearly equal FLOAT values are " +
                                " causing a false positive mismatch.";
                    }
                    if (! expectedObj.equals(actualObj)){
                        return message;
                    }
                }
            }
            ++ii;
        }
        return "";
    }

}
