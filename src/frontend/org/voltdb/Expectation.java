/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb;

import org.voltdb.VoltProcedure.VoltAbortException;

public class Expectation {

    static enum Type {
        EXPECT_EMPTY,
        EXPECT_ONE_ROW,
        EXPECT_ZERO_OR_ONE_ROW,
        EXPECT_NON_EMPTY,
        EXPECT_SCALAR,
        EXPECT_SCALAR_LONG,
        EXPECT_SCALAR_MATCH,
    }

    final private Type m_type;
    final private long m_scalar;

    Expectation(Type t) {
        assert (t != null);
        m_type = t;
        m_scalar = 0;
    }

    Expectation(Type t, long scalar) {
        m_type = t;
        m_scalar = scalar;
    }

    static void fail(String procedureName, SQLStmt stmt, int batchIndex,
            String errMsg) throws VoltAbortException {

        String fullMsg = "Expectation failing in procedure: " + procedureName + "\n";
        fullMsg += "  Running SQL: " + stmt.getText() + "\n";
        fullMsg += "  Error message: " + errMsg;

        throw new VoltAbortException(fullMsg);
    }

    static void check(String procedureName, SQLStmt stmt, int batchIndex,
            Expectation expectation, VoltTable table) throws VoltAbortException {
        if (expectation == null)
            return;

        assert(table != null);
        int rowCount = table.getRowCount();

        switch (expectation.m_type) {
        case EXPECT_EMPTY:
            if (rowCount != 0) {
                fail(procedureName, stmt, batchIndex,
                     String.format("Expected zero row, but got %d", rowCount));
            }
            return;
        case EXPECT_ONE_ROW:
            if (rowCount != 1) {
                fail(procedureName, stmt, batchIndex,
                     String.format("Expected one row, but got %d", rowCount));
            }
            return;
        case EXPECT_ZERO_OR_ONE_ROW:
            if (rowCount > 1) {
                fail(procedureName, stmt, batchIndex,
                     String.format("Expected zero or one rows, but got %d", rowCount));
            }
            return;
        case EXPECT_NON_EMPTY:
            if (rowCount == 0) {
                fail(procedureName, stmt, batchIndex,
                     String.format("Expected one or more rows, but got %d", rowCount));
            }
            return;
        case EXPECT_SCALAR:
            if (checkScalar(table) == false) {
                fail(procedureName, stmt, batchIndex, "Expected scalar value");
            }
            return;
        case EXPECT_SCALAR_LONG:
            if (checkScalarLong(table) == false) {
                fail(procedureName, stmt, batchIndex, "Expected scalar long value");
            }
            return;
        case EXPECT_SCALAR_MATCH:
            if (checkScalarLong(table) == false) {
                fail(procedureName, stmt, batchIndex, "Expected scalar long value");
            }
            if (table.asScalarLong() != expectation.m_scalar) {
                fail(procedureName, stmt, batchIndex,
                        String.format("Expected scalar %d, but got %d", expectation.m_scalar, table.asScalarLong()));
            }
            return;
        }
    }

    static boolean checkScalar(VoltTable table) {
        if (table.getRowCount() != 1) return false;
        if (table.getColumnCount() != 1) return false;
        return true;
    }

    static boolean checkScalarLong(VoltTable table) {
        if (table.getRowCount() != 1) return false;
        if (table.getColumnCount() != 1) return false;
        if (table.getColumnType(0) != VoltType.BIGINT) return false;
        return true;
    }
}
