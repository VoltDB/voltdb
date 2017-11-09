/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

package org.voltdb.sysprocs;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.RateLimitedLogger;
import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.SQLStmt;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltDB;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.compiler.StatementCompiler;
import org.voltdb.sysprocs.NibbleDeleteSP.ComparisonConstant;
import org.voltdb.utils.CatalogUtil;

public class NibbleDeleteMP extends VoltSystemProcedure {
    private static VoltLogger hostLog = new VoltLogger("HOST");

    private static ColumnInfo[] schema = new ColumnInfo[] {
            /* number of rows be deleted in this invocation */
            new ColumnInfo("DELETED_ROWS", VoltType.BIGINT),
            /* number of rows to be deleted after this invocation */
            new ColumnInfo("LEFTOVER_ROWS", VoltType.BIGINT)
    };

    public long[] getPlanFragmentIds() {
        return new long[]{};
    }

    public DependencyPair executePlanFragment(
            Map<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params, SystemProcedureExecutionContext context) {
        // Never called, we do all the work in run()
        return null;
    }

    /**
     * Execute a set of queued inserts. Ensure each insert successfully
     * inserts one row. Throw exception if not.
     *
     * @return Count of rows inserted or upserted.
     * @throws VoltAbortException if any failure at all.
     */
    VoltTable executeSQLOnTheFly(Statement catStmt, Object[] params) throws VoltAbortException {
        // Create a SQLStmt instance on the fly
        // This unusual to do, as they are typically required to be final instance variables.
        // This only works because the SQL text and plan is identical from the borrowed procedure.
        SQLStmt stmt = new SQLStmt(catStmt.getSqltext());
        m_runner.initSQLStmt(stmt, catStmt);

        voltQueueSQL(stmt, params);
        return voltExecuteSQL()[0];
    }

    /**
     * Nibble delete procedure for replicated tables
     *
     * @param ctx         Internal API provided to all system procedures
     * @param tableName   Name of persistent partitioned table
     * @param columnName  A column in the given table that its value can be used to provide
     *                    order for delete action. (Unique or non-unique) index is expected
     *                    on the column, if not a warning message will be printed.
     * @param comparison  0-"GREATER_THAN", 1-"LESS_THAN", 2-"GREATER_THAN_OR_EQUAL",
     *                    3-"LESS_THAN_OR_EQUAL", 4-"EQUAL"
     * @param table       value to compare
     * @param chunksize   maximum number of rows allow to be deleted
     * @return how many rows are deleted and how many rows left to be deleted (if any)
     */
    public VoltTable run(SystemProcedureExecutionContext ctx,
                         String tableName, String columnName,
                         int comparison, VoltTable table, long chunksize) {
        // Some basic checks
        Table catTable = ctx.getDatabase().getTables().getIgnoreCase(tableName);
        if (catTable == null) {
            throw new VoltAbortException("Table not present in catalog");
        }
        if (!catTable.getIsreplicated()) {
            throw new VoltAbortException(
                    String.format("%s incompatible with partitioned table %s.",
                            this.getClass().getName(), tableName));
        }
        Column column = catTable.getColumns().get(columnName);
        if (column == null) {
            throw new VoltAbortException(
                    String.format("Column %s does not exist in table %s", columnName, tableName));
        }

        if (!CatalogUtil.hasIndex(catTable, column)) {
            RateLimitedLogger.tryLogForMessage(System.currentTimeMillis(),
                    60, TimeUnit.SECONDS,
                    hostLog, Level.WARN,
                    "Column %s doesn't have an index, it may leads to very slow delete "
                            + "which requires full table scan.", column.getTypeName());
        }

        // so far should only be single column, single row table
        int columnCount = table.getColumnCount();
        if (columnCount > 1) {
            throw new VoltAbortException("Only support one input parameter now.");
        }
        assert(columnCount == 1);
        table.resetRowPosition();
        table.advanceRow();
        Object[] params = new Object[columnCount];
        for (int i = 0; i < columnCount; i++) {
            params[i] = table.get(i, table.getColumnType(i));
        }
        assert (params.length == 1);

        VoltType expectedType = VoltType.values()[column.getType()];
        VoltType actualType = VoltType.typeFromObject(params[0]);
        if (actualType != expectedType) {
            throw new VoltAbortException(
                    String.format("Parameter type %s doesn't match column type %s",
                            actualType.toString(), expectedType.toString()));
        }

        if (comparison < ComparisonConstant.GREATER_THAN.ordinal() ||
                comparison > ComparisonConstant.EQUAL.ordinal()) {
            throw new VoltAbortException("Invalid comparison constant: " + comparison);
        }
        ComparisonConstant comparisonConstant = ComparisonConstant.values()[comparison];

        // TODO: should cache the plan in somewhere
        Procedure newCatProc = StatementCompiler.compileNibbleDeleteProcedure(catTable,
                this.getClass().getName() + "-" + tableName, column, comparisonConstant);

        Statement countStmt = newCatProc.getStatements().get(VoltDB.ANON_STMT_NAME + "0");
        Statement deleteStmt = newCatProc.getStatements().get(VoltDB.ANON_STMT_NAME + "1");
        Statement valueAtStmt = newCatProc.getStatements().get(VoltDB.ANON_STMT_NAME + "2");
        if (countStmt == null || deleteStmt == null || valueAtStmt == null) {
            throw new VoltAbortException(
                    String.format("Unable to find SQL statement for found table %s: BAD",
                            tableName));
        }

        Object valueAtBoundary = null;
        VoltTable result = null;
        result = executeSQLOnTheFly(countStmt, params);
        long rowCount = result.asScalarLong();
        // If number of rows meet the criteria is more than chunk size, pick the column value
        // which offset equals to chunk size as new predicate.
        // TODO:how to delete rows match exactly to the chunk size while not causing the
        // non-deterministic issue?
        // Please be noted that it means rows be deleted can be more than chunk size, normally
        // data has higher cardinality won't exceed the limit a lot,  but low cardinality data
        // might cause this procedure to delete a large number of rows than the limit chunk size.
        if (rowCount > chunksize) {
            result = executeSQLOnTheFly(valueAtStmt, new Object[] { chunksize });
            valueAtBoundary = result.fetchRow(0).get(0, actualType);
        }
        result = executeSQLOnTheFly(deleteStmt,
                (valueAtBoundary == null) ? params : new Object[] {valueAtBoundary});
        long deletedRows = result.asScalarLong();

        // Return rows be deleted in this run and rows left for next run
        VoltTable retTable = new VoltTable(schema);
        retTable.addRow(deletedRows, rowCount - deletedRows);
        return retTable;
    }

}
