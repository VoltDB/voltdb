/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.voltcore.logging.VoltLogger;
import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.ProcedureRunner;
import org.voltdb.SQLStmt;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltDB;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.sysprocs.LowImpactDeleteNT.ComparisonOperation;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.VoltTable.ColumnInfo;

public class MigrateRowsBase extends VoltSystemProcedure {
    VoltLogger exportLog = new VoltLogger("EXPORT");

    private static ColumnInfo[] schema = new ColumnInfo[] {
            new ColumnInfo("MIGRATED_ROWS", VoltType.BIGINT),  /* number of rows be migrated in this invocation */
            new ColumnInfo("LEFT_ROWS", VoltType.BIGINT)      /* number of rows to be deleted after this invocation */
    };

    public long[] getPlanFragmentIds() {
        return new long[]{};
    }

    public DependencyPair executePlanFragment(
            Map<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params, SystemProcedureExecutionContext context) {
        return null;
    }

    /**
     * Execute a pre-compiled adHoc SQL statement, throw exception if not.
     *
     * @return Count of rows inserted or upserted.
     * @throws VoltAbortException if any failure at all.
     */
    VoltTable executePrecompiledSQL(Statement catStmt, Object[] params, boolean replicated)
            throws VoltAbortException {
        // Create a SQLStmt instance on the fly
        // This is unusual to do, as they are typically required to be final instance variables.
        // This only works because the SQL text and plan is identical from the borrowed procedure.
        SQLStmt stmt = new SQLStmt(catStmt.getSqltext());
        if (replicated) {
            stmt.setInCatalog(false);
        }
        m_runner.initSQLStmt(stmt, catStmt);

        voltQueueSQL(stmt, params);
        return voltExecuteSQL()[0];
    }

    /**
     * Migrate procedure for partitioned or replicated tables
     *
     * @param ctx         Internal API provided to all system procedures
     * @param partitionParam Partition parameter used to match invocation to partition
     * @param tableName   Name of persistent table
     * @param columnName  A column in the given table that its value can be used to provide
     *                    order for delete action. (Unique or non-unique) index is expected
     *                    on the column, if not a warning message will be printed.
     * @param compStr     ">", "<", ">=", "<=", "=="
     * @param parameter   value to compare
     * @param chunksize   maximum number of rows allow to be migrated
     * @param replicated  partitioned or replicated table
     * @return how many rows are migrated and how many rows left to be migrated (if any)
     */
    VoltTable migrateRowsCommon(SystemProcedureExecutionContext ctx,
            String tableName,
            String columnName,
            String compStr,
            VoltTable paramTable,
            long chunksize,
            boolean replicated) {
        VoltTable table = new VoltTable(schema);
        // Some basic checks
        if (chunksize <= 0) {
            throw new VoltAbortException(
                    "Chunk size must be positive, current value is" + chunksize);
        }
        Table catTable = ctx.getDatabase().getTables().getIgnoreCase(tableName);
        if (catTable == null) {
            throw new VoltAbortException(
                    String.format("Table %s doesn't present in catalog", tableName));
        }
        Collection<Column> keys = CatalogUtil.getPrimaryKeyColumns(catTable);
        if (keys.isEmpty()) {
            throw new VoltAbortException(
                    String.format("Primary key doesn't present in %s", catTable));
        }

        if (replicated ^ catTable.getIsreplicated()) {
            throw new VoltAbortException(
                    String.format("%s incompatible with %s table %s.",
                            replicated ? "@NibbleExportMP" : "@NibbleExportSP",
                            catTable.getIsreplicated() ? "replicated" : "partitioned", tableName));
        }
        Column column = catTable.getColumns().get(columnName);
        if (column == null) {
            throw new VoltAbortException(
                    String.format("Column %s does not exist in table %s", columnName, tableName));
        }

        // TO DO:verify all required indices

        // so far should only be single column, single row table
        int columnCount = paramTable.getColumnCount();
        if (columnCount > 1) {
            throw new VoltAbortException("More than one input parameter is not supported right now.");
        }
        assert(columnCount == 1);
        paramTable.resetRowPosition();
        paramTable.advanceRow();
        Object[] params = new Object[columnCount];
        for (int i = 0; i < columnCount; i++) {
            params[i] = paramTable.get(i, paramTable.getColumnType(i));
        }
        assert (params.length == 1);

        VoltType expectedType = VoltType.values()[column.getType()];
        VoltType actualType = VoltType.typeFromObject(params[0]);
        if (actualType != expectedType) {
            throw new VoltAbortException(String.format("Parameter type %s doesn't match column type %s",
                            actualType.toString(), expectedType.toString()));
        }

        ComparisonOperation op = ComparisonOperation.fromString(compStr);

        ProcedureRunner pr = ctx.getSiteProcedureConnection().getMigrateProcRunner(
                tableName + ".autogenMigrate"+ op.toString(), catTable, column, op);

        Procedure newCatProc = pr.getCatalogProcedure();
        Statement countStmt = newCatProc.getStatements().get(VoltDB.ANON_STMT_NAME + "0");
        Statement migrateStmt = newCatProc.getStatements().get(VoltDB.ANON_STMT_NAME + "1");
        Statement valueAtStmt = newCatProc.getStatements().get(VoltDB.ANON_STMT_NAME + "2");
        if (countStmt == null || migrateStmt == null || valueAtStmt == null) {
            throw new VoltAbortException(
                    String.format("Unable to find SQL statement for found table %s: BAD",
                            tableName));
        }

        Object cutoffValue = null;
        VoltTable result = null;
        result = executePrecompiledSQL(countStmt, params, replicated);
        long rowCount = result.asScalarLong();
        // If number of rows meet the criteria is more than chunk size, pick the column value
        // which offset equals to chunk size as new predicate.
        // Please be noted that it means rows be deleted can be more than chunk size, normally
        // data has higher cardinality won't exceed the limit a lot,  but low cardinality data
        // might cause this procedure to delete a large number of rows than the limit chunk size.
        if (op != ComparisonOperation.EQ) {
            if (rowCount > chunksize) {
                result = executePrecompiledSQL(valueAtStmt, new Object[] { chunksize }, replicated);
                cutoffValue = result.fetchRow(0).get(0, actualType);
            }
        }
        result = executePrecompiledSQL(migrateStmt,
                                       cutoffValue == null ? params : new Object[] {cutoffValue},
                                       replicated);
        long deletedRows = result.asScalarLong();

        // Return rows be deleted in this run and rows left for next run
        VoltTable retTable = new VoltTable(schema);
        retTable.addRow(deletedRows, rowCount - deletedRows);
        return retTable;
    }
}