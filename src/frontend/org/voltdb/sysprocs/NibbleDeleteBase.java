/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltdb.DefaultProcedureManager;
import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.ProcedureRunner;
import org.voltdb.SQLStmt;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltDB;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.sysprocs.LowImpactDeleteNT.ComparisonOperation;

public class NibbleDeleteBase extends VoltSystemProcedure {

    private static VoltLogger hostLog = new VoltLogger("HOST");

    private static ColumnInfo[] schema = new ColumnInfo[] {
            new ColumnInfo("DELETED_ROWS", VoltType.BIGINT),  /* number of rows be deleted in this invocation */
            new ColumnInfo("LEFT_ROWS", VoltType.BIGINT) /* number of rows to be deleted after this invocation */
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
     * Execute a pre-compiled adHoc SQL statement, throw exception if not.
     *
     * @return Count of rows inserted or upserted.
     * @throws VoltAbortException if any failure at all.
     */
    VoltTable executePrecompiledSQL(Statement catStmt, Object[] params, boolean replicated)
            throws VoltAbortException
    {
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

    boolean hasIndex(Table table, Column column) {
        // look for all indexes where the first column matches our index
        List<Index> candidates = new ArrayList<>();
        for (Index catIndexIterator : table.getIndexes()) {
            for (ColumnRef colRef : catIndexIterator.getColumns()) {
                // we only care about the first index
                if (colRef.getIndex() != 0) continue;
                if (colRef.getColumn() == column) {
                    candidates.add(catIndexIterator);
                }
            }
        }
        // error no index found
        if (candidates.size() == 0) {
            String msg = String.format("Could not find index to support LowImpactDelete on column %s.%s. ",
                    table.getTypeName(), column.getTypeName());
            msg += String.format("Please create an index where column %s.%s is the first or only indexed column.",
                    table.getTypeName(), column.getTypeName());
            throw new VoltAbortException(msg);
        }
        // now make sure index is countable (which also ensures ordered because countable non-ordered isn't a thing)
        // note countable ordered indexes are the default... so something weird happened if this is the case
        // Then go and pick the best index sorted by uniqueness, columncount

        long indexCount = candidates.stream()
                                    .filter(i -> i.getCountable())
                                    .count();

        if (indexCount == 0) {
            String msg = String.format("Count not find index to support LowImpactDelete on column %s.%s. ",
                    table.getTypeName(), column.getTypeName());
            msg += String.format("Indexes must support ordering and ranking (as default indexes do).",
                    table.getTypeName(), column.getTypeName());
            throw new VoltAbortException(msg);
        }

      return indexCount > 0;
  }

    VoltTable nibbleDeleteCommon(SystemProcedureExecutionContext ctx,
                                 String tableName,
                                 String columnName,
                                 String compStr,
                                 VoltTable paramTable,
                                 long chunksize,
                                 boolean replicated)
    {
        // Some basic checks
        if (chunksize <= 0) {
            throw new VoltAbortException(
                    "Chunk size must be positive, current value is" + chunksize);
        }
        Table catTable = ctx.getDatabase().getTables().getIgnoreCase(tableName);
        if (catTable == null) {
            throw new VoltAbortException(
                    String.format("Table %s doesn't present in catalog",
                            tableName));
        }
        if (replicated ^ catTable.getIsreplicated()) {
            throw new VoltAbortException(
                    String.format("%s incompatible with %s table %s.",
                            replicated ? "@NibbleDeleteMP" : "@NibbleDeleteSP",
                            catTable.getIsreplicated() ? "replicated" : "partitioned", tableName));
        }
        Column column = catTable.getColumns().get(columnName);
        if (column == null) {
            throw new VoltAbortException(
                    String.format("Column %s does not exist in table %s", columnName, tableName));
        }

        if (!hasIndex(catTable, column)) {
            hostLog.rateLimitedWarn(60, "Column %s doesn't have an index, it may lead to very slow delete "
                                    + "which requires full table scan.", column.getTypeName());
        }

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
            throw new VoltAbortException(
                    String.format("Parameter type %s doesn't match column type %s",
                            actualType.toString(), expectedType.toString()));
        }

        ComparisonOperation op = ComparisonOperation.fromString(compStr);

        ProcedureRunner pr = ctx.getSiteProcedureConnection().getNibbleDeleteProcRunner(
                tableName + "." + DefaultProcedureManager.NIBBLE_DELETE_PROC, catTable, column, op);
        Procedure newCatProc = pr.getCatalogProcedure();

        Statement countStmt = newCatProc.getStatements().get(VoltDB.ANON_STMT_NAME + "0");
        Statement deleteStmt = newCatProc.getStatements().get(VoltDB.ANON_STMT_NAME + "1");
        Statement valueAtStmt = newCatProc.getStatements().get(VoltDB.ANON_STMT_NAME + "2");
        if (countStmt == null || deleteStmt == null || valueAtStmt == null) {
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
        result = executePrecompiledSQL(deleteStmt,
                                       cutoffValue == null ? params : new Object[] {cutoffValue},
                                       replicated);
        long deletedRows = result.asScalarLong();

        // Return rows be deleted in this run and rows left for next run
        VoltTable retTable = new VoltTable(schema);
        retTable.addRow(deletedRows, rowCount - deletedRows);
        return retTable;
    }
}
