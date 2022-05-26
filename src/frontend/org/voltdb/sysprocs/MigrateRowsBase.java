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
import org.voltdb.iv2.TxnEgo;
import org.voltdb.sysprocs.LowImpactDeleteNT.ComparisonOperation;

public class MigrateRowsBase extends VoltSystemProcedure {
    VoltLogger exportLog = new VoltLogger("EXPORT");

    private static ColumnInfo[] schema = new ColumnInfo[] {
            new ColumnInfo("MIGRATED_ROWS", VoltType.BIGINT),  /* number of rows be migrated in this invocation */
            new ColumnInfo("LEFT_ROWS", VoltType.BIGINT)       /* number of rows to be deleted after this invocation */
    };

    @Override
    public long[] getPlanFragmentIds() {
        return new long[]{};
    }

    @Override
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

    void verifyRequiredIndices(Table table, Column column) {
        // look for all indexes where the first column matches our index
        List<Index> candidates = new ArrayList<>();
        boolean hasMigratingIndex = false;
        for (Index idx : table.getIndexes()) {
            if (!hasMigratingIndex) {
                hasMigratingIndex = idx.getMigrating();
            }
            for (ColumnRef colRef : idx.getColumns()) {
                // we only care about the first index
                if (colRef.getIndex() == 0 && colRef.getColumn()==column) {
                    candidates.add(idx);
                }
            }
        }
        if (!hasMigratingIndex) {
            String msg = String.format("Could not find migrating index for column %s.%s, example: \"CREATE INDEX myindex ON %s(%s) WHERE NOT MIGRATING\"",
                    table.getTypeName(), column.getTypeName(), table.getTypeName(), column.getTypeName());
            throw new VoltAbortException(msg);
        }
        // error no index found
        if (candidates.size() == 0) {
            String msg = String.format("Could not find index to support migrate on column %s.%s. ",
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
            String msg = String.format("Coudt not find index to support Migrate on column %s.%s. ",
                    table.getTypeName(), column.getTypeName());
            msg += String.format("Indexes must support ordering and ranking (as default indexes do).",
                    table.getTypeName(), column.getTypeName());
            throw new VoltAbortException(msg);
        }
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

        if (replicated != catTable.getIsreplicated()) {
            throw new VoltAbortException(
                    String.format("%s incompatible with %s table %s.",
                            replicated ? "@MigrateRowsMP" : "@MigrateRowsSP",
                            catTable.getIsreplicated() ? "replicated" : "partitioned", tableName));
        }
        Column column = catTable.getColumns().get(columnName);
        if (column == null) {
            throw new VoltAbortException(
                    String.format("Column %s does not exist in table %s", columnName, tableName));
        }

        // verify all required indices
        verifyRequiredIndices(catTable, column);
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
                tableName + "." + DefaultProcedureManager.NIBBLE_MIGRATE_PROC, catTable, column, op);

        Procedure newCatProc = pr.getCatalogProcedure();
        Statement countStmt = newCatProc.getStatements().get(VoltDB.ANON_STMT_NAME + "0");
        Statement valueAtStmt = newCatProc.getStatements().get(VoltDB.ANON_STMT_NAME + "1");
        Statement migrateStmt = newCatProc.getStatements().get(VoltDB.ANON_STMT_NAME + "2");

        if (countStmt == null || migrateStmt == null || valueAtStmt == null) {
            throw new VoltAbortException(
                    String.format("Unable to find SQL statement for migrate on table %s",
                            tableName));
        }

        Object cutoffValue = null;
        VoltTable result = null;
        result = executePrecompiledSQL(countStmt, params, replicated);
        long rowCount = result.asScalarLong();
        if (rowCount > 0 && exportLog.isDebugEnabled()) {
            exportLog.debug("Migrate on table " + tableName +
                    " on partition " + ctx.getPartitionId() +
                    " reported " + rowCount + " matching rows. txnid:" + TxnEgo.txnIdToString(m_runner.getTxnState().txnId) + " sphandle:" +
                    m_runner.getTxnState().m_spHandle);
        }

        // If number of rows meet the criteria is more than chunk size, pick the column value
        // which offset equals to chunk size as new predicate.
        // Please be noted that it means rows be deleted can be more than chunk size, normally
        // data has higher cardinality won't exceed the limit a lot,  but low cardinality data
        // might cause this procedure to delete a large number of rows than the limit chunk size.
        if (op != ComparisonOperation.EQ) {
            if (rowCount > chunksize) {
                result = executePrecompiledSQL(valueAtStmt, new Object[] { chunksize }, replicated);
                cutoffValue = result.fetchRow(0).get(0, actualType);

                if (exportLog.isDebugEnabled()) {
                    exportLog.debug("Migrate on table " + tableName +
                            (replicated ? "" : " on partition " + ctx.getPartitionId()) +
                            " reported " + cutoffValue + " target ttl");
                }
            }
        }
        result = executePrecompiledSQL(migrateStmt,
                                       cutoffValue == null ? params : new Object[] {cutoffValue},
                                       replicated);
        long migratedRows = result.asScalarLong();

        // Return rows be deleted in this run and rows left for next run
        VoltTable retTable = new VoltTable(schema);
        retTable.addRow(migratedRows, rowCount - migratedRows);
        return retTable;
    }
}
