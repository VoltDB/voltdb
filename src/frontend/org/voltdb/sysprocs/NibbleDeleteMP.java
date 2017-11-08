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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.RateLimitedLogger;
import org.voltdb.CatalogContext;
import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.SQLStmt;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltDB;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.PlanFragment;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;
import org.voltdb.catalog.Table;
import org.voltdb.compiler.PlannerTool;
import org.voltdb.compiler.StatementCompiler;
import org.voltdb.expressions.ParameterValueExpression;
import org.voltdb.planner.CompiledPlan;
import org.voltdb.planner.StatementPartitioning;
import org.voltdb.sysprocs.NibbleDeleteSP.ComparisonConstant;
import org.voltdb.types.QueryType;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.Encoder;

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
        // TODO Auto-generated method stub
        return null;
    }

    private String genSelectSql(Table table, Column column,
            ComparisonConstant comparison) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT COUNT(*) FROM " + table.getTypeName());
        sb.append(" WHERE " + column.getName() + " " + comparison.toString() + " ?;");
        return sb.toString();
    }

    private String genDeleteSql(Table table, Column column,
            ComparisonConstant comparison) {
        StringBuilder sb = new StringBuilder();
        sb.append("DELETE FROM " + table.getTypeName());
        sb.append(" WHERE " + column.getName() + " " + comparison.toString() + " ?;");
        return sb.toString();
    }

    private String genValueAtOffsetSql(Table table, Column column) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT " + column.getName() + " FROM " + table.getTypeName());
        sb.append(" ORDER BY " + column.getName());
        sb.append(" ASC OFFSET ? LIMIT 1;");
        return sb.toString();
    }

    private Procedure addProcedure(Table catTable, String tableName) {
        // fake db makes it easy to create procedures that aren't part of the main catalog
        Database fakeDb = new Catalog().getClusters().add("cluster").getDatabases().add("database");
        Procedure newCatProc = fakeDb.getProcedures().add(this.getClass().getName() + "-" + tableName);
        newCatProc.setClassname(this.getClass().getName() + "-" + tableName);
        newCatProc.setDefaultproc(false);
        newCatProc.setEverysite(false);
        newCatProc.setHasjava(false);
        newCatProc.setPartitioncolumn(null);
        newCatProc.setPartitionparameter(-1);
        newCatProc.setPartitiontable(catTable);
        newCatProc.setReadonly(false);
        newCatProc.setSinglepartition(false);
        newCatProc.setSystemproc(false);
        return newCatProc;
    }

    private void addStatement(Table catTable, Procedure newCatProc, String sqlText, String index) {
        CatalogMap<Statement> statements = newCatProc.getStatements();
        assert(statements != null);

        // determine the type of the query
        QueryType qtype = QueryType.getFromSQL(sqlText);

        CatalogContext context = VoltDB.instance().getCatalogContext();
        PlannerTool plannerTool = context.m_ptool;
        CompiledPlan plan = plannerTool.planSqlCore(sqlText, StatementPartitioning.forceMP());
        /* since there can be multiple statements in a procedure,
         * we name the statements starting from 'sql0' even for single statement procedures
         * since we reuse the same code for single and multi-statement procedures
         *     statements of all single statement procedures are named 'sql0'
        */

        Statement stmt = statements.add(VoltDB.ANON_STMT_NAME + index);
        stmt.setSqltext(sqlText);
        stmt.setReadonly(newCatProc.getReadonly());
        stmt.setQuerytype(qtype.getValue());
        stmt.setSinglepartition(newCatProc.getSinglepartition());
        stmt.setIscontentdeterministic(true);
        stmt.setIsorderdeterministic(true);
        stmt.setNondeterminismdetail("NO CONTENT FOR DEFAULT PROCS");
        stmt.setSeqscancount(plan.countSeqScans());
        stmt.setReplicatedtabledml(!newCatProc.getReadonly() && catTable.getIsreplicated());

        // Input Parameters
        // We will need to update the system catalogs with this new information
        for (int i = 0; i < plan.getParameters().length; ++i) {
            StmtParameter catalogParam = stmt.getParameters().add(String.valueOf(i));
            catalogParam.setIndex(i);
            ParameterValueExpression pve = plan.getParameters()[i];
            catalogParam.setJavatype(pve.getValueType().getValue());
            catalogParam.setIsarray(pve.getParamIsVector());
        }

        PlanFragment frag = stmt.getFragments().add("0");

        // compute a hash of the plan
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            assert(false);
            System.exit(-1); // should never happen with healthy jvm
        }

        byte[] planBytes = StatementCompiler.writePlanBytes(frag, plan.rootPlanGraph);
        md.update(planBytes, 0, planBytes.length);
        // compute the 40 bytes of hex from the 20 byte sha1 hash of the plans
        md.reset();
        md.update(planBytes);
        frag.setPlanhash(Encoder.hexEncode(md.digest()));

        if (plan.subPlanGraph != null) {
            frag.setHasdependencies(true);
            frag.setNontransactional(true);
            frag.setMultipartition(true);

            frag = stmt.getFragments().add("1");
            frag.setHasdependencies(false);
            frag.setNontransactional(false);
            frag.setMultipartition(true);
            byte[] subBytes = StatementCompiler.writePlanBytes(frag, plan.subPlanGraph);
            // compute the 40 bytes of hex from the 20 byte sha1 hash of the plans
            md.reset();
            md.update(subBytes);
            frag.setPlanhash(Encoder.hexEncode(md.digest()));
        }
        else {
            frag.setHasdependencies(false);
            frag.setNontransactional(false);
            frag.setMultipartition(false);
        }
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
        assert(columnCount == 1);
        table.resetRowPosition();
        Object[] params = new Object[columnCount];
        for (int i = 0; table.advanceRow(); i++) {
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

        Procedure newCatProc = addProcedure(catTable, tableName);
        String countingQuery = genSelectSql(catTable, column,comparisonConstant);
        addStatement(catTable, newCatProc, countingQuery, "0");
        String deleteQuery = genDeleteSql(catTable, column, comparisonConstant);
        addStatement(catTable, newCatProc, deleteQuery, "1");
        String valueAtQuery = genValueAtOffsetSql(catTable, column);
        addStatement(catTable, newCatProc, valueAtQuery, "2");

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
