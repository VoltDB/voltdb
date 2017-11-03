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

import org.voltdb.CatalogContext;
import org.voltdb.CatalogContext.ProcedurePartitionInfo;
import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltDB;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.PlanFragment;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;
import org.voltdb.catalog.Table;
import org.voltdb.compiler.PlannerTool;
import org.voltdb.compiler.StatementCompiler;
import org.voltdb.expressions.ParameterValueExpression;
import org.voltdb.planner.CompiledPlan;
import org.voltdb.planner.StatementPartitioning;
import org.voltdb.types.QueryType;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.Encoder;

public class NibbleDeleteSP extends VoltSystemProcedure {

    public static enum ComparisonConstant {
        GREATER_THAN (">"),
        LESS_THAN ("<"),
        GREATER_OR_EQUAL (">="),
        LESS_OR_EQUAL ("<="),
        EQUAL ("=");

        private final String m_symbol;

        private ComparisonConstant(String symbol) {
            m_symbol = symbol;
        }

        public String toString() { return m_symbol; }
    }

    public long[] getPlanFragmentIds() {
        return new long[]{};
    }

    public DependencyPair executePlanFragment(
            Map<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params, SystemProcedureExecutionContext context) {
        // Never called, we do all the work in run()
        return null;
    }

    private String genereateSelectSql(Table table, Column column,
            ComparisonConstant comparison) {
        // TODO: to be implemented.
        // Index scan first then delete? Performance impact?
        return null;
    }

    private String generateNibbleDeleteSql(Table table, Column column,
            ComparisonConstant comparison, long chunksize) {
        StringBuilder sb = new StringBuilder();

        sb.append("DELETE FROM " + table.getTypeName());
        sb.append(" WHERE " + column.getName() + " " + comparison.toString() + " ?");
        sb.append(" LIMIT " + chunksize);
        sb.append(" ORDER BY " + column.getName());
        sb.append(";");

        return sb.toString();
    }

    private Procedure compileProcedure(Table catTable, String tableName, String sqlText) {
        CatalogContext context = VoltDB.instance().getCatalogContext();
        PlannerTool plannerTool = context.m_ptool;
        CompiledPlan plan = plannerTool.planSqlCore(sqlText, StatementPartitioning.forceSP());
        // fake db makes it easy to create procedures that aren't part of the main catalog
        Database fakeDb = new Catalog().getClusters().add("cluster").getDatabases().add("database");
        Column partitionColumn = catTable.getPartitioncolumn();
        Procedure newCatProc = fakeDb.getProcedures().add(this.getClass().getName() + "-" + tableName);
        newCatProc.setClassname(this.getClass().getName() + "-" + tableName);
        newCatProc.setDefaultproc(false);
        newCatProc.setEverysite(false);
        newCatProc.setHasjava(false);
        newCatProc.setPartitioncolumn(partitionColumn);
        newCatProc.setPartitionparameter(partitionColumn.getIndex());
        newCatProc.setPartitiontable(catTable);
        newCatProc.setReadonly(false);
        newCatProc.setSinglepartition(true);
        newCatProc.setSystemproc(false);
        newCatProc.setAttachment(
                new ProcedurePartitionInfo(
                        VoltType.get((byte)partitionColumn.getType()),
                        partitionColumn.getIndex()));

        CatalogMap<Statement> statements = newCatProc.getStatements();
        assert(statements != null);
        /* since there can be multiple statements in a procedure,
         * we name the statements starting from 'sql0' even for single statement procedures
         * since we reuse the same code for single and multi-statement procedures
         *     statements of all single statement procedures are named 'sql0'
        */
        Statement stmt = statements.add(VoltDB.ANON_STMT_NAME + "0");
        stmt.setSqltext(sqlText);
        stmt.setReadonly(newCatProc.getReadonly());
        stmt.setQuerytype(QueryType.DELETE.getValue());
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

        // set the procedure parameter types from the statement parameter types
        int paramCount = 0;
        for (StmtParameter stmtParam : CatalogUtil.getSortedCatalogItems(stmt.getParameters(), "index")) {
            // name each parameter "param1", "param2", etc...
            ProcParameter procParam = newCatProc.getParameters().add("param" + String.valueOf(paramCount));
            procParam.setIndex(stmtParam.getIndex());
            procParam.setIsarray(stmtParam.getIsarray());
            procParam.setType(stmtParam.getJavatype());
            paramCount++;
        }

        return newCatProc;
    }

    /**
     * Nibble delete procedure for partitioned tables
     *
     * @param ctx         Internal API provided to all system procedures
     * @param partitionParam Partition parameter used to match invocation to partition
     * @param tableName   Name of persistent partitioned table
     * @param columnName  A column in the given table that its value can be used to provide
     *                    order for delete action. (Unique or non-unique) index is expected
     *                    on the column, if not a warning message will be printed.
     * @param comparison  "GREATER_THAN", "LESS_THAN"
     * @param value       value to compare
     * @param chunksize   maximum number of rows allow to be deleted
     * @return how many rows are deleted and how many rows left to be deleted (if any)
     */
    public VoltTable run(SystemProcedureExecutionContext ctx, String tableName, String columnName,
            ComparisonConstant comparison, Object value, long chunksize) {
        // Some basic checks
        Table catTable = ctx.getDatabase().getTables().getIgnoreCase(tableName);
        if (catTable == null) {
            throw new VoltAbortException("Table not present in catalog");
        }
        if (catTable.getIsreplicated()) {
            throw new VoltAbortException(
                    String.format("%s incompatible with replicated table %s.",
                            this.getClass().getName(), tableName));
        }
        Column column = catTable.getColumns().get(columnName);
        if (column == null) {
            throw new VoltAbortException(
                    String.format("Column %s does not exist in table %s", columnName, tableName));
        }
        if (VoltType.typeFromObject(value) != VoltType.valueOf(column.getTypeName())) {
            throw new VoltAbortException(
                    String.format("Parameter type %s doesn't match column type %s",
                            VoltType.typeFromObject(value).toString(),
                            column.getTypeName()));
        }

        // Generate sql text for given column name and comparison constant
        String sqlText = generateNibbleDeleteSql(catTable, column, comparison, chunksize);

        Procedure newCatProc = compileProcedure(catTable, tableName, )


        return null;
    }

}
