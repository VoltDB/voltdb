/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.compiler;

import org.hsqldb_voltpatches.HSQLInterface;
import org.voltdb.VoltType;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.PlanFragment;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;
import org.voltdb.compiler.VoltCompiler.VoltCompilerException;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.planner.CompiledPlan;
import org.voltdb.planner.PartitioningForStatement;
import org.voltdb.planner.PlanningErrorException;
import org.voltdb.planner.QueryPlanner;
import org.voltdb.planner.TrivialCostModel;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.DeletePlanNode;
import org.voltdb.plannodes.InsertPlanNode;
import org.voltdb.plannodes.PlanNodeList;
import org.voltdb.plannodes.SchemaColumn;
import org.voltdb.plannodes.UpdatePlanNode;
import org.voltdb.types.QueryType;
import org.voltdb.utils.BuildDirectoryUtils;
import org.voltdb.utils.Encoder;

/**
 * Compiles individual SQL statements and updates the given catalog.
 * <br/>Invokes the Optimizer to generate plans.
 *
 */
public abstract class StatementCompiler {

    public static final int DEFAULT_MAX_JOIN_TABLES = 5;

    static void compile(VoltCompiler compiler, HSQLInterface hsql,
            Catalog catalog, Database db, DatabaseEstimates estimates,
            Statement catalogStmt, String stmt, String joinOrder, PartitioningForStatement partitioning)
    throws VoltCompiler.VoltCompilerException {

        // Cleanup whitespace newlines for catalog compatibility
        // and to make statement parsing easier.
        stmt = stmt.replaceAll("\n", " ");
        stmt = stmt.trim();
        compiler.addInfo("Compiling Statement: " + stmt);

        // determine the type of the query
        QueryType qtype = QueryType.INVALID;
        boolean statementRO = true;
        if (stmt.toLowerCase().startsWith("insert")) {
            qtype = QueryType.INSERT;
            statementRO = false;
        }
        else if (stmt.toLowerCase().startsWith("update")) {
            qtype = QueryType.UPDATE;
            statementRO = false;
        }
        else if (stmt.toLowerCase().startsWith("delete")) {
            qtype = QueryType.DELETE;
            statementRO = false;
        }
        else if (stmt.toLowerCase().startsWith("select")) {
            // This covers simple select statements as well as UNIONs and other set operations that are being used with default precedence
            // as in "select ... from ... UNION select ... from ...;"
            // Even if set operations are not currently supported, let them pass as "select" statements to let the parser sort them out.
            qtype = QueryType.SELECT;
        }
        else if (stmt.toLowerCase().startsWith("(")) {
            // There does not seem to be a need to support parenthesized DML statements, so assume a read-only statement.
            // If that assumption is wrong, then it has probably gotten to the point that we want to drop this up-front
            // logic in favor of relying on the full parser/planner to determine the cataloged query type and read-only-ness.
            // Parenthesized query statements are typically complex set operations (UNIONS, etc.)
            // requiring parenthesis to explicitly determine precedence,
            // but they MAY be as simple as a needlessly parenthesized single select statement:
            // "( select * from table );" is valid SQL.
            // So, assume QueryType.SELECT.
            // If set operations require their own QueryType in the future, that's probably another case
            // motivating diving right in to the full parser/planner without this pre-check.
            // We don't want to be re-implementing the parser here -- this has already gone far enough.
            qtype = QueryType.SELECT;
        }
        // else:
        // All the known statements are handled above, so default to cataloging an invalid read-only statement
        // and leave it to the parser/planner to more intelligently reject the statement as unsupported.

        catalogStmt.setReadonly(statementRO);
        catalogStmt.setQuerytype(qtype.getValue());

        // put the data in the catalog that we have
        catalogStmt.setSqltext(stmt);
        catalogStmt.setSinglepartition(partitioning.wasSpecifiedAsSingle());
        catalogStmt.setBatched(false);
        catalogStmt.setParamnum(0);


        String name = catalogStmt.getParent().getTypeName() + "-" + catalogStmt.getTypeName();
        String sql = catalogStmt.getSqltext();
        String stmtName = catalogStmt.getTypeName();
        String procName = catalogStmt.getParent().getTypeName();
        TrivialCostModel costModel = new TrivialCostModel();
        QueryPlanner planner = new QueryPlanner(
                sql, stmtName, procName,  catalog.getClusters().get("cluster"), db,
                partitioning, hsql, estimates, false, DEFAULT_MAX_JOIN_TABLES,
                costModel, null, joinOrder);

        CompiledPlan plan = null;
        try {
            planner.parse();
            plan = planner.plan();
            assert(plan != null);
        } catch (PlanningErrorException e) {
            // These are normal expectable errors -- don't normally need a stack-trace.
            String msg = "Failed to plan for statement (" + catalogStmt.getTypeName() + ") " + catalogStmt.getSqltext();
            if (e.getMessage() != null) {
                msg += " Error: \"" + e.getMessage() + "\"";
            }
            throw compiler.new VoltCompilerException(msg);
        }
        catch (Exception e) {
            e.printStackTrace();
            throw compiler.new VoltCompilerException("Failed to plan for stmt: " + catalogStmt.getTypeName());
        }

        // Check order determinism before accessing the detail which it caches.
        boolean orderDeterministic = plan.isOrderDeterministic();
        catalogStmt.setIsorderdeterministic(orderDeterministic);
        boolean contentDeterministic = plan.isContentDeterministic();
        catalogStmt.setIscontentdeterministic(contentDeterministic);
        String nondeterminismDetail = plan.nondeterminismDetail();
        catalogStmt.setNondeterminismdetail(nondeterminismDetail);

        catalogStmt.setSeqscancount(plan.countSeqScans());

        // Input Parameters
        // We will need to update the system catalogs with this new information
        for (int i = 0; i < plan.parameters.length; ++i) {
            VoltType type = plan.parameters[i];
            StmtParameter catalogParam = catalogStmt.getParameters().add(String.valueOf(i));
            catalogParam.setJavatype(type.getValue());
            catalogParam.setIndex(i);
        }

        // Output Columns
        int index = 0;
        for (SchemaColumn col : plan.columns.getColumns())
        {
            Column catColumn = catalogStmt.getOutput_columns().add(String.valueOf(index));
            catColumn.setNullable(false);
            catColumn.setIndex(index);
            if (col.getColumnAlias() != null && !col.getColumnAlias().equals(""))
            {
                catColumn.setName(col.getColumnAlias());
            }
            else
            {
                catColumn.setName(col.getColumnName());
            }
            catColumn.setType(col.getType().getValue());
            catColumn.setSize(col.getSize());
            index++;
        }
        catalogStmt.setReplicatedtabledml(plan.replicatedTableDML);
        partitioning.setIsReplicatedTableDML(plan.replicatedTableDML);

        // output the explained plan to disk (or caller) for debugging
        StringBuilder planDescription = new StringBuilder(1000); // Initial capacity estimate.
        planDescription.append("SQL: ").append(plan.sql);
        planDescription.append("\nCOST: ").append(plan.cost);
        planDescription.append("\nPLAN:\n");
        planDescription.append(plan.explainedPlan);
        String planString = planDescription.toString();
        BuildDirectoryUtils.writeFile("statement-winner-plans", name + ".txt", planString);
        compiler.captureDiagnosticContext(planString);

        // set the explain plan output into the catalog (in hex)
        catalogStmt.setExplainplan(Encoder.hexEncode(plan.explainedPlan));

        // Now update our catalog information
        PlanFragment planFragment = catalogStmt.getFragments().add("0");
        planFragment.setHasdependencies(plan.subPlanGraph != null);
        // mark a fragment as non-transactional if it never touches a persistent table
        planFragment.setNontransactional(!fragmentReferencesPersistentTable(plan.rootPlanGraph));
        planFragment.setMultipartition(plan.subPlanGraph != null);
        writePlanBytes(compiler, planFragment, plan.rootPlanGraph);

        if (plan.subPlanGraph != null) {
            planFragment = catalogStmt.getFragments().add("1");
            planFragment.setHasdependencies(false);
            planFragment.setNontransactional(false);
            planFragment.setMultipartition(true);
            writePlanBytes(compiler, planFragment, plan.subPlanGraph);
        }

        // Planner should have rejected with an exception any statement with an unrecognized type.
        int validType = catalogStmt.getQuerytype();
        assert(validType != QueryType.INVALID.getValue());
    }

    static void writePlanBytes(VoltCompiler compiler, PlanFragment fragment, AbstractPlanNode planGraph)
    throws VoltCompilerException {
        // get the plan bytes
        PlanNodeList node_list = new PlanNodeList(planGraph);
        String json = node_list.toJSONString();
        compiler.captureDiagnosticJsonFragment(json);
        // Place serialized version of PlanNodeTree into a PlanFragment
        try {
            FastSerializer fs = new FastSerializer(true, false);
            fs.write(json.getBytes());
            String hexString = fs.getHexEncodedBytes();
            fragment.setPlannodetree(hexString);
        } catch (Exception e) {
            e.printStackTrace();
            throw compiler.new VoltCompilerException(e.getMessage());
        }
    }

    /**
     * Check through a plan graph and return true if it ever touches a persistent table.
     */
    static boolean fragmentReferencesPersistentTable(AbstractPlanNode node) {
        if (node == null)
            return false;

        // these nodes can read/modify persistent tables
        if (node instanceof AbstractScanPlanNode)
            return true;
        if (node instanceof InsertPlanNode)
            return true;
        if (node instanceof DeletePlanNode)
            return true;
        if (node instanceof UpdatePlanNode)
            return true;

        // recursively check out children
        for (int i = 0; i < node.getChildCount(); i++) {
            AbstractPlanNode child = node.getChild(i);
            if (fragmentReferencesPersistentTable(child))
                return true;
        }

        // if nothing found, return false
        return false;
    }
}
