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

import java.util.Collections;

import org.hsqldb_voltpatches.HSQLInterface;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.PlanFragment;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.planner.CompiledPlan;
import org.voltdb.planner.ParameterInfo;
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

        boolean compilerDebug = System.getProperties().contains("compilerdebug");

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
        PlanNodeList node_list = null;
        TrivialCostModel costModel = new TrivialCostModel();
        QueryPlanner planner = new QueryPlanner(
                catalog.getClusters().get("cluster"), db, partitioning, hsql, estimates, false);

        CompiledPlan plan = null;
        try {
            plan = planner.compilePlan(costModel, catalogStmt.getSqltext(), joinOrder,
                    catalogStmt.getTypeName(), catalogStmt.getParent().getTypeName(), DEFAULT_MAX_JOIN_TABLES, null);
        } catch (PlanningErrorException e) {
            // These are normal expectable errors -- don't normally need a stack-trace.
            throw compiler.new VoltCompilerException("Failed to plan for stmt: " + catalogStmt.getTypeName());
        } catch (Exception e) {
            e.printStackTrace();
            throw compiler.new VoltCompilerException("Failed to plan for stmt: " + catalogStmt.getTypeName());
        }
        if (plan == null) {
            String msg = "Failed to plan for statement type("
                + catalogStmt.getTypeName() + ") "
                + catalogStmt.getSqltext();
            String plannerMsg = planner.getErrorMessage();
            if (plannerMsg != null) {
                msg += " Error: \"" + plannerMsg + "\"";
            }
            throw compiler.new VoltCompilerException(msg);
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
        for (ParameterInfo param : plan.parameters) {
            StmtParameter catalogParam = catalogStmt.getParameters().add(String.valueOf(param.index));
            catalogParam.setJavatype(param.type.getValue());
            catalogParam.setIndex(param.index);
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

        int i = 0;
        Collections.sort(plan.fragments);
        for (CompiledPlan.Fragment fragment : plan.fragments) {
            node_list = new PlanNodeList(fragment.planGraph);

            // Now update our catalog information
            String planFragmentName = Integer.toString(i);
            PlanFragment planFragment = catalogStmt.getFragments().add(planFragmentName);

            // mark a fragment as non-transactional if it never touches a persistent table
            planFragment.setNontransactional(!fragmentReferencesPersistentTable(fragment.planGraph));

            planFragment.setHasdependencies(fragment.hasDependencies);
            planFragment.setMultipartition(fragment.multiPartition);

            String json = node_list.toJSONString();
            compiler.captureDiagnosticJsonFragment(json);

            // if we're generating more than just explain plans
            if (compilerDebug) {
                String prettyJson = null;

                try {
                    JSONObject jobj = new JSONObject(json);
                    prettyJson = jobj.toString(4);
                } catch (JSONException e2) {
                    e2.printStackTrace();
                    throw compiler.new VoltCompilerException(e2.getMessage());
                }

                // output the plan to disk as pretty json for debugging
                BuildDirectoryUtils.writeFile("statement-winner-plan-fragments", name + "-" + String.valueOf(i) + ".txt",
                                              prettyJson);

                // output the plan to disk for debugging
                BuildDirectoryUtils.writeFile("statement-winner-plan-fragments", name + String.valueOf(i) + ".dot",
                                              node_list.toDOTString(name + "-" + String.valueOf(i)));
            }

            // Place serialized version of PlanNodeTree into a PlanFragment
            try {
                FastSerializer fs = new FastSerializer(true, false);
                fs.write(json.getBytes());
                String hexString = fs.getHexEncodedBytes();
                planFragment.setPlannodetree(hexString);
            } catch (Exception e) {
                e.printStackTrace();
                throw compiler.new VoltCompilerException(e.getMessage());
            }

            // increment the counter for fragment id
            i++;
        }
        // Planner should have rejected with an exception any statement with an unrecognized type.
        int validType = catalogStmt.getQuerytype();
        assert(validType != QueryType.INVALID.getValue());
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
