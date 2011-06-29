/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

import java.io.PrintStream;
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

    static void compile(VoltCompiler compiler, HSQLInterface hsql,
            Catalog catalog, Database db, DatabaseEstimates estimates,
            Statement catalogStmt, String stmt, String joinOrder, boolean singlePartition)
    throws VoltCompiler.VoltCompilerException {

        // Cleanup whitespace newlines for catalog compatibility
        // and to make statement parsing easier.
        stmt = stmt.replaceAll("\n", " ");
        stmt = stmt.trim();
        compiler.addInfo("Compiling Statement: " + stmt);

        // determine the type of the query
        QueryType qtype;
        if (stmt.toLowerCase().startsWith("insert")) {
            qtype = QueryType.INSERT;
            catalogStmt.setReadonly(false);
        }
        else if (stmt.toLowerCase().startsWith("update")) {
            qtype = QueryType.UPDATE;
            catalogStmt.setReadonly(false);
        }
        else if (stmt.toLowerCase().startsWith("delete")) {
            qtype = QueryType.DELETE;
            catalogStmt.setReadonly(false);
        }
        else if (stmt.toLowerCase().startsWith("select")) {
            qtype = QueryType.SELECT;
            catalogStmt.setReadonly(true);
        }
        else {
            throw compiler.new VoltCompilerException("Unparsable SQL statement: " + stmt);
        }
        catalogStmt.setQuerytype(qtype.getValue());

        // put the data in the catalog that we have
        catalogStmt.setSqltext(stmt);
        catalogStmt.setSinglepartition(singlePartition);
        catalogStmt.setBatched(false);
        catalogStmt.setParamnum(0);

        String name = catalogStmt.getParent().getTypeName() + "-" + catalogStmt.getTypeName();
        PlanNodeList node_list = null;
        TrivialCostModel costModel = new TrivialCostModel();

        QueryPlanner planner = new QueryPlanner(
                catalog.getClusters().get("cluster"), db, hsql, estimates, true,
                false);

        CompiledPlan plan = null;
        try {
            plan = planner.compilePlan(costModel, catalogStmt.getSqltext(), joinOrder,
                    catalogStmt.getTypeName(), catalogStmt.getParent().getTypeName(),
                    catalogStmt.getSinglepartition(), null);
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

        // Input Parameters
        // We will need to update the system catalogs with this new information
        // If this is an adhoc query then there won't be any parameters
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

        // output the explained plan to disk for debugging
        PrintStream plansOut = BuildDirectoryUtils.getDebugOutputPrintStream(
                "statement-winner-plans", name + ".txt");
        plansOut.println("SQL: " + plan.sql);
        plansOut.println("COST: " + Double.toString(plan.cost));
        plansOut.println("PLAN:\n");
        plansOut.println(plan.explainedPlan);
        plansOut.close();

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

            String json = null;
            try {
                JSONObject jobj = new JSONObject(node_list.toJSONString());
                json = jobj.toString(4);
            } catch (JSONException e2) {
                e2.printStackTrace();
                throw compiler.new VoltCompilerException(e2.getMessage());
            }

            // output the plan to disk for debugging
            plansOut = BuildDirectoryUtils.getDebugOutputPrintStream(
                    "statement-winner-plan-fragments", name + "-" + String.valueOf(i++) + ".txt");
            plansOut.println(json);
            plansOut.close();

            // output the plan to disk for debugging
            plansOut = BuildDirectoryUtils.getDebugOutputPrintStream(
                    "statement-winner-plan-fragments", name + String.valueOf(i) + ".dot");
            plansOut.println(node_list.toDOTString(name + "-" + String.valueOf(i)));
            plansOut.close();

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
