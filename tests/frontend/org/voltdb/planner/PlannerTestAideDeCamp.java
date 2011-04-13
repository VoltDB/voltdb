/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.planner;

import java.io.PrintStream;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;

import org.hsqldb_voltpatches.HSQLInterface;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltdb.VoltType;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;
import org.voltdb.compiler.DDLCompiler;
import org.voltdb.compiler.DatabaseEstimates;
import org.voltdb.compiler.VoltCompiler;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.PlanNodeList;
import org.voltdb.plannodes.SchemaColumn;
import org.voltdb.types.QueryType;
import org.voltdb.utils.BuildDirectoryUtils;
import org.voltdb.utils.Pair;

/**
 * Some utility functions to compile SQL statements for plan generation tests.
 */
public class PlannerTestAideDeCamp {

    private final Catalog catalog;
    private final Procedure proc;
    private final HSQLInterface hsql;
    private final Database db;
    int compileCounter = 0;

    /**
     * Loads the schema at ddlurl and setups a voltcompiler / hsql instance.
     * @param ddlurl URL to the schema/ddl file.
     * @param basename Unique string, JSON plans [basename]-stmt-#_json.txt on disk
     * @throws Exception
     */
    public PlannerTestAideDeCamp(URL ddlurl, String basename) throws Exception {
        catalog = new Catalog();
        catalog.execute("add / clusters cluster");
        catalog.execute("add /clusters[cluster] databases database");
        db = catalog.getClusters().get("cluster").getDatabases().get("database");
        proc = db.getProcedures().add(basename);

        String schemaPath = URLDecoder.decode(ddlurl.getPath(), "UTF-8");

        VoltCompiler compiler = new VoltCompiler();
        hsql = HSQLInterface.loadHsqldb();
        //hsql.runDDLFile(schemaPath);
        DDLCompiler ddl_compiler = new DDLCompiler(compiler, hsql);
        ddl_compiler.loadSchema(schemaPath);
        ddl_compiler.compileToCatalog(catalog, db);
    }

    /**
     * Cleans up HSQL. Mandatory - call this when done!
     */
    public void tearDown() {
        hsql.close();
    }

    public Catalog getCatalog() {
        return catalog;
    }

    public List<AbstractPlanNode> compile(String sql, int paramCount)
    {
        return compile(sql, paramCount, false);
    }

    public List<AbstractPlanNode> compile(String sql, int paramCount, boolean singlePartition) {
        return compile(sql, paramCount, singlePartition, null);
    }
    /**
     * Compile a statement and return the final plan graph.
     * @param sql
     * @param paramCount
     */
    public List<AbstractPlanNode> compile(String sql, int paramCount, boolean singlePartition, String joinOrder)
    {
        Statement catalogStmt = proc.getStatements().add("stmt-" + String.valueOf(compileCounter++));
        catalogStmt.setSqltext(sql);
        catalogStmt.setSinglepartition(singlePartition);
        catalogStmt.setBatched(false);
        catalogStmt.setParamnum(paramCount);

        // determine the type of the query
        QueryType qtype = QueryType.SELECT;
        catalogStmt.setReadonly(true);
        if (sql.toLowerCase().startsWith("insert")) {
            qtype = QueryType.INSERT;
            catalogStmt.setReadonly(false);
        }
        if (sql.toLowerCase().startsWith("update")) {
            qtype = QueryType.UPDATE;
            catalogStmt.setReadonly(false);
        }
        if (sql.toLowerCase().startsWith("delete")) {
            qtype = QueryType.DELETE;
            catalogStmt.setReadonly(false);
        }
        catalogStmt.setQuerytype(qtype.getValue());
        // name will look like "basename-stmt-#"
        String name = catalogStmt.getParent().getTypeName() + "-" + catalogStmt.getTypeName();

        DatabaseEstimates estimates = new DatabaseEstimates();
        TrivialCostModel costModel = new TrivialCostModel();
        QueryPlanner planner =
            new QueryPlanner(catalog.getClusters().get("cluster"), db, hsql,
                             estimates, true, false);

        CompiledPlan plan = null;
        plan = planner.compilePlan(costModel, catalogStmt.getSqltext(), joinOrder, catalogStmt.getTypeName(),
                                   catalogStmt.getParent().getTypeName(), catalogStmt.getSinglepartition(), null);

        if (plan == null)
        {
            String msg = "planner.compilePlan returned null plan";
            String plannerMsg = planner.getErrorMessage();
            if (plannerMsg != null)
            {
                msg += " with error: \"" + plannerMsg + "\"";
            }
            throw new NullPointerException(msg);
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
            catColumn.setName(col.getColumnName());
            catColumn.setType(col.getType().getValue());
            catColumn.setSize(col.getSize());
            index++;
        }

        List<PlanNodeList> nodeLists = new ArrayList<PlanNodeList>();
        for (CompiledPlan.Fragment fragment : plan.fragments) {
            PlanNodeList nodeList = new PlanNodeList(fragment.planGraph);
            nodeLists.add(nodeList);
        }

        //Store the list of parameters types and indexes in the plan node list.
        List<Pair<Integer, VoltType>> parameters = nodeLists.get(0).getParameters();
        for (ParameterInfo param : plan.parameters) {
            Pair<Integer, VoltType> parameter = new Pair<Integer, VoltType>(param.index, param.type);
            parameters.add(parameter);
        }

        // Now update our catalog information
        // HACK: We're using the node_tree's hashCode() as it's name. It would be really
        //     nice if the Catalog code give us an guid without needing a name first...

        String json = null;
        try {
            JSONObject jobj = new JSONObject(nodeLists.get(0).toJSONString());
            json = jobj.toString(4);
        } catch (JSONException e2) {
            // TODO Auto-generated catch block
            e2.printStackTrace();
            System.exit(-1);
        }

        //
        // We then stick a serialized version of PlanNodeTree into a PlanFragment
        //
        try {
            PrintStream plansJSONOut = BuildDirectoryUtils.getDebugOutputPrintStream(
                    "statement-plans", name + "_json.txt");
            plansJSONOut.print(json);
            plansJSONOut.close();

            PrintStream plansDOTOut = BuildDirectoryUtils.getDebugOutputPrintStream(
                     "statement-plans", name + ".dot");
            plansDOTOut.print(nodeLists.get(0).toDOTString("name"));
            plansDOTOut.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        List<AbstractPlanNode> plannodes = new ArrayList<AbstractPlanNode>();
        for (PlanNodeList nodeList : nodeLists) {
            plannodes.add(nodeList.getRootPlanNode());
        }

        return plannodes;
    }

}
