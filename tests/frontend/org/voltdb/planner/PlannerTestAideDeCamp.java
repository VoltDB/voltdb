/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;

import org.hsqldb_voltpatches.HSQLInterface;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltcore.utils.Pair;
import org.voltdb.VoltType;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Group;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;
import org.voltdb.compiler.DDLCompiler;
import org.voltdb.compiler.DatabaseEstimates;
import org.voltdb.compiler.PartitionMap;
import org.voltdb.compiler.StatementCompiler;
import org.voltdb.compiler.VoltCompiler;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.PlanNodeList;
import org.voltdb.plannodes.SchemaColumn;
import org.voltdb.types.QueryType;
import org.voltdb.utils.BuildDirectoryUtils;

/**
 * Some utility functions to compile SQL statements for plan generation tests.
 */
public class PlannerTestAideDeCamp {

    private final Catalog catalog;
    private final Procedure proc;
    private final HSQLInterface hsql;
    private final Database db;
    int compileCounter = 0;

    private CompiledPlan m_currentPlan = null;

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
        PartitionMap partitionMap = new PartitionMap(compiler);
        DDLCompiler ddl_compiler = new DDLCompiler(compiler, hsql, partitionMap, db.getGroups());
        ddl_compiler.loadSchema(schemaPath);
        ddl_compiler.compileToCatalog(catalog, db);
    }

    public void tearDown() {
    }

    public Catalog getCatalog() {
        return catalog;
    }

    public Database getDatabase() {
        return db;
    }

    /**
     * Compile a statement and return the head of the plan.
     * @param sql
     */
    public CompiledPlan compileAdHocPlan(String sql)
    {
        compile(sql, 0, null, null, true, false);
        return m_currentPlan;
    }

    public List<AbstractPlanNode> compile(String sql, int paramCount)
    {
        return compile(sql, paramCount, false, null);
    }

    public List<AbstractPlanNode> compile(String sql, int paramCount, boolean singlePartition) {
        return compile(sql, paramCount, singlePartition, null);
    }

    public List<AbstractPlanNode> compile(String sql, int paramCount, boolean singlePartition, String joinOrder) {
        Object partitionBy = null;
        if (singlePartition) {
            partitionBy = "Forced single partitioning";
        }
        return compile(sql, paramCount, joinOrder, partitionBy, true, false);
    }

    /**
     * Compile and cache the statement and plan and return the final plan graph.
     * @param sql
     * @param paramCount
     */
    public List<AbstractPlanNode> compile(String sql, int paramCount, String joinOrder, Object partitionParameter, boolean inferSP, boolean lockInSP)
    {
        Statement catalogStmt = proc.getStatements().add("stmt-" + String.valueOf(compileCounter++));
        catalogStmt.setSqltext(sql);
        catalogStmt.setSinglepartition(partitionParameter != null);
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
        PartitioningForStatement partitioning = new PartitioningForStatement(partitionParameter, inferSP, lockInSP);
        QueryPlanner planner =
            new QueryPlanner(catalogStmt.getSqltext(), catalogStmt.getTypeName(),
                    catalogStmt.getParent().getTypeName(), catalog.getClusters().get("cluster"),
                    db, partitioning, hsql, estimates, false, StatementCompiler.DEFAULT_MAX_JOIN_TABLES,
                    costModel, null, joinOrder);

        CompiledPlan plan = null;
        planner.parse();
        plan = planner.plan();
        assert(plan != null);

        // Input Parameters
        // We will need to update the system catalogs with this new information
        // If this is an adhoc query then there won't be any parameters
        for (int i = 0; i < plan.parameters.length; ++i) {
            StmtParameter catalogParam = catalogStmt.getParameters().add(String.valueOf(i));
            catalogParam.setJavatype(plan.parameters[i].getValue());
            catalogParam.setIndex(i);
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
        nodeLists.add(new PlanNodeList(plan.rootPlanGraph));
        if (plan.subPlanGraph != null) {
            nodeLists.add(new PlanNodeList(plan.subPlanGraph));
        }

        //Store the list of parameters types and indexes in the plan node list.
        List<Pair<Integer, VoltType>> parameters = nodeLists.get(0).getParameters();
        for (int i = 0; i < plan.parameters.length; ++i) {
            Pair<Integer, VoltType> parameter = new Pair<Integer, VoltType>(i, plan.parameters[i]);
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
            BuildDirectoryUtils.writeFile("statement-plans", name + "_json.txt", json);
            BuildDirectoryUtils.writeFile("statement-plans", name + ".dot", nodeLists.get(0).toDOTString("name"));
        } catch (Exception e) {
            e.printStackTrace();
        }

        List<AbstractPlanNode> plannodes = new ArrayList<AbstractPlanNode>();
        for (PlanNodeList nodeList : nodeLists) {
            plannodes.add(nodeList.getRootPlanNode());
        }

        m_currentPlan = plan;
        return plannodes;
    }

}
