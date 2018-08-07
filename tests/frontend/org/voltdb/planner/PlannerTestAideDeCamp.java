/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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
import org.hsqldb_voltpatches.HSQLInterface.HSQLParseException;
import org.hsqldb_voltpatches.VoltXMLElement;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;
import org.voltdb.compiler.DatabaseEstimates;
import org.voltdb.compiler.DeterminismMode;
import org.voltdb.compiler.VoltCompiler;
import org.voltdb.compiler.VoltCompiler.DdlProceduresToLoad;
import org.voltdb.expressions.ParameterValueExpression;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.PlanNodeList;
import org.voltdb.types.QueryType;
import org.voltdb.utils.BuildDirectoryUtils;

/**
 * Some utility functions to compile SQL statements for plan generation tests.
 */
public class PlannerTestAideDeCamp {

    private final Procedure proc;
    private final HSQLInterface hsql;
    private final Database db;
    int compileCounter = 0;
    boolean m_planForLargeQueries = false;

    private CompiledPlan m_currentPlan = null;

    /**
     * Loads the schema at ddlurl and setups a voltcompiler / hsql instance.
     * @param ddlurl URL to the schema/ddl file.
     * @param basename Unique string, JSON plans [basename]-stmt-#_json.txt on disk
     * @throws Exception
     */
    public PlannerTestAideDeCamp(URL ddlurl, String basename) throws Exception {
        assert(ddlurl != null);
        String schemaPath = URLDecoder.decode(ddlurl.getPath(), "UTF-8");
        VoltCompiler compiler = new VoltCompiler(false);
        hsql = HSQLInterface.loadHsqldb(ParameterizationInfo.getParamStateManager());
        VoltCompiler.DdlProceduresToLoad no_procs = DdlProceduresToLoad.NO_DDL_PROCEDURES;
        compiler.loadSchema(hsql, no_procs, schemaPath);
        db = compiler.getCatalogDatabase();
        proc = db.getProcedures().add(basename);
    }

    public void tearDown() {
    }

    public Database getDatabase() {
        return db;
    }

    public VoltXMLElement compileToXML(String sql) throws HSQLParseException {
        return hsql.getXMLCompiledStatement(sql);
    }
    /**
     * Compile a statement and return the head of the plan.
     * @param sql
     * @param detMode
     */
    CompiledPlan compileAdHocPlan(String sql, DeterminismMode detMode) {
        compile(sql, 0, null, true, false, detMode);
        return m_currentPlan;
    }

    CompiledPlan compileAdHocPlan(String sql, boolean inferPartitioning, boolean singlePartition, DeterminismMode detMode) {
        compile(sql, 0, null, inferPartitioning, singlePartition, detMode);
        return m_currentPlan;
    }

    List<AbstractPlanNode> compile(String sql, int paramCount, boolean inferPartitioning, boolean singlePartition, String joinOrder) {
        return compile(sql, paramCount, joinOrder, inferPartitioning, singlePartition, DeterminismMode.SAFER);
    }

    /**
     * Compile and cache the statement and plan and return the final plan graph.
     */
    private List<AbstractPlanNode> compile(String sql, int paramCount, String joinOrder, boolean inferPartitioning, boolean forceSingle, DeterminismMode detMode)
    {
        String stmtLabel = "stmt-" + String.valueOf(compileCounter++);

        Statement catalogStmt = proc.getStatements().add(stmtLabel);
        catalogStmt.setSqltext(sql);
        catalogStmt.setSinglepartition(forceSingle);

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
        StatementPartitioning partitioning;
        if (inferPartitioning) {
            partitioning = StatementPartitioning.inferPartitioning();
        } else if (forceSingle) {
            partitioning = StatementPartitioning.forceSP();
        } else {
            partitioning = StatementPartitioning.forceMP();
        }
        String procName = catalogStmt.getParent().getTypeName();

        CompiledPlan plan = null;
        // This try-with-resources block acquires a global lock on all planning
        // This is required until we figure out how to do parallel planning.
        try (QueryPlanner planner = new QueryPlanner(sql, stmtLabel, procName, db,
                                                     partitioning, hsql, estimates, false,
                                                     costModel, null, joinOrder, detMode, m_planForLargeQueries)) {

            planner.parse();
            plan = planner.plan();
            assert(plan != null);
        }

        // Partitioning optionally inferred from the planning process.
        if (partitioning.isInferred()) {
            catalogStmt.setSinglepartition(partitioning.isInferredSingle());
        }

        // Input Parameters
        // We will need to update the system catalogs with this new information
        for (int i = 0; i < plan.getParameters().length; ++i) {
            StmtParameter catalogParam = catalogStmt.getParameters().add(String.valueOf(i));
            ParameterValueExpression pve = plan.getParameters()[i];
            catalogParam.setJavatype(pve.getValueType().getValue());
            catalogParam.setIsarray(pve.getParamIsVector());
            catalogParam.setIndex(i);
        }

        List<PlanNodeList> nodeLists = new ArrayList<>();
        nodeLists.add(new PlanNodeList(plan.rootPlanGraph, false));
        if (plan.subPlanGraph != null) {
            nodeLists.add(new PlanNodeList(plan.subPlanGraph, false));
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
            return null;
        }

        //
        // We then stick a serialized version of PlanNodeTree into a PlanFragment
        //
        try {
            BuildDirectoryUtils.writeFile("statement-plans", name + "_json.txt", json, true);
            BuildDirectoryUtils.writeFile("statement-plans", name + ".dot", nodeLists.get(0).toDOTString("name"), true);
        } catch (Exception e) {
            e.printStackTrace();
        }

        List<AbstractPlanNode> plannodes = new ArrayList<>();
        for (PlanNodeList nodeList : nodeLists) {
            plannodes.add(nodeList.getRootPlanNode());
        }

        m_currentPlan = plan;
        return plannodes;
    }

    public Catalog getCatalog() {
        return db.getCatalog();
    }
    public String getCatalogString() {
        return db.getCatalog().serialize();
    }

    public void planForLargeQueries(boolean b) {
        m_planForLargeQueries = b;
    }

    public boolean isPlanningForLargeQueries() {
        return m_planForLargeQueries;
    }
}
