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

package org.voltdb.planner;

import java.io.File;

import org.hsqldb_voltpatches.VoltXMLElement;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltdb.compiler.DatabaseEstimates;
import org.voltdb.compiler.DeterminismMode;
import org.voltdb.compiler.ScalarValueHints;
import org.voltdb.compiler.VoltCompiler;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.PlanNodeList;
import org.voltdb.utils.BuildDirectoryUtils;

/**
 * The helper class to pick the best cost plan given raw plan, and to
 * output a set of complete and correct query plans. The best plan will be selected
 * by computing resource usage statistics for the plans, then using those statistics to
 * compute the cost of a specific plan. The plan with the lowest cost wins.
 *
 */
public class PlanSelector implements Cloneable {
    /** pointer to the database estimates */
    DatabaseEstimates m_estimates;
    /** The name of the sql statement to be planned */
    String m_stmtName;
    /** The name of the procedure containing the sql statement to be planned */
    String m_procName;
    /** SQL stmt text to be planned */
    String m_sql;
    /** The current cost model to evaluate plans with */
    AbstractCostModel m_costModel;
    /** Hints to use for planning */
    ScalarValueHints[] m_paramHints;
    /** The best cost plan after the evaluation */
    CompiledPlan m_bestPlan = null;
    /** The filename for the best plan */
    String m_bestFilename = null;
    /** Plan statistics */
    PlanStatistics m_stats = null;
    /** The id of the plan under the evaluation */
    int m_planId = 0;
    /** Determinism mode (for micro optimizations) */
    final DeterminismMode m_detMode;
    /** Parameters to drive the output */
    boolean m_quietPlanner;
    boolean m_fullDebug = VoltCompiler.DEBUG_MODE;

    /**
     * Initialize plan processor.
     *
     * @param db Catalog info about schema, metadata and procedures.
     * @param estimates database estimates
     * @param stmtName The name of the sql statement to be planned.
     * @param procName The name of the procedure containing the sql statement to be planned.
     * @param sql SQL stmt text to be planned.
     * @param costModel The current cost model to evaluate plans with
     * @param paramHints Hints.
     * @param detMode Determinism mode (for micro optimizations)
     * @param quietPlanner Controls the output.
     * @param fullDebug Controls the debug output.
     */
    public PlanSelector(DatabaseEstimates estimates,
            String stmtName, String procName, String sql,
            AbstractCostModel costModel, ScalarValueHints[] paramHints,
            DeterminismMode detMode, boolean quietPlanner)
    {
        m_estimates = estimates;
        m_stmtName = stmtName;
        m_procName = procName;
        m_sql = sql;
        m_costModel = costModel;
        m_paramHints = paramHints;
        m_detMode = detMode;
        m_quietPlanner = quietPlanner;
    }

    /**
     * Clone itself.
     * @return deep copy of self
     */
    @Override
    public Object clone() {
        return new PlanSelector(m_estimates, m_stmtName, m_procName, m_sql,
                m_costModel, m_paramHints, m_detMode, m_quietPlanner);
    }

    /**
     * @param parsedStmt
     */
    public void outputParsedStatement(AbstractParsedStmt parsedStmt) {
        // output a description of the parsed stmt
        if (!m_quietPlanner) {
            BuildDirectoryUtils.writeFile("statement-parsed", m_procName + "_" + m_stmtName + ".txt",
                    parsedStmt.toString(), true);
        }
    }

    /**
     * @param xmlSQL
     */
    public void outputCompiledStatement(VoltXMLElement xmlSQL) {
        if (!m_quietPlanner) {
            // output the xml from hsql to disk for debugging
            BuildDirectoryUtils.writeFile("statement-hsql-xml", m_procName + "_" + m_stmtName + ".xml",
                    xmlSQL.toString(), true);
        }
    }


    public void outputParameterizedCompiledStatement(VoltXMLElement parameterizedXmlSQL) {
        if (!m_quietPlanner && m_fullDebug) {
            // output the xml from hsql to disk for debugging
            BuildDirectoryUtils.writeFile("statement-hsql-xml", m_procName + "_" + m_stmtName + "-parameterized.xml",
                    parameterizedXmlSQL.toString(), true);
        }
    }

    /** Picks the best cost plan for a given raw plan
     * @param rawplan
     */
    public void considerCandidatePlan(CompiledPlan plan, AbstractParsedStmt parsedStmt) {
        //System.out.println(String.format("[Raw plan]:%n%s", rawplan.rootPlanGraph.toExplainPlanString()));

        // run the set of microptimizations, which may return many plans (or not)
        ScanDeterminizer.apply(plan, m_detMode);

        // add in the sql to the plan
        plan.sql = m_sql;

        // compute resource usage using the single stats collector
        m_stats = new PlanStatistics();
        AbstractPlanNode planGraph = plan.rootPlanGraph;

        // compute statistics about a plan
        planGraph.computeEstimatesRecursively(m_stats, m_estimates, m_paramHints);

        // compute the cost based on the resources using the current cost model
        plan.cost = m_costModel.getPlanCost(m_stats);

        // filename for debug output
        String filename = String.valueOf(m_planId++);

        //* enable for debug */ System.out.println("DEBUG [new plan]: Cost:" + plan.cost + plan.rootPlanGraph.toExplainPlanString());

        // find the minimum cost plan
        if (m_bestPlan == null || plan.cost < m_bestPlan.cost) {
            // free the PlanColumns held by the previous best plan
            m_bestPlan = plan;
            m_bestFilename = filename;
            //* enable for debug */ System.out.println("DEBUG [Best plan] updated ***\n");
        }

        outputPlan(plan, planGraph, filename);
    }

    public void finalizeOutput() {
        if (m_quietPlanner) {
            return;
        }
        outputPlan(m_bestPlan, m_bestPlan.rootPlanGraph, m_bestFilename);

        // find out where debugging is going
        String prefix = BuildDirectoryUtils.getBuildDirectoryPath() +
                "/" + BuildDirectoryUtils.debugRootPrefix + "statement-all-plans/" +
                m_procName + "_" + m_stmtName + "/";
        String winnerFilename, winnerFilenameRenamed;

        // if outputting full stuff
        if (m_fullDebug) {
            // rename the winner json plan
            winnerFilename = prefix + m_bestFilename + "-json.txt";
            winnerFilenameRenamed = prefix + "WINNER-" + m_bestFilename + "-json.txt";
            renameFile(winnerFilename, winnerFilenameRenamed);

            // rename the winner dot plan
            winnerFilename = prefix + m_bestFilename + ".dot";
            winnerFilenameRenamed = prefix + "WINNER-" + m_bestFilename + ".dot";
            renameFile(winnerFilename, winnerFilenameRenamed);
        }

        // rename the winner explain plan
        winnerFilename = prefix + m_bestFilename + ".txt";
        winnerFilenameRenamed = prefix + "WINNER-" + m_bestFilename + ".txt";
        renameFile(winnerFilename, winnerFilenameRenamed);

        if (m_fullDebug) {
            // output the plan statistics to disk for debugging
            BuildDirectoryUtils.writeFile("statement-stats", m_procName + "_" + m_stmtName + ".txt",
                    m_stats.toString(), true);
        }
    }

    /**
     * @param plan
     * @param plan graph
     * @param filename
     */
    private void outputPlan(CompiledPlan plan, AbstractPlanNode planGraph, String filename) {
        if (!m_quietPlanner) {
            if (m_fullDebug) {
                outputPlanFullDebug(plan, planGraph, filename);
            }

            // get the explained plan for the node
            plan.explainedPlan = planGraph.toExplainPlanString();
            outputExplainedPlan(plan, filename);
        }
    }

    /**
     * @param plan
     * @param filename
     */
    private void outputExplainedPlan(CompiledPlan plan, String filename) {
        BuildDirectoryUtils.writeFile("statement-all-plans/" + m_procName + "_" + m_stmtName,
                                      filename + ".txt",
                                      plan.explainedPlan,
                                      true);
    }

    public static String outputPlanDebugString(AbstractPlanNode planGraph) throws JSONException {
        PlanNodeList nodeList = new PlanNodeList(planGraph, false);

        // get the json serialized version of the plan
        String json = null;

        String crunchJson = nodeList.toJSONString();
        //System.out.println(crunchJson);
        //System.out.flush();
        JSONObject jobj = new JSONObject(crunchJson);
        json = jobj.toString(4);
        return json;
    }

    /**
     * @param plan
     * @param planGraph
     * @param filename
     * @return error message if any
     */
    private String outputPlanFullDebug(CompiledPlan plan, AbstractPlanNode planGraph, String filename) {
        // GENERATE JSON DEBUGGING OUTPUT BEFORE WE CLEAN UP THE
        // PlanColumns
        // convert a tree into an execution list
        PlanNodeList nodeList = new PlanNodeList(planGraph, plan.getIsLargeQuery());

        String json;
        try {
            json = outputPlanDebugString(planGraph);
        } catch (JSONException e2) {
            // Any plan that can't be serialized to JSON to
            // write to debugging output is also going to fail
            // to get written to the catalog, to sysprocs, etc.
            // Just bail.
            String errorMsg = "Plan for sql: '" + plan.sql +
                               "' can't be serialized to JSON";
            // This case used to exit the planner
            // -- a strange behavior for something that only gets called when full debug output is enabled.
            // For now, just skip the output and go on to the next plan.
            return errorMsg;
        }
        // output a description of the parsed stmt
        json = "PLAN:\n" + json;
        json = "COST: " + String.valueOf(plan.cost) + "\n" + json;
        assert (plan.sql != null);
        json = "SQL: " + plan.sql + "\n" + json;

        // write json to disk
        BuildDirectoryUtils.writeFile("statement-all-plans/" + m_procName + "_" + m_stmtName,
                                      filename + "-json.txt",
                                      json,
                                      true);

        // create a graph friendly version
        BuildDirectoryUtils.writeFile("statement-all-plans/" + m_procName + "_" + m_stmtName,
                                      filename + ".dot",
                                      nodeList.toDOTString("name"),
                                      true);
        return null;
    }

    /**
     * @param filename
     * @param filenameRenamed
     */
    private static void renameFile(String filename, String filenameRenamed) {
        File file;
        File fileRenamed;
        file = new File(filename);
        fileRenamed = new File(filenameRenamed);
        file.renameTo(fileRenamed);
    }
}
