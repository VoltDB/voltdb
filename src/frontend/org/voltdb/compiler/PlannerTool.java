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
import org.hsqldb_voltpatches.HSQLInterface.HSQLParseException;
import org.voltdb.CatalogContext;
import org.voltdb.logging.VoltLogger;
import org.voltdb.planner.CompiledPlan;
import org.voltdb.planner.CompiledPlan.Fragment;
import org.voltdb.planner.QueryPlanner;
import org.voltdb.planner.TrivialCostModel;
import org.voltdb.plannodes.PlanNodeList;
import org.voltdb.utils.Encoder;

/**
 * Planner tool accepts an already compiled VoltDB catalog and then
 * interactively accept SQL and outputs plans on standard out.
 */
public class PlannerTool {

    private static final VoltLogger hostLog = new VoltLogger("HOST");

    final CatalogContext m_context;
    final HSQLInterface m_hsql;

    public static final int AD_HOC_JOINED_TABLE_LIMIT = 5;

    public static class Result {
        String onePlan = null;
        String allPlan = null;
        boolean replicatedDML = false;

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("RESULT {\n");
            sb.append("  ONE: ").append(onePlan == null ? "null" : onePlan).append("\n");
            sb.append("  ALL: ").append(allPlan == null ? "null" : allPlan).append("\n");
            sb.append("  RTD: ").append(replicatedDML ? "true" : "false").append("\n");
            sb.append("}");
            return sb.toString();
        }
    }

    public PlannerTool(final CatalogContext context) {
        assert(context != null);
        m_context = context;

        // LOAD HSQL
        m_hsql = HSQLInterface.loadHsqldb();
        String hexDDL = m_context.database.getSchema();
        String ddl = Encoder.hexDecodeToString(hexDDL);
        String[] commands = ddl.split("\n");
        for (String command : commands) {
            String decoded_cmd = Encoder.hexDecodeToString(command);
            decoded_cmd = decoded_cmd.trim();
            if (decoded_cmd.length() == 0)
                continue;
            try {
                m_hsql.runDDLCommand(decoded_cmd);
            }
            catch (HSQLParseException e) {
                // need a good error message here
                throw new RuntimeException("Error creating hsql: " + e.getMessage() + " in DDL statement: " + decoded_cmd);
            }
        }

        hostLog.info("hsql loaded");
    }

    // this is probably not super important, but is probably
    // a performance win for tests that fire these up all the time
    public void shutdown() {
        m_hsql.close();
    }

    public Result planSql(String sql, boolean singlePartition) {
        Result retval = new Result();

        if ((sql == null) || (sql.length() == 0)) {
            throw new RuntimeException("Can't plan empty or null SQL.");
        }
        // remove any spaces or newlines
        sql = sql.trim();

        hostLog.debug("received sql stmt: " + sql);

        //////////////////////
        // PLAN THE STMT
        //////////////////////

        TrivialCostModel costModel = new TrivialCostModel();
        QueryPlanner planner = new QueryPlanner(
                m_context.cluster, m_context.database, singlePartition, m_hsql, new DatabaseEstimates(), false, true);
        CompiledPlan plan = null;
        try {
            plan = planner.compilePlan(costModel, sql, null, "PlannerTool", "PlannerToolProc", AD_HOC_JOINED_TABLE_LIMIT, null);
        } catch (Exception e) {
            throw new RuntimeException("Error creating planner: " + e.getMessage(), e);
        }
        if (plan == null) {
            String plannerMsg = planner.getErrorMessage();
            if (plannerMsg != null) {
                throw new RuntimeException("ERROR: " + plannerMsg + "\n");
            }
            else {
                throw new RuntimeException("ERROR: UNKNOWN PLANNING ERROR\n");
            }
        }
        if (plan.parameters.size() > 0) {
            throw new RuntimeException("ERROR: PARAMETERIZATION IN AD HOC QUERY");
        }

        if (plan.isContentDeterministic() == false) {
            String potentialErrMsg =
                "Statement has a non-deterministic result - statement: \"" +
                sql + "\" , reason: " + plan.nondeterminismDetail();
            // throw new RuntimeException(potentialErrMsg);
            hostLog.warn(potentialErrMsg);
        }

        //log("finished planning stmt:");
        //log("SQL: " + plan.sql);
        //log("COST: " + Double.toString(plan.cost));
        //log("PLAN:\n");
        //log(plan.explainedPlan);

        assert(plan.fragments.size() <= 2);

        //////////////////////
        // OUTPUT THE RESULT
        //////////////////////

        // print out the run-at-every-partition fragment
        for (int i = 0; i < plan.fragments.size(); i++) {
            Fragment frag = plan.fragments.get(i);
            PlanNodeList planList = new PlanNodeList(frag.planGraph);
            String serializedPlan = planList.toJSONString();
            String encodedPlan = serializedPlan; //Encoder.compressAndBase64Encode(serializedPlan);
            if (frag.multiPartition) {
                retval.allPlan = encodedPlan;
            }
            else {
                retval.onePlan = encodedPlan;
            }
        }

        retval.replicatedDML = plan.replicatedTableDML;
        return retval;
    }
}
