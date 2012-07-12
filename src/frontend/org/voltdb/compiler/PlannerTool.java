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

import java.util.List;

import org.hsqldb_voltpatches.HSQLInterface;
import org.hsqldb_voltpatches.HSQLInterface.HSQLParseException;
import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltDB;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.planner.CompiledPlan;
import org.voltdb.planner.CompiledPlan.Fragment;
import org.voltdb.planner.ParameterInfo;
import org.voltdb.planner.PartitioningForStatement;
import org.voltdb.planner.QueryPlanner;
import org.voltdb.planner.TrivialCostModel;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.PlanNodeList;
import org.voltdb.utils.Encoder;

/**
 * Planner tool accepts an already compiled VoltDB catalog and then
 * interactively accept SQL and outputs plans on standard out.
 */
public class PlannerTool {

    private static final VoltLogger hostLog = new VoltLogger("HOST");

    final Database m_database;
    final Cluster m_cluster;
    final HSQLInterface m_hsql;

    public static final int AD_HOC_JOINED_TABLE_LIMIT = 5;

    public static class Result {
        byte[] onePlan = null;
        byte[] allPlan = null;
        boolean replicatedDML = false;
        boolean nonDeterministic = false;
        Object partitionParam;
        List<ParameterInfo> params;

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("RESULT {\n");
            sb.append("  ONE: ").append(onePlan == null ? "null" : new String(onePlan, VoltDB.UTF8ENCODING)).append("\n");
            sb.append("  ALL: ").append(allPlan == null ? "null" : new String(allPlan, VoltDB.UTF8ENCODING)).append("\n");
            sb.append("  RTD: ").append(replicatedDML ? "true" : "false").append("\n");
            sb.append("  PARAM: ").append(partitionParam == null ? "null" : partitionParam.toString()).append("\n");
            sb.append("}");
            return sb.toString();
        }
    }

    public PlannerTool(final Cluster cluster, final Database database) {
        assert(cluster != null);
        assert(database != null);

        m_database = database;
        m_cluster = cluster;

        // LOAD HSQL
        m_hsql = HSQLInterface.loadHsqldb();
        String hexDDL = m_database.getSchema();
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

    public Result planSql(String sqlIn, Object partitionParam, boolean inferSP, boolean allowParameterization) {
        if ((sqlIn == null) || (sqlIn.length() == 0)) {
            throw new RuntimeException("Can't plan empty or null SQL.");
        }
        // remove any spaces or newlines
        String sql = sqlIn.trim();

        hostLog.debug("received sql stmt: " + sql);

        //Reset plan node id counter
        AbstractPlanNode.resetPlanNodeIds();

        //////////////////////
        // PLAN THE STMT
        //////////////////////

        TrivialCostModel costModel = new TrivialCostModel();
        PartitioningForStatement partitioning = new PartitioningForStatement(partitionParam, inferSP, inferSP);
        QueryPlanner planner = new QueryPlanner(
                m_cluster, m_database, partitioning, m_hsql, new DatabaseEstimates(), true);
        CompiledPlan plan = null;
        try {
            plan = planner.compilePlan(costModel, sql, null, "PlannerTool", "PlannerToolProc", AD_HOC_JOINED_TABLE_LIMIT, null);
        } catch (Exception e) {
            throw new RuntimeException("Error compiling query: " + e.getMessage(), e);
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

        if (!allowParameterization && plan.parameters.size() > 0) {
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
        Result retval = new Result();

        /*
         * Copy the parameter information
         */
        retval.params = plan.parameters;
        for (Fragment frag : plan.fragments) {
            PlanNodeList planList = new PlanNodeList(frag.planGraph);
            byte[] serializedPlan = planList.toJSONString().getBytes(VoltDB.UTF8ENCODING);
            byte[] encodedPlan = serializedPlan; //Encoder.compressAndBase64Encode(serializedPlan);
            if (frag.multiPartition) {
                assert(retval.allPlan == null);
                retval.allPlan = encodedPlan;
            }
            else {
                assert(retval.onePlan == null);
                retval.onePlan = encodedPlan;
            }
        }

        retval.replicatedDML = plan.replicatedTableDML;
        retval.nonDeterministic = !plan.isContentDeterministic() || !plan.isOrderDeterministic();
        retval.partitionParam = partitioning.effectivePartitioningValue();
        return retval;
    }
}
