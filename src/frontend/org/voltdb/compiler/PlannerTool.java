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
import org.voltcore.logging.VoltLogger;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.planner.CompiledPlan;
import org.voltdb.planner.PartitioningForStatement;
import org.voltdb.planner.QueryPlanner;
import org.voltdb.planner.TrivialCostModel;
import org.voltdb.plannodes.AbstractPlanNode;
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

    public AdHocPlannedStatement planSql(String sqlIn, Object partitionParam, boolean inferSP, boolean allowParameterization) {
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
            plan = planner.compilePlan(costModel, sql, null, "PlannerTool", "PlannerToolProc", AD_HOC_JOINED_TABLE_LIMIT, null, true);
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

        if (!allowParameterization &&
            (plan.extractedParamValues.size() == 0) &&
            (plan.parameters.length > 0))
        {
            throw new RuntimeException("ERROR: PARAMETERIZATION IN AD HOC QUERY");
        }

        if (plan.isContentDeterministic() == false) {
            String potentialErrMsg =
                "Statement has a non-deterministic result - statement: \"" +
                sql + "\" , reason: " + plan.nondeterminismDetail();
            // throw new RuntimeException(potentialErrMsg);
            hostLog.warn(potentialErrMsg);
        }

        //////////////////////
        // OUTPUT THE RESULT
        //////////////////////

        return new AdHocPlannedStatement(plan);
    }
}
