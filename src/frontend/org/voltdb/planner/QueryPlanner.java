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

package org.voltdb.planner;

import org.hsqldb_voltpatches.HSQLInterface;
import org.hsqldb_voltpatches.HSQLInterface.HSQLParseException;
import org.hsqldb_voltpatches.VoltXMLElement;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.compiler.DatabaseEstimates;
import org.voltdb.compiler.ScalarValueHints;
import org.voltdb.plannodes.AbstractPlanNode;

/**
 * The query planner accepts catalog data, SQL statements from the catalog, then
 * outputs the plan with the lowest cost according to the cost model.
 *
 */
public class QueryPlanner {
    PartitioningForStatement m_partitioning;
    HSQLInterface m_HSQL;
    DatabaseEstimates m_estimates;
    Cluster m_cluster;
    Database m_db;
    String m_recentErrorMsg;
    boolean m_quietPlanner;
    final boolean m_fullDebug;

    /**
     * Initialize planner with physical schema info and a reference to HSQLDB parser.
     *
     * @param catalogCluster Catalog info about the physical layout of the cluster.
     * @param catalogDb Catalog info about schema, metadata and procedures.
     * @param partitioning Describes the specified and inferred partition context.
     * @param HSQL HSQLInterface pointer used for parsing SQL into XML.
     * @param useGlobalIds
     */
    public QueryPlanner(Cluster catalogCluster, Database catalogDb, PartitioningForStatement partitioning,
                        HSQLInterface HSQL, DatabaseEstimates estimates,
                        boolean suppressDebugOutput) {
        assert(HSQL != null);
        assert(catalogCluster != null);
        assert(catalogDb != null);

        m_HSQL = HSQL;
        m_db = catalogDb;
        m_cluster = catalogCluster;
        m_partitioning = partitioning;
        m_estimates = estimates;
        //m_quietPlanner = suppressDebugOutput;
        //m_fullDebug = System.getProperties().contains("compilerdebug");
        m_quietPlanner = false;
        m_fullDebug = true;
    }

    /**
     * Get the best plan for the SQL statement given, assuming the given costModel.
     *
     * @param costModel The current cost model to evaluate plans with.
     * @param sql SQL stmt text to be planned.
     * @param sql Suggested join order to be used for the query
     * @param stmtName The name of the sql statement to be planned.
     * @param procName The name of the procedure containing the sql statement to be planned.
     * @param singlePartition Is the stmt single-partition?
     * @param paramHints
     * @return The best plan found for the SQL statement or null if none can be found.
     */
    public CompiledPlan compilePlan(
            AbstractCostModel costModel,
            String sql,
            String joinOrder,
            String stmtName,
            String procName,
            int maxTablesPerJoin,
            ScalarValueHints[] paramHints) {
        assert(costModel != null);
        assert(sql != null);
        assert(stmtName != null);
        assert(procName != null);

        // reset any error message
        m_recentErrorMsg = null;

        // Reset plan node ids to start at 1 for this plan
        AbstractPlanNode.resetPlanNodeIds();

        // use HSQLDB to get XML that describes the semantics of the statement
        // this is much easier to parse than SQL and is checked against the catalog
        VoltXMLElement xmlSQL = null;
        try {
            xmlSQL = m_HSQL.getXMLCompiledStatement(sql);
        } catch (HSQLParseException e) {
            // XXXLOG probably want a real log message here
            m_recentErrorMsg = e.getMessage();
            return null;
        }

        // Init PlanProcessor
        PlanSelector planSelector = new PlanSelector(m_cluster, m_db, m_estimates, stmtName,
                procName, sql, costModel, paramHints, m_quietPlanner, m_fullDebug);
        planSelector.outputCompiledStatement(xmlSQL);
        // Init Assembler
        PlanAssembler assembler = new PlanAssembler(m_cluster, m_db, m_partitioning, planSelector);

        // Get a parsed statement from the xml
        // The callers of compilePlan are ready to catch any exceptions thrown here.
        AbstractParsedStmt parsedStmt = AbstractParsedStmt.parse(sql, xmlSQL, m_db, joinOrder);
        if (parsedStmt == null)
        {
            m_recentErrorMsg = "Failed to parse SQL statement: " + sql;
            return null;
        }
        if ((parsedStmt.tableList.size() > maxTablesPerJoin) && (parsedStmt.joinOrder == null)) {
            m_recentErrorMsg = "Failed to parse SQL statement: " + sql + " because a join of > 5 tables was requested"
                               + " without specifying a join order. See documentation for instructions on manually" +
                                 " specifying a join order";
            return null;
        }

        planSelector.outputParsedStatement(parsedStmt);

        // find the plan with minimal cost
        // Hint to the assembler that plan needs send node to be added
        boolean isTopPlan = true;
        CompiledPlan bestPlan = assembler.getBestCostPlan(parsedStmt, isTopPlan);

        // make sure we got a winner
        if (bestPlan == null) {
            m_recentErrorMsg = assembler.getErrorMessage();
            if (m_recentErrorMsg == null) {
                m_recentErrorMsg = "Unable to plan for statement. Error unknown.";
            }
            return null;
        }

        // reset all the plan node ids for a given plan
        // this makes the ids deterministic
        bestPlan.resetPlanNodeIds();

        // split up the plan everywhere we see send/recieve into multiple plan fragments
        Fragmentizer.fragmentize(bestPlan, m_db);
        return bestPlan;
    }

    public String getErrorMessage() {
        return m_recentErrorMsg;
    }
}
