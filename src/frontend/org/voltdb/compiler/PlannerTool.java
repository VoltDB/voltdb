/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltdb.compiler;

import java.util.List;

import org.hsqldb_voltpatches.HSQLInterface;
import org.hsqldb_voltpatches.HSQLInterface.HSQLParseException;
import org.voltcore.logging.VoltLogger;
import org.voltdb.ParameterSet;
import org.voltdb.PlannerStatsCollector;
import org.voltdb.PlannerStatsCollector.CacheUse;
import org.voltdb.StatsAgent;
import org.voltdb.StatsSelector;
import org.voltdb.VoltDB;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.common.Constants;
import org.voltdb.planner.BoundPlan;
import org.voltdb.planner.CompiledPlan;
import org.voltdb.planner.CorePlan;
import org.voltdb.planner.QueryPlanner;
import org.voltdb.planner.StatementPartitioning;
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
    final byte[] m_catalogHash;
    final AdHocCompilerCache m_cache;
    static PlannerStatsCollector m_plannerStats;

    public static final int AD_HOC_JOINED_TABLE_LIMIT = 5;

    public PlannerTool(final Cluster cluster, final Database database, byte[] catalogHash)
    {
        assert(cluster != null);
        assert(database != null);

        m_database = database;
        m_cluster = cluster;
        m_catalogHash = catalogHash;
        m_cache = AdHocCompilerCache.getCacheForCatalogHash(catalogHash);

        // LOAD HSQL
        m_hsql = HSQLInterface.loadHsqldb();
        String binDDL = m_database.getSchema();
        String ddl = Encoder.decodeBase64AndDecompress(binDDL);
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

        hostLog.debug("hsql loaded");

        // Create and register a singleton planner stats collector, if this is the first time.
        // In mock test environments there may be no stats agent.
        synchronized (this) {
            if (m_plannerStats == null) {
                final StatsAgent statsAgent = VoltDB.instance().getStatsAgent();
                if (statsAgent != null) {
                    m_plannerStats = new PlannerStatsCollector(-1);
                    statsAgent.registerStatsSource(StatsSelector.PLANNER, -1, m_plannerStats);
                }
            }
        }
    }

    public AdHocPlannedStatement planSqlForTest(String sqlIn) {
        StatementPartitioning infer = StatementPartitioning.inferPartitioning();
        return planSql(sqlIn, infer);
    }

    /**
     * Stripped down compile that is ONLY used to plan default procedures.
     */
    public synchronized CompiledPlan planSqlCore(String sql, StatementPartitioning partitioning) {
        TrivialCostModel costModel = new TrivialCostModel();
        DatabaseEstimates estimates = new DatabaseEstimates();
        QueryPlanner planner = new QueryPlanner(
            sql, "PlannerTool", "PlannerToolProc", m_cluster, m_database,
            partitioning, m_hsql, estimates, true,
            AD_HOC_JOINED_TABLE_LIMIT, costModel, null, null, DeterminismMode.FASTER);

        CompiledPlan plan = null;
        try {
            // do the expensive full planning.
            planner.parse();
            plan = planner.plan();
            assert(plan != null);
        }
        catch (Exception e) {
            throw new RuntimeException("Error compiling query: " + e.toString(), e);
        }

        if (plan == null) {
            throw new RuntimeException("Null plan received in PlannerTool.planSql");
        }

        return plan;
    }

    synchronized AdHocPlannedStatement planSql(String sqlIn, StatementPartitioning partitioning) {
        CacheUse cacheUse = CacheUse.FAIL;
        if (m_plannerStats != null) {
            m_plannerStats.startStatsCollection();
        }
        try {
            if ((sqlIn == null) || (sqlIn.length() == 0)) {
                throw new RuntimeException("Can't plan empty or null SQL.");
            }
            // remove any spaces or newlines
            String sql = sqlIn.trim();

            // No caching for forced single partition or forced multi partition SQL,
            // since these options potentially get different plans that may be invalid
            // or sub-optimal in other contexts. Likewise, plans cached from other contexts
            // may be incompatible with these options.
            // If this presents a planning performance problem, we could consider maintaining
            // separate caches for the 3 cases or maintaining up to 3 plans per cache entry
            // if the cases tended to have mostly overlapping queries.
            if (partitioning.isInferred()) {
                // Check the literal cache for a match.
                AdHocPlannedStatement cachedPlan = m_cache.getWithSQL(sqlIn);
                if (cachedPlan != null) {
                    cacheUse = CacheUse.HIT1;
                    return cachedPlan;
                }
                else {
                    cacheUse = CacheUse.MISS;
                }
            }

            // Reset plan node id counter
            AbstractPlanNode.resetPlanNodeIds();

            //////////////////////
            // PLAN THE STMT
            //////////////////////

            TrivialCostModel costModel = new TrivialCostModel();
            DatabaseEstimates estimates = new DatabaseEstimates();
            QueryPlanner planner = new QueryPlanner(
                    sql, "PlannerTool", "PlannerToolProc", m_cluster, m_database,
                    partitioning, m_hsql, estimates, true,
                    AD_HOC_JOINED_TABLE_LIMIT, costModel, null, null, DeterminismMode.FASTER);

            CompiledPlan plan = null;
            String[] extractedLiterals = null;
            String parsedToken = null;
            try {
                planner.parse();
                parsedToken = planner.parameterize();
                if (partitioning.isInferred()) {
                    // if cacheable, check the cache for a matching pre-parameterized plan
                    // if plan found, build the full plan using the parameter data in the
                    // QueryPlanner.
                    assert(parsedToken != null);
                    extractedLiterals = planner.extractedParamLiteralValues();
                    List<BoundPlan> boundVariants = m_cache.getWithParsedToken(parsedToken);
                    if (boundVariants != null) {
                        assert( ! boundVariants.isEmpty());
                        BoundPlan matched = null;
                        for (BoundPlan boundPlan : boundVariants) {
                            if (boundPlan.allowsParams(extractedLiterals)) {
                                matched = boundPlan;
                                break;
                            }
                        }
                        if (matched != null) {
                            CorePlan core = matched.m_core;
                            ParameterSet params = planner.extractedParamValues(core.parameterTypes);
                            AdHocPlannedStatement ahps = new AdHocPlannedStatement(sql.getBytes(Constants.UTF8ENCODING),
                                                                                   core,
                                                                                   params,
                                                                                   null);
                            ahps.setBoundConstants(matched.m_constants);
                            m_cache.put(sql, parsedToken, ahps, extractedLiterals);
                            cacheUse = CacheUse.HIT2;
                            return ahps;
                        }
                    }
                }

                // If not caching or there was no cache hit, do the expensive full planning.
                plan = planner.plan();
                assert(plan != null);
            } catch (Exception e) {
                throw new RuntimeException("Error compiling query: " + e.toString(), e);
            }

            if (plan == null) {
                throw new RuntimeException("Null plan received in PlannerTool.planSql");
            }

            //////////////////////
            // OUTPUT THE RESULT
            //////////////////////
            CorePlan core = new CorePlan(plan, m_catalogHash);
            AdHocPlannedStatement ahps = new AdHocPlannedStatement(plan, core);

            if (partitioning.isInferred()) {

                // Note either the parameter index (per force to a user-provided parameter) or
                // the actual constant value of the partitioning key inferred from the plan.
                // Either or both of these two values may simply default
                // to -1 and to null, respectively.
                core.setPartitioningParamIndex(partitioning.getInferredParameterIndex());
                core.setPartitioningParamValue(partitioning.getInferredPartitioningValue());

                if (planner.compiledAsParameterizedPlan()) {
                    assert(parsedToken != null);
                    // Again, plans with inferred partitioning are the only ones supported in the cache.
                    m_cache.put(sqlIn, parsedToken, ahps, extractedLiterals);
                }
            }
            return ahps;
        }
        finally {
            if (m_plannerStats != null) {
                m_plannerStats.endStatsCollection(m_cache.getLiteralCacheSize(), m_cache.getCoreCacheSize(), cacheUse, -1);
            }
        }
    }
}
