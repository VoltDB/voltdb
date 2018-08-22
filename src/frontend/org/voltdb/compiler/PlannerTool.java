/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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
import org.voltdb.catalog.Database;
import org.voltdb.common.Constants;
import org.voltdb.planner.BoundPlan;
import org.voltdb.planner.CompiledPlan;
import org.voltdb.planner.CorePlan;
import org.voltdb.planner.ParameterizationInfo;
import org.voltdb.planner.PlanningErrorException;
import org.voltdb.planner.QueryPlanner;
import org.voltdb.planner.StatementPartitioning;
import org.voltdb.planner.TrivialCostModel;
import org.voltdb.utils.CompressionService;
import org.voltdb.utils.Encoder;

/**
 * Planner tool accepts an already compiled VoltDB catalog and then
 * interactively accept SQL and outputs plans on standard out.
 *
 * Used only for ad hoc queries.
 */
public class PlannerTool {
    private static final VoltLogger hostLog = new VoltLogger("HOST");
    private static final VoltLogger compileLog = new VoltLogger("COMPILE");

    private Database m_database;
    private byte[] m_catalogHash;
    private AdHocCompilerCache m_cache;
    private long m_adHocLargeFallbackCount = 0;

    private final HSQLInterface m_hsql;

    private static PlannerStatsCollector m_plannerStats;

    public PlannerTool(final Database database, byte[] catalogHash)
    {
        assert(database != null);

        m_database = database;
        m_catalogHash = catalogHash;
        m_cache = AdHocCompilerCache.getCacheForCatalogHash(catalogHash);

        // LOAD HSQL
        m_hsql = HSQLInterface.loadHsqldb(ParameterizationInfo.getParamStateManager());
        String binDDL = m_database.getSchema();
        String ddl = CompressionService.decodeBase64AndDecompress(binDDL);
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
        if (m_plannerStats == null) {
            synchronized (this.getClass()) {
                if (m_plannerStats == null) {
                    final StatsAgent statsAgent = VoltDB.instance().getStatsAgent();
                    // In mock test environments there may be no stats agent.
                    if (statsAgent != null) {
                        m_plannerStats = new PlannerStatsCollector(-1);
                        statsAgent.registerStatsSource(StatsSelector.PLANNER, -1, m_plannerStats);
                    }
                }
            }
        }
    }

    public PlannerTool updateWhenNoSchemaChange(Database database, byte[] catalogHash) {
        m_database = database;
        m_catalogHash = catalogHash;
        m_cache = AdHocCompilerCache.getCacheForCatalogHash(catalogHash);

        return this;
    }

    public HSQLInterface getHSQLInterface() {
        return m_hsql;
    }

    public long getAdHocLargeFallbackCount() {
        return m_adHocLargeFallbackCount;
    }

    public AdHocPlannedStatement planSqlForTest(String sqlIn) {
        StatementPartitioning infer = StatementPartitioning.inferPartitioning();
        return planSql(sqlIn, infer, false, null, false, false);
    }

    private void logException(Exception e, String fmtLabel) {
        compileLog.error(fmtLabel + ": ", e);
    }

    /**
     * Stripped down compile that is ONLY used to plan default procedures.
     */
    public synchronized CompiledPlan planSqlCore(String sql, StatementPartitioning partitioning) {
        TrivialCostModel costModel = new TrivialCostModel();
        DatabaseEstimates estimates = new DatabaseEstimates();

        CompiledPlan plan = null;
        // This try-with-resources block acquires a global lock on all planning
        // This is required until we figure out how to do parallel planning.
        try (QueryPlanner planner = new QueryPlanner(
                sql, "PlannerTool", "PlannerToolProc", m_database,
                partitioning, m_hsql, estimates, !VoltCompiler.DEBUG_MODE,
                costModel, null, null, DeterminismMode.FASTER, false)) {

            // do the expensive full planning.
            planner.parse();
            plan = planner.plan();
            assert(plan != null);
        }
        catch (Exception e) {
            /*
             * Don't log PlanningErrorExceptions or HSQLParseExceptions, as they
             * are at least somewhat expected.
             */
            String loggedMsg = "";
            if (!(e instanceof PlanningErrorException || e instanceof HSQLParseException)) {
                logException(e, "Error compiling query");
                loggedMsg = " (Stack trace has been written to the log.)";
            }
            if (e.getMessage() != null) {
                throw new RuntimeException("SQL error while compiling query: " + e.getMessage() + loggedMsg, e);
            }
            throw new RuntimeException("SQL error while compiling query: " + e.toString() + loggedMsg, e);
        }

        if (plan == null) {
            throw new RuntimeException("Null plan received in PlannerTool.planSql");
        }

        return plan;
    }

    public synchronized AdHocPlannedStatement planSql(String sql, StatementPartitioning partitioning,
            boolean isExplainMode, final Object[] userParams, boolean isSwapTables, boolean isLargeQuery) {

        CacheUse cacheUse = CacheUse.FAIL;
        if (m_plannerStats != null) {
            m_plannerStats.startStatsCollection();
        }
        boolean hasUserQuestionMark = false;
        boolean wrongNumberParameters = false;
        try {
            if ((sql == null) || (sql = sql.trim()).isEmpty()) {    // remove any spaces or newlines
                throw new RuntimeException("Can't plan empty or null SQL.");
            }

            // No caching for forced single partition or forced multi partition SQL,
            // since these options potentially get different plans that may be invalid
            // or sub-optimal in other contexts. Likewise, plans cached from other contexts
            // may be incompatible with these options.
            // If this presents a planning performance problem, we could consider maintaining
            // separate caches for the 3 cases or maintaining up to 3 plans per cache entry
            // if the cases tended to have mostly overlapping queries.
            //
            // Large queries are not cached.  Their plans are different than non-large queries
            // with the same SQL text, and in general we expect them to be slow.  If at some
            // point it seems worthwhile to cache such plans, we can explore it.
            if (partitioning.isInferred() && !isLargeQuery) {
                // Check the literal cache for a match.
                AdHocPlannedStatement cachedPlan = m_cache.getWithSQL(sql);
                if (cachedPlan != null) {
                    cacheUse = CacheUse.HIT1;
                    return cachedPlan;
                }
                else {
                    cacheUse = CacheUse.MISS;
                }
            }

            //////////////////////
            // PLAN THE STMT
            //////////////////////

            CompiledPlan plan = null;
            boolean planHasExceptionsWhenParameterized = false;
            String[] extractedLiterals = null;
            String parsedToken = null;

            TrivialCostModel costModel = new TrivialCostModel();
            DatabaseEstimates estimates = new DatabaseEstimates();
            // This try-with-resources block acquires a global lock on all planning
            // This is required until we figure out how to do parallel planning.
            try (QueryPlanner planner = new QueryPlanner(
                    sql,
                    "PlannerTool",
                    "PlannerToolProc",
                    m_database,
                    partitioning,
                    m_hsql,
                    estimates,
                    !VoltCompiler.DEBUG_MODE,
                    costModel,
                    null,
                    null,
                    DeterminismMode.FASTER,
                    isLargeQuery)) {

                if (isSwapTables) {
                    planner.planSwapTables();
                } else {
                    planner.parse();
                }
                parsedToken = planner.parameterize();

                // check the parameters count
                // check user input question marks with input parameters
                int inputParamsLengh = userParams == null ? 0: userParams.length;
                if (planner.getAdhocUserParamsCount() > CompiledPlan.MAX_PARAM_COUNT) {
                    throw new PlanningErrorException(
                            "The statement's parameter count " + planner.getAdhocUserParamsCount() +
                            " must not exceed the maximum " + CompiledPlan.MAX_PARAM_COUNT);
                }
                if (planner.getAdhocUserParamsCount() != inputParamsLengh) {
                    wrongNumberParameters = true;
                    if (!isExplainMode) {
                        throw new PlanningErrorException(String.format(
                                "Incorrect number of parameters passed: expected %d, passed %d",
                                planner.getAdhocUserParamsCount(), inputParamsLengh));
                    }
                }
                hasUserQuestionMark  = planner.getAdhocUserParamsCount() > 0;

                // do not put wrong parameter explain query into cache
                if (!wrongNumberParameters && partitioning.isInferred() && !isLargeQuery) {
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
                            ParameterSet params = null;
                            if (planner.compiledAsParameterizedPlan()) {
                                params = planner.extractedParamValues(core.parameterTypes);
                            } else if (hasUserQuestionMark) {
                                params = ParameterSet.fromArrayNoCopy(userParams);
                            } else {
                                // No constants AdHoc queries
                                params = ParameterSet.emptyParameterSet();
                            }

                            AdHocPlannedStatement ahps = new AdHocPlannedStatement(sql.getBytes(Constants.UTF8ENCODING),
                                                                                   core,
                                                                                   params,
                                                                                   null);
                            ahps.setBoundConstants(matched.m_constants);
                            // parameterized plan from the cache does not have exception
                            m_cache.put(sql, parsedToken, ahps, extractedLiterals, hasUserQuestionMark, false);
                            cacheUse = CacheUse.HIT2;
                            return ahps;
                        }
                    }
                }

                // If not caching or there was no cache hit, do the expensive full planning.
                plan = planner.plan();
                if (plan.getStatementPartitioning() != null) {
                    partitioning = plan.getStatementPartitioning();
                }
                if (plan.getIsLargeQuery() != isLargeQuery) {
                    ++m_adHocLargeFallbackCount;
                }

                planHasExceptionsWhenParameterized = planner.wasBadPameterized();
            }
            catch (Exception e) {
                /*
                 * Don't log PlanningErrorExceptions or HSQLParseExceptions, as
                 * they are at least somewhat expected.
                 */
                String loggedMsg = "";
                if (!((e instanceof PlanningErrorException) || (e instanceof HSQLParseException))) {
                    logException(e, "Error compiling query");
                    loggedMsg = " (Stack trace has been written to the log.)";
                }
                if (e.getMessage() != null) {
                    throw new RuntimeException("SQL error while compiling query: " + e.getMessage() + loggedMsg, e);
                }
                throw new RuntimeException("SQL error while compiling query: " + e.toString() + loggedMsg, e);
            }

            //////////////////////
            // OUTPUT THE RESULT
            //////////////////////
            CorePlan core = new CorePlan(plan, m_catalogHash);
            AdHocPlannedStatement ahps = new AdHocPlannedStatement(plan, core);

            // Do not put wrong parameter explain query into cache.
            // Also, do not put large query plans into the cache.
            if (!wrongNumberParameters && partitioning.isInferred() && !isLargeQuery) {

                // Note either the parameter index (per force to a user-provided parameter) or
                // the actual constant value of the partitioning key inferred from the plan.
                // Either or both of these two values may simply default
                // to -1 and to null, respectively.
                core.setPartitioningParamIndex(partitioning.getInferredParameterIndex());
                core.setPartitioningParamValue(partitioning.getInferredPartitioningValue());


                assert(parsedToken != null);
                // Again, plans with inferred partitioning are the only ones supported in the cache.
                m_cache.put(sql, parsedToken, ahps, extractedLiterals, hasUserQuestionMark, planHasExceptionsWhenParameterized);
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
