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

package org.voltdb.compiler;

import org.hsqldb_voltpatches.HSQLInterface;
import org.voltcore.logging.VoltLogger;
import org.voltdb.ParameterSet;
import org.voltdb.catalog.Database;
import org.voltdb.common.Constants;
import org.voltdb.exceptions.PlanningErrorException;
import org.voltdb.planner.BoundPlan;
import org.voltdb.planner.CompiledPlan;
import org.voltdb.planner.CorePlan;
import org.voltdb.planner.QueryPlanner;
import org.voltdb.planner.StatementPartitioning;
import org.voltdb.planner.TrivialCostModel;

import java.util.List;

/**
 * (One of the) Core part of query planning, considering all options such as LargeQueryMode, ExplainMode, SwapTable,
 * and plan cache uses/updates.
 */
final class SqlPlanner {
    private final Database m_database;
    private final HSQLInterface m_hsql;
    private final String m_sql;
    private final boolean m_isLargeQuery, m_isSwapTables, m_isExplainMode;
    private final Object[] m_userParams;
    private final AdHocCompilerCache m_cache;
    private final VoltLogger m_logger;
    // outcomes
    private final CompiledPlan m_plan;
    private AdHocPlannedStatement m_adhocPlan = null;
    private boolean m_hasQuestionMark = false;
    private boolean m_wrongNumberParameters = false;
    private boolean m_hasExceptionWhenParameterized = false;
    private StatementPartitioning m_partitioning;
    private long m_adHocLargeFallbackCount;
    private String m_parsedToken;
    private String[] m_extractedLiterals;

    SqlPlanner(Database database, StatementPartitioning partitioning, HSQLInterface hsql, String sql,
            boolean isLargeQuery, boolean isSwapTables, boolean isExplainMode, long adHocLargeFallbackCount,
            Object[] userParams, AdHocCompilerCache cache, VoltLogger logger) {
        m_database = database;
        m_partitioning = partitioning;
        m_hsql = hsql;
        m_sql = sql;
        m_isLargeQuery = isLargeQuery;
        m_isSwapTables = isSwapTables;
        m_isExplainMode = isExplainMode;
        m_adHocLargeFallbackCount = adHocLargeFallbackCount;
        m_userParams = userParams;
        m_cache = cache;
        m_logger = logger;
        m_plan = cacheOrPlan();
    }

    CompiledPlan getCompiledPlan() {
        return m_plan;
    }

    AdHocPlannedStatement getAdhocPlan() {
        return m_adhocPlan;
    }

    boolean hasQuestionMark() {
        return m_hasQuestionMark;
    }

    boolean hasExceptionWhenParameterized() {
        return m_hasExceptionWhenParameterized;
    }

    long getAdHocLargeFallBackCount() {
        return m_adHocLargeFallbackCount;
    }

    String getParsedToken() {
        return m_parsedToken;
    }

    StatementPartitioning getPartitioning() {
        return m_partitioning;
    }

    String[] getExtractedLiterals() {
        return m_extractedLiterals;
    }

    boolean isCacheable() {
        // NOTE: internally (within this class), the method is "stable" after the point plan() is called;
        // externally, it's always stable.
        return ! m_wrongNumberParameters && ! m_isLargeQuery && m_partitioning.isInferred();
    }

    private boolean isCached(List<BoundPlan> boundVariants, QueryPlanner planner) {
        BoundPlan matched = null;
        for (BoundPlan boundPlan : boundVariants) {
            if (boundPlan.allowsParams(m_extractedLiterals)) {
                matched = boundPlan;
                final CorePlan core = matched.m_core;
                final ParameterSet params;
                if (planner.compiledAsParameterizedPlan()) {
                    params = planner.extractedParamValues(core.parameterTypes);
                } else if (m_hasQuestionMark) {
                    params = ParameterSet.fromArrayNoCopy(m_userParams);
                } else { // No constants AdHoc queries
                    params = ParameterSet.emptyParameterSet();
                }
                m_adhocPlan = new AdHocPlannedStatement(m_sql.getBytes(Constants.UTF8ENCODING),
                        core, params, null);
                m_adhocPlan.setBoundConstants(matched.m_constants);
                // parameterized plan from the cache does not have exception
                m_cache.put(m_sql, m_parsedToken, m_adhocPlan, m_extractedLiterals, m_hasQuestionMark, false);
            }
        }
        return matched != null;
    }

    private CompiledPlan plan(QueryPlanner planner) {
        final CompiledPlan plan = planner.plan();
        if (plan.getStatementPartitioning() != null) {
            m_partitioning = plan.getStatementPartitioning();
        }
        if (plan.getIsLargeQuery() != m_isLargeQuery) {
            ++m_adHocLargeFallbackCount;
        }
        m_hasExceptionWhenParameterized = planner.wasBadPameterized();
        return plan;
    }

    private CompiledPlan cacheOrPlan() {
        // This try-with-resources block acquires a global lock on all planning
        // This is required until we figure out how to do parallel planning.
        try (QueryPlanner planner = new QueryPlanner(
                m_sql, "PlannerTool", "PlannerToolProc", m_database,
                m_partitioning, m_hsql, new DatabaseEstimates(), !VoltCompiler.DEBUG_MODE, new TrivialCostModel(),
                null, null, DeterminismMode.FASTER, m_isLargeQuery, false)) {
            if (m_isSwapTables) {
                planner.planSwapTables();
            } else {
                planner.parse();
            }
            m_parsedToken = planner.parameterize();

            // check the parameters count
            // check user input question marks with input parameters
            int inputParamsLength = m_userParams == null ? 0 : m_userParams.length;
            if (planner.getAdhocUserParamsCount() > CompiledPlan.MAX_PARAM_COUNT) {
                throw new PlanningErrorException(
                        "The statement's parameter count " + planner.getAdhocUserParamsCount() +
                                " must not exceed the maximum " + CompiledPlan.MAX_PARAM_COUNT);
            } else if (planner.getAdhocUserParamsCount() != inputParamsLength) {
                m_wrongNumberParameters = true;
                if (! m_isExplainMode) {
                    throw new PlanningErrorException(String.format(
                            "Incorrect number of parameters passed: expected %d, passed %d",
                            planner.getAdhocUserParamsCount(), inputParamsLength));
                }
            }
            m_hasQuestionMark = planner.getAdhocUserParamsCount() > 0;

            // do not put wrong parameter explain query into cache
            if (isCacheable()) {
                // if cache-able, check the cache for a matching pre-parameterized plan
                // if plan found, build the full plan using the parameter data in the
                // QueryPlanner.
                assert(m_parsedToken != null);
                m_extractedLiterals = planner.extractedParamLiteralValues();
                final List<BoundPlan> boundVariants = m_cache.getWithParsedToken(m_parsedToken);
                assert boundVariants == null || ! boundVariants.isEmpty();
                if (boundVariants != null && isCached(boundVariants, planner)) {
                    // Plan cache is hit: caller of this class need to set cacheUse to CacheUse.HIT2
                    return null;
                }
            }
            // If not caching or there was no cache hit, do the expensive full planning.
            return plan(planner);
        } catch (Exception e) {
            /*
             * Don't log PlanningErrorExceptions or HSQLParseExceptions, as
             * they are at least somewhat expected.
             */
            String loggedMsg = "";
            if (! (e instanceof PlanningErrorException)) {
                m_logger.error("Error compiling query: ", e);
                loggedMsg = " (Stack trace has been written to the log.)";
            }
            throw new RuntimeException(String.format("SQL error while compiling query: %s%s",
                    e.getMessage() == null ? e.toString() : e.getMessage(), loggedMsg), e);
        }
    }
}
