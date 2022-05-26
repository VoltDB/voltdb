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

import static org.voltdb.planner.QueryPlanner.fragmentizePlan;
import static org.voltdb.plannerv2.utils.VoltRelUtil.calciteToVoltDBPlan;

import java.util.concurrent.ThreadLocalRandom;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.hsqldb_voltpatches.HSQLInterface;
import org.hsqldb_voltpatches.HSQLInterface.HSQLParseException;
import org.voltcore.logging.VoltLogger;
import org.voltdb.PlannerStatsCollector;
import org.voltdb.PlannerStatsCollector.CacheUse;
import org.voltdb.StatsAgent;
import org.voltdb.StatsSelector;
import org.voltdb.VoltDB;
import org.voltdb.catalog.Database;
import org.voltdb.exceptions.PlanningErrorException;
import org.voltdb.planner.CompiledPlan;
import org.voltdb.planner.CorePlan;
import org.voltdb.planner.ParameterizationInfo;
import org.voltdb.planner.QueryPlanner;
import org.voltdb.planner.StatementPartitioning;
import org.voltdb.planner.TrivialCostModel;
import org.voltdb.plannerv2.SqlTask;
import org.voltdb.plannerv2.VoltPlanner;
import org.voltdb.plannerv2.VoltSchemaPlus;
import org.voltdb.plannerv2.guards.PlannerFallbackException;
import org.voltdb.plannerv2.rel.logical.VoltLogicalRel;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalRel;
import org.voltdb.plannerv2.rules.PlannerRules.Phase;
import org.voltdb.plannerv2.utils.VoltRelUtil;
import org.voltdb.sysprocs.AdHocNTBase;
import org.voltdb.utils.CompressionService;
import org.voltdb.utils.Encoder;

/**
 * Planner tool accepts an already compiled VoltDB catalog and then
 * interactively accept SQL and outputs plans on standard out.
 *
 * Used only for AdHoc queries.
 */
public class PlannerTool {
    private static final VoltLogger hostLog = new VoltLogger("HOST");
    private static final VoltLogger compileLog = new VoltLogger("COMPILE");

    private Database m_database;
    private byte[] m_catalogHash;
    private AdHocCompilerCache m_cache;
    private SchemaPlus m_schemaPlus;
    private long m_adHocLargeFallbackCount = 0;
    private long m_adHocLargeModeCount = 0;

    private final HSQLInterface m_hsql;

    private static PlannerStatsCollector m_plannerStats;

    // If -Dlarge_mode_ratio=xx is specified via ant, the value will show up in the environment variables and
    // take higher priority. Otherwise, the value specified via VOLTDB_OPTS will take effect.
    // If the test is started by ant and -Dlarge_mode_ratio is not set, it will take a default value "-1" which
    // we should ignore.
    private final double m_largeModeRatio = Double.parseDouble((System.getenv("LARGE_MODE_RATIO") == null ||
            System.getenv("LARGE_MODE_RATIO").equals("-1")) ?
            System.getProperty("LARGE_MODE_RATIO", "0") :
            System.getenv("LARGE_MODE_RATIO"));

    public PlannerTool(final Database database, byte[] catalogHash) {
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
            if (decoded_cmd.isEmpty()) {
                continue;
            }
            try {
                m_hsql.runDDLCommand(decoded_cmd);
            } catch (HSQLParseException e) {
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

    public PlannerTool(final Database database, byte[] catalogHash, SchemaPlus schemaPlus) {
        this(database, catalogHash);
        m_schemaPlus = schemaPlus;
    }

    public PlannerTool updateWhenNoSchemaChange(Database database, byte[] catalogHash) {
        m_database = database;
        m_catalogHash = catalogHash;
        m_cache = AdHocCompilerCache.getCacheForCatalogHash(catalogHash);
        if (AdHocNTBase.USING_CALCITE) {
            // Do not use Calcite to process DDLs, until we have full support of all DDLs, as well as
            // catalog commands such as "DR TABLE foo".
            m_schemaPlus = VoltSchemaPlus.from(m_database);
        }
        return this;
    }

    public HSQLInterface getHSQLInterface() {
        return m_hsql;
    }

    public long getAdHocLargeFallbackCount() {
        return m_adHocLargeFallbackCount;
    }

    public long getAdHocLargeModeCount() {
        return m_adHocLargeModeCount;
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
                costModel, null, null, DeterminismMode.FASTER, false, false)) {

            // do the expensive full planning.
            planner.parse();
            plan = planner.plan();
            assert(plan != null);
        } catch (Exception e) {
            /*
             * Don't log PlanningErrorExceptions or HSQLParseExceptions, as they
             * are at least somewhat expected.
             */
            String loggedMsg = "";
            if (!(e instanceof PlanningErrorException)) {
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

    public static synchronized CompiledPlan getCompiledPlanCalcite(SchemaPlus schemaPlus, SqlNode sqlNode)
            throws ValidationException, RelConversionException, PlannerFallbackException{
        // TRAIL [Calcite-AdHoc-DQL/DML:4] PlannerTool.planSqlCalcite()
        VoltPlanner planner = new VoltPlanner(schemaPlus);

        // Validate the task's SqlNode.
        SqlNode validatedNode = planner.validate(sqlNode);

        // Convert SqlNode to RelNode.
        RelNode rel = planner.convert(validatedNode);
        compileLog.info("ORIGINAL\n" + RelOptUtil.toString(rel));

        JoinCounter scanCounter = new JoinCounter();
        rel.accept(scanCounter);
        boolean canCommuteJoins = scanCounter.canCommuteJoins();

        // Drill has SUBQUERY_REWRITE and WINDOW_REWRITE here, add?
        // See Drill's DefaultSqlHandler.convertToRel()

        // Take Drill's DefaultSqlHandler.convertToRawDrel() as reference.
        // We probably need FILTER_SET_OP_TRANSPOSE_RULE and PROJECT_SET_OP_TRANSPOSE_RULE?
        // They need to be run by the Hep planner (CALCITE-1271).

        RelTraitSet requiredLogicalOutputTraits = planner.getEmptyTraitSet().replace(VoltLogicalRel.CONVENTION);
        // Apply Calcite logical rules
        // See comments in PlannerPrograms.directory.LOGICAL to find out
        // what each rule is used for.
        RelNode transformed = planner.transform(Phase.LOGICAL.ordinal(), requiredLogicalOutputTraits, rel);

        compileLog.info("LOGICAL\n" + RelOptUtil.toString(transformed));

        // Add RelDistribution trait definition to the planner to make Calcite aware of the new trait.
        //
        // If RelDistributionTraitDef is added to the planner as the initial traits,
        // ProjectToCalcRule.onMatch() will fire RelMdDistribution.calc() which will result in
        // an AssertionError. It is cheaper to manually add RelDistributionTrait here than replacing all
        // the LogicalCalc and Calc-related rules to fix this.
        planner.addRelTraitDef(RelDistributionTraitDef.INSTANCE);

        // Add RelDistributions.ANY trait to the rel tree.
        transformed = VoltRelUtil.addTraitRecursively(transformed, RelDistributions.ANY);

        // Apply MP query fallback rules
        // As of 9.0, only SP AdHoc queries are using this new planner.
        transformed = VoltPlanner.transformHep(Phase.MP_FALLBACK, transformed);

        // Transform RIGHT Outer joins to LEFT ones
        transformed = VoltPlanner.transformHep(Phase.LOGICAL_JOIN, transformed);

        // Prepare the set of RelTraits required of the root node at the termination of the physical conversion phase.
        // RelDistributions.ANY can satisfy any other types of RelDistributions.
        // See RelDistributions.RelDistributionImpl.satisfies()
        RelTraitSet requiredPhysicalOutputTraits = transformed.getTraitSet()
                .replace(VoltPhysicalRel.CONVENTION)
                .replace(RelDistributions.ANY);

        // Apply physical conversion rules.
        final Phase physicalPhase = canCommuteJoins ?
                Phase.PHYSICAL_CONVERSION_WITH_JOIN_COMMUTE : Phase.PHYSICAL_CONVERSION;
        transformed = planner.transform(physicalPhase.ordinal(), requiredPhysicalOutputTraits, transformed);

        // apply inlining rules.
        transformed = VoltPlanner.transformHep(Phase.INLINE, HepMatchOrder.ARBITRARY, transformed, true);

        CompiledPlan compiledPlan = new CompiledPlan(false);
        try {
            // assume not large query
            calciteToVoltDBPlan((VoltPhysicalRel) transformed, compiledPlan);

            compiledPlan.explainedPlan = compiledPlan.rootPlanGraph.toExplainPlanString();
            // Renumber the plan node ids to start with 1
            compiledPlan.resetPlanNodeIds(1);
        } catch (Exception e){
            throw new PlannerFallbackException(e);
        }
        planner.close();
        fragmentizePlan(compiledPlan);

        return compiledPlan;
    }

    /**
     * Plan a query with the Calcite planner.
     * @param task the query to plan.
     * @return a planned statement.
     */
    public synchronized AdHocPlannedStatement planSqlCalcite(SqlTask task)
            throws ValidationException, RelConversionException, PlannerFallbackException {
        CompiledPlan plan = getCompiledPlanCalcite(
                // TODO: we need a reliable way to sync Calcite's SchemaPlus from VoltDB's Catalog,
                // esp. since we start relying on Calcite to operate on 'CREATE TABLE' statements.
                // See VoltCompiler#compileDatabase().
                VoltSchemaPlus.from(m_database)/*m_schemaPlus*/,
                task.getParsedQuery());
        plan.sql = task.getSQL();
        CorePlan core = new CorePlan(plan, m_catalogHash);
        core.setPartitioningParamValue(plan.getPartitioningValue());

        // TODO Calcite ready: enable when we are ready
        throw new PlannerFallbackException("planSqlCalcite not ready");
        // return new AdHocPlannedStatement(plan, core);
    }

    public synchronized AdHocPlannedStatement planSql(
            String sql, StatementPartitioning partitioning, boolean isExplainMode, final Object[] userParams,
            boolean isSwapTables, boolean isLargeQuery) {
        // large_mode_ratio will force execution of SQL queries to use the "large" path (for read-only queries)
        // a certain percentage of the time
        if (m_largeModeRatio > 0 && !isLargeQuery) {
            if (m_largeModeRatio >= 1 || m_largeModeRatio > ThreadLocalRandom.current().nextDouble()) {
                isLargeQuery = true;
                m_adHocLargeModeCount++;
            }
        }
        CacheUse cacheUse = CacheUse.FAIL;
        if (m_plannerStats != null) {
            m_plannerStats.startStatsCollection();
        }
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
                } else {
                    cacheUse = CacheUse.MISS;
                }
            }

            //////////////////////
            // PLAN THE STMT
            //////////////////////

            final SqlPlanner planner = new SqlPlanner(m_database, partitioning, m_hsql, sql,
                    isLargeQuery, isSwapTables, isExplainMode, m_adHocLargeFallbackCount, userParams, m_cache, compileLog);
            final CompiledPlan plan = planner.getCompiledPlan();
            final AdHocPlannedStatement adhocPlan = planner.getAdhocPlan();
            assert (plan == null) != (adhocPlan == null) : "It should be either planned or cached";
            partitioning = planner.getPartitioning();
            m_adHocLargeFallbackCount = planner.getAdHocLargeFallBackCount();
            if (adhocPlan != null) {
                cacheUse = CacheUse.HIT2;   // IMPORTANT
                return adhocPlan;
            } else {
                final String parsedToken = planner.getParsedToken();
                //////////////////////
                // OUTPUT THE RESULT
                //////////////////////
                final CorePlan core = new CorePlan(plan, m_catalogHash);
                final AdHocPlannedStatement ahps = new AdHocPlannedStatement(plan, core);

                // Do not put wrong parameter explain query into cache.
                // Also, do not put large query plans into the cache.
                if (planner.isCacheable()) {
                    // Note either the parameter index (per force to a user-provided parameter) or
                    // the actual constant value of the partitioning key inferred from the plan.
                    // Either or both of these two values may simply default
                    // to -1 and to null, respectively.
                    core.setPartitioningParamIndex(partitioning.getInferredParameterIndex());
                    core.setPartitioningParamValue(partitioning.getInferredPartitioningValue());
                    assert (parsedToken != null);
                    // Again, plans with inferred partitioning are the only ones supported in the cache.
                    m_cache.put(sql, parsedToken, ahps, planner.getExtractedLiterals(), planner.hasQuestionMark(),
                            planner.hasExceptionWhenParameterized());
                }
                return ahps;
            }
        } finally {
            if (m_plannerStats != null) {
                m_plannerStats.endStatsCollection(m_cache.getLiteralCacheSize(), m_cache.getCoreCacheSize(), cacheUse, -1);
            }
        }
    }

    // RelShuttle to count number of joins in the RelNode to decide
    // whether to apply join commute rules or not
    public static class JoinCounter extends RelShuttleImpl {
        private final static int DEFAULT_MAX_JOIN_TABLES = 6;

        private int joinCount = 0;

        public boolean canCommuteJoins() {
            return joinCount < DEFAULT_MAX_JOIN_TABLES;
        }

        public boolean hasJoins() {
            return joinCount > 0;
        }

        @Override
        public RelNode visit(LogicalJoin join) {
            ++joinCount;
            return super.visit(join);
        }

    }

}
