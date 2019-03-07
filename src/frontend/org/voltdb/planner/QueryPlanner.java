/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.hsqldb_voltpatches.HSQLInterface;
import org.hsqldb_voltpatches.HSQLInterface.HSQLParseException;
import org.hsqldb_voltpatches.VoltXMLElement;
import org.voltdb.ParameterSet;
import org.voltdb.VoltType;
import org.voltdb.catalog.*;
import org.voltdb.compiler.DatabaseEstimates;
import org.voltdb.compiler.DeterminismMode;
import org.voltdb.compiler.ScalarValueHints;
import org.voltdb.compiler.VoltXMLElementHelper;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.microoptimizations.MicroOptimizationRunner;
import org.voltdb.planner.parseinfo.StmtCommonTableScan;
import org.voltdb.plannodes.*;
import org.voltdb.types.ConstraintType;

/**
 * The query planner accepts catalog data, SQL statements from the catalog, then
 * outputs the plan with the lowest cost according to the cost model.
 *
 */
public class QueryPlanner implements AutoCloseable {
    private String m_sql;
    private String m_stmtName;
    private String m_procName;
    private HSQLInterface m_HSQL;
    private DatabaseEstimates m_estimates;
    private Database m_db;
    private String m_recentErrorMsg;
    private StatementPartitioning m_partitioning;
    private AbstractCostModel m_costModel;
    private ScalarValueHints[] m_paramHints;
    private String m_joinOrder;
    private DeterminismMode m_detMode;
    private PlanSelector m_planSelector;
    private boolean m_isUpsert;
    private boolean m_isLargeQuery;

    // generated by parse(..)
    private VoltXMLElement m_xmlSQL = null;
    private ParameterizationInfo m_paramzInfo = null;

    // generated by parameterize(...)
    private int m_adhocUserParamsCount = 0;

    // generated by plan(...)
    private boolean m_hasExceptionWhenParameterized = false;

    private static final boolean DEBUGGING_STATIC_MODE_TO_RETRY_ON_ERROR = false;

    public final static String UPSERT_TAG = "isUpsert";

    private static final Lock PLANNER_LOCK = new ReentrantLock();

    /**
     * Initialize planner with physical schema info and a reference to HSQLDB parser.
     *
     * NOTE: Until the planner can handle planning multiple statements in parallel,
     * creating an instance of this object will lock the global PLANNER_LOCK, which must
     * be released by calling this class's close method.
     *
     * This class implements AutoCloseable, so the easiest way to achieve this is like so:
     * try (QueryPlanner planner = new QueryPlanner(...)) {
     *     <do all the planning here>
     * }
     *
     * @param sql Literal SQL statement to parse
     * @param stmtName The name of the statement for logging/debugging
     * @param procName The name of the proc for logging/debugging
     * @param catalogDb Catalog info about schema, metadata and procedures.
     * @param partitioning Describes the specified and inferred partition context.
     * @param HSQL HSQLInterface pointer used for parsing SQL into XML.
     * @param estimates
     * @param suppressDebugOutput
     * @param maxTablesPerJoin
     * @param costModel The current cost model to evaluate plans with.
     * @param paramHints
     * @param joinOrder
     */
    public QueryPlanner(String sql,
                        String stmtName,
                        String procName,
                        Database catalogDb,
                        StatementPartitioning partitioning,
                        HSQLInterface HSQL,
                        DatabaseEstimates estimates,
                        boolean suppressDebugOutput,
                        AbstractCostModel costModel,
                        ScalarValueHints[] paramHints,
                        String joinOrder,
                        DeterminismMode detMode,
                        boolean isLargeQuery) {
        PLANNER_LOCK.lock();
        assert(sql != null);
        assert(stmtName != null);
        assert(procName != null);
        assert(HSQL != null);
        assert(catalogDb != null);
        assert(costModel != null);
        assert(detMode != null);

        m_sql = sql;
        m_stmtName = stmtName;
        m_procName = procName;
        m_HSQL = HSQL;
        m_db = catalogDb;
        m_estimates = estimates;
        m_partitioning = partitioning;
        m_costModel = costModel;
        m_paramHints = paramHints;
        m_joinOrder = joinOrder;
        m_detMode = detMode;
        m_isLargeQuery = isLargeQuery;
        m_planSelector = new PlanSelector(m_estimates, m_stmtName,
                m_procName, m_sql, m_costModel, m_paramHints, m_detMode,
                suppressDebugOutput);
        m_isUpsert = false;
    }

    @Override
    public void close() {
        PLANNER_LOCK.unlock();
    }

    /**
     * Parse a SQL literal statement into an unplanned, intermediate representation.
     * This is normally followed by a call to
     * {@link this#plan(AbstractCostModel, String, String, String, String, int, ScalarValueHints[]) },
     * but splitting these two affords an opportunity to check a cache for a plan matching
     * the auto-parameterized parsed statement.
     */
    public void parse() throws PlanningErrorException {
        // reset any error message
        m_recentErrorMsg = null;

        // Reset plan node ids to start at 1 for this plan
        AbstractPlanNode.resetPlanNodeIds();

        // determine the type of the query
        //
        // (Hmmm...  seems like this pre-processing of the SQL text
        // and subsequent placement of UPSERT_TAG should be pushed down
        // into getXMLCompiledStatement)
        m_sql = m_sql.trim();
        if (m_sql.length() > 6 && m_sql.substring(0,6).toUpperCase().startsWith("UPSERT")) { // ENG-7395
            m_isUpsert = true;
            m_sql = "INSERT" + m_sql.substring(6);
        }

        // use HSQLDB to get XML that describes the semantics of the statement
        // this is much easier to parse than SQL and is checked against the catalog
        try {
            ParameterizationInfo.resetCurrentParamIndex();

            m_xmlSQL = m_HSQL.getXMLCompiledStatement(m_sql);
            //* enable to debug */ System.out.println("DEBUG: HSQL parsed:" + m_xmlSQL);
        }
        catch (HSQLParseException e) {
            // XXXLOG probably want a real log message here
            throw new PlanningErrorException(e.getMessage());
        }

        if (m_xmlSQL.name.equals("migrate")) {
            final VoltXMLElement condition =
                    VoltXMLElementHelper.getFirstChild(VoltXMLElementHelper.getFirstChild(m_xmlSQL, "condition"),
                            "operation");
            final AbstractExpression expr = ExpressionUtil.from(condition);
            final Map<String, String> attributes = m_xmlSQL.attributes;
            assert attributes.size() == 1;
            final Table targetTable = m_db.getTables().get(attributes.get("table"));
            assert targetTable != null;
            final CatalogMap<TimeToLive> ttls = targetTable.getTimetolive();
            if (ttls.isEmpty()) {
                 throw new PlanningErrorException(String.format(
                         "%s: Cannot migrate from table %s because it does not have a TTL column",
                         m_sql, targetTable.getTypeName()));
            } else {
                final Column ttl = ttls.iterator().next().getTtlcolumn();
                final TupleValueExpression columnExpression = new TupleValueExpression(
                        targetTable.getTypeName(), ttl.getName(), ttl.getIndex());
                final Set<AbstractExpression> terminals = new HashSet<>();
                ExpressionUtil.collectTerminals(expr, terminals);
                if (! terminals.contains(columnExpression)) {
                    throw new PlanningErrorException(String.format(
                            "%s: Cannot migrate from table %s because the WHERE caluse does not contain TTL column %s",
                            m_sql, targetTable.getTypeName(), ttl.getName()));
                }
            }
        }

        if (m_isUpsert) {
            assert(m_xmlSQL.name.equalsIgnoreCase("INSERT"));
            // for AdHoc cache distinguish purpose which is based on the XML
            m_xmlSQL.attributes.put(UPSERT_TAG, "true");
        }

        m_planSelector.outputCompiledStatement(m_xmlSQL);
    }

    // Generate a Volt XML tree for a hypothetical SWAP TABLE statement.
    // This can take any form that makes it easy for the planner to turn
    // it into an AbstractParsedStmt and then an AbstractPlanNode (tree).
    private VoltXMLElement forgeVoltXMLForSwapTables(
            String theTable, String otherTable) {
        if (theTable.equalsIgnoreCase(otherTable)) {
            throw new PlanningErrorException("Can not swap table \"" + theTable + "\" with itself.");
        }
        VoltXMLElement result = new VoltXMLElement("swap");
        result.attributes.put("thetable", theTable.toUpperCase());
        result.attributes.put("othertable", otherTable.toUpperCase());
        return result;
    }

    /**
     * This method behaves similarly to parse(), but allows the caller to pass in XML
     * to avoid re-parsing SQL text that has already gone through HSQL.
     *
     * @param  xmlSql  XML produced by previous invocation of HSQL
     * */
    public void parseFromXml(VoltXMLElement xmlSQL) {
        m_recentErrorMsg = null;
        m_xmlSQL = xmlSQL;
        if (m_xmlSQL.attributes.containsKey(UPSERT_TAG)) {
            m_isUpsert = true;
        }

        m_planSelector.outputCompiledStatement(m_xmlSQL);
    }

    /**
     * Auto-parameterize all of the literals in the parsed SQL statement.
     *
     * @return An opaque token representing the parsed statement with (possibly) parameterization.
     */
    public String parameterize() {
        Set<Integer> paramIds = new HashSet<>();
        ParameterizationInfo.findUserParametersRecursively(m_xmlSQL, paramIds);
        m_adhocUserParamsCount = paramIds.size();

        m_paramzInfo = null;
        if (paramIds.size() == 0) {
            m_paramzInfo = ParameterizationInfo.parameterize(m_xmlSQL);
        }

        // skip plans with pre-existing parameters and plans that don't parameterize
        // assume a user knows how to cache/optimize these
        if (m_paramzInfo != null) {
            // if requested output the second version of the parsed plan
            m_planSelector.outputParameterizedCompiledStatement(m_paramzInfo.getParameterizedXmlSQL());
            return m_paramzInfo.getParameterizedXmlSQL().toMinString();
        }

        // fallback when parameterization is
        return m_xmlSQL.toMinString();
    }

    public String[] extractedParamLiteralValues() {
        if (m_paramzInfo == null) {
            return null;
        }
        return m_paramzInfo.getParamLiteralValues();
    }

    public ParameterSet extractedParamValues(VoltType[] parameterTypes) {
        if (m_paramzInfo == null) {
            return null;
        }
        return m_paramzInfo.extractedParamValues(parameterTypes);
    }

    /**
     * Get the best plan for the SQL statement given, assuming the given costModel.
     *
     * @return The best plan found for the SQL statement.
     * @throws PlanningErrorException on failure.
     */
    public CompiledPlan plan() throws PlanningErrorException {
        // reset any error message
        m_recentErrorMsg = null;

        // what's going to happen next:
        //  If a parameterized statement exists, try to make a plan with it
        //  On success return the plan.
        //  On failure, try the plan again without parameterization

        if (m_paramzInfo != null) {
            try {
                // compile the plan with new parameters
                CompiledPlan plan = compileFromXML(m_paramzInfo.getParameterizedXmlSQL(),
                                                   m_paramzInfo.getParamLiteralValues());
                if (plan != null) {
                    if (plan.extractParamValues(m_paramzInfo)) {
                        return plan;
                    }
                }
                else if (DEBUGGING_STATIC_MODE_TO_RETRY_ON_ERROR) {
                    compileFromXML(m_paramzInfo.getParameterizedXmlSQL(),
                                   m_paramzInfo.getParamLiteralValues());
                }
                // fall through to try replan without parameterization.
            }
            catch (Exception | StackOverflowError e) {
                // ignore any errors planning with parameters
                // fall through to re-planning without them
                m_hasExceptionWhenParameterized = true;

                // note, expect real planning errors ignored here to be thrown again below
                m_recentErrorMsg = null;
                m_partitioning.resetAnalysisState();
            }
        }

        // if parameterization isn't requested or if it failed, plan here
        CompiledPlan plan = compileFromXML(m_xmlSQL, null);
        if (plan == null) {
            if (DEBUGGING_STATIC_MODE_TO_RETRY_ON_ERROR) {
                plan = compileFromXML(m_xmlSQL, null);
            }
            throw new PlanningErrorException(m_recentErrorMsg);
        }

        return plan;
    }

    /**
     * @return Was this statement planned with auto-parameterization?
     */
    public boolean compiledAsParameterizedPlan() {
        return m_paramzInfo != null;
    }

    public int getAdhocUserParamsCount() {
        return m_adhocUserParamsCount;
    }

    public boolean wasBadPameterized() {
        return m_hasExceptionWhenParameterized;
    }

    /**
     * Find the best plan given the VoltXMLElement.  By best here we mean the plan
     * which is scored the best according to our plan metric scoring.  The plan
     * metric scoring takes into account join order and index use, but it does
     * not take into account the output schema.  Consequently, we don't compute the
     * output schema for the plan nodes until after the best plan is discovered.
     *
     * The order here is:
     * <ol>
     * <li>
     *   Parse the VoltXMLElement to create an AbstractParsedStatement.  This has
     *   a second effect of loading lists of join orders and access paths for planning.
     *   For us, an access path is a way of scanning something scannable.  It's a generalization
     *   of the notion of scanning a table or an index.
     * </li>
     * <li>
     *   Create a PlanAssembler, and ask it for the best cost plan.  This uses the
     *   side data created by the parser in the previous step.
     * </li>
     * <li>
     *   If the plan is read only, slap a SendPlanNode on the front.  Presumably
     *   an insert, delete or upsert will have added the SendPlanNode into the plan node tree already.
     * </li>
     * <li>
     *   Compute the output schema.  This computes the output schema for each
     *   node recursively, using a node specific method.
     * </li>
     * <li>
     *   Resolve the column indices.  This makes sure that the indices of all
     *   TVEs in the output columns refer to the right input columns.
     * </li>
     * <li>
     *   Do some final cleaning up and verifying of the plan.  For example,
     *   We renumber the nodes starting at 1.
     * </li>
     * </ol>
     *
     * @param xmlSQL
     * @param paramValues
     * @return
     */
    private CompiledPlan compileFromXML(VoltXMLElement xmlSQL, String[] paramValues) {
        // Get a parsed statement from the xml
        // The callers of compilePlan are ready to catch any exceptions thrown here.
        // Simple constant expressions (i.e. "1 + 1" or "(2 * 4 + 2)/3") are evaluated and substituted by HSQL;
        // but expressions with functions (i.e. "cast(power(2, 3) to int)" are not.
        AbstractParsedStmt parsedStmt = AbstractParsedStmt.parse(null, m_sql, xmlSQL, paramValues, m_db, m_joinOrder);
        if (parsedStmt == null) {
            m_recentErrorMsg = "Failed to parse SQL statement: " + getOriginalSql();
            return null;
        }

        if (m_isUpsert) {
            // no insert/upsert with joins
            if (parsedStmt.m_tableList.size() != 1) {
                m_recentErrorMsg = "UPSERT is supported only with one single table: " + getOriginalSql();
                return null;
            }

            Table tb = parsedStmt.m_tableList.get(0);
            Constraint pkey = null;
            for (Constraint ct: tb.getConstraints()) {
                if (ct.getType() == ConstraintType.PRIMARY_KEY.getValue()) {
                    pkey = ct;
                    break;
                }
            }

            if (pkey == null) {
                m_recentErrorMsg = "Unsupported UPSERT table without primary key: " + getOriginalSql();
                return null;
            }
        }
        if(parsedStmt instanceof ParsedSelectStmt || parsedStmt instanceof ParsedUnionStmt) {
            final MVQueryRewriter rewriter;
            if (parsedStmt instanceof ParsedSelectStmt) {
                rewriter = new MVQueryRewriter((ParsedSelectStmt) parsedStmt);
            } else {
                rewriter = new MVQueryRewriter((ParsedUnionStmt) parsedStmt);
            }
            if (rewriter.rewrite() && m_paramzInfo != null) { // if query is rewritten the #parameters is likely reduced.
                m_paramzInfo.rewrite();
            }
        }
        m_planSelector.outputParsedStatement(parsedStmt);

        if (m_isLargeQuery) {
            if (parsedStmt.isDML()
                    || (parsedStmt instanceof ParsedSelectStmt
                            && ((ParsedSelectStmt)parsedStmt).hasWindowFunctionExpression())) {
                m_isLargeQuery = false;
            }
        }

        // Init Assembler. Each plan assembler requires a new instance of the PlanSelector
        // to keep track of the best plan
        PlanAssembler assembler = new PlanAssembler(m_db, m_partitioning,
                (PlanSelector) m_planSelector.clone(), m_isLargeQuery);
        // find the plan with minimal cost
        CompiledPlan bestPlan = assembler.getBestCostPlan(parsedStmt);

        // This processing of bestPlan outside/after getBestCostPlan
        // allows getBestCostPlan to be called both here and
        // in PlanAssembler.getNextUnion on each branch of a union.

        // make sure we got a winner
        if (bestPlan == null) {
            if (DEBUGGING_STATIC_MODE_TO_RETRY_ON_ERROR) {
                assembler.getBestCostPlan(parsedStmt);
            }
            m_recentErrorMsg = assembler.getErrorMessage();
            if (m_recentErrorMsg == null) {
                m_recentErrorMsg = "Unable to plan for statement. Error unknown.";
            }
            return null;
        }

        // Calculate the UDF dependences.
        Collection<String> dependees = parsedStmt.calculateUDFDependees();
        if (dependees != null) {
            bestPlan.getUDFDependees().addAll(dependees);
        }

        if (bestPlan.isReadOnly()) {
            SendPlanNode sendNode = new SendPlanNode();
            // connect the nodes to build the graph
            sendNode.addAndLinkChild(bestPlan.rootPlanGraph);
            // this plan is final, generate schema and resolve all the column index references
            bestPlan.rootPlanGraph = sendNode;
        }

        // Execute the generateOutputSchema and resolveColumnIndexes once for the best plan
        bestPlan.rootPlanGraph.generateOutputSchema(m_db);
        // Make sure the schemas for base and recursive plans in common table scans
        // have identical schemas.
        harmonizeCommonTableSchemas(bestPlan);
        bestPlan.rootPlanGraph.resolveColumnIndexes();
        // Now that the plan is all together we
        // can compute the best selection microoptimizations.
        MicroOptimizationRunner.applyAll(bestPlan,
                                         parsedStmt,
                                         MicroOptimizationRunner.Phases.AFTER_COMPLETE_PLAN_ASSEMBLY);
        if (parsedStmt instanceof ParsedSelectStmt) {
            ((ParsedSelectStmt)parsedStmt).checkPlanColumnMatch(bestPlan.rootPlanGraph.getOutputSchema());
        }

        // reset all the plan node ids for a given plan
        // this makes the ids deterministic
        bestPlan.resetPlanNodeIds(1);

        // Output the best plan debug info
        assembler.finalizeBestCostPlan();

        // split up the plan everywhere we see send/receive into multiple plan fragments
        List<AbstractPlanNode> receives = bestPlan.rootPlanGraph.findAllNodesOfClass(AbstractReceivePlanNode.class);
        if (receives.size() > 1) {
            // Have too many receive node for two fragment plan limit
            m_recentErrorMsg = "This join of multiple partitioned tables is too complex. "
                    + "Consider simplifying its subqueries: " + getOriginalSql();
            return null;
        }

        /*/ enable for debug ...
        if (receives.size() > 1) {
            System.out.println(plan.rootPlanGraph.toExplainPlanString());
        }
        // ... enable for debug */
        if (receives.size() == 1) {
            AbstractReceivePlanNode recvNode = (AbstractReceivePlanNode) receives.get(0);
            fragmentize(bestPlan, recvNode);
        }

        return bestPlan;
    }

    /**
     * Make sure that schemas in base and recursive plans
     * in common table scans have identical schemas.  This
     * is important because otherwise we will get data
     * corruption in the EE.  We look for SeqScanPlanNodes,
     * then look for a common table scan, and ask the scan
     * node to harmonize its schemas.
     *
     * @param plan
     */
    private void harmonizeCommonTableSchemas(CompiledPlan plan) {
        List<AbstractPlanNode> seqScanNodes = plan.rootPlanGraph.findAllNodesOfClass(SeqScanPlanNode.class);
        for (AbstractPlanNode planNode : seqScanNodes) {
            SeqScanPlanNode seqScanNode = (SeqScanPlanNode)planNode;
            StmtCommonTableScan scan = seqScanNode.getCommonTableScan();
            if (scan != null) {
                scan.harmonizeOutputSchema();
            }
        }
    }

    private static void fragmentize(CompiledPlan plan, AbstractReceivePlanNode recvNode) {
        assert(recvNode.getChildCount() == 1);
        AbstractPlanNode childNode = recvNode.getChild(0);
        assert(childNode instanceof SendPlanNode);
        SendPlanNode sendNode = (SendPlanNode) childNode;

        // disconnect the send and receive nodes
        sendNode.clearParents();
        recvNode.clearChildren();

        plan.subPlanGraph = sendNode;
        return;
    }

    private String getOriginalSql() {
        if (! m_isUpsert) {
            return m_sql;
        }

        return "UPSERT" + m_sql.substring(6);
    }

    /**
     * Fast track the parsing for the pseudo-statement generated by the
     * {@literal @}SwapTables system stored procedure.
     * <p>
     * If this functionality turns out to fall within the behavior of a
     * more general SWAP TABLE statement supported in a (future) SQL
     * parser, the system stored procedure can fall back to using it.
     * Extending the HSQL parser NOW to support a "SWAP TABLE" statement
     * JUST for this special case, but keeping that support disabled for
     * the normal {@literal @}AdHoc and compiled stored procedures code paths would
     * be overkill at this point.
     * So we settle for this early return of a "forged" parser result.
     * <p>
     * Note that we don't allow this from an {@literal @}AdHoc compilation.  See
     * ENG-12368.
     */
    public void planSwapTables() {
        // reset any error message
        m_recentErrorMsg = null;

        // Reset plan node ids to start at 1 for this plan
        AbstractPlanNode.resetPlanNodeIds();

        assert(m_sql.startsWith("@SwapTables "));
        String[] swapTableArgs = m_sql.split(" ");
        m_xmlSQL = forgeVoltXMLForSwapTables(
                swapTableArgs[1], swapTableArgs[2]);
        m_planSelector.outputCompiledStatement(m_xmlSQL);
    }
}
