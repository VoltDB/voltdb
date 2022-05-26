/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.planner;

import java.net.URL;
import java.util.*;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.hsqldb_voltpatches.HSQLInterface.HSQLParseException;
import org.hsqldb_voltpatches.VoltXMLElement;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Database;
import org.voltdb.compiler.DeterminismMode;
import org.voltdb.exceptions.PlanningErrorException;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.NestLoopPlanNode;
import org.voltdb.plannodes.OrderByPlanNode;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.JoinType;
import org.voltdb.types.PlanMatcher;
import org.voltdb.types.PlanNodeType;
import org.voltdb.types.PlannerType;
import org.voltdb.types.SortDirectionType;

import junit.framework.TestCase;

public class PlannerTestCase extends TestCase {

    protected PlannerTestAideDeCamp m_aide;
    private boolean m_byDefaultInferPartitioning = true;
    private boolean m_byDefaultPlanForSinglePartition;
    final private int m_defaultParamCount = 0;
    private String m_noJoinOrder = null;
    private PlannerType m_plannerType = PlannerType.VOLTDB;

    public PlannerTestCase() {
        super();
    }

    PlannerTestCase(PlannerType plannerType) {
        super("PlannerTestCase");
        m_plannerType = plannerType;
    }
    /**
     * @param sql
     * @return
     */
    private int countQuestionMarks(String sql) {
        int paramCount = 0;
        int skip = 0;
        while (true) {
            // Yes, we ARE assuming that test queries don't contain quoted question marks.
            skip = sql.indexOf('?', skip);
            if (skip == -1) {
                break;
            }
            skip++;
            paramCount++;
        }
        return paramCount;
    }

    protected void failToCompile(String sql, String... patterns) {
        int paramCount = countQuestionMarks(sql);
        try {
            List<AbstractPlanNode> unexpected = m_aide.compile(m_plannerType, sql, paramCount,
                    m_byDefaultInferPartitioning, m_byDefaultPlanForSinglePartition, null);
            printExplainPlan(unexpected);
            fail("Expected planner failure, but found success.");
        }
        catch (Exception ex) {
            String result = ex.toString();
            for (String pattern : patterns) {
                if ( ! result.contains(pattern)) {
                    fail("Did not find pattern '" + pattern + "' in error string '" + result + "'");
                }
            }
        }
    }

    protected CompiledPlan compileAdHocPlan(String sql) {
        return compileAdHocPlan(sql, DeterminismMode.SAFER);
    }

    protected CompiledPlan compileAdHocPlan(String sql, DeterminismMode detMode) {
        CompiledPlan cp = null;
        try {
            cp = m_aide.compileAdHocPlan(m_plannerType, sql, detMode);
            assertNotNull(cp);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail(ex.getMessage());
        }
        return cp;
    }

    /**
     * Fetch compiled planned based on provided partitioning information.
     * @param sql: SQL statement
     * @param inferPartitioning: Flag to indicate whether to use infer or forced partitioning
     *                           when generating plan. True to use infer partitioning info,
     *                           false for forced partitioning
     * @param forcedSP: Flag to indicate whether to generate plan for forced SP or MP.
     *                  If inferPartitioing flag is set to true, this flag is ignored
     * @param detMode: Specifies determinism mode - Faster or Safer
     * @return: Compiled plan based on specified input parameters
     */

    protected CompiledPlan compileAdHocPlan(String sql,
                                            boolean inferPartitioning,
                                            boolean forcedSP,
                                            DeterminismMode detMode) {
        CompiledPlan cp =
                m_aide.compileAdHocPlan(m_plannerType, sql, inferPartitioning, forcedSP, detMode);
        assertNotNull("CompiledPlan is NULL", cp);
        return cp;
    }

    /**
     * This is exactly like compileAdHocPlan, but it may throw an
     * error.  We added this because we sometimes call this from a
     * planner test just to find out if a string compiles.  We
     * need to know more about failures than just that they failed.
     * We need to know why the failed so we can check the error
     * messages.
     *
     * @param sql
     * @return
     */
    protected CompiledPlan compileAdHocPlanThrowing(String sql,
                                                    boolean inferPartitioning,
                                                    boolean forcedSP,
                                                    DeterminismMode detMode) {
        CompiledPlan cp = compileAdHocPlan(sql, inferPartitioning, forcedSP, detMode);
        assertNotNull("CompiledPlan is NULL", cp);
        return cp;
    }

    protected List<AbstractPlanNode> compileInvalidToFragments(String sql) {
        boolean planForSinglePartitionFalse = false;
        return compileWithJoinOrderToFragments(sql, m_defaultParamCount,
                planForSinglePartitionFalse, m_noJoinOrder);
    }

    /** A helper here where the junit test can assert success */
    protected List<AbstractPlanNode> compileToFragments(String sql) {
        return compileToFragments(sql, false);
    }

    protected List<AbstractPlanNode> compileToFragments(String sql, boolean planForSinglePartition) {
        return compileWithJoinOrderToFragments(sql, planForSinglePartition, m_noJoinOrder);
    }

    protected List<AbstractPlanNode> compileToFragmentsForSinglePartition(String sql) {
        boolean planForSinglePartitionFalse = false;
        return compileWithJoinOrderToFragments(sql, planForSinglePartitionFalse, m_noJoinOrder);
    }


    /** A helper here where the junit test can assert success */
    protected List<AbstractPlanNode> compileWithJoinOrderToFragments(String sql, String joinOrder) {
        boolean planForSinglePartitionFalse = false;
        return compileWithJoinOrderToFragments(sql, planForSinglePartitionFalse, joinOrder);
    }

    /** A helper here where the junit test can assert success */
    private List<AbstractPlanNode> compileWithJoinOrderToFragments(String sql,
                                                                   boolean planForSinglePartition,
                                                                   String joinOrder) {
        try {
            // Yes, we ARE assuming that test queries don't contain quoted question marks.
            int paramCount = StringUtils.countMatches(sql, "?");
            return compileWithJoinOrderToFragments(sql, paramCount, planForSinglePartition, joinOrder);
        }
        catch (PlanningErrorException pe) {
            fail("Query: '" + sql + "' threw " + pe);
            return null; // dead code.
        }
    }

    /** A helper here where the junit test can assert success */
    private List<AbstractPlanNode> compileWithJoinOrderToFragments(String sql, int paramCount,
                                                                   boolean planForSinglePartition,
                                                                   String joinOrder) {
        //* enable to debug */ System.out.println("DEBUG: compileWithJoinOrderToFragments(\"" + sql + "\", " + planForSinglePartition + ", \"" + joinOrder + "\")");
        List<AbstractPlanNode> pn = m_aide.compile(m_plannerType, sql, paramCount, m_byDefaultInferPartitioning, m_byDefaultPlanForSinglePartition, joinOrder);
        assertTrue(pn != null);
        assertFalse(pn.isEmpty());
        assertTrue(pn.get(0) != null);
        if (planForSinglePartition) {
            if (pn.size() != 1) {
                System.err.printf("Plan: %s\n", pn);
                System.err.printf("Error: pn.size == %d, should be 1\n", pn.size());
            }
            assertTrue(pn.size() == 1);
        }
        return pn;
    }

    protected AbstractPlanNode compileSPWithJoinOrder(String sql, String joinOrder) {
        try {
            return compileWithCountedParamsAndJoinOrder(sql, joinOrder);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail();
            return null;
        }
    }

    protected void compileWithInvalidJoinOrder(String sql, String joinOrder) throws Exception {
        compileWithJoinOrderToFragments(sql, m_defaultParamCount, m_byDefaultPlanForSinglePartition, joinOrder);
    }


    private AbstractPlanNode compileWithCountedParamsAndJoinOrder(String sql,
                                                                  String joinOrder) throws Exception {
        // Yes, we ARE assuming that test queries don't contain quoted question marks.
        int paramCount = StringUtils.countMatches(sql, "?");
        return compileSPWithJoinOrder(sql, paramCount, joinOrder);
    }

    /**
     * Assert that the plan for a statement produces a plan that meets some
     * basic expectations.
     * @param sql a statement to plan
     *            as if for a single-partition stored procedure
     * @param nOutputColumns the expected number of plan result columns,
     *                       because of the planner's history of such errors
     * @param nodeTypes the expected node types of the resulting plan tree
     *                  listed in top-down order with wildcard support.
     *                  See assertTopDownTree.
     * @return the plan for more detailed testing.
     */
    protected AbstractPlanNode compileToTopDownTree(String sql,
            int nOutputColumns, PlanNodeType... nodeTypes) {
        return compileToTopDownTree(sql, nOutputColumns, false, nodeTypes);
    }

    protected AbstractPlanNode compileToTopDownTree(String sql,
            int nOutputColumns, boolean hasOptionalSelectProjection, PlanNodeType... nodeTypes) {
        // Yes, we ARE assuming that test queries don't
        // contain quoted question marks.
        int paramCount = StringUtils.countMatches(sql, "?");
        AbstractPlanNode result = compileSPWithJoinOrder(sql, paramCount, null);
        assertEquals(nOutputColumns, result.getOutputSchema().size());
        assertTopDownTree(result, hasOptionalSelectProjection, nodeTypes);
        return result;
    }

    /** A helper here where the junit test can assert success */
    protected AbstractPlanNode compile(String sql) {
        // Yes, we ARE assuming that test queries don't contain quoted question marks.
        int paramCount = StringUtils.countMatches(sql, "?");
        return compileSPWithJoinOrder(sql, paramCount, null);
    }

    /** A helper here where the junit test can assert success */
    protected AbstractPlanNode compileForSinglePartition(String sql) {
        // Yes, we ARE assuming that test queries don't contain quoted question marks.
        int paramCount = StringUtils.countMatches(sql, "?");
        boolean m_infer = m_byDefaultInferPartitioning;
        boolean m_forceSP = m_byDefaultInferPartitioning;
        m_byDefaultInferPartitioning = false;
        m_byDefaultPlanForSinglePartition = true;

        AbstractPlanNode pn = compileSPWithJoinOrder(sql, paramCount, null);
        m_byDefaultInferPartitioning = m_infer;
        m_byDefaultPlanForSinglePartition = m_forceSP;
        return pn;
    }

    /** A helper here where the junit test can assert success */
    protected AbstractPlanNode compileSPWithJoinOrder(String sql,
                                                      int paramCount,
                                                      String joinOrder) {
        List<AbstractPlanNode> pns = null;
        try {
            pns = compileWithJoinOrderToFragments(sql, paramCount, m_byDefaultPlanForSinglePartition, joinOrder);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail(ex.getMessage());
        }
        assertTrue(pns.get(0) != null);
        return pns.get(0);
    }

    /**
     *  Find all the aggregate nodes in a fragment, whether they are hash, serial or partial.
     * @param fragment     Fragment to search for aggregate plan nodes
     * @return a list of all the nodes we found
     */
    protected static List<AbstractPlanNode> findAllAggPlanNodes(AbstractPlanNode fragment) {
        List<AbstractPlanNode> aggNodes = fragment.findAllNodesOfType(PlanNodeType.AGGREGATE);
        List<AbstractPlanNode> hashAggNodes = fragment.findAllNodesOfType(PlanNodeType.HASHAGGREGATE);
        List<AbstractPlanNode> partialAggNodes = fragment.findAllNodesOfType(PlanNodeType.PARTIALAGGREGATE);

        aggNodes.addAll(hashAggNodes);
        aggNodes.addAll(partialAggNodes);
        return aggNodes;
    }


    protected VoltXMLElement compileToXML(String SQL) throws HSQLParseException {
        return m_aide.compileToXML(SQL);
    }

    protected void setupSchema(URL ddlURL, String basename,
                               boolean planForSinglePartition) throws Exception {
        m_aide = new PlannerTestAideDeCamp(ddlURL, basename);
        m_byDefaultPlanForSinglePartition = planForSinglePartition;
    }

    protected void setupSchema(boolean inferPartitioning, URL ddlURL,
                               String basename) throws Exception {
        m_byDefaultInferPartitioning = inferPartitioning;
        m_aide = new PlannerTestAideDeCamp(ddlURL, basename);
    }

    public String getCatalogString() {
        return m_aide.getCatalogString();
    }

    public Catalog getCatalog() {
        return m_aide.getCatalog();
    }

    protected Database getDatabase() {
        return m_aide.getDatabase();
    }

    protected void printExplainPlan(List<AbstractPlanNode> planNodes) {
        for (AbstractPlanNode apn: planNodes) {
            System.out.println(apn.toExplainPlanString());
        }
    }

    protected String buildExplainPlan(List<AbstractPlanNode> planNodes) {
        String explain = "";
        for (AbstractPlanNode apn: planNodes) {
            explain += apn.toExplainPlanString() + '\n';
        }
        return explain;
    }

    protected void checkQueriesPlansAreDifferent(String sql1, String sql2, String msg) {
        assertFalse(msg, areQueryPlansEqual(sql1, sql2));
    }

    protected void checkQueriesPlansAreTheSame(String sql1, String sql2) {
        assertEquals(buildExplainPlan(compileToFragments(sql1)), buildExplainPlan(compileToFragments(sql2)));
    }

    private boolean areQueryPlansEqual(String sql1, String sql2) {
        return buildExplainPlan(compileToFragments(sql1))
                .equals(buildExplainPlan(compileToFragments(sql2)));
    }


    /**
     * Call this function to verify that an order by plan node has the
     * sort expressions and directions we expect.
     *
     * @param orderByPlanNode The plan node to test.
     * @param columnDescrs Pairs of expressions and sort directions. There
     *                     must be an even number of these, the even
     *                     numbered ones must be expressions and the odd
     *                     numbered ones must be sort directions.  This is
     *                     numbering starting at 0.  So, they must be in
     *                     the order expr, direction, expr, direction, and
     *                     so forth.
     */
    protected void verifyOrderByPlanNode(OrderByPlanNode  orderByPlanNode,
                                         Object       ... columnDescrs) {
        // We should have an even number of columns
        assertEquals(0, columnDescrs.length % 2);
        List<AbstractExpression> exprs = orderByPlanNode.getSortExpressions();
        List<SortDirectionType>  dirs  = orderByPlanNode.getSortDirections();
        assertEquals(exprs.size(), dirs.size());
        assertEquals(columnDescrs.length/2, exprs.size());
        for (int idx = 0; idx < exprs.size(); ++idx) {
            // Assert that an expected one-part name matches a tve by column name
            // and an expected two-part name matches a tve by table and column name.
            AbstractExpression expr = exprs.get(idx);
            assertTrue(expr instanceof TupleValueExpression);
            TupleValueExpression tve = (TupleValueExpression)expr;
            String expectedNames[] = ((String)columnDescrs[2*idx]).split("\\.");
            String columnName = null;
            int nParts = expectedNames.length;
            if (nParts > 1) {
                assertEquals(2, nParts);
                String tableName = expectedNames[0].toUpperCase();
                assertEquals(tableName, tve.getTableName().toUpperCase());
            }
            // In either case, the column name must match the LAST part.
            columnName = expectedNames[nParts-1].toUpperCase();
            assertEquals(columnName, tve.getColumnName().toUpperCase());

            SortDirectionType dir = dirs.get(idx);
            assertEquals(columnDescrs[2*idx+1], dir);
        }
    }

    /**
     * Assert that a plan's left-most branch is made up of plan nodes of
     * specified classes.
     * @param expectedClasses a list of expected AbstractPlanNode classes
     * @param actualPlan the top of a plan node tree expected to have instances
     *                   of the expected classes along its left-most branch
     *                   listed from top to bottom.
     */
    static protected void assertClassesMatchNodeChain(
            List<Class<? extends AbstractPlanNode>> expectedClasses,
            AbstractPlanNode actualPlan) {
        AbstractPlanNode pn = actualPlan;
        for (Class<? extends AbstractPlanNode> c : expectedClasses) {
            assertFalse("The actual plan is shallower than expected",
                    pn == null);
            assertTrue("Expected plan to contain an instance of " + c.getSimpleName() +", "
                    + "instead found " + pn.getClass().getSimpleName(),
                    c.isInstance(pn));
            if (pn.getChildCount() > 0) {
                pn = pn.getChild(0);
            }
            else {
                pn = null;
            }
        }

        assertTrue("Actual plan longer than expected", pn == null);
    }

    /**
     * Find a specific node in a plan tree following the left-most path,
     * (child[0]), and asserting the expected class of each plan node along the
     * way, inclusive of the start and end.
     * @param expectedClasses a list of expected AbstractPlanNode classes
     * @param actualPlan the top of a plan node tree expected to have instances
     *                   of the expected classes along its left-most branch
     *                   listed in top-down order.
     * @return the child node matching the last expected class in the list.
     *                   It need not be a leaf node.
     */
    protected static AbstractPlanNode followAssertedLeftChain(
            AbstractPlanNode start,
            PlanNodeType startType, PlanNodeType... nodeTypes) {
        return followAssertedLeftChain(start, false, startType, nodeTypes);
    }

    protected static AbstractPlanNode followAssertedLeftChain(
            AbstractPlanNode start,
            boolean hasOptionalProjection,
            PlanNodeType startType,
            PlanNodeType... nodeTypes)
    {
        AbstractPlanNode result = start;
        assertEquals(startType, result.getPlanNodeType());
        for (int nodeNumber = 0; nodeNumber < nodeTypes.length; nodeNumber += 1) {
            PlanNodeType type = nodeTypes[nodeNumber];
            assertTrue(result.getChildCount() > 0);
            result = result.getChild(0);
            if (hasOptionalProjection
                    && (nodeNumber == 0)
                    && (result.getPlanNodeType() == PlanNodeType.PROJECTION)) {
                nodeNumber -= 1;
            } else {
                assertEquals(type, result.getPlanNodeType());
            }
        }
        return result;
    }

    /**
     * Assert that a plan's left-most branch is made up of plan nodes of
     * expected classes.
     * @param expectedClasses a list of expected AbstractPlanNode classes
     * @param actualPlan the top of a plan node tree expected to have instances
     *                   of the expected classes along its left-most branch
     *                   listed from top to bottom.
     */
    protected static void assertLeftChain(
            AbstractPlanNode start, PlanNodeType... nodeTypes) {
        AbstractPlanNode pn = start;
        for (PlanNodeType type : nodeTypes) {
            assertFalse("Child node(s) are missing from the actual plan chain.",
                    pn == null);
            if ( ! type.equals(pn.getPlanNodeType())) {
                fail("Expecting plan node of type " + type + ", " +
                        "instead found " + pn.getPlanNodeType() + ".");
            }
            pn = (pn.getChildCount() > 0) ? pn.getChild(0) : null;
        }
        assertTrue("Actual plan chain was longer than expected",
                pn == null);
    }

    /**
     * Assert that a two-fragment plan's coordinator fragment does a simple
     * projection.  Sometimes the projection is optimized away, and
     * that's ok.
     **/
    protected static void assertProjectingCoordinator(
            List<AbstractPlanNode> lpn) {
        AbstractPlanNode pn;
        pn = lpn.get(0);
        assertTopDownTreeWithOptionalSelectProjection(pn, PlanNodeType.SEND,
                PlanNodeType.RECEIVE);
    }

    /**
     * Assert that a two-fragment plan's coordinator fragment does a left join
     * with a specific replicated table on its outer side.
     **/
    protected static void assertReplicatedLeftJoinCoordinator(
            List<AbstractPlanNode> lpn, String replicatedTable) {
        AbstractPlanNode pn;
        AbstractPlanNode node;
        NestLoopPlanNode nlj;
        SeqScanPlanNode seqScan;
        pn = lpn.get(0);
        assertTopDownTree(pn,
                true,
                PlanNodeType.SEND,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.RECEIVE);
        node = followAssertedLeftChain(pn, true,
                PlanNodeType.SEND,
                PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        assertEquals(JoinType.LEFT, nlj.getJoinType());
        assertEquals(2, nlj.getChildCount());
        seqScan = (SeqScanPlanNode) nlj.getChild(0);
        assertEquals(replicatedTable, seqScan.getTargetTableName().toUpperCase());
    }

    // Print a tree of plan nodes by type.
    protected void printPlanNodes(AbstractPlanNode root, int fragmentNumber, int numberOfFragments) {
        System.out.printf("  Plan for fragment %d of %d\n",
                          fragmentNumber,
                          numberOfFragments);
        String lines[] = root.toExplainPlanString().split("\n");
        System.out.printf("    Explain:\n");
        for (String line : lines) {
            System.out.printf("      %s\n", line);
        }
        System.out.printf("    Nodes:\n");
        for (;root != null;
                root = (root.getChildCount() > 0) ? root.getChild(0) : null) {
            System.out.printf("      Node type %s\n", root.getPlanNodeType());
            for (int idx = 1; idx < root.getChildCount(); idx += 1) {
                System.out.printf("        Child %d: %s\n", idx, root.getChild(idx).getPlanNodeType());
            }
            for (Entry<PlanNodeType, AbstractPlanNode> entry : root.getInlinePlanNodes().entrySet()) {
                System.out.printf("        Inline %s\n", entry.getKey());
            }
            for (Entry<PlanNodeType, AbstractPlanNode> entry : root.getInlinePlanNodes().entrySet()) {
                System.out.printf("        Inline %s\n", entry.getKey());
            }
        }
    }

    /**
     * Assert that an expression tree contains the expected types of expressions
     * in the order listed, assuming a top-down left-to-right depth-first
     * traversal through left, right, and args children.
     * A null expression type in the list will match any expression
     * node or tree at the corresponding position.
     **/
    protected static void assertExprTopDownTree(AbstractExpression start,
            ExpressionType... exprTypes) {
        assertNotNull(start);
        Stack<AbstractExpression> stack = new Stack<>();
        stack.push(start);
        for (ExpressionType type : exprTypes) {
            // Process each node before its children or later siblings.
            AbstractExpression parent;
            try {
                parent = stack.pop();
            }
            catch (EmptyStackException ese) {
                fail("No expression was found in the tree to match type " + type);
                return; // This dead code hushes warnings.
            }
            List<AbstractExpression> args = parent.getArgs();
            AbstractExpression rightExpr = parent.getRight();
            AbstractExpression leftExpr = parent.getLeft();
            int argCount = (args == null) ? 0 : args.size();
            int childCount = argCount +
                    (rightExpr == null ? 0 : 1) +
                    (leftExpr == null ? 0 : 1);
            if (type == null) {
                // A null type wildcard matches any child TREE or NODE.
                System.out.println("DEBUG: Suggestion -- expect " +
                        parent.getExpressionType() +
                        " with " + childCount + " direct children.");
                continue;
            }
            assertEquals(type, parent.getExpressionType());
            // Iterate from the last child to the first.
            while (argCount > 0) {
                // Push each child to be processed before its parent's
                // or its own later siblings (already pushed).
                stack.push(parent.getArgs().get(--argCount));
            }
            if (rightExpr != null) {
                stack.push(rightExpr);
            }
            if (leftExpr != null) {
                stack.push(leftExpr);
            }
        }
        assertTrue("Extra expression node(s) (" + stack.size() +
                ") were found in the tree with no expression type to match",
                stack.isEmpty());
    }

    /**
     * Assert that a plan node tree contains the expected types of plan nodes
     * in the order listed, assuming a top-down left-to-right depth-first
     * traversal through the child vector. A null plan node type in the list
     * will match any plan node or subtree at the corresponding position.
     **/
    protected static void assertTopDownTree(AbstractPlanNode start,
            PlanNodeType... nodeTypes) {
        assertTopDownTree(start, false, nodeTypes);
    }

    protected static void assertTopDownTreeWithOptionalSelectProjection(AbstractPlanNode start,
            PlanNodeType... nodeTypes) {
        assertTopDownTree(start, true, nodeTypes);
    }

    protected static void assertTopDownTree(AbstractPlanNode start,
                                            boolean hasOptionalProjection,
                                            PlanNodeType ... nodeTypes) {
        Stack<AbstractPlanNode> stack = new Stack<>();
        stack.push(start);

        for (int nodeNumber = 0; nodeNumber < nodeTypes.length; nodeNumber += 1) {
            PlanNodeType type = nodeTypes[nodeNumber];
            // Process each node before its children or later siblings.
            AbstractPlanNode parent;
            try {
                parent = stack.pop();
            }
            catch (EmptyStackException ese) {
                fail("No node was found in the tree to match node type " + type);
                return; // This dead code hushes warnings.
            }
            int childCount = parent.getChildCount();
            if (type == null) {
                // A null type wildcard matches any child TREE or NODE.
                System.out.println("DEBUG: Suggestion -- expect " +
                        parent.getPlanNodeType() +
                        " with " + childCount + " direct children.");
                continue;
            }
            // If we have specified that this has an optional
            // projection node, then we may find a projection
            // node at node number 1.  This is from the select
            // list projection, but we may have optimized it away.
            if (nodeNumber == 1 && hasOptionalProjection) {
                if (PlanNodeType.PROJECTION == parent.getPlanNodeType()) {
                    assertEquals(1, parent.getChildCount());
                    // We don't actually want to use up this
                    // type from nodeTypes.  So undo the increment
                    // we are about to do.
                    nodeNumber -= 1;
                    stack.push(parent.getChild(0));
                    continue;
                }
            }
            assertEquals(type, parent.getPlanNodeType());
            // Iterate from the last child to the first.
            while (childCount > 0) {
                // Push each child to be processed before its parent's
                // or its own later (already pushed) siblings.
                stack.push(parent.getChild(--childCount));
            }
        }
        assertTrue("Extra plan node(s) (" + stack.size() +
                ") were found in the tree with no node type to match",
                stack.isEmpty());
    }

    private String planNodeListString(List<PlanNodeType> list) {
        StringBuilder buf = new StringBuilder();
        String sep = "";
        for (PlanNodeType pnt : list) {
            buf.append(sep)
               .append(pnt.name());
            sep = ", ";
        }
        return buf.toString();
    }

    protected static class PlanWithInlineNodes implements PlanMatcher {
        PlanNodeType m_type = null;

        List<PlanNodeType> m_branches = new ArrayList<>();
        public PlanWithInlineNodes(PlanNodeType mainType, PlanNodeType ... nodes) {
            m_type = mainType;
            for (PlanNodeType node : nodes) {
                m_branches.add(node);
            }
        }

        @Override
        public String match(AbstractPlanNode node) {
            PlanNodeType mainNodeType = node.getPlanNodeType();
            if (m_type != mainNodeType) {
                return String.format("PlanWithInlineNode: expected main plan node type %s, got %s "
                                     + "at plan node id %d.",
                                     m_type, mainNodeType,
                                     node.getPlanNodeId());
            }
            for (PlanNodeType nodeType : m_branches) {
                AbstractPlanNode inlineNode = node.getInlinePlanNode(nodeType);
                if (inlineNode == null) {
                    return String.format("Expected inline node type %s but didn't find it "
                                         + "at plan node id %d.",
                                         nodeType.name(),
                                         node.getPlanNodeId());
                }
            }
            if (m_branches.size() != node.getInlinePlanNodes().size()) {
                StringBuilder buf = new StringBuilder();
                String sep = "";
                for (PlanNodeType pnt : m_branches) {
                    buf.append(sep).append(pnt.name());
                }
                String expected = buf.toString();
                buf = new StringBuilder();
                sep = "";
                for (Map.Entry<PlanNodeType, AbstractPlanNode> entry : node.getInlinePlanNodes().entrySet()) {
                    buf.append(sep).append(entry.getKey().name());
                    sep = ", ";
                }
                String found = buf.toString();
                return String.format("Expected %d inline nodes (%s), found %d (%s) at node id %d.",
                                     m_branches.size(),
                                     expected,
                                     node.getInlinePlanNodes().size(),
                                     found,
                                     node.getPlanNodeId());
            }
            return null;
        }
    }

    protected static PlanWithInlineNodes planWithInlineNodes(PlanNodeType mainType,
                                                             PlanNodeType ... inlineTypes) {
        return new PlanWithInlineNodes(mainType, inlineTypes);
    }

    /**
     * Make a PlanMatcher out of an object.  The object
     * really wants to be a PlanMatcher itself.  But it's
     * very convenient to just allow a plan node type here,
     * so we relax the type system.
     *
     * @param obj
     * @return
     */
    private static PlanMatcher makePlanMatcher(Object obj) {
        if (obj instanceof PlanNodeType) {
            return (node) -> {
                        PlanNodeType pnt = (PlanNodeType)obj;
                        if (node.getPlanNodeType() != (PlanNodeType)obj) {
                            return String.format("Expected Plan Node Type %s not %s at node id %d",
                                                 pnt.name(),
                                                 node.getPlanNodeType().name(),
                                                 node.getPlanNodeId());
                        }
                        return null;
                    };
        }
        else if (obj instanceof PlanMatcher) {
            return (PlanMatcher)obj;
        }
        else {
            throw new PlanningErrorException("Bad fragment specification type.");
        }
    }

    protected static class FragmentSpec implements PlanMatcher {
        private List<PlanMatcher> m_nodeSpecs = new ArrayList<>();

        public FragmentSpec(Object ... nodeSpecs) {
            for (int idx = 0; idx < nodeSpecs.length; idx += 1) {
                m_nodeSpecs.add(makePlanMatcher(nodeSpecs[idx]));
            }
        }

        @Override
        public String match(AbstractPlanNode node) {
            int idx;
            for (idx = 0; node != null && idx < m_nodeSpecs.size(); idx += 1) {
                PlanMatcher pm = m_nodeSpecs.get(idx);
                String err = pm.match(node);
                if (err != null) {
                    return err;
                }
                // Nodes with multiple children, such as join
                // nodes, will have their own matchers, and will
                // stop here.
                if (node.getChildCount() > 0) {
                    node = node.getChild(0);
                }
                else {
                    node = null;
                }
            }
            if (idx < m_nodeSpecs.size()) {
                return "Expected "
                       + (m_nodeSpecs.size() + 1)
                       + " nodes in plan, not "
                       + (idx+1) ;
            }
            if (node != null) {
                return "Expected fewer nodes in plan at node "
                       + (idx + 1)
                       + ": "
                       + node.getPlanNodeType();
            }
            return null;
        }
    }

    protected static FragmentSpec fragSpec(Object ... nodeSpecs) {
        return new FragmentSpec(nodeSpecs);
    }

    protected static class SomeOrNoneOrAllOf implements PlanMatcher {
        boolean m_needAll;
        boolean m_needSome;
        String m_failMessage;
        List<PlanMatcher> m_allMatchers = new ArrayList<>();

        /**
         * Match some or none or all of a set of conditions.
         * <ul>
         *   <li>To match all conditions specify needAll true.</li>
         *   <li>To match at least one condition specify needSome true and needAll false.</li>
         *   <li>To match none of the conditions specify needSome and needAll to be both false.</li>
         * </ul>
         *
         * @param needAll If this is true we need to match all conditions.  If
         *                this is false then the semantics depends on needSome.
         * @param needSome If this is true we need only match some conditions.
         *                 If this and needAll are both false we need to fail
         *                 all the conditions.
         * @param failMessage The error message to return if we can't find
         *                    anything better.
         * @param matchers The conditions to match.  These can be either PlanNodeType
         *                 enumerals or else PlanMatcher objects.  At least one
         *                 matcher must be specified.
         */
        public SomeOrNoneOrAllOf(boolean needAll, boolean needSome, String failMessage, Object ...matchers) {
            m_needAll = needAll;
            m_needSome = needAll || needSome;
            m_failMessage = failMessage;
            if (matchers.length == 0) {
                throw new PlanningErrorException("Need at least one matcher here.");
            }
            for (Object obj : matchers) {
                m_allMatchers.add(makePlanMatcher(obj));
            }
        }

        @Override
        public String match(AbstractPlanNode node) {
            for (PlanMatcher pm : m_allMatchers) {
                String error = pm.match(node);
                if (error != null) {
                    if (m_needAll) {
                        return error;
                    }
                }
                else if (!m_needAll) {
                    // We didn't get an error message and
                    // we need some to be true but maybe not
                    // all.  So return null, which is success.
                    if (m_needSome) {
                        return null;
                    }
                    else {
                        // We got a successful match, but we didn't
                        // want any.  So return the fallback message.
                        return m_failMessage;
                    }
                }
            }
            if (m_needAll) {
                // if m_needAll is true, then we never
                // saw an error.  Otherwise we would have
                // returned earlier.  So return null, which
                // is success.
                return null;
            }
            // If m_needAll is false and m_needSome is true
            // then we must have seen nothing but failures.
            // So this is a failure.
            if (m_needSome) {
                return m_failMessage;
            }
            else {
                // If m_needAll and m_needSome are both
                // false then we want to match no condition.
                // But that's exactly what we have seen, so
                // we want to return null, which is success.
                return null;
            }
        }
    }

    /**
     * Match all the given node specifications.  Specifications
     * after the first failure are not evaluated.
     *
     * Note that, unlike someOf or noneOf, there is no fail
     * message.  The first failing specification is the error
     * message.
     *
     * @param nodeSpecs  The specifications.
     * @return
     */
    protected PlanMatcher allOf(PlanMatcher ... nodeSpecs) {
        return new SomeOrNoneOrAllOf(true, true, "This cannot happen.", (Object[])nodeSpecs);
    }

    /**
     * Match at least one of the given node specifications.  Only specifications
     * up until the first success are evaluated.
     *
     * @param failMessage A message to return if no specification succeeds.
     * @param nodeSpecs The specifications.
     * @return
     */
    protected PlanMatcher someOf(String failMessage, PlanMatcher ... nodeSpecs) {
        return new SomeOrNoneOrAllOf(false, true, failMessage, (Object[])nodeSpecs);
    }
    /**
     * Ensure that no specification succeeds.
     *
     * @param failMessage A message to return if some specification succeeds.
     *                    We need this because success is denoted by a null error message.
     * @param nodeSpecs The specifications.
     * @return
     */
    protected PlanMatcher noneOf(String failMessage, PlanMatcher ... nodeSpecs) {
        return new SomeOrNoneOrAllOf(false, false, failMessage, (Object[])nodeSpecs);
    }
    /**
     * Validate a plan.  This is kind of like
     * PlannerTestCase.compileToTopDownTree.  The differences are
     * <ol>
     *   <li>We can compile MP plans and SP plans</li>
     *   <li>We can describe plan nodes with inline nodes
     *       pretty handily.</li>
     * </ol>
     *
     * See TestWindowFunctions.testWindowFunctionWithIndex or
     * TestPlansInsertIntoSelect for examples of the use of this
     * function.  TestUnion also has a function which uses this
     * scheme.
     *
     * @param SQL The SQL statement text.
     * @param types Specifications of the plans for the fragments.
     *              These are most easily specified with the function
     *              fragSpec.
     */
    protected void validatePlan(String SQL,
                                FragmentSpec ... spec) {
        // All this System.out.printf nonsense should be changed
        // to a log message somehow.
        List<AbstractPlanNode> fragments;
        if (spec.length > 1) {
            fragments = compileToFragments(SQL);
        } else {
            fragments = new ArrayList<>();
            fragments.add(compileForSinglePartition(SQL));
        }
        System.out.printf("SQL: %s\n", SQL);
        for (int idx = 0; idx < fragments.size(); idx += 1) {
            AbstractPlanNode node = fragments.get(idx);
            System.out.printf("Node %d/%d:\n%s\n", idx + 1, fragments.size(), node.toExplainPlanString());
            printJSONString(node);
        }
        assertEquals(String.format("Expected %d fragments, not %d",
                                   spec.length,
                                   fragments.size()),
                     spec.length,
                     fragments.size());
        for (int idx = 0; idx < fragments.size(); idx += 1) {
            String error = spec[idx].match(fragments.get(idx));
            assertNull(error, error);
        }
    }

    private void printJSONString(AbstractPlanNode node) {
        try {
            String jsonString = PlanSelector.outputPlanDebugString(node);
            System.out.printf("Json:\n%s\n", jsonString);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
