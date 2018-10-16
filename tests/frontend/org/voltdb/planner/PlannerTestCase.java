/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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
import org.voltcore.utils.Pair;
import org.voltdb.VoltType;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Database;
import org.voltdb.compiler.DeterminismMode;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.plannodes.*;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.JoinType;
import org.voltdb.types.PlanMatcher;
import org.voltdb.types.PlanNodeType;
import org.voltdb.types.SortDirectionType;

import junit.framework.TestCase;

public class PlannerTestCase extends TestCase {

    private PlannerTestAideDeCamp m_aide;
    private boolean m_byDefaultInferPartitioning = true;
    private boolean m_byDefaultPlanForSinglePartition;
    final private int m_defaultParamCount = 0;
    private String m_noJoinOrder = null;
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
            List<AbstractPlanNode> unexpected = m_aide.compile(sql, paramCount,
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
            cp = m_aide.compileAdHocPlan(sql, detMode);
            assertTrue(cp != null);
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
        CompiledPlan cp = null;
        try {
            cp = m_aide.compileAdHocPlan(sql, inferPartitioning, forcedSP, detMode);
            assertTrue(cp != null);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail();
        }
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
        CompiledPlan cp = null;
        cp = m_aide.compileAdHocPlan(sql, inferPartitioning, forcedSP, detMode);
        assertTrue(cp != null);
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
        List<AbstractPlanNode> pn = m_aide.compile(sql, paramCount, m_byDefaultInferPartitioning, m_byDefaultPlanForSinglePartition, joinOrder);
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

    protected void planForLargeQueries(boolean b) {
        m_aide.planForLargeQueries(b);
    }

    protected boolean isPlanningForLargeQueries() {
        return m_aide.isPlanningForLargeQueries();
    }

    public String getCatalogString() {
        return m_aide.getCatalogString();
    }

    public Catalog getCatalog() {
        return m_aide.getCatalog();
    }

    Database getDatabase() {
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

    /**
     * For use in lambdas.
     */
    protected interface IIndexScanMatcher {
        boolean match(IndexScanPlanNode p);
    }

    /**
     * Match some kind of index scan plan node.
     */
    protected static class IndexScanPlanMatcher implements PlanMatcher {
        private String      m_indexName;

        /**
         * Create an {@link IndexScanPlanMatcher} which must have a
         * given named index.
         *
         * @param indexName The index we need to match.
         */
        public IndexScanPlanMatcher(String indexName) {
            assert(indexName != null);
            m_indexName = indexName;
        }

        @Override
        public String match(AbstractPlanNode node, int fn, int nf) {
            if ( ! (node instanceof IndexScanPlanNode) ) {
                return String.format("Expected IndexScanPlanNode, not %s: fragment %d/%d, id %d",
                                     node.getPlanNodeType(),
                                     fn, nf, node.getPlanNodeId());
            }

            if (m_indexName != null) {
                String idxName = ((IndexScanPlanNode) node).getTargetIndexName();
                if (!m_indexName.equals(idxName)) {
                    return String.format("Expected IndexScanPlanNode of index %s, not %s: fragment %d/%d, id %d",
                                         m_indexName, idxName, fn, nf, node.getPlanNodeId());
                }
            }
            return null;
        }

        @Override
        public String matchName() {
            return "INDEX_SCAN_NODE[" + m_indexName + "]";
        }
    }

    public static class MatchSchemaColumn {
        String m_columnName;
        VoltType m_columnType;
        int m_columnIndex;
        public MatchSchemaColumn(String columnName, VoltType columnType, int columnIndex) {
            m_columnName = columnName;
            m_columnType = columnType;
            m_columnIndex = columnIndex;
        }
    }

    public static MatchSchemaColumn[] makeSchema(Object ... args) {
        assertTrue(args.length % 3 == 0);
        MatchSchemaColumn[] answer = new MatchSchemaColumn[args.length/3];
        for (int idx = 0; idx < args.length/3; idx += 1) {
            assertTrue(args[3*idx] instanceof String);
            assertTrue(args[3*idx+1] instanceof VoltType);
            assertTrue(args[3*idx+2] instanceof Integer);
            answer[idx] = new MatchSchemaColumn((String)args[3*idx],
                                                (VoltType)args[3*idx+1],
                                                (Integer)args[3*idx+2]);
        }
        return answer;
    }
    /**
     * This matcher matches a node schema.  If the name is null, it
     * matches everything.
     */
    protected static class OutputSchemaMatcher implements PlanMatcher {
        private MatchSchemaColumn[] m_schema;

        public OutputSchemaMatcher(Object ... args) {
            assertTrue(args.length % 3 == 0);
            m_schema = new MatchSchemaColumn[args.length/3];
            for (int idx = 0; idx < args.length/3; idx += 1) {
                assertTrue(args[3*idx] instanceof String);
                assertTrue(args[3*idx+1] instanceof VoltType);
                assertTrue(args[3*idx+2] instanceof Integer);
                m_schema[idx] = new MatchSchemaColumn((String)args[3*idx],
                                                      (VoltType)args[3*idx+1],
                                                      (Integer)args[3*idx+2]);
            }
        }

        @Override
        public String match(AbstractPlanNode node,
                            int fragmentNo,
                            int numFragments) {
            int idx;
            NodeSchema schema = node.getOutputSchema();
            for (idx = 0; idx < m_schema.length && idx < schema.size(); idx += 1) {
                String name = m_schema[idx].m_columnName;
                VoltType type = m_schema[idx].m_columnType;
                int index = m_schema[idx].m_columnIndex;
                if ( index < 0 || schema.size() <= index) {
                    return String.format("Index %d is too big for Output Schema Column of %s node whose length is %d: fragment %d/%d, id %d",
                                         index,
                                         node.getPlanNodeType(),
                                         schema.size(),
                                         fragmentNo,
                                         numFragments,
                                         node.getPlanNodeId());
                }

                SchemaColumn col = schema.getColumn(index);
                if (name != null &&
                        ! ( name.equals(col.getColumnAlias())
                                || name.equals(col.getColumnName()))) {
                    return String.format("Expected schema column with name or alias %s , not name %s, alias %s at %d: fragment %d/%d, id %d",
                                         name,
                                         col.getColumnName(),
                                         col.getColumnAlias(),
                                         index,
                                         fragmentNo,
                                         numFragments,
                                         node.getPlanNodeId());
                }
                if (col.getValueType() != type) {
                    return String.format("Expected schema type %s, found %s in column %d: fragment %d/%d, id %d",
                                         type,
                                         col.getValueType(),
                                         index,
                                         fragmentNo,
                                         numFragments,
                                         node.getPlanNodeId());
                }
            }
            return null;
        }

        @Override
        public String matchName() {
            return null;
        }
    }
    /**
     * This class matches a substring of the conventional
     * explain plan string.  It's most often used with
     * allOf.
     */
    protected static class ExplainStringMatcher implements PlanMatcher {
        String m_expected;

        public ExplainStringMatcher(String expected) {
            m_expected = expected;
        }

        @Override
        public String match(AbstractPlanNode node,
                            int fragmentNo,
                            int numFragments) {
            if (node.toExplainPlanString().contains(m_expected)) {
                return null;
            }
            return String.format("Expected \"%s\" in plan explain string: fragment %d/%d, id %d. ",
                                 m_expected,
                                 fragmentNo,
                                 numFragments,
                                 node.getPlanNodeId());
        }

        @Override
        public String matchName() {
            return "ExplainPlanContain[" + m_expected + "]";
        }
    }
    /**
     * Match an aggregate node of some kind.  This can
     * be hash, partial or serial aggregate.
     */
    protected static class AggregateNodeMatcher implements PlanMatcher {
        private PlanMatcher m_mainMatcher;
        private ExpressionType[] m_aggOps;

        /**
         * Create a plan matcher which will match a specific aggregate,
         * with a set of aggregate operations.
         * @param mainMatcher The node matcher.  This a plan matcher.
         *                    Any node which matches this matcher must
         *                    be an instance of AggregatePlanNode.
         * @param aggOps An array of aggregate expressions.  This must
         *               be a subset of the node's aggregate operations.
         */
        public AggregateNodeMatcher(PlanMatcher mainMatcher,
                                    ExpressionType ... aggOps) {
            m_mainMatcher = mainMatcher;
            m_aggOps = aggOps;
        }

        /**
         * Create a plan matcher which will match any aggregate
         * plan node, but require that there are at least the given
         * aggregate operations.
         *
         * @param aggOps
         */
        public AggregateNodeMatcher(ExpressionType ... aggOps) {
            m_mainMatcher = null;
            m_aggOps = aggOps;
        }
        @Override
        public String match(AbstractPlanNode node,
                            int fragmentNo,
                            int numFragments) {
            // This is really a test failure.  The matcher
            // should be something like an AggregatePlanNode.
            assertTrue(String.format(
                            "Expected an AggregatePlanNode, not %s: fragment %d/%d, id %d.",
                            node.getPlanNodeType(),
                            fragmentNo,
                            numFragments,
                            node.getPlanNodeId()),
                       node instanceof AggregatePlanNode);
            AggregatePlanNode pn = (AggregatePlanNode) node;
            String err = null;
            if (m_mainMatcher != null) {
                err = m_mainMatcher.match(node, fragmentNo, numFragments);
                if (err != null) {
                    return err;
                }
            }
            List<ExpressionType> expTypes = pn.getAggregateTypes();
            for (ExpressionType type : m_aggOps) {
                if ( ! expTypes.contains(type)) {
                    return String.format("Expected aggregate %s here: fragment %d/%d, id %d",
                                         type, fragmentNo, numFragments, node.getPlanNodeId());
                }
            }
            return null;
        }

        @Override
        public String matchName() {
            return String.format("AggregateNode(%s)",
                                 m_mainMatcher.matchName());
        }
    }

    protected interface NodeTester {
        boolean match(AbstractPlanNode node);
    }

    /**
     * This class is used most often with allOf.  It
     * takes a description and a test function, which is
     * a boolean predicate on nodes.  It is alot like
     * the PlanMatcher interface, but it make things
     * a bit more easy to read.
     */
    protected class NodeTestMatcher implements PlanMatcher {
        String m_name;
        NodeTester m_tester;

        public NodeTestMatcher(String name,
                               NodeTester tester) {
            m_name = name;
            m_tester = tester;
        }

        @Override
        public String match(AbstractPlanNode node,
                            int fragmentNo,
                            int numFragments) {
            if (m_tester.match(node)) {
                return null;
            }
            return String.format("NodeTest %s failed: fragment %d/%d, id %d",
                                 m_name,
                                 fragmentNo,
                                 numFragments,
                                 node.getPlanNodeId());
        }

        @Override
        public String matchName() {
            return m_name;
        }
    }
    /**
     * This is a plan node which is optional.  If the match
     * function is applied to a node and fails we don't
     * move on to the next node.  This just defers to
     * the parameter node.  The magic happens in validatePlan.
     *
     * This is useful for writing a function which validates
     * plan patterns.  For example, we may sometimes have two
     * sql statements, S1 and S2, have similar plans, but one
     * has an extra projection node.  We can write a function
     * to match them, match the projection node if it
     * shows up and just continue matching if the projection
     * node does not show up.
     */
    protected static class OptionalPlanNode implements PlanMatcher {

        private final PlanMatcher m_nodeMatcher;

        public OptionalPlanNode(PlanMatcher nodeMatcher) {
            m_nodeMatcher = nodeMatcher;
        }
        @Override
        public String match(AbstractPlanNode node,
                            int fragmentNo,
                            int numFragments) {
            return m_nodeMatcher.match(node, fragmentNo, numFragments);
        }

        @Override
        public String matchName() {
            return m_nodeMatcher.matchName();
        }
    }

    /**
     * Match if a condition holds.  Otherwise treat this as
     * OptionalPlanNode.  If
     */
    protected static class MatchIf extends OptionalPlanNode {
        boolean m_condition;
        public MatchIf(boolean condition, PlanMatcher matcher) {
            super(matcher);
            m_condition = condition;
        }

        @Override
        public String match(AbstractPlanNode node,
                            int fragmentNo,
                            int numFragments) {
            if ( ! m_condition ) {
                return "MatchIf fails";
            } else {
                String err = super.match(node, fragmentNo, numFragments);
                if (err != null) {
                    return err;
                }
                return null;
            }
        }

        @Override
        public String matchName() {
            return "MatchIf";
        }
    }
    /**
     * Match a plan with inline nodes.  All inline nodes must be
     * listed.  Optional inline nodes are not allowed here.
     */
    protected static class PlanWithInlineNodes implements PlanMatcher {
        PlanMatcher m_type = null;

        List<PlanMatcher> m_branches = new ArrayList<>();

        /**
         * Create a matcher for a plan node with inline nodes.
         *
         * @param mainType This is the matcher for the main node.
         *                 It is typically a node type, but it coule
         *                 be something like {@link IndexScanPlanMatcher}
         *                 or the final static member AbstractJoinPlanNodeMatcher.
         * @param nodes These are matchers for the inline nodes.  These are
         *              typically node types, but they can be any PlanMatcher.
         */
        public PlanWithInlineNodes(PlanMatcher mainType, PlanMatcher ... nodes) {
            m_type = mainType;
            for (PlanMatcher node : nodes) {
                m_branches.add(node);
            }
        }

        private String branchNames() {
            StringBuilder sb = new StringBuilder();
            String sep = "";
            for (PlanMatcher branch : m_branches) {
                sb.append(sep).append(branch.matchName());
                sep = ", ";
            }
            return sb.toString();
        }

        /**
         * Find an inline plan node type whose node matches a PlanMatcher.
         * Usually the PlanMatcher is a PlanNodeType, so we
         * just look it up.  Otherwise we need to grovel through
         * the inline plan node map.
         *
         * @param node The node containing inline nodes.
         * @param pm The matcher
         * @param fn The fragment number
         * @param nf The number of fragments
         * @return A plan node type or null
         */
        private PlanNodeType findMatchingNode(AbstractPlanNode node, PlanMatcher branch, int fn, int nf) {
            if (branch instanceof PlanNodeType) {
                PlanNodeType type = (PlanNodeType)branch;
                if (node.getInlinePlanNode(type) != null) {
                    return type;
                }
                return null;
            }
            // Try to match all the entries.  It's unfortunate
            // that we have to do a search like this.
            for (Map.Entry<PlanNodeType, AbstractPlanNode> entry : node.getInlinePlanNodes().entrySet()) {
                // Null is a match.
                if (null == branch.match(entry.getValue(), fn, nf)) {
                    return entry.getKey();
                }
            }
            return null;
        }
        @Override
        public String match(AbstractPlanNode node, int fragmentNo, int numberFragments) {
            String err = m_type.match(node, fragmentNo, numberFragments);
            if (err != null) {
                return err;
            }

            // This is the inline types we will try to match.
            Set<PlanNodeType> inlineTypes = new HashSet<>();
            inlineTypes.addAll(node.getInlinePlanNodes().keySet());
            for (PlanMatcher branch : m_branches) {
                AbstractPlanNode inlineNode = null;
                PlanNodeType matchingType = findMatchingNode(node, branch, fragmentNo, numberFragments);
                if (matchingType == null) {
                    return String.format("Expected inline node type %s but didn't find it: "
                                                 + "fragment %d/%d, id %d.",
                                         branch.matchName(),
                                         fragmentNo,
                                         numberFragments,
                                         node.getPlanNodeId());
                }
                inlineTypes.remove(matchingType);
            }
            if (! inlineTypes.isEmpty()) {
                String expected = branchNames();
                StringBuilder buf = new StringBuilder();
                String sep = "";
                for (Entry<PlanNodeType, AbstractPlanNode> entry : node.getInlinePlanNodes().entrySet()) {
                    buf.append(sep).append(entry.getKey().name());
                    sep = ", ";
                }
                String found = buf.toString();
                return String.format("Expected %d inline nodes (%s), found %d (%s): fragment %d/%d, id %d",
                                     m_branches.size(),
                                     expected,
                                     node.getInlinePlanNodes().size(),
                                     found,
                                     fragmentNo,
                                     numberFragments,
                                     node.getPlanNodeId());
            }
            return null;
        }

        @Override
        public String matchName() {
            return m_type.toString() + "[" + branchNames() + "]";
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
            return new PlanMatcher() {
                public String match(AbstractPlanNode node, int fragmentNo, int numberFragments) {
                    PlanNodeType pnt = (PlanNodeType)obj;
                    if (node.getPlanNodeType() != (PlanNodeType)obj) {
                        return String.format("Expected Plan Node Type %s not %s: fragment %d/%d, id %d",
                                             pnt.name(),
                                             node.getPlanNodeType().name(),
                                             fragmentNo,
                                             numberFragments,
                                             node.getPlanNodeId());
                    }
                    return null;
                }

                public String matchName() {
                    return ((PlanNodeType)obj).toString();
                }
            };
        }
        else if (obj instanceof PlanMatcher) {
            return (PlanMatcher)obj;
        }
        else {
            throw new PlanningErrorException("Bad fragment specification type.");
        }
    }

    protected static PlanMatcher AbstractScanPlanNodeMatcher
            = new PlanMatcher() {
                public String match(AbstractPlanNode p, int fragmentNo, int numFragments) {
                    if (p instanceof AbstractScanPlanNode) {
                        return null;
                    }
                    return String.format("Expected AbstractScanPlanNode, not %s: fragment %d/%d, id %d",
                                         p.getPlanNodeType(), fragmentNo, numFragments, p.getPlanNodeId());
                }

                public String matchName() {
                    return "AbstractScanPlanNodeMatcher";
                }
            };

    protected final static PlanMatcher AbstractJoinPlanNodeMatcher
            = new PlanMatcher() {
                    public String match(AbstractPlanNode p,
                                        int fragmentNo,
                                        int numFragments) {
                        if (p instanceof AbstractJoinPlanNode) {
                            return null;
                        }
                        return String.format("Expected AbstractJoinPlanNode, not %s: fragment %d/%d, id %d",
                                             p.getPlanNodeType(), fragmentNo, numFragments, p.getPlanNodeId());
                    }

                    public String matchName() {
                        return "AbstractJoinPlanNodeMatcher";
                    }
                };

    /**
     * A matcher that matches any kind of receive node.
     */
    protected final static PlanMatcher AbstractReceivePlanMatcher
            = new PlanMatcher() {

        @Override
        public String match(AbstractPlanNode node,
                            int fragmentNo,
                            int numFragments) {
            switch (node.getPlanNodeType()) {
                case RECEIVE:
                    return null;
                case MERGERECEIVE:
                    if (node.getInlinePlanNode(PlanNodeType.ORDERBY) == null) {
                        return String.format("MERGERECEIVE node expects an inline ORDERBY: fragment %d/%d, id %d",
                                             fragmentNo, numFragments, node.getPlanNodeId());
                    }
                    return null;
                default:
                    return String.format("Expected some kind of receive node, not %s: fragment %d/%d, id %d",
                                         node.getPlanNodeType(),
                                         fragmentNo,
                                         numFragments,
                                         node.getPlanNodeId());
            }
        }

        @Override
        public String matchName() {
            return "AbstractReceivePlanMatcher";
        }
    };

    protected final PlanMatcher MergeReceivePlanMatcher = new PlanMatcher() {
        @Override
        public String match(AbstractPlanNode node, int fragmentNo, int numFragments) {
            if (node.getPlanNodeType() != PlanNodeType.MERGERECEIVE) {
                return String.format("Expected MERGERECEIVE node, not %s: fragment %d/d id %d",
                        node.getPlanNodeType(),
                        fragmentNo,
                        numFragments,
                        node.getPlanNodeId());
            }
            if (node.getInlinePlanNode(PlanNodeType.ORDERBY) == null) {
                return String.format("MERGERECEIVE node expects an inline ORDERBY: fragment %d/%d, id %d",
                        fragmentNo,
                        numFragments,
                        node.getPlanNodeId());
            }
            // Guess we are happy with this.
            return null;
        }

        @Override
        public String matchName() {
            return "MergeReceivePlanMatcher";
        }
    };

    protected static class FragmentSpec implements PlanMatcher {
        private List<PlanMatcher> m_nodeSpecs = new ArrayList<>();

        public FragmentSpec(Object ... nodeSpecs) {
            for (int idx = 0; idx < nodeSpecs.length; idx += 1) {
                m_nodeSpecs.add(makePlanMatcher(nodeSpecs[idx]));
            }
        }

        @Override
        public String match(AbstractPlanNode node, int fragmentNo, int numberFragments) {
            int idx;
            for (idx = 0; node != null && idx < m_nodeSpecs.size(); idx += 1) {
                PlanMatcher pm = m_nodeSpecs.get(idx);
                // Magic AnyFragment matches everything
                // after this, with no testing.
                if (pm instanceof AnyFragment) {
                    return null;
                }
                String err = pm.match(node, fragmentNo, numberFragments);
                if (err != null) {
                    /*
                     * This is the magic of OptionalPlanNode.
                     * If this spec fails, then just ignore it.
                     */
                    if ( pm instanceof OptionalPlanNode ) {
                        continue;
                    }
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
                       + node.getPlanNodeType()
                       + ", id: "
                       + node.getPlanNodeId();
            }
            return null;
        }

        public String matchName() {
            return "fragmentSpec";
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
        public String match(AbstractPlanNode node, int fragmentNo, int numFragments) {
            for (PlanMatcher pm : m_allMatchers) {
                String error = pm.match(node, fragmentNo, numFragments);
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

        @Override
        public String matchName() {
            if (m_needAll) {
                return "allOf";
            }
            if (m_needSome) {
                return "someOf";
            }
            return "noneOf";
        }

    }

    /**
     * A matcher which will match any fragment.  This is
     * all nodes of any fragment.  This is magic in the
     * matching routine.
     */
    protected static class AnyFragment implements PlanMatcher {

        @Override
        public String match(AbstractPlanNode node,
                            int fragmentNo,
                            int numFragments) {
            return null;
        }

        @Override
        public String matchName() {
            return "AnyFragment";
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
        validatePlan(SQL, true, spec);
    }

    /**
     * Validate the plan of a SQL query.  We allow the possibility of printing
     * the JSON and explain strings of the plan.
     *
     * @param SQL       A SQL query.
     * @param printPlan If this is true we print the JSON plan before
     *                  validating it.
     * @param spec      The specifications of the plans.
     */
    protected void validatePlan(String SQL,
                                boolean printPlan,
                                FragmentSpec ... spec) {
        List<AbstractPlanNode> fragments = compileToFragments(SQL);
        printJSONPlan(printPlan, SQL, fragments);
        assertEquals(String.format("Expected %d fragments, not %d",
                                   spec.length,
                                   fragments.size()),
                     spec.length,
                     fragments.size());
        for (int idx = 0; idx < fragments.size(); idx += 1) {
            String error = spec[idx].match(fragments.get(idx), idx + 1, fragments.size());
            assertNull(error, error);
        }
    }

    protected void printJSONPlan(boolean printPlan,
                                 String SQL) {
        if (printPlan) {
            printJSONPlan(printPlan, SQL, compileToFragments(SQL));
        }
    }

    protected void printJSONPlan(boolean printPlan,
                                 String SQL,
                                 List<AbstractPlanNode> fragments) {
        if (printPlan) {
            System.out.printf("SQL: %s\n", SQL);
        }
        if (printPlan) {
            for (int idx = 0; idx < fragments.size(); idx += 1) {
                AbstractPlanNode node = fragments.get(idx);
                System.out.printf("Fragment %d/%d:\n%s\n", idx + 1, fragments.size(), node.toExplainPlanString());
                printJSONString(node);
            }
        }
    }

    private void printJSONString(AbstractPlanNode node) {
        String jsonString = node.toJSONExplainString(m_aide.m_planForLargeQueries);
        System.out.printf("Json:\n%s\n", jsonString);
    }

}
