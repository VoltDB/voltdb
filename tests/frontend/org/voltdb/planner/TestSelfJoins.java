/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

import java.util.List;

import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ConstantValueExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AggregatePlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.NestLoopIndexPlanNode;
import org.voltdb.plannodes.NestLoopPlanNode;
import org.voltdb.plannodes.NodeSchema;
import org.voltdb.plannodes.OrderByPlanNode;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.plannodes.SchemaColumn;
import org.voltdb.plannodes.SendPlanNode;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.IndexLookupType;
import org.voltdb.types.PlanNodeType;

public class TestSelfJoins  extends PlannerTestCase {

    public void testSelfJoin() {
        AbstractPlanNode pn = compile("select * FROM R1 A JOIN R1 B ON A.C = B.C WHERE B.A > 0 AND A.C < 3");
        pn = pn.getChild(0).getChild(0);
        assertTrue(pn instanceof NestLoopPlanNode);
        assertEquals(4, pn.getOutputSchema().getColumns().size());
        assertEquals(2, pn.getChildCount());
        AbstractPlanNode c = pn.getChild(0);
        assertTrue(c instanceof SeqScanPlanNode);
        SeqScanPlanNode ss = (SeqScanPlanNode) c;
        assertEquals("R1", ss.getTargetTableName());
        assertEquals("A", ss.getTargetTableAlias());
        assertEquals(ExpressionType.COMPARE_LESSTHAN, ss.getPredicate().getExpressionType());
        c = pn.getChild(1);
        assertTrue(c instanceof SeqScanPlanNode);
        ss = (SeqScanPlanNode) c;
        assertEquals("R1", ss.getTargetTableName());
        assertEquals("B", ss.getTargetTableAlias());
        assertEquals(ExpressionType.COMPARE_GREATERTHAN, ss.getPredicate().getExpressionType());

        pn = compile("select * FROM R1 JOIN R1 B ON R1.C = B.C");
        pn = pn.getChild(0).getChild(0);
        assertTrue(pn instanceof NestLoopPlanNode);
        assertEquals(4, pn.getOutputSchema().getColumns().size());
        assertEquals(2, pn.getChildCount());
        c = pn.getChild(0);
        assertTrue(c instanceof SeqScanPlanNode);
        ss = (SeqScanPlanNode) c;
        assertEquals("R1", ss.getTargetTableName());
        assertEquals("R1", ss.getTargetTableAlias());
        c = pn.getChild(1);
        assertTrue(c instanceof SeqScanPlanNode);
        ss = (SeqScanPlanNode) c;
        assertEquals("R1", ss.getTargetTableName());
        assertEquals("B", ss.getTargetTableAlias());

        pn = compile("select A.A, A.C, B.A, B.C FROM R1 A JOIN R1 B ON A.C = B.C");
        pn = pn.getChild(0).getChild(0);
        assertTrue(pn instanceof NestLoopPlanNode);
        assertEquals(4, pn.getOutputSchema().getColumns().size());

        pn = compile("select A,C  FROM R1 A JOIN R2 B USING(A)");
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        NodeSchema ns = pn.getOutputSchema();
        for (SchemaColumn sc : ns.getColumns()) {
            AbstractExpression e = sc.getExpression();
            assertTrue(e instanceof TupleValueExpression);
            TupleValueExpression tve = (TupleValueExpression) e;
            assertNotSame(-1, tve.getColumnIndex());
        }
    }

    public void testOuterSelfJoin() {
        // A.C = B.C Inner-Outer join Expr stays at the NLJ as Join predicate
        // A.A > 1 Outer Join Expr stays at the the NLJ as pre-join predicate
        // B.A < 0 Inner Join Expr is pushed down to the inner SeqScan node
        AbstractPlanNode pn = compile("select * FROM R1 A LEFT JOIN R1 B ON A.C = B.C AND A.A > 1 AND B.A < 0");
        pn = pn.getChild(0).getChild(0);
        assertTrue(pn instanceof NestLoopPlanNode);
        NestLoopPlanNode nl = (NestLoopPlanNode) pn;
        assertNotNull(nl.getPreJoinPredicate());
        AbstractExpression p = nl.getPreJoinPredicate();
        assertEquals(ExpressionType.COMPARE_GREATERTHAN, p.getExpressionType());
        assertNotNull(nl.getJoinPredicate());
        p = nl.getJoinPredicate();
        assertEquals(ExpressionType.COMPARE_EQUAL, p.getExpressionType());
        assertNull(nl.getWherePredicate());
        assertEquals(2, nl.getChildCount());
        SeqScanPlanNode c = (SeqScanPlanNode) nl.getChild(0);
        assertNull(c.getPredicate());
        c = (SeqScanPlanNode) nl.getChild(1);
        assertNotNull(c.getPredicate());
        p = c.getPredicate();
        assertEquals(ExpressionType.COMPARE_LESSTHAN, p.getExpressionType());
    }

    public void testPartitionedSelfJoin() {
        // SELF JOIN of the partitioned table on the partitioned column
        AbstractPlanNode pn = compile("select * FROM P1 A JOIN P1 B ON A.A = B.A");
        assertTrue(pn instanceof SendPlanNode);

        // SELF JOIN on non-partitioned columns
        failToCompile("select * FROM P1 A JOIN P1 B ON A.C = B.A",
                "Join of multiple partitioned tables has insufficient join criteria");
        // SELF JOIN on non-partitioned column
        failToCompile("select * FROM P1 A JOIN P1 B ON A.C = B.C",
                      "Join of multiple partitioned tables has insufficient join criteria");
    }

    public void testInvalidSelfJoin() {
        // SELF JOIN with an identical alias
        failToCompile("select * FROM R1 A JOIN R1 A ON A.A = A.A",
                "Not unique table/alias: A");
        failToCompile("select * FROM R1 A JOIN R2 A ON A.A = A.A",
                "Not unique table/alias: A");
    }

    public void testIndexedSelfJoin() {
        AbstractPlanNode.enableVerboseExplainForDebugging();
        IndexScanPlanNode c;
        AbstractPlanNode apn;
        AbstractPlanNode pn;
        NestLoopIndexPlanNode nlij;
        List<AbstractExpression> searchKeys;
        // SELF JOIN using two different indexes on the same table
        // sometimes with a surviving sort ordering that supports GROUP BY and/or ORDER BY.

        apn = compile("select * FROM R2 A, R2 B WHERE A.A = B.A AND B.C > 1 ORDER BY B.C");
        //* for debug */ System.out.println(apn.toExplainPlanString());
        // Some day, the wasteful projection node will not be here to skip.
        pn = apn.getChild(0).getChild(0);
        assertTrue(pn instanceof NestLoopIndexPlanNode);
        nlij = (NestLoopIndexPlanNode) pn;
        assertNull(nlij.getPreJoinPredicate());
        assertNull(nlij.getJoinPredicate());
        assertNull(nlij.getWherePredicate());
        assertEquals(1, nlij.getChildCount());
        c = (IndexScanPlanNode) nlij.getChild(0);
        assertNull(c.getPredicate());
        assertEquals(IndexLookupType.GT, c.getLookupType());
        searchKeys = c.getSearchKeyExpressions();
        assertEquals(1, searchKeys.size());
        assertTrue(searchKeys.get(0) instanceof ConstantValueExpression);
        c = (IndexScanPlanNode) nlij.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        assertEquals(IndexLookupType.GTE, c.getLookupType());
        assertNull(c.getPredicate());
        searchKeys = c.getSearchKeyExpressions();
        assertEquals(1, searchKeys.size());
        assertTrue(searchKeys.get(0) instanceof TupleValueExpression);

        apn = compile("select * FROM R2 A, R2 B WHERE A.A = B.A AND B.C > 1 ORDER BY B.A, B.C");
        //* for debug */ System.out.println(apn.toExplainPlanString());
        // Some day, the wasteful projection node will not be here to skip.
        pn = apn.getChild(0).getChild(0);
        assertTrue(pn instanceof OrderByPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof NestLoopIndexPlanNode);
        nlij = (NestLoopIndexPlanNode) pn;
        assertNull(nlij.getPreJoinPredicate());
        assertNull(nlij.getJoinPredicate());
        assertNull(nlij.getWherePredicate());
        assertEquals(1, nlij.getChildCount());
        c = (IndexScanPlanNode) nlij.getChild(0);
        assertNull(c.getPredicate());
        assertEquals(IndexLookupType.GT, c.getLookupType());
        searchKeys = c.getSearchKeyExpressions();
        assertEquals(1, searchKeys.size());
        assertTrue(searchKeys.get(0) instanceof ConstantValueExpression);
        c = (IndexScanPlanNode) nlij.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        assertEquals(IndexLookupType.GTE, c.getLookupType());
        assertNull(c.getPredicate());
        searchKeys = c.getSearchKeyExpressions();
        assertEquals(1, searchKeys.size());
        assertTrue(searchKeys.get(0) instanceof TupleValueExpression);

        apn = compile("select * FROM R2 A, R2 B WHERE A.A = B.A AND B.A > 1 ORDER BY B.A, B.C");
        //* for debug */ System.out.println(apn.toExplainPlanString());
        // Some day, the wasteful projection node will not be here to skip.
        pn = apn.getChild(0).getChild(0);
        assertTrue(pn instanceof NestLoopIndexPlanNode);
        nlij = (NestLoopIndexPlanNode) pn;
        assertNull(nlij.getPreJoinPredicate());
        assertNull(nlij.getJoinPredicate());
        assertNull(nlij.getWherePredicate());
        assertEquals(1, nlij.getChildCount());
        assertTrue(nlij.getChild(0) instanceof IndexScanPlanNode);
        c = (IndexScanPlanNode) nlij.getChild(0);
        assertEquals(IndexLookupType.GT, c.getLookupType());
        searchKeys = c.getSearchKeyExpressions();
        assertEquals(1, searchKeys.size());
        assertTrue(searchKeys.get(0) instanceof ConstantValueExpression);
        c = (IndexScanPlanNode) nlij.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        assertEquals(IndexLookupType.GTE, c.getLookupType());
        assertNull(c.getPredicate());
        searchKeys = c.getSearchKeyExpressions();
        assertEquals(1, searchKeys.size());
        assertTrue(searchKeys.get(0) instanceof TupleValueExpression);

        apn = compile("select B.C, MAX(A.C) FROM R2 A, R2 B WHERE A.A = B.A AND B.C > 1 GROUP BY B.C ORDER BY B.C");
        //* for debug */ System.out.println(apn.toExplainPlanString());
        // Some day, the wasteful projection node will not be here to skip.
        pn = apn.getChild(0);
        assertNotNull(AggregatePlanNode.getInlineAggregationNode(pn));
        assertTrue(pn instanceof NestLoopIndexPlanNode);
        nlij = (NestLoopIndexPlanNode) pn;
        assertNull(nlij.getPreJoinPredicate());
        assertNull(nlij.getJoinPredicate());
        assertNull(nlij.getWherePredicate());
        assertEquals(1, nlij.getChildCount());
        c = (IndexScanPlanNode) nlij.getChild(0);
        assertNull(c.getPredicate());
        assertEquals(IndexLookupType.GT, c.getLookupType());
        searchKeys = c.getSearchKeyExpressions();
        assertEquals(1, searchKeys.size());
        assertTrue(searchKeys.get(0) instanceof ConstantValueExpression);
        c = (IndexScanPlanNode) nlij.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        assertEquals(IndexLookupType.GTE, c.getLookupType());
        assertNull(c.getPredicate());
        searchKeys = c.getSearchKeyExpressions();
        assertEquals(1, searchKeys.size());
        assertTrue(searchKeys.get(0) instanceof TupleValueExpression);

        apn = compile("select B.C, B.A FROM R2 A, R2 B WHERE A.A = B.A AND B.C > 1 GROUP BY B.A, B.C ORDER BY B.A, B.C");
        //* for debug */ System.out.println(apn.toExplainPlanString());
        // Some day, the wasteful projection node will not be here to skip.
        pn = apn.getChild(0).getChild(0);
        assertTrue(pn instanceof OrderByPlanNode);
        pn = pn.getChild(0);
        assertNotNull(AggregatePlanNode.getInlineAggregationNode(pn));
        assertTrue(pn instanceof NestLoopIndexPlanNode);
        nlij = (NestLoopIndexPlanNode) pn;
        assertNull(nlij.getPreJoinPredicate());
        assertNull(nlij.getJoinPredicate());
        assertNull(nlij.getWherePredicate());
        assertEquals(1, nlij.getChildCount());
        c = (IndexScanPlanNode) nlij.getChild(0);
        assertNull(c.getPredicate());
        assertEquals(IndexLookupType.GT, c.getLookupType());
        searchKeys = c.getSearchKeyExpressions();
        assertEquals(1, searchKeys.size());
        assertTrue(searchKeys.get(0) instanceof ConstantValueExpression);
        c = (IndexScanPlanNode) nlij.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        assertEquals(IndexLookupType.GTE, c.getLookupType());
        assertNull(c.getPredicate());
        searchKeys = c.getSearchKeyExpressions();
        assertEquals(1, searchKeys.size());
        assertTrue(searchKeys.get(0) instanceof TupleValueExpression);

        apn = compile("select B.C, B.A FROM R2 A, R2 B WHERE A.A = B.A AND B.A > 1 GROUP BY B.A, B.C ORDER BY B.A, B.C");
        //* for debug */ System.out.println(apn.toExplainPlanString());
        pn = apn.getChild(0);
        assertNotNull(AggregatePlanNode.getInlineAggregationNode(pn));
        assertTrue(pn instanceof NestLoopIndexPlanNode);
        nlij = (NestLoopIndexPlanNode) pn;
        assertNull(nlij.getPreJoinPredicate());
        assertNull(nlij.getJoinPredicate());
        assertNull(nlij.getWherePredicate());
        assertEquals(1, nlij.getChildCount());
        assertTrue(nlij.getChild(0) instanceof IndexScanPlanNode);
        c = (IndexScanPlanNode) nlij.getChild(0);
        assertEquals(IndexLookupType.GT, c.getLookupType());
        searchKeys = c.getSearchKeyExpressions();
        assertEquals(1, searchKeys.size());
        assertTrue(searchKeys.get(0) instanceof ConstantValueExpression);
        c = (IndexScanPlanNode) nlij.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        assertEquals(IndexLookupType.GTE, c.getLookupType());
        assertNull(c.getPredicate());
        searchKeys = c.getSearchKeyExpressions();
        assertEquals(1, searchKeys.size());
        assertTrue(searchKeys.get(0) instanceof TupleValueExpression);

        // Here's a case that can't be optimized because it purposely uses the "wrong" alias
        // in the GROUP BY and ORDER BY.
        apn = compile("select B.C, B.A FROM R2 A, R2 B WHERE A.A = B.A AND B.C > 1 GROUP BY B.A, A.C ORDER BY B.A, A.C");
        //* for debug */ System.out.println(apn.toExplainPlanString());

        // Complex ORDER BY case: GROUP BY columns that are not in the display column list
        pn = apn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof OrderByPlanNode);
        pn = pn.getChild(0);
        assertNotNull(AggregatePlanNode.getInlineAggregationNode(pn));
        assertTrue(pn instanceof NestLoopIndexPlanNode);
        nlij = (NestLoopIndexPlanNode) pn;
        assertNull(nlij.getPreJoinPredicate());
        assertNull(nlij.getJoinPredicate());
        assertNull(nlij.getWherePredicate());
        assertEquals(1, nlij.getChildCount());
        c = (IndexScanPlanNode) nlij.getChild(0);
        assertNull(c.getPredicate());
        assertEquals(IndexLookupType.GT, c.getLookupType());
        searchKeys = c.getSearchKeyExpressions();
        assertEquals(1, searchKeys.size());
        assertTrue(searchKeys.get(0) instanceof ConstantValueExpression);
        c = (IndexScanPlanNode) nlij.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        assertEquals(IndexLookupType.GTE, c.getLookupType());
        assertNull(c.getPredicate());
        searchKeys = c.getSearchKeyExpressions();
        assertEquals(1, searchKeys.size());
        assertTrue(searchKeys.get(0) instanceof TupleValueExpression);

        // This variant shows that the GROUP BY can be a permutation of the sort order
        // without messing up the optimization
        apn = compile("select B.C, B.A FROM R2 A, R2 B WHERE A.A = B.A AND B.A > 1 GROUP BY B.C, B.A ORDER BY B.A, B.C");
        //* for debug */ System.out.println(apn.toExplainPlanString());
        // Some day, the wasteful projection node will not be here to skip.
        pn = apn.getChild(0);
        assertNotNull(AggregatePlanNode.getInlineAggregationNode(pn));
        assertTrue(pn instanceof NestLoopIndexPlanNode);
        nlij = (NestLoopIndexPlanNode) pn;
        assertNull(nlij.getPreJoinPredicate());
        assertNull(nlij.getJoinPredicate());
        assertNull(nlij.getWherePredicate());
        assertEquals(1, nlij.getChildCount());
        assertTrue(nlij.getChild(0) instanceof IndexScanPlanNode);
        c = (IndexScanPlanNode) nlij.getChild(0);
        assertEquals(IndexLookupType.GT, c.getLookupType());
        searchKeys = c.getSearchKeyExpressions();
        assertEquals(1, searchKeys.size());
        assertTrue(searchKeys.get(0) instanceof ConstantValueExpression);
        c = (IndexScanPlanNode) nlij.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        assertEquals(IndexLookupType.GTE, c.getLookupType());
        assertNull(c.getPredicate());
        searchKeys = c.getSearchKeyExpressions();
        assertEquals(1, searchKeys.size());
        assertTrue(searchKeys.get(0) instanceof TupleValueExpression);
   }

    @Override
    protected void setUp() throws Exception {
        setupSchema(TestJoinOrder.class.getResource("testself-joins-ddl.sql"), "testselfjoins", false);
    }
}
