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
import org.voltdb.expressions.OperatorExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.plannodes.AbstractJoinPlanNode;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.AggregatePlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.NestLoopIndexPlanNode;
import org.voltdb.plannodes.NestLoopPlanNode;
import org.voltdb.plannodes.NodeSchema;
import org.voltdb.plannodes.OrderByPlanNode;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.plannodes.ReceivePlanNode;
import org.voltdb.plannodes.SchemaColumn;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.IndexLookupType;
import org.voltdb.types.JoinType;
import org.voltdb.types.PlanNodeType;

public class TestPlansJoin extends PlannerTestCase {

    public void testBasicInnerJoin() {
        // select * with ON clause should return all columns from all tables
        AbstractPlanNode pn = compile("select * FROM R1 JOIN R2 ON R1.C = R2.C");
        AbstractPlanNode n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        //assertEquals(JoinType.INNER, nlj.getJoinType());
        for (int ii = 0; ii < 2; ii++) {
            assertTrue(n.getChild(ii) instanceof SeqScanPlanNode);
        }
        assertEquals(5, pn.getOutputSchema().getColumns().size());

        // select * with USING clause should contain only one column for each column from the USING expression
        pn = compile("select * FROM R1 JOIN R2 USING(C)");
        assertTrue(pn.getChild(0).getChild(0) instanceof NestLoopPlanNode);
        assertEquals(4, pn.getOutputSchema().getColumns().size());

        pn = compile("select A,C,D FROM R1 JOIN R2 ON R1.C = R2.C");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        assertEquals(3, pn.getOutputSchema().getColumns().size());

        pn = compile("select A,C,D FROM R1 JOIN R2 USING(C)");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        assertEquals(3, pn.getOutputSchema().getColumns().size());

        pn = compile("select R1.A, R2.C, R1.D FROM R1 JOIN R2 ON R1.C = R2.C");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        assertEquals(3, pn.getOutputSchema().getColumns().size());
        assertTrue("R1".equalsIgnoreCase(pn.getOutputSchema().getColumns().get(0).getTableName()));
        assertTrue("R2".equalsIgnoreCase(pn.getOutputSchema().getColumns().get(1).getTableName()));

        // The output table for C canbe either R1 or R2 because it's an INNER join
        pn = compile("select R1.A, C, R1.D FROM R1 JOIN R2 USING(C)");
        n = pn.getChild(0).getChild(0);
        String table = pn.getOutputSchema().getColumns().get(1).getTableName();
        assertTrue(n instanceof NestLoopPlanNode);
        assertEquals(3, pn.getOutputSchema().getColumns().size());
        assertTrue(pn.getOutputSchema().getColumns().get(0).getTableName().equalsIgnoreCase("R1"));
        assertTrue("R2".equalsIgnoreCase(table) || "R1".equalsIgnoreCase(table));

        // Column from USING expression can not have qualifier in the SELECT clause
        failToCompile("select R1.C FROM R1 JOIN R2 USING(C)",
                      "user lacks privilege or object not found: R1.C");
        failToCompile("select R2.C FROM R1 JOIN R2 USING(C)",
                      "user lacks privilege or object not found: R2.C");
        failToCompile("select R2.C FROM R1 JOIN R2 USING(X)",
                      "user lacks privilege or object not found: X");
        failToCompile("select R2.C FROM R1 JOIN R2 ON R1.X = R2.X",
                      "user lacks privilege or object not found: R1.X");
    }

    public void testBasicThreeTableInnerJoin() {
        AbstractPlanNode pn = compile("select * FROM R1 JOIN R2 ON R1.C = R2.C JOIN R3 ON R3.C = R2.C");
        AbstractPlanNode n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        assertTrue(n.getChild(0) instanceof NestLoopPlanNode);
        assertTrue(n.getChild(1) instanceof SeqScanPlanNode);
        assertEquals(7, pn.getOutputSchema().getColumns().size());

        pn = compile("select R1.C, R2.C R3.C FROM R1 INNER JOIN R2 ON R1.C = R2.C INNER JOIN R3 ON R3.C = R2.C");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        assertTrue(n.getChild(0) instanceof NestLoopPlanNode);
        assertTrue(n.getChild(1) instanceof SeqScanPlanNode);

        pn = compile("select C FROM R1 INNER JOIN R2 USING (C) INNER JOIN R3 USING(C)");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        assertTrue(n.getChild(0) instanceof NestLoopPlanNode);
        assertTrue(n.getChild(1) instanceof SeqScanPlanNode);
        assertEquals(1, pn.getOutputSchema().getColumns().size());

        pn = compile("select C FROM R1 INNER JOIN R2 USING (C), R3 WHERE R1.A = R3.A");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        assertTrue(n.getChild(0) instanceof NestLoopIndexPlanNode);
        assertTrue(n.getChild(1) instanceof SeqScanPlanNode);
        assertEquals(1, pn.getOutputSchema().getColumns().size());

        failToCompile("select C, R3.C FROM R1 INNER JOIN R2 USING (C) INNER JOIN R3 ON R1.C = R3.C",
                      "user lacks privilege or object not found: R1.C");
    }

    public void testScanJoinConditions() {
        AbstractPlanNode pn = compile("select * FROM R1 WHERE R1.C = 0");
        AbstractPlanNode n = pn.getChild(0);
        assertTrue(n instanceof AbstractScanPlanNode);
        AbstractScanPlanNode scan = (AbstractScanPlanNode) n;
        AbstractExpression p = scan.getPredicate();
        assertEquals(ExpressionType.COMPARE_EQUAL, p.getExpressionType());

        pn = compile("select * FROM R1, R2 WHERE R1.A = R2.A AND R1.C > 0");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof AbstractJoinPlanNode);
        p = ((AbstractJoinPlanNode) n).getJoinPredicate();
        assertEquals(ExpressionType.COMPARE_EQUAL, p.getExpressionType());
        n = n.getChild(0);
        assertTrue(n instanceof AbstractScanPlanNode);
        assertTrue(((AbstractScanPlanNode) n).getTargetTableName().equalsIgnoreCase("R1"));
        p = ((AbstractScanPlanNode) n).getPredicate();
        assertEquals(ExpressionType.COMPARE_GREATERTHAN, p.getExpressionType());

        pn = compile("select * FROM R1, R2 WHERE R1.A = R2.A AND R1.C > R2.C");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof AbstractJoinPlanNode);
        p = ((AbstractJoinPlanNode) n).getJoinPredicate();
        assertEquals(ExpressionType.CONJUNCTION_AND, p.getExpressionType());
        if (p.getLeft().getExpressionType() == ExpressionType.COMPARE_EQUAL) {
            assertEquals(ExpressionType.COMPARE_EQUAL, p.getLeft().getExpressionType());
            assertEquals(ExpressionType.COMPARE_LESSTHAN, p.getRight().getExpressionType());
        } else {
            assertEquals(ExpressionType.COMPARE_LESSTHAN, p.getLeft().getExpressionType());
            assertEquals(ExpressionType.COMPARE_EQUAL, p.getRight().getExpressionType());
        }
        assertNull(((AbstractScanPlanNode)n.getChild(0)).getPredicate());
        assertNull(((AbstractScanPlanNode)n.getChild(1)).getPredicate());

        pn = compile("select * FROM R1 JOIN R2 ON R1.A = R2.A WHERE R1.C > 0");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof AbstractJoinPlanNode);
        p = ((AbstractJoinPlanNode) n).getJoinPredicate();
        assertEquals(ExpressionType.COMPARE_EQUAL, p.getExpressionType());
        n = n.getChild(0);
        assertTrue(n instanceof AbstractScanPlanNode);
        assertTrue("R1".equalsIgnoreCase(((AbstractScanPlanNode) n).getTargetTableName()));
        p = ((AbstractScanPlanNode) n).getPredicate();
        assertEquals(ExpressionType.COMPARE_GREATERTHAN, p.getExpressionType());

        pn = compile("select * FROM R1 JOIN R2 ON R1.A = R2.A WHERE R1.C > R2.C");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof AbstractJoinPlanNode);
        p = ((AbstractJoinPlanNode) n).getJoinPredicate();
        assertEquals(ExpressionType.CONJUNCTION_AND, p.getExpressionType());
        if (ExpressionType.COMPARE_EQUAL == p.getLeft().getExpressionType()) {
            assertEquals(ExpressionType.COMPARE_EQUAL, p.getLeft().getExpressionType());
            assertEquals(ExpressionType.COMPARE_LESSTHAN, p.getRight().getExpressionType());
        } else {
            assertEquals(ExpressionType.COMPARE_LESSTHAN, p.getLeft().getExpressionType());
            assertEquals(ExpressionType.COMPARE_EQUAL, p.getRight().getExpressionType());
        }
        assertNull(((AbstractScanPlanNode)n.getChild(0)).getPredicate());
        assertNull(((AbstractScanPlanNode)n.getChild(1)).getPredicate());

        pn = compile("select * FROM R1, R2, R3 WHERE R1.A = R2.A AND R1.C = R3.C AND R1.A > 0");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof AbstractJoinPlanNode);
        p = ((AbstractJoinPlanNode) n).getJoinPredicate();
        assertEquals(ExpressionType.COMPARE_EQUAL, p.getExpressionType());
        AbstractPlanNode c = n.getChild(0);
        assertTrue(c instanceof AbstractJoinPlanNode);
        p = ((AbstractJoinPlanNode) c).getJoinPredicate();
        assertEquals(ExpressionType.COMPARE_EQUAL, p.getExpressionType());
        c = c.getChild(0);
        assertTrue(c instanceof AbstractScanPlanNode);
        p = ((AbstractScanPlanNode) c).getPredicate();
        assertEquals(ExpressionType.COMPARE_GREATERTHAN, p.getExpressionType());
        c = n.getChild(1);
        assertTrue("R3".equalsIgnoreCase(((AbstractScanPlanNode) c).getTargetTableName()));
        assertEquals(null, ((AbstractScanPlanNode) c).getPredicate());

        pn = compile("select * FROM R1 JOIN R2 on R1.A = R2.A AND R1.C = R2.C where R1.A > 0");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof AbstractJoinPlanNode);
        p = ((AbstractJoinPlanNode) n).getJoinPredicate();
        assertEquals(ExpressionType.CONJUNCTION_AND, p.getExpressionType());
        assertEquals(ExpressionType.COMPARE_EQUAL, p.getLeft().getExpressionType());
        assertEquals(ExpressionType.COMPARE_EQUAL, p.getRight().getExpressionType());
        n = n.getChild(0);
        assertTrue(n instanceof AbstractScanPlanNode);
        assertTrue("R1".equalsIgnoreCase(((AbstractScanPlanNode) n).getTargetTableName()));
        p = ((AbstractScanPlanNode) n).getPredicate();
        assertEquals(ExpressionType.COMPARE_GREATERTHAN, p.getExpressionType());

        pn = compile("select A,C FROM R1 JOIN R2 USING (A, C)");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof AbstractJoinPlanNode);
        p = ((AbstractJoinPlanNode) n).getJoinPredicate();
        assertEquals(ExpressionType.CONJUNCTION_AND, p.getExpressionType());
        assertEquals(ExpressionType.COMPARE_EQUAL, p.getLeft().getExpressionType());
        assertEquals(ExpressionType.COMPARE_EQUAL, p.getRight().getExpressionType());

        pn = compile("select A,C FROM R1 JOIN R2 USING (A, C) WHERE A > 0");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof AbstractJoinPlanNode);
        p = ((AbstractJoinPlanNode) n).getJoinPredicate();
        assertEquals(ExpressionType.CONJUNCTION_AND, p.getExpressionType());
        assertEquals(ExpressionType.COMPARE_EQUAL, p.getLeft().getExpressionType());
        assertEquals(ExpressionType.COMPARE_EQUAL, p.getRight().getExpressionType());
        n = n.getChild(1);
        assertTrue(n instanceof AbstractScanPlanNode);
        scan = (AbstractScanPlanNode) n;
        assertTrue(scan.getPredicate() != null);
        assertEquals(ExpressionType.COMPARE_GREATERTHAN, scan.getPredicate().getExpressionType());

        pn = compile("select * FROM R1 JOIN R2 ON R1.A = R2.A JOIN R3 ON R1.C = R3.C WHERE R1.A > 0");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        p = ((NestLoopPlanNode) n).getJoinPredicate();
        assertEquals(ExpressionType.COMPARE_EQUAL, p.getExpressionType());
        n = n.getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        NestLoopPlanNode nlj = (NestLoopPlanNode) n;
        assertEquals(ExpressionType.COMPARE_EQUAL, nlj.getJoinPredicate().getExpressionType());
        n = n.getChild(0);
        assertTrue(n instanceof AbstractScanPlanNode);
        assertTrue(((AbstractScanPlanNode) n).getTargetTableName().equalsIgnoreCase("R1"));
        p = ((AbstractScanPlanNode) n).getPredicate();
        assertEquals(ExpressionType.COMPARE_GREATERTHAN, p.getExpressionType());
    }

    public void testDisplayColumnFromUsingCondition() {
        AbstractPlanNode pn = compile("select  max(A) FROM R1 JOIN R2 USING(A)");
        pn = pn.getChild(0);
        assertNotNull(AggregatePlanNode.getInlineAggregationNode(pn));
        assertTrue(pn instanceof NestLoopPlanNode);
        NodeSchema ns = pn.getOutputSchema();
        for (SchemaColumn sc : ns.getColumns()) {
            AbstractExpression e = sc.getExpression();
            assertTrue(e instanceof TupleValueExpression);
            TupleValueExpression tve = (TupleValueExpression) e;
            assertNotSame(-1, tve.getColumnIndex());
        }

        pn = compile("select distinct(A) FROM R1 JOIN R2 USING(A)");
        pn = pn.getChild(0);
        assertTrue(pn instanceof NestLoopPlanNode);
        ns = pn.getOutputSchema();
        for (SchemaColumn sc : ns.getColumns()) {
            AbstractExpression e = sc.getExpression();
            assertTrue(e instanceof TupleValueExpression);
            TupleValueExpression tve = (TupleValueExpression) e;
            assertNotSame(-1, tve.getColumnIndex());
        }

        pn = compile("select  A  FROM R1 JOIN R2 USING(A) ORDER BY A");
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        ns = pn.getOutputSchema();
        for (SchemaColumn sc : ns.getColumns()) {
            AbstractExpression e = sc.getExpression();
            assertTrue(e instanceof TupleValueExpression);
            TupleValueExpression tve = (TupleValueExpression) e;
            assertNotSame(-1, tve.getColumnIndex());
        }
        pn = pn.getChild(0);
        assertTrue(pn instanceof OrderByPlanNode);
        ns = pn.getOutputSchema();
        for (SchemaColumn sc : ns.getColumns()) {
            AbstractExpression e = sc.getExpression();
            assertTrue(e instanceof TupleValueExpression);
            TupleValueExpression tve = (TupleValueExpression) e;
            assertNotSame(-1, tve.getColumnIndex());
        }

        List<AbstractPlanNode> apl;
        AbstractPlanNode node;
        SeqScanPlanNode seqScan;
        NestLoopPlanNode nlj;

        apl = compileToFragments("select * FROM P1 LABEL JOIN R2 USING(A) WHERE A > 0 and R2.C >= 5");
        pn = apl.get(1);
        node = pn.getChild(0);
        assertTrue(node instanceof NestLoopPlanNode);
        assertEquals(ExpressionType.COMPARE_EQUAL,
                     ((NestLoopPlanNode)node).getJoinPredicate().getExpressionType());
        assertTrue(node.getChild(0) instanceof SeqScanPlanNode);
        seqScan = (SeqScanPlanNode)node.getChild(0);
        assertTrue(seqScan.getPredicate() == null);
        node = node.getChild(1);
        assertTrue(node instanceof SeqScanPlanNode);
        seqScan = (SeqScanPlanNode)node;
        assertEquals(ExpressionType.CONJUNCTION_AND, seqScan.getPredicate().getExpressionType());

        apl = compileToFragments("select * FROM P1 LABEL LEFT JOIN R2 USING(A) WHERE A > 0");
        pn = apl.get(1);
        node = pn.getChild(0);
        assertTrue(node instanceof NestLoopPlanNode);
        nlj = (NestLoopPlanNode) node;
        assertTrue(JoinType.LEFT == nlj.getJoinType());
        assertEquals(ExpressionType.COMPARE_EQUAL, nlj.getJoinPredicate().getExpressionType());
        seqScan = (SeqScanPlanNode)node.getChild(0);
        assertTrue(seqScan.getPredicate() != null);
        assertEquals(ExpressionType.COMPARE_GREATERTHAN, seqScan.getPredicate().getExpressionType());

        apl = compileToFragments("select A FROM R2 LABEL RIGHT JOIN P1 AP1 USING(A) WHERE A > 0");
        pn = apl.get(0);
        ns = pn.getOutputSchema();
        assertEquals(1, ns.size());
        SchemaColumn sc = ns.getColumns().get(0);
        assertEquals("AP1", sc.getTableAlias());
        assertEquals("P1", sc.getTableName());
        pn = apl.get(1);
        node = pn.getChild(0);
        assertTrue(node instanceof NestLoopPlanNode);
        nlj = (NestLoopPlanNode) node;
        assertTrue(JoinType.LEFT == nlj.getJoinType());
        assertEquals(ExpressionType.COMPARE_EQUAL, nlj.getJoinPredicate().getExpressionType());
        seqScan = (SeqScanPlanNode)node.getChild(0);
        assertTrue(seqScan.getPredicate() != null);
        assertEquals(ExpressionType.COMPARE_GREATERTHAN, seqScan.getPredicate().getExpressionType());
        ns = seqScan.getOutputSchema();
        assertEquals(1, ns.size());
        sc = ns.getColumns().get(0);
        assertEquals("AP1", sc.getTableAlias());
        assertEquals("P1", sc.getTableName());

    }

    public void testTransitiveValueEquivalenceConditions() {
        // R1.A = R2.A AND R2.A = 1 => R1.A = 1 AND R2.A = 1
        AbstractPlanNode pn = compile("select * FROM R1 LEFT JOIN R2 ON R1.A = R2.A AND R2.A = 1 ");
        AbstractPlanNode n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof AbstractJoinPlanNode);
        AbstractJoinPlanNode jn = (AbstractJoinPlanNode) n;
        assertNull(jn.getJoinPredicate());
        AbstractExpression p = jn.getPreJoinPredicate();
        assertNotNull(p);
        assertEquals(ExpressionType.COMPARE_EQUAL, p.getExpressionType());
        assertEquals(ExpressionType.VALUE_CONSTANT, p.getLeft().getExpressionType());
        assertEquals(ExpressionType.VALUE_TUPLE, p.getRight().getExpressionType());
        assertTrue(jn.getChild(1) instanceof SeqScanPlanNode);
        SeqScanPlanNode ssn = (SeqScanPlanNode)jn.getChild(1);
        assertNotNull(ssn.getPredicate());
        p = ssn.getPredicate();
        assertEquals(ExpressionType.COMPARE_EQUAL, p.getExpressionType());
        assertEquals(ExpressionType.VALUE_TUPLE, p.getLeft().getExpressionType());
        assertEquals(ExpressionType.VALUE_CONSTANT, p.getRight().getExpressionType());

        // Same test but now R2 is outer table R1.A = R2.A AND R2.A = 1 => R1.A = 1 AND R2.A = 1
        pn = compile("select * FROM R2 LEFT JOIN R1 ON R1.A = R2.A AND R2.A = 1 ");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof AbstractJoinPlanNode);
        jn = (AbstractJoinPlanNode) n;
        assertNull(jn.getJoinPredicate());
        p = jn.getPreJoinPredicate();
        assertNotNull(p);
        assertEquals(ExpressionType.COMPARE_EQUAL, p.getExpressionType());
        assertEquals(ExpressionType.VALUE_TUPLE, p.getLeft().getExpressionType());
        assertEquals(ExpressionType.VALUE_CONSTANT, p.getRight().getExpressionType());
        assertTrue(jn.getChild(1) instanceof SeqScanPlanNode);
        ssn = (SeqScanPlanNode)jn.getChild(1);
        assertNotNull(ssn.getPredicate());
        p = ssn.getPredicate();
        assertEquals(ExpressionType.COMPARE_EQUAL, p.getExpressionType());
        assertEquals(ExpressionType.VALUE_TUPLE, p.getLeft().getExpressionType());
        assertEquals(ExpressionType.VALUE_CONSTANT, p.getRight().getExpressionType());

        // R1.A = R2.A AND R2.C = 1 => R1.A = R2.A AND R2.C = 1
        pn = compile("select * FROM R1 LEFT JOIN R2 ON R1.A = R2.A AND R2.C = 1 ");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof AbstractJoinPlanNode);
        p = ((AbstractJoinPlanNode) n).getJoinPredicate();
        assertNotNull(p);
        assertEquals(ExpressionType.COMPARE_EQUAL, p.getExpressionType());
        AbstractExpression l = p.getLeft();
        AbstractExpression r = p.getRight();
        assertEquals(ExpressionType.VALUE_TUPLE, l.getExpressionType());
        assertEquals(ExpressionType.VALUE_TUPLE, r.getExpressionType());

        // R1.A = R2.A AND ABS(R2.C) = 1 => R1.A = R2.A AND ABS(R2.C) = 1
        pn = compile("select * FROM R1 LEFT JOIN R2 ON R1.A = R2.A AND ABS(R2.C) = 1 ");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof AbstractJoinPlanNode);
        p = ((AbstractJoinPlanNode) n).getJoinPredicate();
        assertNotNull(p);
        assertEquals(ExpressionType.COMPARE_EQUAL, p.getExpressionType());
        l = p.getLeft();
        r = p.getRight();
        assertEquals(ExpressionType.VALUE_TUPLE, l.getExpressionType());
        assertEquals(ExpressionType.VALUE_TUPLE, r.getExpressionType());

        // R1.A = R3.A - NLIJ
        pn = compile("select * FROM R1 LEFT JOIN R3 ON R1.A = R3.A");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopIndexPlanNode);

        // R1.A = R3.A and R1.A = 4 =>  R3.A = 4 and R1.A = 4  -- NLJ/IndexScan
        pn = compile("select * FROM R1 LEFT JOIN R3 ON R1.A = R3.A and R1.A = 4");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        p = ((AbstractJoinPlanNode) n).getJoinPredicate();
        // R1.A = R3.A and R3.A = 4 =>  R3.A = 4 and R1.A = 4  -- NLJ/IndexScan
        pn = compile("select * FROM R1 LEFT JOIN R3 ON R1.A = R3.A and R3.A = 4");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        p = ((AbstractJoinPlanNode) n).getJoinPredicate();


    }

    public void testFunctionJoinConditions() {
        AbstractPlanNode pn = compile("select * FROM R1 JOIN R2 ON ABS(R1.A) = ABS(R2.A) ");
        AbstractPlanNode n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof AbstractJoinPlanNode);
        AbstractExpression p = ((AbstractJoinPlanNode) n).getJoinPredicate();
        assertEquals(ExpressionType.COMPARE_EQUAL, p.getExpressionType());
        assertEquals(ExpressionType.FUNCTION, p.getLeft().getExpressionType());
        assertEquals(ExpressionType.FUNCTION, p.getRight().getExpressionType());

        pn = compile("select * FROM R1 ,R2 WHERE ABS(R1.A) = ABS(R2.A) ");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof AbstractJoinPlanNode);
        p = ((AbstractJoinPlanNode) n).getJoinPredicate();
        assertEquals(ExpressionType.COMPARE_EQUAL, p.getExpressionType());
        assertEquals(ExpressionType.FUNCTION, p.getLeft().getExpressionType());
        assertEquals(ExpressionType.FUNCTION, p.getRight().getExpressionType());

        pn = compile("select * FROM R1 ,R2");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof AbstractJoinPlanNode);
        p = ((AbstractJoinPlanNode) n).getJoinPredicate();
        assertNull(p);

        // USING expression can have only comma separated list of column names
        failToCompile("select * FROM R1 JOIN R2 USING (ABS(A))",
                      "user lacks privilege or object not found: ABS");
    }

    public void testIndexJoinConditions() {
        AbstractPlanNode pn = compile("select * FROM R3 WHERE R3.A = 0");
        AbstractPlanNode n = pn.getChild(0);
        assertTrue(n instanceof IndexScanPlanNode);
        assertNull(((IndexScanPlanNode) n).getPredicate());

        pn = compile("select * FROM R3 WHERE R3.A > 0 and R3.A < 5 and R3.C = 4");
        n = pn.getChild(0);
        assertTrue(n instanceof IndexScanPlanNode);
        IndexScanPlanNode indexScan = (IndexScanPlanNode) n;
        AbstractExpression p = indexScan.getPredicate();
        assertEquals(ExpressionType.COMPARE_EQUAL, p.getExpressionType());
        p = indexScan.getEndExpression();
        assertEquals(ExpressionType.COMPARE_LESSTHAN, p.getExpressionType());
        assertEquals(IndexLookupType.GT, indexScan.getLookupType());

        pn = compile("select * FROM R3, R2 WHERE R3.A = R2.A AND R3.C > 0 and R2.C >= 5");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopIndexPlanNode);
        assertNull(((NestLoopIndexPlanNode) n).getJoinPredicate());
        indexScan = (IndexScanPlanNode)n.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        assertEquals(IndexLookupType.EQ, indexScan.getLookupType());
        assertEquals(ExpressionType.COMPARE_EQUAL, indexScan.getEndExpression().getExpressionType());
        assertEquals(ExpressionType.COMPARE_GREATERTHAN, indexScan.getPredicate().getExpressionType());
        AbstractPlanNode seqScan = n.getChild(0);
        assertTrue(seqScan instanceof SeqScanPlanNode);
        assertEquals(ExpressionType.COMPARE_GREATERTHANOREQUALTO, ((SeqScanPlanNode)seqScan).getPredicate().getExpressionType());

        pn = compile("select * FROM R3 JOIN R2 ON R3.A = R2.A WHERE R3.C > 0 and R2.C >= 5");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopIndexPlanNode);
        assertNull(((NestLoopIndexPlanNode) n).getJoinPredicate());
        indexScan = (IndexScanPlanNode)n.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        assertEquals(IndexLookupType.EQ, indexScan.getLookupType());
        assertEquals(ExpressionType.COMPARE_EQUAL, indexScan.getEndExpression().getExpressionType());
        seqScan = n.getChild(0);
        assertTrue(seqScan instanceof SeqScanPlanNode);
        assertEquals(ExpressionType.COMPARE_GREATERTHANOREQUALTO, ((SeqScanPlanNode)seqScan).getPredicate().getExpressionType());

        pn = compile("select * FROM R3 JOIN R2 USING(A) WHERE R3.C > 0 and R2.C >= 5");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopIndexPlanNode);
        assertNull(((NestLoopIndexPlanNode) n).getJoinPredicate());
        indexScan = (IndexScanPlanNode)n.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        assertEquals(IndexLookupType.EQ, indexScan.getLookupType());
        assertEquals(ExpressionType.COMPARE_EQUAL, indexScan.getEndExpression().getExpressionType());
        seqScan = n.getChild(0);
        assertTrue(seqScan instanceof SeqScanPlanNode);
        assertEquals(ExpressionType.COMPARE_GREATERTHANOREQUALTO, ((SeqScanPlanNode)seqScan).getPredicate().getExpressionType());

        pn = compile("select * FROM R3 JOIN R2 ON R3.A = R2.A JOIN R1 ON R2.A = R1.A WHERE R3.C > 0 and R2.C >= 5");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        p = ((NestLoopPlanNode) n).getJoinPredicate();
        assertEquals(ExpressionType.COMPARE_EQUAL, p.getExpressionType());
        assertEquals(ExpressionType.VALUE_TUPLE, p.getLeft().getExpressionType());
        assertEquals(ExpressionType.VALUE_TUPLE, p.getRight().getExpressionType());
        seqScan = n.getChild(1);
        assertTrue(seqScan instanceof SeqScanPlanNode);
        n = n.getChild(0);
        assertTrue(n instanceof NestLoopIndexPlanNode);
        NestLoopIndexPlanNode nlij = (NestLoopIndexPlanNode) n;
        indexScan = (IndexScanPlanNode)nlij.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        assertEquals(IndexLookupType.EQ, indexScan.getLookupType());
        assertEquals(ExpressionType.COMPARE_EQUAL, indexScan.getEndExpression().getExpressionType());
        seqScan = nlij.getChild(0);
        assertTrue(seqScan instanceof SeqScanPlanNode);
        assertEquals(ExpressionType.COMPARE_GREATERTHANOREQUALTO, ((SeqScanPlanNode)seqScan).getPredicate().getExpressionType());

    }

    public void testIndexInnerJoin() {
        AbstractPlanNode pn;
        AbstractPlanNode n;
        NestLoopIndexPlanNode nli;
        AbstractPlanNode c0;
        pn = compile("select * FROM R3 JOIN R1 ON R1.C = R3.A");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopIndexPlanNode);
        assertTrue(n.getChild(0) instanceof SeqScanPlanNode);
        assertNotNull(n.getInlinePlanNode(PlanNodeType.INDEXSCAN));

        // Test ORDER BY optimization on indexed self-join, ordering by LHS
        pn = compile("select X.A FROM R5 X, R5 Y WHERE X.A = Y.A ORDER BY X.A");
        n = pn.getChild(0);
        assertTrue(n instanceof ProjectionPlanNode);
        n = n.getChild(0);
        assertTrue(n instanceof NestLoopIndexPlanNode);
        nli = (NestLoopIndexPlanNode) n;
        assertEquals(1, nli.getChildCount());
        c0 = nli.getChild(0);
        assertTrue(c0 instanceof IndexScanPlanNode);
        assertTrue(((IndexScanPlanNode) c0).getTargetTableAlias().equalsIgnoreCase("X"));

        // Test ORDER BY optimization on indexed self-join, ordering by RHS
        pn = compile("select X.A FROM R5 X, R5 Y WHERE X.A = Y.A ORDER BY Y.A");
        n = pn.getChild(0);
        assertTrue(n instanceof ProjectionPlanNode);
        n = n.getChild(0);
        assertTrue(n instanceof NestLoopIndexPlanNode);
        nli = (NestLoopIndexPlanNode) n;
        assertEquals(1, nli.getChildCount());
        c0 = nli.getChild(0);
        assertTrue(c0 instanceof IndexScanPlanNode);
        assertTrue(((IndexScanPlanNode) c0).getTargetTableAlias().equalsIgnoreCase("Y"));

        // Test safety guarding misapplication of ORDER BY optimization on indexed self-join,
        // when ordering by combination of LHS and RHS columns.
        // These MAY become valid optimization cases when ENG-4728 is done,
        // using transitive equality to determine that the ORDER BY clause can be re-expressed
        // as being based on only one of the two table scans.
        pn = compile("select X.A, X.C FROM R4 X, R4 Y WHERE X.A = Y.A ORDER BY X.A, Y.C");
        n = pn.getChild(0);
        assertTrue(n instanceof ProjectionPlanNode);
        n = n.getChild(0);
        assertTrue(n instanceof OrderByPlanNode);
        n = n.getChild(0);
        assertTrue(n instanceof NestLoopIndexPlanNode);

        pn = compile("select X.A FROM R4 X, R4 Y WHERE X.A = Y.A ORDER BY Y.A, X.C");
        n = pn.getChild(0);
        assertTrue(n instanceof ProjectionPlanNode);
        n = n.getChild(0);
        assertTrue(n instanceof OrderByPlanNode);
        n = n.getChild(0);
        assertTrue(n instanceof NestLoopIndexPlanNode);
    }

    public void testMultiColumnJoin() {
        // Test multi column condition on non index columns
        AbstractPlanNode pn = compile("select A, C FROM R2 JOIN R1 USING(A, C)");
        AbstractPlanNode n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        NestLoopPlanNode nlj = (NestLoopPlanNode) n;
        AbstractExpression pred = nlj.getJoinPredicate();
        assertNotNull(pred);
        assertEquals(ExpressionType.CONJUNCTION_AND, pred.getExpressionType());

        pn = compile("select R1.A, R2.A FROM R2 JOIN R1 on R1.A = R2.A and R1.C = R2.C");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        nlj = (NestLoopPlanNode) n;
        pred = nlj.getJoinPredicate();
        assertNotNull(pred);
        assertEquals(ExpressionType.CONJUNCTION_AND, pred.getExpressionType());

       // Test multi column condition on index columns
        pn = compile("select A FROM R2 JOIN R3 USING(A)");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopIndexPlanNode);
        NestLoopIndexPlanNode nlij = (NestLoopIndexPlanNode) n;
        assertEquals(IndexLookupType.EQ, ((IndexScanPlanNode) nlij.getInlinePlanNode(PlanNodeType.INDEXSCAN)).getLookupType());

        pn = compile("select R3.A, R2.A FROM R2 JOIN R3 ON R3.A = R2.A");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopIndexPlanNode);
        nlij = (NestLoopIndexPlanNode) n;
        pred = ((IndexScanPlanNode) nlij.getInlinePlanNode(PlanNodeType.INDEXSCAN)).getPredicate();
        assertEquals(IndexLookupType.EQ, ((IndexScanPlanNode) nlij.getInlinePlanNode(PlanNodeType.INDEXSCAN)).getLookupType());

        pn = compile("select A, C FROM R3 JOIN R2 USING(A, C)");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopIndexPlanNode);
        nlij = (NestLoopIndexPlanNode) n;
        pred = ((IndexScanPlanNode) nlij.getInlinePlanNode(PlanNodeType.INDEXSCAN)).getPredicate();
        assertNotNull(pred);
        assertEquals(ExpressionType.COMPARE_EQUAL, pred.getExpressionType());

        pn = compile("select R3.A, R2.A FROM R3 JOIN R2 ON R3.A = R2.A AND R3.C = R2.C");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopIndexPlanNode);
        nlij = (NestLoopIndexPlanNode) n;
        pred = ((IndexScanPlanNode) nlij.getInlinePlanNode(PlanNodeType.INDEXSCAN)).getPredicate();
        assertNotNull(pred);
        assertEquals(ExpressionType.COMPARE_EQUAL, pred.getExpressionType());
}

    public void testDistributedInnerJoin() {
        // JOIN replicated and one distributed table
        AbstractPlanNode pn = compile("select * FROM R1 JOIN P2 ON R1.C = P2.A");
        AbstractPlanNode n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof ReceivePlanNode);

        // Join multiple distributed tables on the partitioned column
        pn = compile("select * FROM P1 JOIN P2 USING(A)");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof ReceivePlanNode);

        // Two Distributed tables join on non-partitioned column
        failToCompile("select * FROM P1 JOIN P2 ON P1.C = P2.E",
                      "Join of multiple partitioned tables has insufficient join criteria.");
    }

    public void testBasicOuterJoin() {
        // select * with ON clause should return all columns from all tables
        AbstractPlanNode pn = compile("select * FROM R1 LEFT JOIN R2 ON R1.C = R2.C");
        AbstractPlanNode n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        NestLoopPlanNode nl = (NestLoopPlanNode) n;
        assertEquals(JoinType.LEFT, nl.getJoinType());
        assertEquals(2, nl.getChildCount());
        AbstractPlanNode c0 = nl.getChild(0);
        assertTrue(c0 instanceof SeqScanPlanNode);
        assertTrue("R1".equalsIgnoreCase(((SeqScanPlanNode) c0).getTargetTableName()));
        AbstractPlanNode c1 = nl.getChild(1);
        assertTrue(c0 instanceof SeqScanPlanNode);
        assertTrue("R2".equalsIgnoreCase(((SeqScanPlanNode) c1).getTargetTableName()));

        pn = compile("select * FROM R1 LEFT JOIN R2 ON R1.C = R2.C AND R1.A = 5");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        nl = (NestLoopPlanNode) n;
        assertEquals(JoinType.LEFT, nl.getJoinType());
        assertEquals(2, nl.getChildCount());
        c0 = nl.getChild(0);
        assertTrue(c0 instanceof SeqScanPlanNode);
        assertTrue("R1".equalsIgnoreCase(((SeqScanPlanNode) c0).getTargetTableName()));
        c1 = nl.getChild(1);
        assertTrue(c0 instanceof SeqScanPlanNode);
        assertTrue("R2".equalsIgnoreCase(((SeqScanPlanNode) c1).getTargetTableName()));
    }

    public void testRightOuterJoin() {
        // select * FROM R1 RIGHT JOIN R2 ON R1.C = R2.C => select * FROM R2 LEFT JOIN R1 ON R1.C = R2.C
        AbstractPlanNode pn = compile("select * FROM R1 RIGHT JOIN R2 ON R1.C = R2.C");
        AbstractPlanNode n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        NestLoopPlanNode nl = (NestLoopPlanNode) n;
        assertEquals(JoinType.LEFT, nl.getJoinType());
        assertEquals(2, nl.getChildCount());
        AbstractPlanNode c0 = nl.getChild(0);
        assertTrue(c0 instanceof SeqScanPlanNode);
        assertTrue("R2".equalsIgnoreCase(((SeqScanPlanNode) c0).getTargetTableName()));
        AbstractPlanNode c1 = nl.getChild(1);
        assertTrue(c1 instanceof SeqScanPlanNode);
        assertTrue("R1".equalsIgnoreCase(((SeqScanPlanNode) c1).getTargetTableName()));

        // Same but with distributed table
        pn = compile("select * FROM P1 RIGHT JOIN R2 ON P1.C = R2.C");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        nl = (NestLoopPlanNode) n;
        assertEquals(JoinType.LEFT, nl.getJoinType());
        assertEquals(2, nl.getChildCount());
        c0 = nl.getChild(0);
        assertTrue(c0 instanceof SeqScanPlanNode);
        assertTrue("R2".equalsIgnoreCase(((SeqScanPlanNode) c0).getTargetTableName()));
        c1 = nl.getChild(1);
        assertTrue(c1 instanceof ReceivePlanNode);

    }

    public void testSeqScanOuterJoinCondition() {
        // R1.C = R2.C Inner-Outer join Expr stays at the NLJ as Join predicate
        AbstractPlanNode pn = compile("select * FROM R1 LEFT JOIN R2 ON R1.C = R2.C");
        AbstractPlanNode n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        NestLoopPlanNode nl = (NestLoopPlanNode) n;
        assertEquals(ExpressionType.COMPARE_EQUAL, nl.getJoinPredicate().getExpressionType());
        assertNull(nl.getWherePredicate());
        assertEquals(2, nl.getChildCount());
        SeqScanPlanNode c0 = (SeqScanPlanNode) nl.getChild(0);
        assertNull(c0.getPredicate());
        SeqScanPlanNode c1 = (SeqScanPlanNode) nl.getChild(1);
        assertNull(c1.getPredicate());

        // R1.C = R2.C Inner-Outer join Expr stays at the NLJ as Join predicate
        // R1.A > 0 Outer Join Expr stays at the the NLJ as pre-join predicate
        // R2.A < 0 Inner Join Expr is pushed down to the inner SeqScan node
        pn = compile("select * FROM R1 LEFT JOIN R2 ON R1.C = R2.C AND R1.A > 0 AND R2.A < 0");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        nl = (NestLoopPlanNode) n;
        assertNotNull(nl.getPreJoinPredicate());
        AbstractExpression p = nl.getPreJoinPredicate();
        assertEquals(ExpressionType.COMPARE_GREATERTHAN, p.getExpressionType());
        assertNotNull(nl.getJoinPredicate());
        p = nl.getJoinPredicate();
        assertEquals(ExpressionType.COMPARE_EQUAL, p.getExpressionType());
        assertNull(nl.getWherePredicate());
        assertEquals(2, nl.getChildCount());
        c0 = (SeqScanPlanNode) nl.getChild(0);
        assertNull(c0.getPredicate());
        c1 = (SeqScanPlanNode) nl.getChild(1);
        assertNotNull(c1.getPredicate());
        p = c1.getPredicate();
        assertEquals(ExpressionType.COMPARE_LESSTHAN, p.getExpressionType());

        // R1.C = R2.C Inner-Outer join Expr stays at the NLJ as Join predicate
        // (R1.A > 0 OR R2.A < 0) Inner-Outer join Expr stays at the NLJ as Join predicate
        pn = compile("select * FROM R1 LEFT JOIN R2 ON R1.C = R2.C AND (R1.A > 0 OR R2.A < 0)");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        nl = (NestLoopPlanNode) n;
        p = nl.getJoinPredicate();
        assertEquals(ExpressionType.CONJUNCTION_AND, p.getExpressionType());
        if (ExpressionType.CONJUNCTION_OR == p.getLeft().getExpressionType()) {
            assertEquals(ExpressionType.CONJUNCTION_OR, p.getLeft().getExpressionType());
        } else {
            assertEquals(ExpressionType.CONJUNCTION_OR, p.getRight().getExpressionType());
        }
        assertNull(nl.getWherePredicate());
        assertEquals(2, nl.getChildCount());
        c0 = (SeqScanPlanNode) nl.getChild(0);
        assertNull(c0.getPredicate());
        c1 = (SeqScanPlanNode) nl.getChild(1);
        assertNull(c1.getPredicate());

        // R1.C = R2.C Inner-Outer join Expr stays at the NLJ as Join predicate
        // R1.A > 0 Outer Where Expr is pushed down to the outer SeqScan node
        // R2.A IS NULL Inner Where Expr stays at the the NLJ as post join (where) predicate
        // (R1.C > R2.C OR R2.C IS NULL) Inner-Outer Where stays at the the NLJ as post join (where) predicate
        pn = compile("select * FROM R1 LEFT JOIN R2 ON R1.C = R2.C WHERE R1.A > 0 AND R2.A IS NULL AND (R1.C > R2.C OR R2.C IS NULL)");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        nl = (NestLoopPlanNode) n;
        assertEquals(JoinType.LEFT, nl.getJoinType());
        assertNotNull(nl.getJoinPredicate());
        p = nl.getJoinPredicate();
        assertEquals(ExpressionType.COMPARE_EQUAL, p.getExpressionType());
        AbstractExpression w = nl.getWherePredicate();
        assertNotNull(w);
        assertEquals(ExpressionType.CONJUNCTION_AND, w.getExpressionType());
        assertEquals(ExpressionType.OPERATOR_IS_NULL, w.getRight().getExpressionType());
        assertEquals(ExpressionType.CONJUNCTION_OR, w.getLeft().getExpressionType());
        assertEquals(2, nl.getChildCount());
        c0 = (SeqScanPlanNode) nl.getChild(0);
        assertEquals(ExpressionType.COMPARE_GREATERTHAN, c0.getPredicate().getExpressionType());
        c1 = (SeqScanPlanNode) nl.getChild(1);
        assertNull(c1.getPredicate());

        // R3.A = R2.A Inner-Outer index join Expr. NLJ predicate.
        // R3.A > 3 Index Outer where expr pushed down to IndexScanPlanNode
        // R3.C < 0 non-index Outer where expr pushed down to IndexScanPlanNode as a predicate
        pn = compile("select * FROM R3 LEFT JOIN R2 ON R3.A = R2.A WHERE R3.A > 3 AND R3.C < 0");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        nl = (NestLoopPlanNode) n;
        assertEquals(JoinType.LEFT, nl.getJoinType());
        AbstractPlanNode outerScan = n.getChild(0);
        assertTrue(outerScan instanceof IndexScanPlanNode);
        IndexScanPlanNode indexScan = (IndexScanPlanNode) outerScan;
        assertEquals(IndexLookupType.GT, indexScan.getLookupType());
        assertNotNull(indexScan.getPredicate());
        assertEquals(ExpressionType.COMPARE_LESSTHAN, indexScan.getPredicate().getExpressionType());

        // R3.C = R2.C Inner-Outer non-index join Expr. NLJ predicate.
        // R3.A > 3 Index null rejecting inner where expr pushed down to IndexScanPlanNode
        // NLJ is simplified to be INNER
        pn = compile("select * FROM R2 LEFT JOIN R3 ON R3.C = R2.C WHERE R3.A > 3");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        nl = (NestLoopPlanNode) n;
        assertEquals(JoinType.INNER, nl.getJoinType());
        outerScan = n.getChild(1);
        assertTrue(outerScan instanceof IndexScanPlanNode);
        indexScan = (IndexScanPlanNode) outerScan;
        assertEquals(IndexLookupType.GT, indexScan.getLookupType());
        assertNull(indexScan.getPredicate());

        pn = compile("select * FROM R2 LEFT JOIN R3 ON R3.A = R2.C WHERE R3.A > 3");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopIndexPlanNode);
        NestLoopIndexPlanNode nli = (NestLoopIndexPlanNode) n;
        assertEquals(JoinType.INNER, nli.getJoinType());
   }

    public void testDistributedSeqScanOuterJoinCondition() {
        // Distributed Outer table
        List<AbstractPlanNode> lpn;
        AbstractPlanNode pn;
        AbstractPlanNode n;
        lpn = compileToFragments("select * FROM P1 LEFT JOIN R2 ON P1.C = R2.C");
        assertEquals(2, lpn.size());
        n = lpn.get(1).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        assertEquals(2, n.getChildCount());
        assertTrue(n.getChild(0) instanceof SeqScanPlanNode);
        assertTrue(n.getChild(1) instanceof SeqScanPlanNode);

        // Distributed Inner table
        pn = compile("select * FROM R2 LEFT JOIN P1 ON P1.C = R2.C");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        NestLoopPlanNode nl = (NestLoopPlanNode) n;
        assertEquals(2, nl.getChildCount());
        assertTrue(nl.getChild(0) instanceof SeqScanPlanNode);
        assertTrue(nl.getChild(1) instanceof ReceivePlanNode);

        // Distributed Inner and Outer table joined on the partition column
        lpn = compileToFragments("select * FROM P1 LEFT JOIN P4 ON P1.A = P4.A");
        assertEquals(2, lpn.size());
        n = lpn.get(1).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        assertEquals(2, n.getChildCount());
        assertTrue(n.getChild(0) instanceof SeqScanPlanNode);
        assertTrue(n.getChild(1) instanceof SeqScanPlanNode);

        // Distributed Inner and Outer table joined on the non-partition column
        failToCompile("select * FROM P1 LEFT JOIN P4 ON P1.A = P4.E",
                "Join of multiple partitioned tables has insufficient join criteria");
    }

    public void testBasicIndexOuterJoin() {
        // R3 is indexed but it's the outer table and the join expression must stay at the NLJ
        // so index can't be used
        AbstractPlanNode pn = compile("select * FROM R3 LEFT JOIN R2 ON R3.A = R2.C");
        AbstractPlanNode n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        NestLoopPlanNode nl = (NestLoopPlanNode) n;
        assertEquals(JoinType.LEFT, nl.getJoinType());
        assertEquals(2, nl.getChildCount());
        AbstractPlanNode c0 = nl.getChild(0);
        assertTrue(c0 instanceof SeqScanPlanNode);
        assertTrue(((SeqScanPlanNode) c0).getTargetTableName().equalsIgnoreCase("R3"));
        AbstractPlanNode c1 = nl.getChild(1);
        assertTrue(c0 instanceof SeqScanPlanNode);
        assertTrue(((SeqScanPlanNode) c1).getTargetTableName().equalsIgnoreCase("R2"));

        // R3 is indexed but it's the outer table so index can't be used
        pn = compile("select * FROM R2 RIGHT JOIN R3 ON R3.A = R2.C");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        nl = (NestLoopPlanNode) n;
        assertEquals(JoinType.LEFT, nl.getJoinType());
        assertEquals(2, nl.getChildCount());
        c0 = nl.getChild(0);
        assertTrue(c0 instanceof SeqScanPlanNode);
        assertTrue(((SeqScanPlanNode) c0).getTargetTableName().equalsIgnoreCase("R3"));
        c1 = nl.getChild(1);
        assertTrue(c0 instanceof SeqScanPlanNode);
        assertTrue(((SeqScanPlanNode) c1).getTargetTableName().equalsIgnoreCase("R2"));

        pn = compile("select * FROM R2 LEFT JOIN R3 ON R2.C = R3.A");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopIndexPlanNode);
        NestLoopIndexPlanNode nli = (NestLoopIndexPlanNode) n;
        assertEquals(JoinType.LEFT, nli.getJoinType());
        assertEquals(1, nli.getChildCount());
        c0 = nli.getChild(0);
        assertTrue(c0 instanceof SeqScanPlanNode);
        assertTrue(((SeqScanPlanNode) c0).getTargetTableName().equalsIgnoreCase("R2"));
        c1 = nli.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        assertNotNull(c1);
        assertTrue(((IndexScanPlanNode) c1).getTargetTableName().equalsIgnoreCase("R3"));
      }

    public void testIndexOuterJoinConditions() {
        // R1.C = R3.A Inner-Outer index join Expr. NLIJ/Inlined IndexScan
        // R3.C > 0 Inner Join Expr is pushed down to the inlined IndexScan node as a predicate
        // R2.A < 6 Outer Join Expr is a pre-join predicate for NLIJ
        AbstractPlanNode pn = compile("select * FROM R2 LEFT JOIN R3 ON R3.A = R2.A AND R3.C > 0 AND R2.A < 6");
        AbstractPlanNode n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopIndexPlanNode);
        NestLoopIndexPlanNode nlij = (NestLoopIndexPlanNode) n;
        assertEquals(JoinType.LEFT, nlij.getJoinType());
        assertNotNull(nlij.getPreJoinPredicate());
        AbstractExpression p = nlij.getPreJoinPredicate();
        assertEquals(ExpressionType.COMPARE_LESSTHAN, p.getExpressionType());
        assertNull(nlij.getJoinPredicate());
        assertNull(nlij.getWherePredicate());
        IndexScanPlanNode indexScan = (IndexScanPlanNode)n.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        assertEquals(IndexLookupType.EQ, indexScan.getLookupType());
        assertEquals(ExpressionType.COMPARE_EQUAL, indexScan.getEndExpression().getExpressionType());
        assertEquals(ExpressionType.COMPARE_GREATERTHAN, indexScan.getPredicate().getExpressionType());
        AbstractPlanNode c1 = n.getChild(0);
        assertTrue(c1 instanceof SeqScanPlanNode);
        assertNull(((SeqScanPlanNode)c1).getPredicate());

        // R1.C = R3.A Inner-Outer non-index join Expr. NLJ/IndexScan
        // R3.A > 0 Inner index Join Expr is pushed down to the inner IndexScan node as an index
        // R3.C != 0 Non-index Inner Join Expression is pushed down to the inner IndexScan node as a predicate
        // R2.A < 6 Outer Join Expr is a pre-join predicate for NLJ
        pn = compile("select * FROM R2 LEFT JOIN R3 ON R3.C = R2.A AND R3.A > 0 AND R3.C != 0 AND R2.A < 6");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        NestLoopPlanNode nlj = (NestLoopPlanNode) n;
        assertEquals(JoinType.LEFT, nlj.getJoinType());
        assertNotNull(nlj.getPreJoinPredicate());
        p = nlj.getPreJoinPredicate();
        assertEquals(ExpressionType.COMPARE_LESSTHAN, p.getExpressionType());
        assertNotNull(nlj.getJoinPredicate());
        assertEquals(ExpressionType.COMPARE_EQUAL, nlj.getJoinPredicate().getExpressionType());
        assertNull(nlj.getWherePredicate());
        c1 = n.getChild(0);
        assertTrue(c1 instanceof SeqScanPlanNode);
        assertNull(((SeqScanPlanNode)c1).getPredicate());
        AbstractPlanNode c2 = n.getChild(1);
        assertTrue(c2 instanceof IndexScanPlanNode);
        indexScan = (IndexScanPlanNode) c2;
        assertEquals(IndexLookupType.GT, indexScan.getLookupType());
        assertNotNull(indexScan.getPredicate());
        assertEquals(ExpressionType.COMPARE_NOTEQUAL, indexScan.getPredicate().getExpressionType());

        // R2.A = R3.A Inner-Outer index join Expr. NLIJ/Inlined IndexScan
        // R3.A IS NULL Inner where expr - part of the NLIJ where predicate
        // R2.A < 6 OR R3.C IS NULL Inner-Outer where expr - part of the NLIJ where predicate
        // R2.A > 3 Outer where expr - pushed down to the outer node
        pn = compile("select * FROM R2 LEFT JOIN R3 ON R3.A = R2.A WHERE R3.A IS NULL AND R2.A > 3 AND (R2.A < 6 OR R3.C IS NULL)");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopIndexPlanNode);
        assertEquals(((NestLoopIndexPlanNode) n).getJoinType(), JoinType.LEFT);
        assertNull(((NestLoopIndexPlanNode) n).getPreJoinPredicate());
        assertNull(((NestLoopIndexPlanNode) n).getJoinPredicate());
        assertNotNull(((NestLoopIndexPlanNode) n).getWherePredicate());
        AbstractExpression w = ((NestLoopIndexPlanNode) n).getWherePredicate();
        assertEquals(ExpressionType.CONJUNCTION_AND, w.getExpressionType());
        assertEquals(ExpressionType.OPERATOR_IS_NULL, w.getRight().getExpressionType());
        assertEquals(ExpressionType.CONJUNCTION_OR, w.getLeft().getExpressionType());
        indexScan = (IndexScanPlanNode)n.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        assertEquals(IndexLookupType.EQ, indexScan.getLookupType());
        assertEquals(ExpressionType.COMPARE_EQUAL, indexScan.getEndExpression().getExpressionType());
        c1 = n.getChild(0);
        assertTrue(c1 instanceof SeqScanPlanNode);
        assertEquals(ExpressionType.COMPARE_GREATERTHAN, ((SeqScanPlanNode)c1).getPredicate().getExpressionType());

    }

    public void testDistributedInnerOuterTable() {
        // Distributed Outer table
        List<AbstractPlanNode> lpn;
        AbstractPlanNode pn;
        AbstractPlanNode n;
        lpn = compileToFragments("select * FROM P1 LEFT JOIN R2 ON P1.C = R2.C");
        assertEquals(2, lpn.size());
        n = lpn.get(1).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        assertEquals(2, n.getChildCount());
        assertTrue(n.getChild(0) instanceof SeqScanPlanNode);
        assertTrue(n.getChild(1) instanceof SeqScanPlanNode);

        // Distributed Inner table
        pn = compile("select * FROM R2 LEFT JOIN P1 ON P1.C = R2.C");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        NestLoopPlanNode nl = (NestLoopPlanNode) n;
        assertEquals(2, nl.getChildCount());
        assertTrue(nl.getChild(0) instanceof SeqScanPlanNode);
        assertTrue(nl.getChild(1) instanceof ReceivePlanNode);

        // Distributed Inner and Outer table joined on the partition column
        lpn = compileToFragments("select * FROM P1 LEFT JOIN P4 ON P1.A = P4.A");
        assertEquals(2, lpn.size());
        n = lpn.get(1).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        assertEquals(2, n.getChildCount());
        assertTrue(n.getChild(0) instanceof SeqScanPlanNode);
        assertTrue(n.getChild(1) instanceof SeqScanPlanNode);

        // Distributed Inner and Outer table joined on the non-partition column
        failToCompile("select * FROM P1 LEFT JOIN P4 ON P1.A = P4.E",
                "Join of multiple partitioned tables has insufficient join criteria");
    }

    public void testDistributedIndexJoinConditions() {
        // Distributed outer table, replicated inner -NLIJ/inlined IndexScan
        List<AbstractPlanNode> lpn;
        //AbstractPlanNode pn;
        AbstractPlanNode n;
        lpn = compileToFragments("select * FROM P1 LEFT JOIN R3 ON P1.C = R3.A");
        assertEquals(2, lpn.size());
        n = lpn.get(1).getChild(0);
        assertTrue(n instanceof NestLoopIndexPlanNode);
        assertEquals(1, n.getChildCount());
        assertTrue(n.getChild(0) instanceof SeqScanPlanNode);

        // Distributed inner  and replicated outer tables -NLJ/IndexScan
        lpn = compileToFragments("select *  FROM R3 LEFT JOIN P2 ON R3.A = P2.A AND P2.A < 0 AND P2.E > 3 WHERE P2.A IS NULL");
        assertEquals(2, lpn.size());
        for (AbstractPlanNode apn: lpn) {
            System.out.println(apn.toExplainPlanString());
        }

        n = lpn.get(0).getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        assertEquals(JoinType.LEFT, ((NestLoopPlanNode) n).getJoinType());
        assertNotNull(((NestLoopPlanNode) n).getJoinPredicate());
        assertNotNull(((NestLoopPlanNode) n).getWherePredicate());
        AbstractPlanNode c = n.getChild(0);
        assertTrue(c instanceof SeqScanPlanNode);
        c = n.getChild(1);
        assertTrue(c instanceof ReceivePlanNode);
        n = lpn.get(1).getChild(0);
        assertTrue(n instanceof IndexScanPlanNode);
        IndexScanPlanNode in = (IndexScanPlanNode) n;
        assertEquals(IndexLookupType.LT, in.getLookupType());

        assertNotNull(in.getPredicate());
        assertEquals(ExpressionType.CONJUNCTION_AND, in.getPredicate().getExpressionType());
        assertEquals(ExpressionType.COMPARE_GREATERTHAN, in.getPredicate().getLeft().getExpressionType());
        assertEquals(ExpressionType.OPERATOR_NOT, in.getPredicate().getRight().getExpressionType());

        // Distributed inner  and outer tables -NLIJ/inlined IndexScan
        lpn = compileToFragments("select *  FROM P2 RIGHT JOIN P3 ON P3.A = P2.A AND P2.A < 0 WHERE P2.A IS NULL");
        assertEquals(2, lpn.size());
        n = lpn.get(1).getChild(0);
        assertTrue(n instanceof NestLoopIndexPlanNode);
        assertEquals(JoinType.LEFT, ((NestLoopIndexPlanNode) n).getJoinType());
        assertNull(((NestLoopIndexPlanNode) n).getJoinPredicate());
        assertNotNull(((NestLoopIndexPlanNode) n).getWherePredicate());
        AbstractExpression w = ((NestLoopIndexPlanNode) n).getWherePredicate();
        assertEquals(ExpressionType.OPERATOR_IS_NULL, w.getExpressionType());
        IndexScanPlanNode indexScan = (IndexScanPlanNode)n.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        assertEquals(IndexLookupType.EQ, indexScan.getLookupType());
        assertEquals(ExpressionType.COMPARE_EQUAL, indexScan.getEndExpression().getExpressionType());
        w = indexScan.getPredicate();
        assertNotNull(w);
        assertEquals(ExpressionType.COMPARE_LESSTHAN, w.getExpressionType());
    }


   public void testNonSupportedJoin() {
       // JOIN with parentheses (HSQL limitation)
       failToCompile("select R2.C FROM (R1 JOIN R2 ON R1.C = R2.C) JOIN R3 ON R1.C = R3.C",
                     "user lacks privilege or object not found: R1.C");
       // JOIN with join hierarchy (HSQL limitation)
       failToCompile("select * FROM R1 JOIN R2 JOIN R3 ON R1.C = R2.C ON R1.C = R3.C",
                     "unexpected token");
       // FUUL JOIN. Temporary restriction
       failToCompile("select R1.C FROM R1 FULL JOIN R2 ON R1.C = R2.C",
                     "VoltDB does not support full outer joins");
       failToCompile("select R1.C FROM R1 FULL OUTER JOIN R2 ON R1.C = R2.C",
                     "VoltDB does not support full outer joins");
   }


   public void testOuterJoinSimplification() {
       AbstractPlanNode pn, n;
       AbstractExpression ex;

       pn = compile("select * FROM R1 LEFT JOIN R2 ON R1.C = R2.C WHERE R2.C IS NOT NULL");
       n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopPlanNode);
       assertEquals(((NestLoopPlanNode) n).getJoinType(), JoinType.INNER);

       pn = compile("select * FROM R1 LEFT JOIN R2 ON R1.C = R2.C WHERE R2.C > 0");
       n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopPlanNode);
       assertEquals(((NestLoopPlanNode) n).getJoinType(), JoinType.INNER);

       pn = compile("select * FROM R1 RIGHT JOIN R2 ON R1.C = R2.C WHERE R1.C > 0");
       n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopPlanNode);
       assertEquals(((NestLoopPlanNode) n).getJoinType(), JoinType.INNER);

       pn = compile("select * FROM R1 LEFT JOIN R3 ON R1.C = R3.C WHERE R3.A > 0");
       n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopPlanNode);
       assertEquals(((NestLoopPlanNode) n).getJoinType(), JoinType.INNER);

       pn = compile("select * FROM R1 LEFT JOIN R3 ON R1.C = R3.A WHERE R3.A > 0");
       n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopIndexPlanNode);
       assertEquals(((NestLoopIndexPlanNode) n).getJoinType(), JoinType.INNER);

       pn = compile("select * FROM R1 LEFT JOIN R2 ON R1.C = R2.C WHERE ABS(R2.C) <  10");
       n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopPlanNode);
       assertEquals(((NestLoopPlanNode) n).getJoinType(), JoinType.INNER);

       pn = compile("select * FROM R1 RIGHT JOIN R2 ON R1.C = R2.C WHERE ABS(R1.C) <  10");
       n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopPlanNode);
       assertEquals(((NestLoopPlanNode) n).getJoinType(), JoinType.INNER);

       pn = compile("select * FROM R1 LEFT JOIN R2 ON R1.C = R2.C WHERE ABS(R1.C) <  10");
       n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopPlanNode);
       assertEquals(((NestLoopPlanNode) n).getJoinType(), JoinType.LEFT);

       pn = compile("select * FROM R1 RIGHT JOIN R2 ON R1.C = R2.C WHERE ABS(R2.C) <  10");
       n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopPlanNode);
       assertEquals(((NestLoopPlanNode) n).getJoinType(), JoinType.LEFT);

       pn = compile("select * FROM R1 LEFT JOIN R2 ON R1.C = R2.C WHERE ABS(R2.C) <  10 AND R1.C = 3");
       n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopPlanNode);
       assertEquals(((NestLoopPlanNode) n).getJoinType(), JoinType.INNER);

       pn = compile("select * FROM R1 LEFT JOIN R2 ON R1.C = R2.C WHERE ABS(R2.C) <  10 OR R2.C IS NOT NULL");
       n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopPlanNode);
       assertEquals(((NestLoopPlanNode) n).getJoinType(), JoinType.INNER);

       pn = compile("select * FROM R1 LEFT JOIN R2 ON R1.C = R2.C WHERE ABS(R1.C) <  10 AND R1.C > 3");
       n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopPlanNode);
       assertEquals(((NestLoopPlanNode) n).getJoinType(), JoinType.LEFT);

       pn = compile("select * FROM R1 LEFT JOIN R2 ON R1.C = R2.C WHERE ABS(R1.C) <  10 OR R2.C IS NOT NULL");
       n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopPlanNode);
       assertEquals(((NestLoopPlanNode) n).getJoinType(), JoinType.LEFT);

       // Test with seqscan with different filers.
       pn = compile("select R2.A, R1.* FROM R1 LEFT OUTER JOIN R2 ON R2.A = R1.A WHERE R2.A > 3");
       //* enable for debug */ System.out.println(pn.toExplainPlanString());
       n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopPlanNode);
       assertEquals(((NestLoopPlanNode) n).getJoinType(), JoinType.INNER);
       ex = ((NestLoopPlanNode) n).getWherePredicate();
       assertEquals(ex, null);

       pn = compile("select R2.A, R1.* FROM R1 LEFT OUTER JOIN R2 ON R2.A = R1.A WHERE R2.A IS NULL");
       n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopPlanNode);
       assertEquals(((NestLoopPlanNode) n).getJoinType(), JoinType.LEFT);
       ex = ((NestLoopPlanNode) n).getWherePredicate();
       assertEquals(ex instanceof OperatorExpression, true);

       pn = compile("select b.A, a.* FROM R1 a LEFT OUTER JOIN R4 b ON b.A = a.A AND b.C = a.C AND a.D = b.D WHERE b.A IS NULL");
       //* enable for debug */ System.out.println(pn.toExplainPlanString());
       n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopIndexPlanNode);
       assertEquals(((NestLoopIndexPlanNode) n).getJoinType(), JoinType.LEFT);
       ex = ((NestLoopIndexPlanNode) n).getWherePredicate();
       assertEquals(ex instanceof OperatorExpression, true);

       pn = compile("select b.A, a.* FROM R1 a LEFT OUTER JOIN R4 b ON b.A = a.A AND b.C = a.C AND a.D = b.D WHERE b.B + b.A IS NULL");
       n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopIndexPlanNode);
       assertEquals(((NestLoopIndexPlanNode) n).getJoinType(), JoinType.LEFT);
       ex = ((NestLoopIndexPlanNode) n).getWherePredicate();
       assertEquals(ex instanceof OperatorExpression, true);
       assertEquals(ex.getLeft() instanceof OperatorExpression, true);

       pn = compile("select a.* FROM R1 a LEFT OUTER JOIN R5 b ON b.A = a.A WHERE b.A IS NULL");
       n = pn.getChild(0).getChild(0);
       assertEquals(((NestLoopIndexPlanNode) n).getJoinType(), JoinType.LEFT);
       ex = ((NestLoopIndexPlanNode) n).getWherePredicate();
       assertEquals(ex instanceof OperatorExpression, true);
   }

   public void testMoreThan5TableJoins() {
       // INNER JOIN with >5 tables.
       compile("select R1.C FROM R3,R2, P1, P2, P3, R1 WHERE R3.A = R2.A and R2.A = P1.A and P1.A = P2.A and P3.A = P2.A and R1.C = R2.C");

       // OUTER JOIN with >5 tables.
       compile("select R1.C FROM R3,R2, P1, P2, P3 LEFT OUTER JOIN R1 ON R1.C = R2.C WHERE R3.A = R2.A and R2.A = P1.A and P1.A = P2.A and P3.A = P2.A");
   }

    @Override
    protected void setUp() throws Exception {
        setupSchema(TestJoinOrder.class.getResource("testplans-join-ddl.sql"), "testplansjoin", false);
    }

}
