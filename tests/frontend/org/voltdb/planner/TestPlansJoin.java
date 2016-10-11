/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import org.apache.commons.lang3.StringUtils;
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
    private static class JoinOp {
        private final String m_string;
        private final ExpressionType m_operator;
        private JoinOp(String string, ExpressionType operator) {
            m_string = string;
            m_operator = operator;
        }

        static JoinOp NOT_DISTINCT =
                new JoinOp("IS NOT DISTINCT FROM",
                        ExpressionType.COMPARE_NOTDISTINCT);
        static JoinOp EQUAL =
                new JoinOp("=", ExpressionType.COMPARE_EQUAL);

        static JoinOp[] JOIN_OPS = new JoinOp[] {EQUAL, NOT_DISTINCT};

        @Override
        public String toString() { return m_string; }
        ExpressionType toOperator() { return m_operator; }
    }

    public void testBasicInnerJoin() {
        for (JoinOp joinOp : JoinOp.JOIN_OPS) {
            perJoinOpTestBasicInnerJoin(joinOp);
        }
    }

    private void perJoinOpTestBasicInnerJoin(JoinOp joinOp) {
        // SELECT * with ON clause should return all columns from all tables
        AbstractPlanNode pn = compile("SELECT * FROM R1 JOIN R2 ON R1.C " + joinOp + " R2.C");
        AbstractPlanNode n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        //assertEquals(JoinType.INNER, nlj.getJoinType());
        for (int ii = 0; ii < 2; ii++) {
            assertTrue(n.getChild(ii) instanceof SeqScanPlanNode);
        }
        assertEquals(5, pn.getOutputSchema().getColumns().size());

        // SELECT * with USING clause should contain only one column for each column from the USING expression
        pn = compile("SELECT * FROM R1 JOIN R2 USING(C)");
        assertTrue(pn.getChild(0).getChild(0) instanceof NestLoopPlanNode);
        assertEquals(4, pn.getOutputSchema().getColumns().size());

        pn = compile("SELECT R1.A,R1.C,D FROM R1 JOIN R2 ON R1.C " + joinOp + " R2.C");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        assertEquals(3, pn.getOutputSchema().getColumns().size());

        pn = compile("SELECT R1.A,C,R1.D FROM R1 JOIN R2 USING(C)");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        assertEquals(3, pn.getOutputSchema().getColumns().size());

        pn = compile("SELECT R1.A, R2.C, R1.D FROM R1 JOIN R2 ON R1.C " + joinOp + " R2.C");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        assertEquals(3, pn.getOutputSchema().getColumns().size());
        assertTrue("R1".equalsIgnoreCase(pn.getOutputSchema().getColumns().get(0).getTableName()));
        assertTrue("R2".equalsIgnoreCase(pn.getOutputSchema().getColumns().get(1).getTableName()));

        // The output table for C canbe either R1 or R2 because it's an INNER join
        pn = compile("SELECT R1.A, C, R1.D FROM R1 JOIN R2 USING(C)");
        n = pn.getChild(0).getChild(0);
        String table = pn.getOutputSchema().getColumns().get(1).getTableName();
        assertTrue(n instanceof NestLoopPlanNode);
        assertEquals(3, pn.getOutputSchema().getColumns().size());
        assertTrue(pn.getOutputSchema().getColumns().get(0).getTableName().equalsIgnoreCase("R1"));
        assertTrue("R2".equalsIgnoreCase(table) || "R1".equalsIgnoreCase(table));

        failToCompile("SELECT R2.C FROM R1 JOIN R2 USING(X)",
                      "user lacks privilege or object not found: X");
        failToCompile("SELECT R2.C FROM R1 JOIN R2 ON R1.X " + joinOp + " R2.X",
                      "user lacks privilege or object not found: R1.X");
        failToCompile("SELECT * FROM R1 JOIN R2 ON R1.C " + joinOp + " R2.C AND 1",
                          "data type of expression is not boolean");
        failToCompile("SELECT * FROM R1 JOIN R2 ON R1.C " + joinOp + " R2.C AND MOD(3,1)=1",
                          "Join with filters that do not depend on joined tables is not supported in VoltDB");
    }

    public void testBasicThreeTableInnerJoin() {
        for (JoinOp joinOp : JoinOp.JOIN_OPS) {
            perJoinOpTestBasicThreeTableInnerJoin(joinOp);
        }
    }

    private void perJoinOpTestBasicThreeTableInnerJoin(JoinOp joinOp) {
        AbstractPlanNode pn = compile("SELECT * FROM R1 JOIN R2 ON R1.C " + joinOp + " R2.C JOIN R3 ON R3.C " + joinOp + " R2.C");
        AbstractPlanNode n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        assertTrue(n.getChild(0) instanceof NestLoopPlanNode);
        assertTrue(n.getChild(1) instanceof SeqScanPlanNode);
        assertEquals(7, pn.getOutputSchema().getColumns().size());

        pn = compile("SELECT R1.C, R2.C R3.C FROM R1 INNER JOIN R2 ON R1.C " + joinOp + " R2.C INNER JOIN R3 ON R3.C " + joinOp + " R2.C");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        assertTrue(n.getChild(0) instanceof NestLoopPlanNode);
        assertTrue(n.getChild(1) instanceof SeqScanPlanNode);

        pn = compile("SELECT C FROM R1 INNER JOIN R2 USING (C) INNER JOIN R3 USING(C)");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        assertTrue(n.getChild(0) instanceof NestLoopPlanNode);
        assertTrue(n.getChild(1) instanceof SeqScanPlanNode);
        assertEquals(1, pn.getOutputSchema().getColumns().size());

        pn = compile("SELECT C FROM R1 INNER JOIN R2 USING (C), R3_NOC WHERE R1.A " + joinOp + " R3_NOC.A");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        if (joinOp == JoinOp.EQUAL) { // weaken test for now
            assertTrue(n.getChild(0) instanceof NestLoopIndexPlanNode);
        }
        assertTrue(n.getChild(1) instanceof SeqScanPlanNode);
        assertEquals(1, pn.getOutputSchema().getColumns().size());
        // Here C could be the C from USING(C), which would be R1.C or R2.C, or else
        // R3.C.  Either is possible, and this is ambiguous.
        failToCompile("SELECT C FROM R1 INNER JOIN R2 USING (C), R3 WHERE R1.A " + joinOp + " R3.A",
                      "Column \"C\" is ambiguous");
    }

    public void testScanJoinConditions() {
        for (JoinOp joinOp : JoinOp.JOIN_OPS) {
            perJoinOpTestScanJoinConditions(joinOp);
        }
    }

    private void perJoinOpTestScanJoinConditions(JoinOp joinOp) {
        AbstractPlanNode pn = compile("SELECT * FROM R1 WHERE R1.C = 0");
        AbstractPlanNode n = pn.getChild(0);
        assertTrue(n instanceof AbstractScanPlanNode);
        AbstractScanPlanNode scan = (AbstractScanPlanNode) n;
        AbstractExpression p = scan.getPredicate();
        assertEquals(ExpressionType.COMPARE_EQUAL, p.getExpressionType());

        pn = compile("SELECT * FROM R1, R2 WHERE R1.A " + joinOp + " R2.A AND R1.C > 0");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof AbstractJoinPlanNode);
        p = ((AbstractJoinPlanNode) n).getJoinPredicate();
        assertEquals(joinOp.toOperator(), p.getExpressionType());
        n = n.getChild(0);
        assertTrue(n instanceof AbstractScanPlanNode);
        assertTrue(((AbstractScanPlanNode) n).getTargetTableName().equalsIgnoreCase("R1"));
        p = ((AbstractScanPlanNode) n).getPredicate();
        assertEquals(ExpressionType.COMPARE_GREATERTHAN, p.getExpressionType());

        pn = compile("SELECT * FROM R1, R2 WHERE R1.A " + joinOp + " R2.A AND R1.C > R2.C");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof AbstractJoinPlanNode);
        p = ((AbstractJoinPlanNode) n).getJoinPredicate();
        assertEquals(ExpressionType.CONJUNCTION_AND, p.getExpressionType());
        if (p.getLeft().getExpressionType() == joinOp.toOperator()) {
            assertEquals(ExpressionType.COMPARE_LESSTHAN, p.getRight().getExpressionType());
        }
        else {
            assertEquals(ExpressionType.COMPARE_LESSTHAN, p.getLeft().getExpressionType());
            assertEquals(joinOp.toOperator(), p.getRight().getExpressionType());
        }
        assertNull(((AbstractScanPlanNode)n.getChild(0)).getPredicate());
        assertNull(((AbstractScanPlanNode)n.getChild(1)).getPredicate());

        pn = compile("SELECT * FROM R1 JOIN R2 ON R1.A " + joinOp + " R2.A WHERE R1.C > 0");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof AbstractJoinPlanNode);
        p = ((AbstractJoinPlanNode) n).getJoinPredicate();
        assertEquals(joinOp.toOperator(), p.getExpressionType());
        n = n.getChild(0);
        assertTrue(n instanceof AbstractScanPlanNode);
        assertTrue("R1".equalsIgnoreCase(((AbstractScanPlanNode) n).getTargetTableName()));
        p = ((AbstractScanPlanNode) n).getPredicate();
        assertEquals(ExpressionType.COMPARE_GREATERTHAN, p.getExpressionType());

        pn = compile("SELECT * FROM R1 JOIN R2 ON R1.A " + joinOp + " R2.A WHERE R1.C > R2.C");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof AbstractJoinPlanNode);
        p = ((AbstractJoinPlanNode) n).getJoinPredicate();
        assertEquals(ExpressionType.CONJUNCTION_AND, p.getExpressionType());
        if (joinOp.toOperator() == p.getLeft().getExpressionType()) {
            assertEquals(ExpressionType.COMPARE_LESSTHAN, p.getRight().getExpressionType());
        }
        else {
            assertEquals(ExpressionType.COMPARE_LESSTHAN, p.getLeft().getExpressionType());
            assertEquals(joinOp.toOperator(), p.getRight().getExpressionType());
        }
        assertNull(((AbstractScanPlanNode)n.getChild(0)).getPredicate());
        assertNull(((AbstractScanPlanNode)n.getChild(1)).getPredicate());

        pn = compile("SELECT * FROM R1, R2, R3 WHERE R1.A " + joinOp + " R2.A AND R1.C " + joinOp + " R3.C AND R1.A > 0");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof AbstractJoinPlanNode);
        p = ((AbstractJoinPlanNode) n).getJoinPredicate();
        assertEquals(joinOp.toOperator(), p.getExpressionType());
        AbstractPlanNode c = n.getChild(0);
        assertTrue(c instanceof AbstractJoinPlanNode);
        p = ((AbstractJoinPlanNode) c).getJoinPredicate();
        assertEquals(joinOp.toOperator(), p.getExpressionType());
        c = c.getChild(0);
        assertTrue(c instanceof AbstractScanPlanNode);
        p = ((AbstractScanPlanNode) c).getPredicate();
        assertEquals(ExpressionType.COMPARE_GREATERTHAN, p.getExpressionType());
        c = n.getChild(1);
        assertTrue("R3".equalsIgnoreCase(((AbstractScanPlanNode) c).getTargetTableName()));
        assertEquals(null, ((AbstractScanPlanNode) c).getPredicate());

        pn = compile("SELECT * FROM R1 JOIN R2 ON R1.A " + joinOp + " R2.A AND R1.C " + joinOp + " R2.C WHERE R1.A > 0");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof AbstractJoinPlanNode);
        p = ((AbstractJoinPlanNode) n).getJoinPredicate();
        assertEquals(ExpressionType.CONJUNCTION_AND, p.getExpressionType());
        assertEquals(joinOp.toOperator(), p.getLeft().getExpressionType());
        assertEquals(joinOp.toOperator(), p.getRight().getExpressionType());
        n = n.getChild(0);
        assertTrue(n instanceof AbstractScanPlanNode);
        assertTrue("R1".equalsIgnoreCase(((AbstractScanPlanNode) n).getTargetTableName()));
        p = ((AbstractScanPlanNode) n).getPredicate();
        assertEquals(ExpressionType.COMPARE_GREATERTHAN, p.getExpressionType());

        pn = compile("SELECT A,C FROM R1 JOIN R2 USING (A, C)");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof AbstractJoinPlanNode);
        p = ((AbstractJoinPlanNode) n).getJoinPredicate();
        assertEquals(ExpressionType.CONJUNCTION_AND, p.getExpressionType());
        assertEquals(ExpressionType.COMPARE_EQUAL, p.getLeft().getExpressionType());
        assertEquals(ExpressionType.COMPARE_EQUAL, p.getRight().getExpressionType());

        pn = compile("SELECT A,C FROM R1 JOIN R2 USING (A, C) WHERE A > 0");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof AbstractJoinPlanNode);
        p = ((AbstractJoinPlanNode) n).getJoinPredicate();
        assertEquals(ExpressionType.CONJUNCTION_AND, p.getExpressionType());
        assertEquals(ExpressionType.COMPARE_EQUAL, p.getLeft().getExpressionType());
        assertEquals(ExpressionType.COMPARE_EQUAL, p.getRight().getExpressionType());
        n = n.getChild(0);
        assertTrue(n instanceof AbstractScanPlanNode);
        scan = (AbstractScanPlanNode) n;
        assertTrue(scan.getPredicate() != null);
        assertEquals(ExpressionType.COMPARE_GREATERTHAN, scan.getPredicate().getExpressionType());

        pn = compile("SELECT * FROM R1 JOIN R2 ON R1.A " + joinOp + " R2.A JOIN R3 ON R1.C " + joinOp + " R3.C WHERE R1.A > 0");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        p = ((NestLoopPlanNode) n).getJoinPredicate();
        assertEquals(joinOp.toOperator(), p.getExpressionType());
        n = n.getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        NestLoopPlanNode nlj = (NestLoopPlanNode) n;
        assertEquals(joinOp.toOperator(), nlj.getJoinPredicate().getExpressionType());
        n = n.getChild(0);
        assertTrue(n instanceof AbstractScanPlanNode);
        assertTrue(((AbstractScanPlanNode) n).getTargetTableName().equalsIgnoreCase("R1"));
        p = ((AbstractScanPlanNode) n).getPredicate();
        assertEquals(ExpressionType.COMPARE_GREATERTHAN, p.getExpressionType());
    }

    public void testDisplayColumnFromUsingCondition() {
        AbstractPlanNode pn = compile("SELECT max(A) FROM R1 JOIN R2 USING(A)");
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

        pn = compile("SELECT distinct(A) FROM R1 JOIN R2 USING(A)");
        pn = pn.getChild(0);
        assertTrue(pn instanceof NestLoopPlanNode);
        ns = pn.getOutputSchema();
        for (SchemaColumn sc : ns.getColumns()) {
            AbstractExpression e = sc.getExpression();
            assertTrue(e instanceof TupleValueExpression);
            TupleValueExpression tve = (TupleValueExpression) e;
            assertNotSame(-1, tve.getColumnIndex());
        }

        pn = compile("SELECT A FROM R1 JOIN R2 USING(A) ORDER BY A");
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

        apl = compileToFragments("SELECT * FROM P1 LABEL JOIN R2 USING(A) WHERE A > 0 AND R2.C >= 5");
        pn = apl.get(1);
        node = pn.getChild(0);
        assertTrue(node instanceof NestLoopPlanNode);
        assertEquals(ExpressionType.COMPARE_EQUAL,
                     ((NestLoopPlanNode)node).getJoinPredicate().getExpressionType());
        assertTrue(node.getChild(0) instanceof SeqScanPlanNode);
        seqScan = (SeqScanPlanNode)node.getChild(0);
        assertEquals(ExpressionType.CONJUNCTION_AND, seqScan.getPredicate().getExpressionType());
        node = node.getChild(1);
        assertTrue(node instanceof SeqScanPlanNode);
        seqScan = (SeqScanPlanNode)node;
        assertTrue(seqScan.getPredicate() == null);

        apl = compileToFragments("SELECT * FROM P1 LABEL LEFT JOIN R2 USING(A) WHERE A > 0");
        pn = apl.get(1);
        node = pn.getChild(0);
        assertTrue(node instanceof NestLoopPlanNode);
        nlj = (NestLoopPlanNode) node;
        assertTrue(JoinType.LEFT == nlj.getJoinType());
        assertEquals(ExpressionType.COMPARE_EQUAL, nlj.getJoinPredicate().getExpressionType());
        seqScan = (SeqScanPlanNode)node.getChild(0);
        assertTrue(seqScan.getPredicate() != null);
        assertEquals(ExpressionType.COMPARE_GREATERTHAN, seqScan.getPredicate().getExpressionType());

        apl = compileToFragments("SELECT A FROM R2 LABEL RIGHT JOIN P1 AP1 USING(A) WHERE A > 0");
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
        for (JoinOp joinOp : JoinOp.JOIN_OPS) {
            perJoinOpTestTransitiveValueEquivalenceConditions(joinOp);
        }
    }

    private void perJoinOpTestTransitiveValueEquivalenceConditions(JoinOp joinOp) {
        // R1.A " + joinOp + " R2.A AND R2.A = 1 => R1.A = 1 AND R2.A = 1
        AbstractPlanNode pn = compile("SELECT * FROM R1 LEFT JOIN R2 ON R1.A " + joinOp + " R2.A AND R2.A = 1 ");
        AbstractPlanNode n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof AbstractJoinPlanNode);
        AbstractJoinPlanNode jn = (AbstractJoinPlanNode) n;
        assertNull(jn.getJoinPredicate());
        AbstractExpression p = jn.getPreJoinPredicate();
        assertNotNull(p);
        assertEquals(joinOp.toOperator(), p.getExpressionType());
        if (p.getLeft().getExpressionType() == ExpressionType.VALUE_CONSTANT) {
            assertEquals(ExpressionType.VALUE_TUPLE, p.getRight().getExpressionType());
        }
        else {
            assertEquals(ExpressionType.VALUE_TUPLE, p.getLeft().getExpressionType());
            assertEquals(ExpressionType.VALUE_CONSTANT, p.getRight().getExpressionType());
        }
        assertTrue(jn.getChild(1) instanceof SeqScanPlanNode);
        SeqScanPlanNode ssn = (SeqScanPlanNode)jn.getChild(1);
        assertNotNull(ssn.getPredicate());
        p = ssn.getPredicate();
        assertEquals(ExpressionType.COMPARE_EQUAL, p.getExpressionType());
        assertEquals(ExpressionType.VALUE_TUPLE, p.getLeft().getExpressionType());
        assertEquals(ExpressionType.VALUE_CONSTANT, p.getRight().getExpressionType());

        // Same test but now R2 is outer table R1.A " + joinOp + " R2.A AND R2.A = 1 => R1.A = 1 AND R2.A = 1
        pn = compile("SELECT * FROM R2 LEFT JOIN R1 ON R1.A " + joinOp + " R2.A AND R2.A = 1 ");
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
        assertEquals(joinOp.toOperator(), p.getExpressionType());
        assertEquals(ExpressionType.VALUE_TUPLE, p.getLeft().getExpressionType());
        assertEquals(ExpressionType.VALUE_CONSTANT, p.getRight().getExpressionType());

        // R1.A " + joinOp + " R2.A AND R2.C = 1 => R1.A " + joinOp + " R2.A AND R2.C = 1
        pn = compile("SELECT * FROM R1 LEFT JOIN R2 ON R1.A " + joinOp + " R2.A AND R2.C = 1 ");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof AbstractJoinPlanNode);
        p = ((AbstractJoinPlanNode) n).getJoinPredicate();
        assertNotNull(p);
        assertEquals(joinOp.toOperator(), p.getExpressionType());
        AbstractExpression l = p.getLeft();
        AbstractExpression r = p.getRight();
        assertEquals(ExpressionType.VALUE_TUPLE, l.getExpressionType());
        assertEquals(ExpressionType.VALUE_TUPLE, r.getExpressionType());

        // R1.A " + joinOp + " R2.A AND ABS(R2.C) = 1 => R1.A " + joinOp + " R2.A AND ABS(R2.C) = 1
        pn = compile("SELECT * FROM R1 LEFT JOIN R2 ON R1.A " + joinOp + " R2.A AND ABS(R2.C) = 1 ");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof AbstractJoinPlanNode);
        p = ((AbstractJoinPlanNode) n).getJoinPredicate();
        assertNotNull(p);
        assertEquals(joinOp.toOperator(), p.getExpressionType());
        l = p.getLeft();
        r = p.getRight();
        assertEquals(ExpressionType.VALUE_TUPLE, l.getExpressionType());
        assertEquals(ExpressionType.VALUE_TUPLE, r.getExpressionType());

        // R1.A " + joinOp + " R3.A - NLIJ
        pn = compile("SELECT * FROM R1 LEFT JOIN R3 ON R1.A " + joinOp + " R3.A");
        n = pn.getChild(0).getChild(0);
        if (joinOp == JoinOp.EQUAL) { // weaken test for now
            assertTrue(n instanceof NestLoopIndexPlanNode);
        }

        // R1.A " + joinOp + " R3.A and R1.A = 4 =>  R3.A = 4 AND R1.A = 4  -- NLJ/IndexScan
        pn = compile("SELECT * FROM R1 LEFT JOIN R3 ON R1.A " + joinOp + " R3.A and R1.A = 4");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        p = ((AbstractJoinPlanNode) n).getJoinPredicate();
        // R1.A " + joinOp + " R3.A and R3.A = 4 =>  R3.A = 4 and R1.A = 4  -- NLJ/IndexScan
        pn = compile("SELECT * FROM R1 LEFT JOIN R3 ON R1.A " + joinOp + " R3.A AND R3.A = 4");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        p = ((AbstractJoinPlanNode) n).getJoinPredicate();


    }

    public void testFunctionJoinConditions() {
        AbstractPlanNode pn = compile("SELECT * FROM R1 JOIN R2 ON ABS(R1.A) = ABS(R2.A) ");
        AbstractPlanNode n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof AbstractJoinPlanNode);
        AbstractExpression p = ((AbstractJoinPlanNode) n).getJoinPredicate();
        assertEquals(ExpressionType.COMPARE_EQUAL, p.getExpressionType());
        assertEquals(ExpressionType.FUNCTION, p.getLeft().getExpressionType());
        assertEquals(ExpressionType.FUNCTION, p.getRight().getExpressionType());

        pn = compile("SELECT * FROM R1 ,R2 WHERE ABS(R1.A) = ABS(R2.A) ");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof AbstractJoinPlanNode);
        p = ((AbstractJoinPlanNode) n).getJoinPredicate();
        assertEquals(ExpressionType.COMPARE_EQUAL, p.getExpressionType());
        assertEquals(ExpressionType.FUNCTION, p.getLeft().getExpressionType());
        assertEquals(ExpressionType.FUNCTION, p.getRight().getExpressionType());

        pn = compile("SELECT * FROM R1 ,R2");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof AbstractJoinPlanNode);
        p = ((AbstractJoinPlanNode) n).getJoinPredicate();
        assertNull(p);

        // USING expression can have only comma separated list of column names
        failToCompile("SELECT * FROM R1 JOIN R2 USING (ABS(A))",
                      "user lacks privilege or object not found: ABS");
    }

    public void testIndexJoinConditions() {
        for (JoinOp joinOp : JoinOp.JOIN_OPS) {
            if (joinOp != JoinOp.EQUAL) { // weaken test for now
                continue;
            }
            perJoinOpTestIndexJoinConditions(joinOp);
        }
    }

    private void perJoinOpTestIndexJoinConditions(JoinOp joinOp) {
        AbstractPlanNode pn = compile("SELECT * FROM R3 WHERE R3.A = 0");
        AbstractPlanNode n = pn.getChild(0);
        assertTrue(n instanceof IndexScanPlanNode);
        assertNull(((IndexScanPlanNode) n).getPredicate());

        pn = compile("SELECT * FROM R3 WHERE R3.A > 0 AND R3.A < 5 AND R3.C = 4");
        n = pn.getChild(0);
        assertTrue(n instanceof IndexScanPlanNode);
        IndexScanPlanNode indexScan = (IndexScanPlanNode) n;
        AbstractExpression p = indexScan.getPredicate();
        assertEquals(ExpressionType.COMPARE_EQUAL, p.getExpressionType());
        p = indexScan.getEndExpression();
        assertEquals(ExpressionType.COMPARE_LESSTHAN, p.getExpressionType());
        assertEquals(IndexLookupType.GT, indexScan.getLookupType());

        pn = compile("SELECT * FROM R3, R2 WHERE R3.A " + joinOp + " R2.A AND R3.C > 0 AND R2.C >= 5");
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

        pn = compile("SELECT * FROM R3 JOIN R2 ON R3.A " + joinOp + " R2.A WHERE R3.C > 0 AND R2.C >= 5");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopIndexPlanNode);
        assertNull(((NestLoopIndexPlanNode) n).getJoinPredicate());
        indexScan = (IndexScanPlanNode)n.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        assertEquals(IndexLookupType.EQ, indexScan.getLookupType());
        assertEquals(ExpressionType.COMPARE_EQUAL, indexScan.getEndExpression().getExpressionType());
        seqScan = n.getChild(0);
        assertTrue(seqScan instanceof SeqScanPlanNode);
        assertEquals(ExpressionType.COMPARE_GREATERTHANOREQUALTO, ((SeqScanPlanNode)seqScan).getPredicate().getExpressionType());

        pn = compile("SELECT * FROM R3 JOIN R2 USING(A) WHERE R3.C > 0 AND R2.C >= 5");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopIndexPlanNode);
        assertNull(((NestLoopIndexPlanNode) n).getJoinPredicate());
        indexScan = (IndexScanPlanNode)n.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        assertEquals(IndexLookupType.EQ, indexScan.getLookupType());
        assertEquals(ExpressionType.COMPARE_EQUAL, indexScan.getEndExpression().getExpressionType());
        seqScan = n.getChild(0);
        assertTrue(seqScan instanceof SeqScanPlanNode);
        assertEquals(ExpressionType.COMPARE_GREATERTHANOREQUALTO, ((SeqScanPlanNode)seqScan).getPredicate().getExpressionType());

        pn = compile("SELECT * FROM R3 JOIN R2 ON R3.A " + joinOp + " R2.A JOIN R1 ON R2.A " + joinOp + " R1.A WHERE R3.C > 0 AND R2.C >= 5");
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

    public void testOpIndexInnerJoin() {
        for (JoinOp joinOp : JoinOp.JOIN_OPS) {
            if (joinOp != JoinOp.EQUAL) { // weaken test for now
                continue;
            }
            perJoinTestOpIndexInnerJoin(joinOp);
        }
    }

    private void perJoinTestOpIndexInnerJoin(JoinOp joinOp) {
        AbstractPlanNode pn;
        AbstractPlanNode n;
        NestLoopIndexPlanNode nli;
        AbstractPlanNode c0;
        pn = compile("SELECT * FROM R3 JOIN R1 ON R1.C " + joinOp + " R3.A");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopIndexPlanNode);
        assertTrue(n.getChild(0) instanceof SeqScanPlanNode);
        assertNotNull(n.getInlinePlanNode(PlanNodeType.INDEXSCAN));

        // Test ORDER BY optimization on indexed self-join, ordering by LHS
        pn = compile("SELECT X.A FROM R5 X, R5 Y WHERE X.A " + joinOp + " Y.A ORDER BY X.A");
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
        pn = compile("SELECT X.A FROM R5 X, R5 Y WHERE X.A " + joinOp + " Y.A ORDER BY Y.A");
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
        pn = compile("SELECT X.A, X.C FROM R4 X, R4 Y WHERE X.A " + joinOp + " Y.A ORDER BY X.A, Y.C");
        n = pn.getChild(0);
        assertTrue(n instanceof ProjectionPlanNode);
        n = n.getChild(0);
        assertTrue(n instanceof OrderByPlanNode);
        n = n.getChild(0);
        assertTrue(n instanceof NestLoopIndexPlanNode);

        pn = compile("SELECT X.A FROM R4 X, R4 Y WHERE X.A " + joinOp + " Y.A ORDER BY Y.A, X.C");
        n = pn.getChild(0);
        assertTrue(n instanceof ProjectionPlanNode);
        n = n.getChild(0);
        assertTrue(n instanceof OrderByPlanNode);
        n = n.getChild(0);
        assertTrue(n instanceof NestLoopIndexPlanNode);
    }

    public void testMultiColumnJoin() {
        for (JoinOp joinOp : JoinOp.JOIN_OPS) {
            perJoinOpTestMultiColumnJoin(joinOp);
        }
    }

    private void perJoinOpTestMultiColumnJoin(JoinOp joinOp) {
        // Test multi column condition on non index columns
        AbstractPlanNode pn = compile("SELECT A, C FROM R2 JOIN R1 USING(A, C)");
        AbstractPlanNode n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        NestLoopPlanNode nlj = (NestLoopPlanNode) n;
        AbstractExpression pred = nlj.getJoinPredicate();
        assertNotNull(pred);
        assertEquals(ExpressionType.CONJUNCTION_AND, pred.getExpressionType());

        pn = compile("SELECT R1.A, R2.A FROM R2 JOIN R1 ON R1.A " + joinOp + " R2.A AND R1.C " + joinOp + " R2.C");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        nlj = (NestLoopPlanNode) n;
        pred = nlj.getJoinPredicate();
        assertNotNull(pred);
        assertEquals(ExpressionType.CONJUNCTION_AND, pred.getExpressionType());

       // Test multi column condition on index columns
        pn = compile("SELECT A FROM R2 JOIN R3 USING(A)");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopIndexPlanNode);
        NestLoopIndexPlanNode nlij = (NestLoopIndexPlanNode) n;
        assertEquals(IndexLookupType.EQ, ((IndexScanPlanNode) nlij.getInlinePlanNode(PlanNodeType.INDEXSCAN)).getLookupType());

        pn = compile("SELECT R3.A, R2.A FROM R2 JOIN R3 ON R3.A " + joinOp + " R2.A");
        n = pn.getChild(0).getChild(0);
        if (joinOp == JoinOp.EQUAL) { // weaken test for now
            assertTrue(n instanceof NestLoopIndexPlanNode);
            nlij = (NestLoopIndexPlanNode) n;
            pred = ((IndexScanPlanNode) nlij.getInlinePlanNode(PlanNodeType.INDEXSCAN)).getPredicate();
            assertEquals(IndexLookupType.EQ, ((IndexScanPlanNode) nlij.getInlinePlanNode(PlanNodeType.INDEXSCAN)).getLookupType());
        }

        pn = compile("SELECT A, C FROM R3 JOIN R2 USING(A, C)");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopIndexPlanNode);
        nlij = (NestLoopIndexPlanNode) n;
        pred = ((IndexScanPlanNode) nlij.getInlinePlanNode(PlanNodeType.INDEXSCAN)).getPredicate();
        assertNotNull(pred);
        assertEquals(ExpressionType.COMPARE_EQUAL, pred.getExpressionType());

        pn = compile("SELECT R3.A, R2.A FROM R3 JOIN R2 ON R3.A " + joinOp + " R2.A AND R3.C " + joinOp + " R2.C");
        n = pn.getChild(0).getChild(0);
        if (joinOp != JoinOp.EQUAL) { // weaken test for now
            return;
        }
        assertTrue(n instanceof NestLoopIndexPlanNode);
        nlij = (NestLoopIndexPlanNode) n;
        pred = ((IndexScanPlanNode) nlij.getInlinePlanNode(PlanNodeType.INDEXSCAN)).getPredicate();
        assertNotNull(pred);
        assertEquals(joinOp.toOperator(), pred.getExpressionType());
    }

    public void testDistributedInnerJoin() {
        for (JoinOp joinOp : JoinOp.JOIN_OPS) {
            perJoinOpTestDistributedInnerJoin(joinOp);
        }
    }

    private void perJoinOpTestDistributedInnerJoin(JoinOp joinOp) {
        // JOIN replicated and one distributed table
        AbstractPlanNode pn;
        AbstractPlanNode n;

        pn = compile("SELECT * FROM R1 JOIN P2 ON R1.C " + joinOp + " P2.A");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof ReceivePlanNode);

        pn = compile("SELECT * FROM R1 JOIN P2 ON R1.C IS NOT DISTINCT FROM P2.A");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof ReceivePlanNode);

        // Join multiple distributed tables on the partitioned column
        pn = compile("SELECT * FROM P1 JOIN P2 USING(A)");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof ReceivePlanNode);

        // Two Distributed tables join on non-partitioned column
        failToCompile("SELECT * FROM P1 JOIN P2 ON P1.C " + joinOp + " P2.E",
                      "This query is not plannable.  The planner cannot guarantee that all rows would be in a single partition.");

        // Two Distributed tables join on non-partitioned column
        failToCompile("SELECT * FROM P1 JOIN P2 ON P1.C IS NOT DISTINCT FROM P2.E",
                      "This query is not plannable.  The planner cannot guarantee that all rows would be in a single partition.");

        // Two Distributed tables join on boolean constant
        failToCompile("SELECT * FROM P1 JOIN P2 ON 1=1",
                      "This query is not plannable.  The planner cannot guarantee that all rows would be in a single partition.");
    }

    public void testBasicOuterJoin() {
        for (JoinOp joinOp : JoinOp.JOIN_OPS) {
            perJoinOpTestBasicOuterJoin(joinOp);
        }
    }

    private void perJoinOpTestBasicOuterJoin(JoinOp joinOp) {
        // SELECT * with ON clause should return all columns from all tables
        AbstractPlanNode pn = compile("SELECT * FROM R1 LEFT JOIN R2 ON R1.C " + joinOp + " R2.C");
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

        pn = compile("SELECT * FROM R1 LEFT JOIN R2 ON R1.C " + joinOp + " R2.C AND R1.A = 5");
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
        for (JoinOp joinOp : JoinOp.JOIN_OPS) {
            perJoinOpTestRightOuterJoin(joinOp);
        }
    }

    private void perJoinOpTestRightOuterJoin(JoinOp joinOp) {
        // SELECT * FROM R1 RIGHT JOIN R2 ON R1.C " + joinOp + " R2.C => SELECT * FROM R2 LEFT JOIN R1 ON R1.C " + joinOp + " R2.C
        AbstractPlanNode pn = compile("SELECT * FROM R1 RIGHT JOIN R2 ON R1.C " + joinOp + " R2.C");
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
        pn = compile("SELECT * FROM P1 RIGHT JOIN R2 ON P1.C " + joinOp + " R2.C");
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
        for (JoinOp joinOp : JoinOp.JOIN_OPS) {
            perJoinOpTestSeqScanOuterJoinCondition(joinOp);
        }
    }

    private void perJoinOpTestSeqScanOuterJoinCondition(JoinOp joinOp) {
        // R1.C " + joinOp + " R2.C Inner-Outer join Expr stays at the NLJ as Join predicate
        AbstractPlanNode pn = compile("SELECT * FROM R1 LEFT JOIN R2 ON R1.C " + joinOp + " R2.C");
        AbstractPlanNode n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        NestLoopPlanNode nl = (NestLoopPlanNode) n;
        assertEquals(joinOp.toOperator(), nl.getJoinPredicate().getExpressionType());
        assertNull(nl.getWherePredicate());
        assertEquals(2, nl.getChildCount());
        SeqScanPlanNode c0 = (SeqScanPlanNode) nl.getChild(0);
        assertNull(c0.getPredicate());
        SeqScanPlanNode c1 = (SeqScanPlanNode) nl.getChild(1);
        assertNull(c1.getPredicate());

        // R1.C " + joinOp + " R2.C Inner-Outer join Expr stays at the NLJ as Join predicate
        // R1.A > 0 Outer Join Expr stays at the the NLJ as pre-join predicate
        // R2.A < 0 Inner Join Expr is pushed down to the inner SeqScan node
        pn = compile("SELECT * FROM R1 LEFT JOIN R2 ON R1.C " + joinOp + " R2.C AND R1.A > 0 AND R2.A < 0");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        nl = (NestLoopPlanNode) n;
        assertNotNull(nl.getPreJoinPredicate());
        AbstractExpression p = nl.getPreJoinPredicate();
        assertEquals(ExpressionType.COMPARE_GREATERTHAN, p.getExpressionType());
        assertNotNull(nl.getJoinPredicate());
        p = nl.getJoinPredicate();
        assertEquals(joinOp.toOperator(), p.getExpressionType());
        assertNull(nl.getWherePredicate());
        assertEquals(2, nl.getChildCount());
        c0 = (SeqScanPlanNode) nl.getChild(0);
        assertNull(c0.getPredicate());
        c1 = (SeqScanPlanNode) nl.getChild(1);
        assertNotNull(c1.getPredicate());
        p = c1.getPredicate();
        assertEquals(ExpressionType.COMPARE_LESSTHAN, p.getExpressionType());

        // R1.C " + joinOp + " R2.C Inner-Outer join Expr stays at the NLJ as Join predicate
        // (R1.A > 0 OR R2.A < 0) Inner-Outer join Expr stays at the NLJ as Join predicate
        pn = compile("SELECT * FROM R1 LEFT JOIN R2 ON R1.C " + joinOp + " R2.C AND (R1.A > 0 OR R2.A < 0)");
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

        // R1.C " + joinOp + " R2.C Inner-Outer join Expr stays at the NLJ as Join predicate
        // R1.A > 0 Outer Where Expr is pushed down to the outer SeqScan node
        // R2.A IS NULL Inner Where Expr stays at the the NLJ as post join (where) predicate
        // (R1.C > R2.C OR R2.C IS NULL) Inner-Outer Where stays at the the NLJ as post join (where) predicate
        pn = compile("SELECT * FROM R1 LEFT JOIN R2 ON R1.C " + joinOp + " R2.C WHERE R1.A > 0 AND R2.A IS NULL AND (R1.C > R2.C OR R2.C IS NULL)");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        nl = (NestLoopPlanNode) n;
        assertEquals(JoinType.LEFT, nl.getJoinType());
        assertNotNull(nl.getJoinPredicate());
        p = nl.getJoinPredicate();
        assertEquals(joinOp.toOperator(), p.getExpressionType());
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

        // R3.A " + joinOp + " R2.A Inner-Outer index join Expr. NLJ predicate.
        // R3.A > 3 Index Outer where expr pushed down to IndexScanPlanNode
        // R3.C < 0 non-index Outer where expr pushed down to IndexScanPlanNode as a predicate
        pn = compile("SELECT * FROM R3 LEFT JOIN R2 ON R3.A " + joinOp + " R2.A WHERE R3.A > 3 AND R3.C < 0");
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

        // R3.C " + joinOp + " R2.C Inner-Outer non-index join Expr. NLJ predicate.
        // R3.A > 3 Index null rejecting inner where expr pushed down to IndexScanPlanNode
        // NLJ is simplified to be INNER
        pn = compile("SELECT * FROM R2 LEFT JOIN R3 ON R3.C " + joinOp + " R2.C WHERE R3.A > 3");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        nl = (NestLoopPlanNode) n;
        assertEquals(JoinType.INNER, nl.getJoinType());
        outerScan = n.getChild(1);
        assertTrue(outerScan instanceof IndexScanPlanNode);
        indexScan = (IndexScanPlanNode) outerScan;
        assertEquals(IndexLookupType.GT, indexScan.getLookupType());
        assertNull(indexScan.getPredicate());

        pn = compile("SELECT * FROM R2 LEFT JOIN R3 ON R3.A " + joinOp + " R2.C WHERE R3.A > 3");
        n = pn.getChild(0).getChild(0);
        if (joinOp != JoinOp.EQUAL) { // weaken test for now
            return;
        }
        assertTrue(n instanceof NestLoopIndexPlanNode);
        NestLoopIndexPlanNode nli = (NestLoopIndexPlanNode) n;
        assertEquals(JoinType.INNER, nli.getJoinType());
   }

    public void testDistributedSeqScanOuterJoinCondition() {
        for (JoinOp joinOp : JoinOp.JOIN_OPS) {
            perJoinOpTestDistributedSeqScanOuterJoinCondition(joinOp);
        }
    }

    private void perJoinOpTestDistributedSeqScanOuterJoinCondition(JoinOp joinOp) {
        // Distributed Outer table
        List<AbstractPlanNode> lpn;
        AbstractPlanNode pn;
        AbstractPlanNode n;
        lpn = compileToFragments("SELECT * FROM P1 LEFT JOIN R2 ON P1.C " + joinOp + " R2.C");
        assertEquals(2, lpn.size());
        n = lpn.get(1).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        assertEquals(2, n.getChildCount());
        assertTrue(n.getChild(0) instanceof SeqScanPlanNode);
        assertTrue(n.getChild(1) instanceof SeqScanPlanNode);

        // Distributed Inner table
        pn = compile("SELECT * FROM R2 LEFT JOIN P1 ON P1.C " + joinOp + " R2.C");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        NestLoopPlanNode nl = (NestLoopPlanNode) n;
        assertEquals(2, nl.getChildCount());
        assertTrue(nl.getChild(0) instanceof SeqScanPlanNode);
        assertTrue(nl.getChild(1) instanceof ReceivePlanNode);

        // Distributed Inner and Outer table joined on the partition column
        lpn = compileToFragments("SELECT * FROM P1 LEFT JOIN P4 ON P1.A " + joinOp + " P4.A");
        assertEquals(2, lpn.size());
        n = lpn.get(1).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        assertEquals(2, n.getChildCount());
        assertTrue(n.getChild(0) instanceof SeqScanPlanNode);
        assertTrue(n.getChild(1) instanceof SeqScanPlanNode);

        // Distributed Inner and Outer table joined on the non-partition column
        failToCompile("SELECT * FROM P1 LEFT JOIN P4 ON P1.A " + joinOp + " P4.E",
                "This query is not plannable.  The planner cannot guarantee that all rows would be in a single partition");
    }

    public void testBasicIndexOuterJoin() {
        for (JoinOp joinOp : JoinOp.JOIN_OPS) {
            perJoinOpTestBasicIndexOuterJoin(joinOp);
        }
    }

    private void perJoinOpTestBasicIndexOuterJoin(JoinOp joinOp) {
        // R3 is indexed but it's the outer table and the join expression must stay at the NLJ
        // so index can't be used
        AbstractPlanNode pn = compile("SELECT * FROM R3 LEFT JOIN R2 ON R3.A " + joinOp + " R2.C");
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
        pn = compile("SELECT * FROM R2 RIGHT JOIN R3 ON R3.A " + joinOp + " R2.C");
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

        pn = compile("SELECT * FROM R2 LEFT JOIN R3 ON R2.C " + joinOp + " R3.A");
        n = pn.getChild(0).getChild(0);
        if (joinOp != JoinOp.EQUAL) { // weaken test for now
            return;
        }
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
        for (JoinOp joinOp : JoinOp.JOIN_OPS) {
            if (joinOp != JoinOp.EQUAL) { // weaken test for now
                continue;
            }
            perJoinOpTestIndexOuterJoinConditions(joinOp);
        }
    }

    private void perJoinOpTestIndexOuterJoinConditions(JoinOp joinOp) {
        // R1.C " + joinOp + " R3.A Inner-Outer index join Expr. NLIJ/Inlined IndexScan
        // R3.C > 0 Inner Join Expr is pushed down to the inlined IndexScan node as a predicate
        // R2.A < 6 Outer Join Expr is a pre-join predicate for NLIJ
        AbstractPlanNode pn = compile("SELECT * FROM R2 LEFT JOIN R3 ON R3.A " + joinOp + " R2.A AND R3.C > 0 AND R2.A < 6");
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

        // R1.C " + joinOp + " R3.A Inner-Outer non-index join Expr. NLJ/IndexScan
        // R3.A > 0 Inner index Join Expr is pushed down to the inner IndexScan node as an index
        // R3.C != 0 Non-index Inner Join Expression is pushed down to the inner IndexScan node as a predicate
        // R2.A < 6 Outer Join Expr is a pre-join predicate for NLJ
        pn = compile("SELECT * FROM R2 LEFT JOIN R3 ON R3.C " + joinOp + " R2.A AND R3.A > 0 AND R3.C != 0 AND R2.A < 6");
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

        // R2.A " + joinOp + " R3.A Inner-Outer index join Expr. NLIJ/Inlined IndexScan
        // R3.A IS NULL Inner where expr - part of the NLIJ where predicate
        // R2.A < 6 OR R3.C IS NULL Inner-Outer where expr - part of the NLIJ where predicate
        // R2.A > 3 Outer where expr - pushed down to the outer node
        pn = compile("SELECT * FROM R2 LEFT JOIN R3 ON R3.A " + joinOp + " R2.A WHERE R3.A IS NULL AND R2.A > 3 AND (R2.A < 6 OR R3.C IS NULL)");
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
        for (JoinOp joinOp : JoinOp.JOIN_OPS) {
            perJoinOpTestDistributedInnerOuterTable(joinOp);
        }
    }

    private void perJoinOpTestDistributedInnerOuterTable(JoinOp joinOp) {
        // Distributed Outer table
        List<AbstractPlanNode> lpn;
        AbstractPlanNode pn;
        AbstractPlanNode n;
        lpn = compileToFragments("SELECT * FROM P1 LEFT JOIN R2 ON P1.C " + joinOp + " R2.C");
        assertEquals(2, lpn.size());
        n = lpn.get(1).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        assertEquals(2, n.getChildCount());
        assertTrue(n.getChild(0) instanceof SeqScanPlanNode);
        assertTrue(n.getChild(1) instanceof SeqScanPlanNode);

        // Distributed Inner table
        pn = compile("SELECT * FROM R2 LEFT JOIN P1 ON P1.C " + joinOp + " R2.C");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        NestLoopPlanNode nl = (NestLoopPlanNode) n;
        assertEquals(2, nl.getChildCount());
        assertTrue(nl.getChild(0) instanceof SeqScanPlanNode);
        assertTrue(nl.getChild(1) instanceof ReceivePlanNode);

        // Distributed Inner and Outer table joined on the partition column
        lpn = compileToFragments("SELECT * FROM P1 LEFT JOIN P4 ON P1.A " + joinOp + " P4.A");
        assertEquals(2, lpn.size());
        n = lpn.get(1).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        assertEquals(2, n.getChildCount());
        assertTrue(n.getChild(0) instanceof SeqScanPlanNode);
        assertTrue(n.getChild(1) instanceof SeqScanPlanNode);

        // Distributed Inner and Outer table joined on the non-partition column
        failToCompile("SELECT * FROM P1 LEFT JOIN P4 ON P1.A " + joinOp + " P4.E",
                "This query is not plannable.  The planner cannot guarantee that all rows would be in a single partition");
    }

    public void testDistributedIndexJoinConditions() {
        for (JoinOp joinOp : JoinOp.JOIN_OPS) {
            if (joinOp != JoinOp.EQUAL) { // weaken test for now
                continue;
            }
            perJoinOpDistributedIndexJoinConditions(joinOp);
        }
    }

    private void perJoinOpDistributedIndexJoinConditions(JoinOp joinOp) {
        // Distributed outer table, replicated inner -NLIJ/inlined IndexScan
        List<AbstractPlanNode> lpn;
        //AbstractPlanNode pn;
        AbstractPlanNode n;
        lpn = compileToFragments("SELECT * FROM P1 LEFT JOIN R3 ON P1.C " + joinOp + " R3.A");
        assertEquals(2, lpn.size());
        n = lpn.get(1).getChild(0);
        if (joinOp != JoinOp.EQUAL) { // weaken test for now
            assertTrue(n instanceof NestLoopIndexPlanNode);
            assertEquals(1, n.getChildCount());
            assertTrue(n.getChild(0) instanceof SeqScanPlanNode);
        }

        // Distributed inner  and replicated outer tables -NLJ/IndexScan
        lpn = compileToFragments("SELECT * FROM R3 LEFT JOIN P2 ON R3.A " + joinOp + " P2.A AND P2.A < 0 AND P2.E > 3 WHERE P2.A IS NULL");
        assertEquals(2, lpn.size());
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
        lpn = compileToFragments("SELECT * FROM P2 RIGHT JOIN P3 ON P3.A " + joinOp + " P2.A AND P2.A < 0 WHERE P2.A IS NULL");
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
        for (JoinOp joinOp : JoinOp.JOIN_OPS) {
            perJoinOpTestNonSupportedJoin(joinOp);
        }
    }

    private void perJoinOpTestNonSupportedJoin(JoinOp joinOp) {
       // JOIN with parentheses (HSQL limitation)
       failToCompile("SELECT R2.C FROM (R1 JOIN R2 ON R1.C " + joinOp + " R2.C) JOIN R3 ON R1.C " + joinOp + " R3.C",
                     "user lacks privilege or object not found: R1.C");
       // JOIN with join hierarchy (HSQL limitation)
       failToCompile("SELECT * FROM R1 JOIN R2 JOIN R3 ON R1.C " + joinOp + " R2.C ON R1.C " + joinOp + " R3.C",
                     "unexpected token");
   }

   public void testOuterJoinSimplification() {
       for (JoinOp joinOp : JoinOp.JOIN_OPS) {
           perJoinOpTestOuterJoinSimplification(joinOp);
       }
   }

   private void perJoinOpTestOuterJoinSimplification(JoinOp joinOp) {
       AbstractPlanNode pn, n;
       AbstractExpression ex;

       pn = compile("SELECT * FROM R1 LEFT JOIN R2 ON R1.C " + joinOp + " R2.C WHERE R2.C IS NOT NULL");
       n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopPlanNode);
       assertEquals(((NestLoopPlanNode) n).getJoinType(), JoinType.INNER);

       pn = compile("SELECT * FROM R1 LEFT JOIN R2 ON R1.C " + joinOp + " R2.C WHERE R2.C > 0");
       n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopPlanNode);
       assertEquals(((NestLoopPlanNode) n).getJoinType(), JoinType.INNER);

       pn = compile("SELECT * FROM R1 RIGHT JOIN R2 ON R1.C " + joinOp + " R2.C WHERE R1.C > 0");
       n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopPlanNode);
       assertEquals(((NestLoopPlanNode) n).getJoinType(), JoinType.INNER);

       pn = compile("SELECT * FROM R1 LEFT JOIN R3 ON R1.C " + joinOp + " R3.C WHERE R3.A > 0");
       n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopPlanNode);
       assertEquals(((NestLoopPlanNode) n).getJoinType(), JoinType.INNER);

       pn = compile("SELECT * FROM R1 LEFT JOIN R3 ON R1.C " + joinOp + " R3.A WHERE R3.A > 0");
       n = pn.getChild(0).getChild(0);
       if (joinOp == JoinOp.EQUAL) { // weaken test for now
           assertTrue(n instanceof NestLoopIndexPlanNode);
           assertEquals(((NestLoopIndexPlanNode) n).getJoinType(), JoinType.INNER);
       }

       pn = compile("SELECT * FROM R1 LEFT JOIN R2 ON R1.C " + joinOp + " R2.C WHERE ABS(R2.C) <  10");
       n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopPlanNode);
       assertEquals(((NestLoopPlanNode) n).getJoinType(), JoinType.INNER);

       pn = compile("SELECT * FROM R1 RIGHT JOIN R2 ON R1.C " + joinOp + " R2.C WHERE ABS(R1.C) <  10");
       n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopPlanNode);
       assertEquals(((NestLoopPlanNode) n).getJoinType(), JoinType.INNER);

       pn = compile("SELECT * FROM R1 LEFT JOIN R2 ON R1.C " + joinOp + " R2.C WHERE ABS(R1.C) <  10");
       n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopPlanNode);
       assertEquals(((NestLoopPlanNode) n).getJoinType(), JoinType.LEFT);

       pn = compile("SELECT * FROM R1 RIGHT JOIN R2 ON R1.C " + joinOp + " R2.C WHERE ABS(R2.C) <  10");
       n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopPlanNode);
       assertEquals(((NestLoopPlanNode) n).getJoinType(), JoinType.LEFT);

       pn = compile("SELECT * FROM R1 LEFT JOIN R2 ON R1.C " + joinOp + " R2.C WHERE ABS(R2.C) <  10 AND R1.C = 3");
       n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopPlanNode);
       assertEquals(((NestLoopPlanNode) n).getJoinType(), JoinType.INNER);

       pn = compile("SELECT * FROM R1 LEFT JOIN R2 ON R1.C " + joinOp + " R2.C WHERE ABS(R2.C) <  10 OR R2.C IS NOT NULL");
       n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopPlanNode);
       assertEquals(((NestLoopPlanNode) n).getJoinType(), JoinType.INNER);

       pn = compile("SELECT * FROM R1 LEFT JOIN R2 ON R1.C " + joinOp + " R2.C WHERE ABS(R1.C) <  10 AND R1.C > 3");
       n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopPlanNode);
       assertEquals(((NestLoopPlanNode) n).getJoinType(), JoinType.LEFT);

       pn = compile("SELECT * FROM R1 LEFT JOIN R2 ON R1.C " + joinOp + " R2.C WHERE ABS(R1.C) <  10 OR R2.C IS NOT NULL");
       n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopPlanNode);
       assertEquals(((NestLoopPlanNode) n).getJoinType(), JoinType.LEFT);

       // Test with seqscan with different filers.
       pn = compile("SELECT R2.A, R1.* FROM R1 LEFT OUTER JOIN R2 ON R2.A " + joinOp + " R1.A WHERE R2.A > 3");
       //* enable for debug */ System.out.println(pn.toExplainPlanString());
       n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopPlanNode);
       assertEquals(((NestLoopPlanNode) n).getJoinType(), JoinType.INNER);
       ex = ((NestLoopPlanNode) n).getWherePredicate();
       assertEquals(ex, null);

       pn = compile("SELECT R2.A, R1.* FROM R1 LEFT OUTER JOIN R2 ON R2.A " + joinOp + " R1.A WHERE R2.A IS NULL");
       n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopPlanNode);
       assertEquals(((NestLoopPlanNode) n).getJoinType(), JoinType.LEFT);
       ex = ((NestLoopPlanNode) n).getWherePredicate();
       assertEquals(ex instanceof OperatorExpression, true);

       pn = compile("SELECT b.A, a.* FROM R1 a LEFT OUTER JOIN R4 b ON b.A = a.A AND b.C = a.C AND a.D = b.D WHERE b.A IS NULL");
       //* enable for debug */ System.out.println(pn.toExplainPlanString());
       n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopIndexPlanNode);
       assertEquals(((NestLoopIndexPlanNode) n).getJoinType(), JoinType.LEFT);
       ex = ((NestLoopIndexPlanNode) n).getWherePredicate();
       assertEquals(ex instanceof OperatorExpression, true);

       pn = compile("SELECT b.A, a.* FROM R1 a LEFT OUTER JOIN R4 b ON b.A = a.A AND b.C = a.C AND a.D = b.D WHERE b.B + b.A IS NULL");
       n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopIndexPlanNode);
       assertEquals(((NestLoopIndexPlanNode) n).getJoinType(), JoinType.LEFT);
       ex = ((NestLoopIndexPlanNode) n).getWherePredicate();
       assertEquals(ex instanceof OperatorExpression, true);
       assertEquals(ex.getLeft() instanceof OperatorExpression, true);

       pn = compile("SELECT a.* FROM R1 a LEFT OUTER JOIN R5 b ON b.A = a.A WHERE b.A IS NULL");
       n = pn.getChild(0).getChild(0);
       assertEquals(((NestLoopIndexPlanNode) n).getJoinType(), JoinType.LEFT);
       ex = ((NestLoopIndexPlanNode) n).getWherePredicate();
       assertEquals(ex instanceof OperatorExpression, true);
   }

   public void testMoreThan5TableJoins() {
       for (JoinOp joinOp : JoinOp.JOIN_OPS) {
           perJoinOpTestMoreThan5TableJoins(joinOp);
       }
   }

   private void perJoinOpTestMoreThan5TableJoins(JoinOp joinOp) {
       // INNER JOIN with >5 tables.
       compile("SELECT R1.C FROM R3,R2, P1, P2, P3, R1 WHERE R3.A " + joinOp + " R2.A AND R2.A " + joinOp + " P1.A AND P1.A " + joinOp + " P2.A AND P3.A " + joinOp + " P2.A AND R1.C " + joinOp + " R2.C");

       // OUTER JOIN with >5 tables.
       compile("SELECT R1.C FROM R3,R2, P1, P2, P3 LEFT OUTER JOIN R1 ON R1.C " + joinOp + " R2.C WHERE R3.A " + joinOp + " R2.A AND R2.A " + joinOp + " P1.A AND P1.A " + joinOp + " P2.A AND P3.A " + joinOp + " P2.A");
   }

   public void testAmbigousIdentifierInSelectList() throws Exception {
       for (JoinOp joinOp : JoinOp.JOIN_OPS) {
           perJoinOpTestAmbigousIdentifierInSelectList(joinOp);
       }
   }

   private void perJoinOpTestAmbigousIdentifierInSelectList(JoinOp joinOp) throws Exception {
       // Simple ambiguous column reference.
       failToCompile("select A, C from R1, R2;", "Column \"A\" is ambiguous.  It's in tables: R1, R2");
       // Ambiguous reference in an arithmetic expression.
       failToCompile("select A + C from R1, R2;", "Column \"A\" is ambiguous.  It's in tables: R1, R2");
       failToCompile("select sqrt(A) from R1, R2;", "Column \"A\" is ambiguous.  It's in tables: R1, R2");
       // Ambiguous reference in a where clause.
       failToCompile("select NOTC from R1, R3_NOC where A > 100;", "Column \"A\" is ambiguous.  It's in tables: R1, R3_NOC");
       failToCompile("select NOTC from R1, R3_NOC where A > sqrt(NOTC);", "Column \"A\" is ambiguous.  It's in tables: R1, R3_NOC");
       failToCompile("select NOTC from R1, R3_NOC where sqrt(A) > sqrt(NOTC);", "Column \"A\" is ambiguous.  It's in tables: R1, R3_NOC");
       failToCompile("select NOTC from R1 join R3_NOC on sqrt(A) > sqrt(NOTC);", "Column \"A\" is ambiguous.  It's in tables: R1, R3_NOC");
       // Ambiguous reference to an unconstrained column in a join.  That is,
       // C is in both R1 and R3, R1 and R3 are joined together, but not on C.
       // Note that we test above for a similar case, with three joined tables.
       failToCompile("select C from R1 inner join R3 using(A);", "Column \"C\" is ambiguous.  It's in tables: R1, R3");
       failToCompile("select C from R1 inner join R3 using(C), R2;", "Column \"C\" is ambiguous.  It's in tables: USING(C), R2");
       // Ambiguous references in group by expressions.
       failToCompile("select NOTC from R1, R3_NOC group by A;", "Column \"A\" is ambiguous.  It's in tables: R1, R3_NOC");
       failToCompile("select NOTC from R1, R3_NOC group by sqrt(A);", "Column \"A\" is ambiguous.  It's in tables: R1, R3_NOC");
       failToCompile("select sqrt(R1.A) from R1, R3_NOC group by R1.A having count(A) + 2 * sum(A) > 2;", "Column \"A\" is ambiguous.  It's in tables: R1, R3_NOC");
       // Ambiguous references in subqueries.
       failToCompile("select ALPHA from (select SQRT(A) as ALPHA from R1) as S1, (select SQRT(C) as ALPHA from R1) as S2;",
                     "Column \"ALPHA\" is ambiguous.  It's in tables: S1, S2");
       failToCompile("select ALPHA from (select SQRT(A), SQRT(C) from R1, R3) as S1, (select SQRT(C) as ALPHA from R1) as S2;",
                     "Column \"A\" is ambiguous.  It's in tables: R1, R3");
       failToCompile("select C from R1 inner join R2 using(C), R3 where R1.A " + joinOp + " R3.A;",
                     "Column \"C\" is ambiguous.  It's in tables: USING(C), R3");
       failToCompile("select R3.C, C from R1 inner join R2 using(C) inner join R3 on C=R3.A;",
                     "Column \"C\" is ambiguous.  It's in tables: USING(C), R3");

       // Ambiguous columns in an order by expression.
       failToCompile("select LR.A, RR.A from R1 LR, R1 RR order by A;", "The name \"A\" in an order by expression is ambiguous.  It's in columns: LR.A, RR.A.");
       // Note that LT.A and RT.A are not considered here.
       failToCompile("select LT.A as LA, RT.A as RA from R1 as LT, R1 as RT order by A;", "Column \"A\" is ambiguous.  It's in tables: LT, RT");
       // Two columns in the select list with the same name.  This complicates
       // checking for order by aliases.
       failToCompile("select LT.A as LA, RT.A as LA from R1 as LT, R1 as RT order by LA;", "The name \"LA\" in an order by expression is ambiguous.  It's in columns: LA(0), LA(1)");
       failToCompile("select NOTC from R1, R3_NOC order by A;", "Column \"A\" is ambiguous.  It's in tables: R1, R3_NOC");
       failToCompile("select NOTC from R1, R3_NOC order by sqrt(A);", "Column \"A\" is ambiguous.  It's in tables: R1, R3_NOC");
       // Ambiguous columns in an order by expression.
       failToCompile("select LR.A, RR.A from R1 LR, R1 RR order by A;", "The name \"A\" in an order by expression is ambiguous.  It's in columns: LR.A, RR.A");
       // Note that LT.A and RT.A are not considered here.
       failToCompile("select LT.A as LA, RT.A as RA from R1 as LT, R1 as RT order by A;", "Column \"A\" is ambiguous.  It's in tables: LT, RT");
       failToCompile("select LT.A, RT.A from R1 as LT, R1 as RT order by A", "The name \"A\" in an order by expression is ambiguous.  It's in columns: LT.A, RT.A");
       // Two columns in the select list with the same name.  This complicates
       // checking for order by aliases.
       failToCompile("select (R1.A + 1) A, A from R1 order by A", "The name \"A\" in an order by expression is ambiguous.  It's in columns: A(0), R1.A.");
       failToCompile("select LT.A as LA, RT.A as LA from R1 as LT, R1 as RT order by LA;", "The name \"LA\" in an order by expression is ambiguous.  It's in columns: LA(0), LA(1)");
       failToCompile("select NOTC from R1, R3_NOC order by A;", "Column \"A\" is ambiguous.  It's in tables: R1, R3_NOC");
       failToCompile("select NOTC from R1, R3_NOC order by sqrt(A);", "Column \"A\" is ambiguous.  It's in tables: R1, R3_NOC");

       // This is not ambiguous.  The two aliases reference the same column.
       compile("SELECT R1.A, A FROM R1 WHERE A > 0;");
       compile("SELECT lr.a FROM r1 lr, r1 rr ORDER BY a;");

       failToCompile("select lr.a a, rr.a a from r1 lr, r2 rr order by a;", "The name \"A\" in an order by expression is ambiguous.  It's in columns: LR.A, RR.A");

       // Since A is in the using list, lr.a and rr.a are the same.
       compile("SELECT lr.a alias, lr.a, a, lr.a + 1 aliasexp, lr.a + 1, a + 1 FROM r1 lr ORDER BY a;");
       compile("SELECT lr.a a, a FROM r1 lr JOIN r1 rr USING(a) ORDER BY a;");
       compile("SELECT lr.a a, rr.a FROM r1 lr JOIN r1 rr USING(a) ORDER BY a;");

       // R1 join R2 on R1.A " + joinOp + " R2.A is not R1 join R2 using(A).
       failToCompile("select lr.a a, rr.a a from r1 lr join r1 rr on lr.a = rr.a order by a;", "The name \"A\" in an order by expression is ambiguous.  It's in columns: LR.A, RR.A");

       // This is not actually an ambiguous query.  This is actually ok.
       compile("SELECT * FROM R2 WHERE A IN (SELECT A FROM R1);");
       compile("SELECT R3.C, C FROM R1 INNER JOIN R2 USING(C) INNER JOIN R3 USING(C);");
       // This one is ok too.  There are several common columns in R2, R1.  But they
       // are fully qualified as R1.A, R2.A and so forth when * is expanded.
       compile("SELECT * FROM R2, R1");
       compile("SELECT R1.C FROM R1 INNER JOIN R2 USING (C), R3");
       compile("SELECT R2.C FROM R1 INNER JOIN R2 USING (C), R3");
       compile("SELECT R1.C FROM R1 INNER JOIN R2 USING (C), R3 WHERE R1.A " + joinOp + " R3.A");
       compile("SELECT R3.C, R1.C FROM R1 INNER JOIN R2 USING(C), R3;");
       compile("SELECT C, C FROM R1 GROUP BY C ORDER BY C;");
   }

    public void testUsingColumns() {
        // Test USING column
        AbstractPlanNode pn = compile("SELECT MAX(R1.A), C FROM R1 FULL JOIN R2 USING (C) WHERE C > 0 GROUP BY C ORDER BY C");

        // ORDER BY column
        pn = pn.getChild(0);
        assertEquals(PlanNodeType.ORDERBY, pn.getPlanNodeType());
        List<AbstractExpression> s = ((OrderByPlanNode)pn).getSortExpressions();
        assertEquals(1, s.size());
        assertEquals(ExpressionType.VALUE_TUPLE, s.get(0).getExpressionType());

        // WHERE
        pn = pn.getChild(0);
        assertEquals(PlanNodeType.NESTLOOP, pn.getPlanNodeType());
        AbstractExpression f = ((NestLoopPlanNode)pn).getWherePredicate();
        assertNotNull(f);
        assertEquals(ExpressionType.OPERATOR_CASE_WHEN, f.getLeft().getExpressionType());

        // GROUP BY
        AbstractPlanNode aggr = pn.getInlinePlanNode(PlanNodeType.HASHAGGREGATE);
        assertNotNull(aggr);
        List<AbstractExpression> g = ((AggregatePlanNode) aggr).getGroupByExpressions();
        assertEquals(1, g.size());
        assertEquals(ExpressionType.OPERATOR_CASE_WHEN, g.get(0).getExpressionType());

        // Test three table full join
        pn = compile("SELECT C FROM R1 FULL JOIN R2 USING (C) FULL JOIN R3 USING (C)");
        pn = pn.getChild(0);
        NodeSchema ns = pn.getOutputSchema();
        assertEquals(1, ns.getColumns().size());
        SchemaColumn col = ns.getColumns().get(0);
        assertEquals("C", col.getColumnAlias());
        AbstractExpression colExp = col.getExpression();
        assertEquals(ExpressionType.OPERATOR_CASE_WHEN, colExp.getExpressionType());
        List<OperatorExpression> caseWhenExprs = colExp.findAllSubexpressionsOfClass(OperatorExpression.class);
        int caseWhenCount = 0;
        for (OperatorExpression caseWhen : caseWhenExprs) {
            if (caseWhen.getExpressionType() == ExpressionType.OPERATOR_CASE_WHEN) {
                ++caseWhenCount;
            }
        }
        assertEquals(2, caseWhenCount);

        // Test three table INNER join. USING C column should be resolved
        pn = compile("SELECT C FROM R1 JOIN R2 USING (C) JOIN R3 USING (C)");
        pn = pn.getChild(0);
        ns = pn.getOutputSchema();
        assertEquals(1, ns.getColumns().size());
        col = ns.getColumns().get(0);
        assertEquals("C", col.getColumnAlias());
        colExp = col.getExpression();
        assertEquals(ExpressionType.VALUE_TUPLE, colExp.getExpressionType());

        // Test two table LEFT join. USING C column should be resolved
        pn = compile("SELECT C FROM R1 LEFT JOIN R2 USING (C)");
        pn = pn.getChild(0);
        ns = pn.getOutputSchema();
        assertEquals(1, ns.getColumns().size());
        col = ns.getColumns().get(0);
        assertEquals("C", col.getColumnAlias());
        colExp = col.getExpression();
        assertEquals(ExpressionType.VALUE_TUPLE, colExp.getExpressionType());

        // Test two table RIGHT join. USING C column should be resolved
        pn = compile("SELECT C FROM R1 RIGHT JOIN R2 USING (C)");
        pn = pn.getChild(0);
        ns = pn.getOutputSchema();
        assertEquals(1, ns.getColumns().size());
        col = ns.getColumns().get(0);
        assertEquals("C", col.getColumnAlias());
        colExp = col.getExpression();
        assertEquals(ExpressionType.VALUE_TUPLE, colExp.getExpressionType());

    }

    public void testJoinOrders() {
        for (JoinOp joinOp : JoinOp.JOIN_OPS) {
            perJoinOpTestJoinOrders(joinOp);
        }
    }

    private void perJoinOpTestJoinOrders(JoinOp joinOp) {
        AbstractPlanNode pn;
        AbstractScanPlanNode sn;
        IndexScanPlanNode isn;

        // R1 is an outer node - has one filter
        pn = compile("SELECT * FROM R2 JOIN R1 USING (C) WHERE R1.A > 0");
        pn = pn.getChild(0).getChild(0);
        assertEquals(PlanNodeType.NESTLOOP, pn.getPlanNodeType());
        sn = (AbstractScanPlanNode) pn.getChild(0);
        assertEquals("R1", sn.getTargetTableName());

        // R2 is an outer node - R2.A = 3 filter is discounter more than R1.A > 0
        pn = compile("SELECT * FROM R1 JOIN R2 USING (C) WHERE R1.A > 0 AND R2.A = 3");
        pn = pn.getChild(0).getChild(0);
        assertEquals(PlanNodeType.NESTLOOP, pn.getPlanNodeType());
        sn = (AbstractScanPlanNode) pn.getChild(0);
        assertEquals("R2", sn.getTargetTableName());

        // R2 is an outer node - R2.A = 3 filter is discounter more than two non-EQ filters
        pn = compile("SELECT * FROM R1 JOIN R2 USING (C) WHERE R1.A > 0 AND R1.A < 3 AND R2.A = 3");
        pn = pn.getChild(0).getChild(0);
        assertEquals(PlanNodeType.NESTLOOP, pn.getPlanNodeType());
        sn = (AbstractScanPlanNode) pn.getChild(0);
        assertEquals("R2", sn.getTargetTableName());

        // R1 is an outer node - EQ + non-EQ overweight EQ
        pn = compile("SELECT * FROM R1 JOIN R2 USING (C) WHERE R1.A = 0 AND R1.D < 3 AND R2.A = 3");
        pn = pn.getChild(0).getChild(0);
        assertEquals(PlanNodeType.NESTLOOP, pn.getPlanNodeType());
        sn = (AbstractScanPlanNode) pn.getChild(0);
        assertEquals("R1", sn.getTargetTableName());

        if (joinOp != JoinOp.EQUAL) { // weaken test for now
            return;
        }
        // Index Join (R3.A) still has a lower cost compare to a Loop Join
        // despite the R3.C = 0 equality filter on the inner node
        pn = compile("SELECT * FROM R1 JOIN R3 ON R3.A " + joinOp + " R1.A WHERE R3.C = 0");
        pn = pn.getChild(0).getChild(0);
        assertEquals(PlanNodeType.NESTLOOPINDEX, pn.getPlanNodeType());
        sn = (AbstractScanPlanNode) pn.getChild(0);
        assertEquals("R1", sn.getTargetTableName());

        // R3.A is an INDEX. Both children are IndexScans. With everything being equal,
        // the Left table (L) has fewer filters and should be an inner node
        pn = compile("SELECT L.A, R.A FROM R3 L JOIN R3 R ON L.A " + joinOp + " R.A WHERE R.A > 3 AND R.C  = 3 AND L.A > 2 ;");
        pn = pn.getChild(0).getChild(0);
        assertEquals(PlanNodeType.NESTLOOPINDEX, pn.getPlanNodeType());
        pn = pn.getChild(0);
        assertEquals(PlanNodeType.INDEXSCAN, pn.getPlanNodeType());
        sn = (AbstractScanPlanNode) pn;
        assertEquals("R", sn.getTargetTableAlias());

        // NLIJ with inline inner IndexScan over R2 using its partial index is a winner
        // over the NLJ with R2 on the outer side
        pn = compile("SELECT * FROM R3 JOIN R2 ON R3.C " + joinOp + " R2.C WHERE R2.C > 100;");
        pn = pn.getChild(0).getChild(0);
        assertEquals(PlanNodeType.NESTLOOPINDEX, pn.getPlanNodeType());
        isn = (IndexScanPlanNode) pn.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        assertEquals("PARTIAL_IND2", isn.getTargetIndexName());
        sn = (AbstractScanPlanNode) pn.getChild(0);
        assertEquals("R3", sn.getTargetTableName());

    }

    public void testExplainHighlights() {
        // These tests of critical aspects of join-related @Explain output were
        // migrated from the regression suite where they really did not belong.
        // They MAY be somewhat redundant with other less stringly tests in this
        // suite, but they do have the advantage of covering some key aspects of
        // explain string generation in an informal easily-maintained way that
        // does not get bogged down in the precise explain string syntax.

        String sql;
        String explained;
        for (JoinOp joinOp : JoinOp.JOIN_OPS) {
            sql = "SELECT P1.A, P1.C, P3.A, P3.F " +
                    "FROM P1 FULL JOIN P3 " +
                    "ON P1.A " + joinOp + " P3.A AND P1.A = ? AND P3.F = 1 " +
                    "ORDER BY P1.A, P1.C, P3.A, P3.F";
            explained = buildExplainPlan(compileToFragments(sql));
            if (joinOp == JoinOp.EQUAL) { // weaken test for now
                assertTrue(explained.contains("NESTLOOP INDEX FULL JOIN"));
            }
            else {
                assertTrue(explained.contains("NEST LOOP FULL JOIN"));
            }
            sql = "SELECT R1.A, R1.C, R3.A, R3.C " +
                    "FROM R1 FULL JOIN R3 " +
                    "ON R3.A " + joinOp + " R1.A AND R3.A < 2 " +
                    "ORDER BY R1.A, R1.D, R3.A, R3.C";
            explained = buildExplainPlan(compileToFragments(sql));
            //* enable to debug */ System.out.println("DEBUG: " + explained);
            if (joinOp == JoinOp.EQUAL) { // weaken test for now
                assertTrue(explained.contains("NESTLOOP INDEX FULL JOIN"));
            }
            else {
                assertTrue(explained.contains("NEST LOOP FULL JOIN"));
            }
            sql = "SELECT LHS.A, LHS.C, RHS.A, RHS.C " +
                    "FROM R3 LHS FULL JOIN R3 RHS " +
                    "ON LHS.A " + joinOp + " RHS.A AND LHS.A < 2 " +
                    "ORDER BY 1, 2, 3, 4";
            explained = buildExplainPlan(compileToFragments(sql));
            //* enable to debug */ System.out.println("DEBUG: " + explained);
            if (joinOp == JoinOp.EQUAL) { // weaken test for now
                assertTrue(explained.contains("NESTLOOP INDEX FULL JOIN"));
            }
            sql = "SELECT * " +
                    "FROM R1 FULL JOIN R2 " +
                    "ON R1.A " + joinOp + " R2.A RIGHT JOIN P2 " +
                    "ON P2.A " + joinOp + " R1.A " +
                    "ORDER BY P2.A";
            explained = buildExplainPlan(compileToFragments(sql));
            //* enable to debug */ System.out.println("DEBUG: " + explained);
            // Account for how IS NOT DISTINCT FROM does not reject all nulls.
            if (joinOp == JoinOp.EQUAL) { // weaken test for now
              assertFalse(explained.contains("FULL"));
              assertEquals(2, StringUtils.countMatches(explained, "LEFT"));
            }
            else {
                assertEquals(1, StringUtils.countMatches(explained, "FULL"));
                assertEquals(1, StringUtils.countMatches(explained, "LEFT"));
            }
            sql = "SELECT * " +
                    "FROM R1 FULL JOIN R2 " +
                    "ON R1.A " + joinOp + " R2.A LEFT JOIN P2 " +
                    "ON P2.A " + joinOp + " R2.A " +
                    "ORDER BY P2.A";
            explained = buildExplainPlan(compileToFragments(sql));
            assertTrue(explained.contains("FULL"));
            sql = "SELECT * " +
                    "FROM R1 RIGHT JOIN R2 " +
                    "ON R1.A " + joinOp + " R2.A FULL JOIN P2 " +
                    "ON R1.A " + joinOp + " P2.A " +
                    "ORDER BY P2.A";
            explained = buildExplainPlan(compileToFragments(sql));
            assertTrue(explained.contains("LEFT"));
            sql = "SELECT * " +
                    "FROM R1 FULL JOIN R2 " +
                    "ON R1.A " + joinOp + " R2.A FULL JOIN P2 " +
                    "ON R1.A " + joinOp + " P2.A " +
                    "ORDER BY P2.A";
            explained = buildExplainPlan(compileToFragments(sql));
            assertEquals(2, StringUtils.countMatches(explained, "FULL"));
            sql = "SELECT MAX(R1.C), A " +
                    "FROM R1 FULL JOIN R2 USING (A) " +
                    "WHERE A > 0 GROUP BY A ORDER BY A";
            explained = buildExplainPlan(compileToFragments(sql));
            assertEquals(1, StringUtils.countMatches(explained, "FULL"));
            sql = "SELECT A " +
                    "FROM R1 FULL JOIN R2 USING (A) " +
                    "FULL JOIN R3 USING(A) " +
                    "WHERE A > 0 ORDER BY A";
            explained = buildExplainPlan(compileToFragments(sql));
            assertEquals(2, StringUtils.countMatches(explained, "FULL"));
            sql = "SELECT L.A " +
                    "FROM R3 L FULL JOIN R3 R " +
                    "ON L.C " + joinOp + " R.C " +
                    "ORDER BY A";
            explained = buildExplainPlan(compileToFragments(sql));
            assertEquals(1, StringUtils.countMatches(explained, "FULL"));
            assertEquals(1, StringUtils.countMatches(explained, "SORT"));
            sql = "SELECT L.A, SUM(L.C) " +
                    "FROM R3 L FULL JOIN R3 R " +
                    "ON L.C " + joinOp + " R.C " +
                    "GROUP BY L.A ORDER BY 1";
            explained = buildExplainPlan(compileToFragments(sql));
            assertEquals(1, StringUtils.countMatches(explained, "FULL"));
            assertEquals(1, StringUtils.countMatches(explained, "SORT"));
            assertEquals(1, StringUtils.countMatches(explained, "Serial AGGREGATION"));
        }
    }

    @Override
    protected void setUp() throws Exception {
        setupSchema(TestJoinOrder.class.getResource("testplans-join-ddl.sql"), "testplansjoin", false);
    }

}
