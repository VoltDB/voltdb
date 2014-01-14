/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.NodeSchema;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.plannodes.SchemaColumn;
import org.voltdb.plannodes.SendPlanNode;
import org.voltdb.plannodes.NestLoopPlanNode;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.types.ExpressionType;

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

    @Override
    protected void setUp() throws Exception {
        setupSchema(TestJoinOrder.class.getResource("testself-joins-ddl.sql"), "testselfjoins", false);
    }

}
