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

import java.util.List;

import org.voltdb.VoltType;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ComparisonExpression;
import org.voltdb.expressions.FunctionExpression;
import org.voltdb.expressions.ParameterValueExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.NestLoopPlanNode;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.plannodes.ReceivePlanNode;
import org.voltdb.plannodes.SendPlanNode;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.plannodes.NodeSchema;
import org.voltdb.plannodes.SchemaColumn;
import org.voltdb.types.JoinType;

public class TestSubQueries   extends PlannerTestCase {

    public void testSubQuery() {
        {
            AbstractPlanNode pn = compile("select A, C FROM (SELECT A, C FROM R1) TEMP WHERE TEMP.A > 0");
            pn = pn.getChild(0);
            assertTrue(pn instanceof SeqScanPlanNode);
            AbstractExpression p = ((SeqScanPlanNode) pn).getPredicate();
            assertTrue(p != null);
            assertTrue(p instanceof ComparisonExpression);
            p = p.getLeft();
            assertTrue(p instanceof TupleValueExpression);
            assertEquals("TEMP", ((TupleValueExpression) p).getTableAlias());
            assertTrue(pn.getChildCount() == 1);
            assertTrue(pn.getChild(0) instanceof SeqScanPlanNode);
            verifyOutputSchema(pn, "A", "C");
        }

        {
            AbstractPlanNode pn = compile("select A1, C1 FROM (SELECT A A1, C C1 FROM R1) TEMP WHERE TEMP.A1 > 0");
            pn = pn.getChild(0);
            assertTrue(pn instanceof SeqScanPlanNode);
            verifyOutputSchema(pn, "A1", "C1");
        }

        {
            AbstractPlanNode pn = compile("select A2 FROM (SELECT A1 AS A2 FROM (SELECT A AS A1 FROM R1) TEMP1 WHERE TEMP1.A1 > 0) TEMP2 WHERE TEMP2.A2 = 3");
            pn = pn.getChild(0);
            assertTrue(pn instanceof SeqScanPlanNode);
            verifyOutputSchema(pn, "A2");
        }

        {
            AbstractPlanNode pn = compile("select A, C FROM (SELECT A FROM R1) TEMP1, (SELECT C FROM R2) TEMP2 WHERE A = C");
            pn = pn.getChild(0).getChild(0);
            assertTrue(pn instanceof NestLoopPlanNode);
            verifyOutputSchema(pn, "A", "C");
        }

        {
            // Function expression  for the temp table
            AbstractPlanNode pn = compile("select ABS(C1) FROM (SELECT A A1, C C1 FROM R1) TEMP WHERE ABS(TEMP.A1) > 3");
            pn = pn.getChild(0);
            assertTrue(pn instanceof SeqScanPlanNode);
            SchemaColumn col = pn.getOutputSchema().getColumns().get(0);
            assertEquals(4, col.getSize());
            assertEquals(VoltType.INTEGER, col.getType());
            verifyFunctionExpr(col.getExpression());
            AbstractExpression p = ((SeqScanPlanNode) pn).getPredicate();
            assertTrue(p != null);
            assertTrue(p instanceof ComparisonExpression);
            p = p.getLeft();
            verifyFunctionExpr(p);
        }

    }

    public void testParameters() {
        AbstractPlanNode pn = compile("select A1 FROM (SELECT A A1 FROM R1 WHERE A>?) TEMP WHERE A1<?");
        pn = pn.getChild(0);
        assertTrue(pn instanceof SeqScanPlanNode);
        AbstractExpression p = ((SeqScanPlanNode) pn).getPredicate();
        assertTrue(p != null);
        assertTrue(p instanceof ComparisonExpression);
        AbstractExpression cp = p.getLeft();
        assertTrue(cp instanceof TupleValueExpression);
        cp = p.getRight();
        assertTrue(cp instanceof ParameterValueExpression);
        assertEquals(1, ((ParameterValueExpression)cp).getParameterIndex().intValue());
        assertTrue(pn.getChildCount() == 1);
        assertTrue(pn.getChild(0) instanceof SeqScanPlanNode);
        SeqScanPlanNode sc = (SeqScanPlanNode) pn.getChild(0);
        assertTrue(sc.getPredicate() != null);
        p = sc.getPredicate();
        assertTrue(p instanceof ComparisonExpression);
        cp = p.getRight();
        assertTrue(cp instanceof ParameterValueExpression);
        assertEquals(0, ((ParameterValueExpression)cp).getParameterIndex().intValue());
    }

    public void testDistributedSubQuery() {
        {
            // Partitioned sub-query
            List<AbstractPlanNode> lpn = compileToFragments("select A, C FROM (SELECT A, C FROM P1) TEMP ");
            assertTrue(lpn.size() == 2);
            AbstractPlanNode n = lpn.get(1);
            n = n.getChild(0);
            assertTrue(n instanceof SeqScanPlanNode);
            assertEquals("TEMP", ((SeqScanPlanNode) n).getTargetTableName());
            n = n.getChild(0);
            assertTrue(n instanceof ProjectionPlanNode);
            assertTrue(n.getChild(0) instanceof IndexScanPlanNode);
        }

        {
            // Two sub-queries. One is partitioned and the other one is replicated
            List<AbstractPlanNode> lpn = compileToFragments("select A, C FROM (SELECT A FROM R1) TEMP1, (SELECT C FROM P2) TEMP2 WHERE TEMP1.A = TEMP2.C ");
            assertTrue(lpn.size() == 2);
            AbstractPlanNode n = lpn.get(0);
            assertTrue(n instanceof SendPlanNode);
            n = lpn.get(1);
            assertTrue(n instanceof SendPlanNode);
            n = n.getChild(0);
            assertTrue(n instanceof NestLoopPlanNode);
            AbstractPlanNode c = n.getChild(0);
            assertTrue(c instanceof SeqScanPlanNode);
            assertEquals("TEMP1", ((SeqScanPlanNode) c).getTargetTableAlias());
            c = n.getChild(1);
            assertTrue(c instanceof SeqScanPlanNode);
            assertEquals("TEMP2", ((SeqScanPlanNode) c).getTargetTableAlias());
        }

        {
            // Join of two multi-partitioned sub-queries on non-partition column. Should fail
            failToCompile("select A, C FROM (SELECT A FROM P1) TEMP1, (SELECT C FROM P2) TEMP2 WHERE TEMP1.A = TEMP2.C ",
                    "Join of multiple partitioned tables has insufficient join criteria.");
        }

        {
            // Join of a single partitioned sub-query and a table. The partition is the same for
            // the table and sub-query
            List<AbstractPlanNode> lpn = compileToFragments("select D1, P2.D FROM (SELECT A, D D1 FROM P1 WHERE A=1) TEMP1, P2 WHERE TEMP1.A = P2.A AND P2.A = 1");
            assertTrue(lpn.size() == 1);
            AbstractPlanNode n = lpn.get(0).getChild(0).getChild(0);
            assertTrue(n instanceof NestLoopPlanNode);
            AbstractPlanNode c = n.getChild(0);
            assertTrue(c instanceof SeqScanPlanNode);
            assertEquals("TEMP1", ((SeqScanPlanNode) c).getTargetTableAlias());
        }

        {
            // Join of two partitioned sub-queries on the partition column
            List<AbstractPlanNode> lpn = compileToFragments("select D1, D2 FROM (SELECT A, D D1 FROM P1 ) TEMP1, (SELECT A, D D2 FROM P2 ) TEMP2 WHERE TEMP1.A = TEMP2.A");
            assertTrue(lpn.size() == 2);
            AbstractPlanNode n = lpn.get(1);
            assertTrue(n instanceof SendPlanNode);
            n = n.getChild(0);
            assertTrue(n instanceof NestLoopPlanNode);
        }

        {
            // Join of a single partitioned sub-queries. The partitions are different
            failToCompile("select D1, D2 FROM (SELECT A, D D1 FROM P1 WHERE A=2) TEMP1, (SELECT A, D D2 FROM P2 WHERE A=2) TEMP2",
            "Join of multiple partitioned tables has insufficient join criteria.");
        }

        {
            // Join of a single partitioned sub-queries. The partitions are different
            failToCompile("select D1, D2 FROM (SELECT A, D D1 FROM P1) TEMP1, (SELECT A, D D2 FROM P2) TEMP2 WHERE TEMP1.A = 1 AND TEMP2.A = 2",
            "Join of multiple partitioned tables has insufficient join criteria.");
        }
     }

    public void testOuterJoinSubQuery() {
        {
            List<AbstractPlanNode> lpn = compileToFragments("SELECT A, C FROM R1 LEFT JOIN (SELECT A, C FROM P1) TEMP ON TEMP.C = R1.C ");
            assertTrue(lpn.size() == 2);
            AbstractPlanNode n = lpn.get(0).getChild(0).getChild(0);
            assertTrue(n instanceof NestLoopPlanNode);
            assertEquals(JoinType.LEFT, ((NestLoopPlanNode) n).getJoinType());
            n = n.getChild(0);
            assertTrue(n instanceof SeqScanPlanNode);
            assertEquals("R1", ((SeqScanPlanNode) n).getTargetTableName());
            n = lpn.get(1).getChild(0);
            assertTrue(n instanceof SeqScanPlanNode);
            assertEquals("TEMP", ((SeqScanPlanNode) n).getTargetTableName());
            assertEquals(1, n.getChildCount());
            n = n.getChild(0);
            assertTrue(n instanceof ProjectionPlanNode);
            assertTrue(n.getChild(0) instanceof IndexScanPlanNode);
        }
    }


//    public void testWhereSubquery() {
//        AbstractPlanNode pn = compile("DELETE FROM R1 WHERE A IN (SELECT A A1 FROM R1 WHERE A>1)");
//        pn = pn.getChild(0);
//    }

    private void verifyOutputSchema(AbstractPlanNode pn, String... columns) {
        NodeSchema ns = pn.getOutputSchema();
        List<SchemaColumn> scs = ns.getColumns();
        for (int i = 0; i < scs.size(); ++i) {
            SchemaColumn col = scs.get(i);
            assertEquals(columns[i], col.getColumnName());
            assertEquals(4, col.getSize());
            assertEquals(VoltType.INTEGER, col.getType());
            assertTrue(col.getExpression() instanceof TupleValueExpression);
            assertTrue(((TupleValueExpression)col.getExpression()).getColumnIndex() != -1);
        }
    }

    private void verifyFunctionExpr(AbstractExpression expr) {
        assertTrue( expr != null);
        assertTrue( expr instanceof FunctionExpression);
        FunctionExpression f = (FunctionExpression) expr;
        List<AbstractExpression> args = f.getArgs();
        assertEquals(1, args.size());
        assertTrue(args.get(0) instanceof TupleValueExpression);
        TupleValueExpression tve = (TupleValueExpression) args.get(0);
        assertEquals(4, tve.getValueSize());
        assertEquals(VoltType.INTEGER, tve.getValueType());
    }

    @Override
    protected void setUp() throws Exception {
        setupSchema(TestJoinOrder.class.getResource("testsub-queries-ddl.sql"), "testsubqueries", false);
    }

}
