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

import org.voltdb.VoltType;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.AbstractSubqueryExpression;
import org.voltdb.expressions.ComparisonExpression;
import org.voltdb.expressions.ParameterValueExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.AggregatePlanNode;
import org.voltdb.plannodes.LimitPlanNode;
import org.voltdb.plannodes.NestLoopIndexPlanNode;
import org.voltdb.plannodes.NestLoopPlanNode;
import org.voltdb.plannodes.NodeSchema;
import org.voltdb.plannodes.OrderByPlanNode;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.plannodes.SchemaColumn;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.PlanNodeType;
import org.voltdb.types.QuantifierType;

public class TestPlansInExistsSubQueries extends PlannerTestCase {

    public void testInExistsGuard() {
        String errorMsg = PlanAssembler.IN_EXISTS_SCALAR_ERROR_MESSAGE;
        String sql;

        sql = "select p2.c from p2 where p2.c > ? and exists (select c from r1 where r1.c = p2.c)";
        failToCompile(sql, errorMsg);

        sql = "select p2.c from p2 where p2.a in (select c from r1)";
        failToCompile(sql, errorMsg);

        sql = "select r2.c from r2 where r2.c > ? and exists (select c from p1 where p1.c = r2.c)";
        failToCompile(sql, errorMsg);

        sql = "select * from P1 as parent where (A,C) in (select 2, C from r2 where r2.c > parent.c group by c)";
        failToCompile(sql, errorMsg);

        sql = "select r2.c from r2 where r2.c > ? and exists (select c from r1 where r1.c = r2.c and "
                + "exists (select c from p1 where p1.c = r1.c ))";
        failToCompile(sql, errorMsg);
    }

    public void testExistsWithUserParams() {
        {
            // Parent query with user's param and subquery with parent TVE
            AbstractPlanNode pn = compile("select r2.c from r2 where r2.c > ? and exists (select c from r1 where r1.c = r2.c)");
            pn = pn.getChild(0);
            assertTrue(pn instanceof AbstractScanPlanNode);
            AbstractScanPlanNode nps = (AbstractScanPlanNode) pn;
            // Check param indexes
            AbstractExpression e = nps.getPredicate();
            AbstractExpression le = e.getLeft();
            assertEquals(ExpressionType.COMPARE_GREATERTHAN, le.getExpressionType());
            AbstractExpression pve = le.getRight();
            assertEquals(ExpressionType.VALUE_PARAMETER, pve.getExpressionType());
            assertEquals(new Integer(0), ((ParameterValueExpression)pve).getParameterIndex());
            AbstractExpression re = e.getRight();
            assertEquals(ExpressionType.OPERATOR_EXISTS, re.getExpressionType());
            AbstractSubqueryExpression se = (AbstractSubqueryExpression) re.getLeft();
            assertEquals(1, se.getArgs().size());
            assertEquals(1, se.getParameterIdxList().size());
            assertEquals(Integer.valueOf(1), se.getParameterIdxList().get(0));
        }
        {
            // Subqueries  with  grand-parent TVE
            AbstractPlanNode pn = compile("select r1.c from r1 where  " +
                    "exists ( select 1 from r2 where exists" +
                    "(select 1 from r3 where r3.a = r1.c))");
            pn = pn.getChild(0);
            assertTrue(pn instanceof AbstractScanPlanNode);
            AbstractScanPlanNode spn = (AbstractScanPlanNode) pn;
            // Check param indexes
            AbstractExpression e = spn.getPredicate();
            assertEquals(ExpressionType.OPERATOR_EXISTS, e.getExpressionType());
            AbstractSubqueryExpression se = (AbstractSubqueryExpression) e.getLeft();
            List<AbstractExpression> args = se.getArgs();
            assertEquals(1, args.size());
            assertEquals(ExpressionType.VALUE_TUPLE, args.get(0).getExpressionType());
            TupleValueExpression tve = (TupleValueExpression) args.get(0);
            assertEquals("R1", tve.getTableName());
            assertEquals("C", tve.getColumnName());
            List<Integer> params = se.getParameterIdxList();
            assertEquals(1, params.size());
            assertEquals(new Integer(0), params.get(0));
            // Child subquery
            pn = se.getSubqueryNode();
            assertTrue(pn instanceof AbstractScanPlanNode);
            spn = (AbstractScanPlanNode) pn;
            e = spn.getPredicate();
            assertEquals(ExpressionType.OPERATOR_EXISTS, e.getExpressionType());
            se = (AbstractSubqueryExpression) e.getLeft();
            // Grand parent subquery
            pn = se.getSubqueryNode();
            assertTrue(pn instanceof AbstractScanPlanNode);
            spn = (AbstractScanPlanNode) pn;
            e = spn.getPredicate();
            assertEquals(ExpressionType.COMPARE_EQUAL, e.getExpressionType());
            e = e.getRight();
            assertEquals(ExpressionType.VALUE_PARAMETER, e.getExpressionType());
            assertEquals(new Integer(0), ((ParameterValueExpression) e).getParameterIndex());
        }
        {
            // Subqueries  with  parent & grand-parent TVE
            AbstractPlanNode pn = compile("select r1.c from r1 where  " +
                    "exists ( select 1 from r2 where r1.d = r2.c and exists" +
                    "(select 1 from r3 where r3.a = r1.c))");
            pn = pn.getChild(0);
            assertTrue(pn instanceof AbstractScanPlanNode);
            AbstractScanPlanNode spn = (AbstractScanPlanNode) pn;
            // Check param indexes
            AbstractExpression e = spn.getPredicate();
            assertEquals(ExpressionType.OPERATOR_EXISTS, e.getExpressionType());
            AbstractSubqueryExpression se = (AbstractSubqueryExpression) e.getLeft();
            List<AbstractExpression> args = se.getArgs();
            assertEquals(2, args.size());
            assertEquals(ExpressionType.VALUE_TUPLE, args.get(0).getExpressionType());
            TupleValueExpression tve = (TupleValueExpression) args.get(0);
            assertEquals("R1", tve.getTableName());
            assertEquals("C", tve.getColumnName());
            tve = (TupleValueExpression) args.get(1);
            assertEquals("R1", tve.getTableName());
            assertEquals("D", tve.getColumnName());
            List<Integer> params = se.getParameterIdxList();
            assertEquals(2, params.size());
            assertEquals(new Integer(0), params.get(0));
            assertEquals(new Integer(1), params.get(1));
            // Child subquery
            pn = se.getSubqueryNode();
            assertTrue(pn instanceof AbstractScanPlanNode);
            spn = (AbstractScanPlanNode) pn;
            e = spn.getPredicate();
            assertEquals(ExpressionType.COMPARE_EQUAL, e.getRight().getExpressionType());
            AbstractExpression ce = e.getRight().getRight();
            assertEquals(ExpressionType.VALUE_PARAMETER, ce.getExpressionType());
            assertEquals(new Integer(1), ((ParameterValueExpression) ce).getParameterIndex());
            // Grand parent subquery
            AbstractExpression gce = e.getLeft();
            assertEquals(ExpressionType.OPERATOR_EXISTS, gce.getExpressionType());
            se = (AbstractSubqueryExpression) gce.getLeft();
            pn = se.getSubqueryNode();
            assertTrue(pn instanceof AbstractScanPlanNode);
            spn = (AbstractScanPlanNode) pn;
            e = spn.getPredicate();
            assertEquals(ExpressionType.COMPARE_EQUAL, e.getExpressionType());
            e = e.getRight();
            assertEquals(ExpressionType.VALUE_PARAMETER, e.getExpressionType());
            assertEquals(new Integer(0), ((ParameterValueExpression) e).getParameterIndex());
        }
        {
            AbstractPlanNode pn = compile("select r2.a from r2 where exists " +
                    "( SELECT 1 from R2 WHERE r2.c = ?)");
            pn = pn.getChild(0);
            assertEquals(PlanNodeType.SEQSCAN, pn.getPlanNodeType());
            SeqScanPlanNode spl = (SeqScanPlanNode) pn;
            AbstractExpression e = spl.getPredicate();
            assertEquals(ExpressionType.OPERATOR_EXISTS, e.getExpressionType());
            AbstractSubqueryExpression subExpr = (AbstractSubqueryExpression) e.getLeft();
            assertEquals(0, subExpr.getParameterIdxList().size());
            // Subquery
            pn = subExpr.getSubqueryNode();
            assertEquals(PlanNodeType.SEQSCAN, pn.getPlanNodeType());
            spl = (SeqScanPlanNode) pn;
            e = spl.getPredicate();
            assertEquals(ExpressionType.COMPARE_EQUAL, e.getExpressionType());
            assertEquals(ExpressionType.VALUE_PARAMETER, e.getRight().getExpressionType());
        }
    }

    public void testParamTveInOutputSchema() {
        AbstractPlanNode pn = compile("select r2.a from r2, r1 where r2.a = r1.a or " +
                "exists (select 1 from r2 where exists(select 1 from r2 where r2.a = r1.c))");
        pn = pn.getChild(0);
        verifyOutputSchema(pn, "A");
        pn = pn.getChild(0);
        assertTrue(pn instanceof NestLoopPlanNode);
        NestLoopPlanNode nlp = (NestLoopPlanNode) pn;
        // looking for the r1.c to be part of the pn output schema
        // it is required by the second subquery
        verifyOutputSchema(nlp, "A", "A", "C");
    }

    public void testInToExist() {
        AbstractPlanNode pn = compile("select r2.c from r2 where r2.a in (select c from r1)");
        pn = pn.getChild(0);
        assertTrue(pn instanceof AbstractScanPlanNode);
        AbstractScanPlanNode spl = (AbstractScanPlanNode) pn;
        // Check param indexes
        AbstractExpression e = spl.getPredicate();
        assertEquals(ExpressionType.OPERATOR_EXISTS, e.getExpressionType());
        AbstractSubqueryExpression subExpr = (AbstractSubqueryExpression) e.getLeft();
        assertEquals(1, subExpr.getArgs().size());
        assertEquals(1, subExpr.getParameterIdxList().size());
        assertEquals(Integer.valueOf(0), subExpr.getParameterIdxList().get(0));
        AbstractExpression tve = subExpr.getArgs().get(0);
        assertTrue(tve instanceof TupleValueExpression);
        assertEquals("R2", ((TupleValueExpression)tve).getTableName());
        assertEquals("A", ((TupleValueExpression)tve).getColumnName());
    }

    public void testInToExistWithUnion() {
        AbstractPlanNode pn = compile("select r2.c from r2 where r2.a in (select c from r1 union (select c from r3 limit 1 offset 2) intersect select c from r2)");
        pn = pn.getChild(0);
        assertTrue(pn instanceof AbstractScanPlanNode);
        AbstractScanPlanNode spl = (AbstractScanPlanNode) pn;
        // Check param indexes
        AbstractExpression e = spl.getPredicate();
        assertEquals(ExpressionType.CONJUNCTION_OR, e.getExpressionType());
        AbstractExpression l = e.getLeft();
        assertEquals(ExpressionType.OPERATOR_EXISTS, l.getExpressionType());
        AbstractExpression r = e.getRight();
        assertEquals(ExpressionType.CONJUNCTION_AND, r.getExpressionType());
        l = r.getLeft();
        assertEquals(ExpressionType.COMPARE_EQUAL, l.getExpressionType());
        assertEquals(QuantifierType.ANY, ((ComparisonExpression) l).getQuantifier());
        r = r.getRight();
        assertEquals(ExpressionType.OPERATOR_EXISTS, r.getExpressionType());
    }

    public void testInToExistWithOffset() {
        AbstractPlanNode pn = compile("select r2.c from r2 where r2.a in (select c from r1 limit 1 offset 3)");
        pn = pn.getChild(0);
        assertTrue(pn instanceof AbstractScanPlanNode);
        AbstractScanPlanNode spl = (AbstractScanPlanNode) pn;
        // Check param indexes
        AbstractExpression e = spl.getPredicate();
        assertEquals(ExpressionType.COMPARE_EQUAL, e.getExpressionType());
        assertEquals(QuantifierType.ANY, ((ComparisonExpression) e).getQuantifier());
    }

    public void testInToExistsComplex() {
        AbstractPlanNode pn = compile("select * from R1 where (A,C) in (select 2, C from r2 where r2.c > r1.c group by c)");
        pn = pn.getChild(0);
        assertTrue(pn instanceof AbstractScanPlanNode);
        AbstractScanPlanNode spn = (AbstractScanPlanNode) pn;
        AbstractExpression e = spn.getPredicate();
        assertEquals(ExpressionType.OPERATOR_EXISTS, e.getExpressionType());
        AbstractSubqueryExpression subExpr = (AbstractSubqueryExpression) e.getLeft();
        assertEquals(3, subExpr.getArgs().size());
        assertEquals(3, subExpr.getParameterIdxList().size());
    }

    public void testNotExists() {
        AbstractPlanNode pn = compile("select r2.c from r2 where not exists (select c from r1 where r1.c = r2.c)");
        pn = pn.getChild(0);
        assertTrue(pn instanceof AbstractScanPlanNode);
        AbstractScanPlanNode nps = (AbstractScanPlanNode) pn;
        AbstractExpression e = nps.getPredicate();
        assertEquals(ExpressionType.OPERATOR_NOT, e.getExpressionType());
        AbstractExpression le = e.getLeft();
        assertEquals(ExpressionType.OPERATOR_EXISTS, le.getExpressionType());
        AbstractSubqueryExpression subExpr = (AbstractSubqueryExpression) le.getLeft();
        assertEquals(1, subExpr.getArgs().size());
        assertEquals(1, subExpr.getParameterIdxList().size());
        assertEquals(Integer.valueOf(0), subExpr.getParameterIdxList().get(0));
    }

    public void testExistsJoin() {
        {
            AbstractPlanNode pn = compile("select a from r1,r2 where exists (" +
                    "select 1 from r3 where r1.d = r3.c and r2.a = r3.c)");
            pn = pn.getChild(0).getChild(0);
            assertEquals(PlanNodeType.NESTLOOP, pn.getPlanNodeType());
            AbstractExpression e = ((NestLoopPlanNode) pn).getJoinPredicate();
            assertEquals(ExpressionType.OPERATOR_EXISTS, e.getExpressionType());
            AbstractSubqueryExpression se = (AbstractSubqueryExpression) e.getLeft();
            List<AbstractExpression> args = se.getArgs();
            assertEquals(2, args.size());
            TupleValueExpression tve = (TupleValueExpression)args.get(0);
            assertEquals("R2", tve.getTableName());
            assertEquals("A", tve.getColumnName());
            tve = (TupleValueExpression)args.get(1);
            assertEquals("R1", tve.getTableName());
            assertEquals("D", tve.getColumnName());
            assertEquals(2, se.getParameterIdxList().size());
            // Child query
            pn = se.getSubqueryNode();
            e = ((AbstractScanPlanNode)pn).getPredicate();
            AbstractExpression le = e.getLeft();
            assertEquals(ExpressionType.VALUE_PARAMETER, le.getRight().getExpressionType());
            AbstractExpression re = e.getRight();
            assertEquals(ExpressionType.VALUE_PARAMETER, re.getRight().getExpressionType());
        }
        {
            AbstractPlanNode pn = compile("select a from r1,r2 where r1.a = r2.a and " +
                    "exists ( select 1 from r3 where r1.a = r3.a)");

            pn = pn.getChild(0).getChild(0);
            assertTrue(pn instanceof NestLoopIndexPlanNode);
            pn = pn.getChild(0);
            assertTrue(pn instanceof SeqScanPlanNode);
            AbstractExpression pred = ((SeqScanPlanNode) pn).getPredicate();
            assertTrue(pred != null);
            assertEquals(ExpressionType.OPERATOR_EXISTS, pred.getExpressionType());
        }
        {
            AbstractPlanNode pn = compile("select a from r1,r2 where " +
                    "exists ( select 1 from r3 where r1.a = r3.a and r2.c = r3.c)");

            pn = pn.getChild(0).getChild(0);
            assertTrue(pn instanceof NestLoopPlanNode);
            AbstractExpression pred = ((NestLoopPlanNode) pn).getJoinPredicate();
            assertTrue(pred != null);
            assertEquals(ExpressionType.OPERATOR_EXISTS, pred.getExpressionType());
            AbstractSubqueryExpression se = (AbstractSubqueryExpression) pred.getLeft();
            List<AbstractExpression> args = se.getArgs();
            assertEquals(2, args.size());
            TupleValueExpression tve = (TupleValueExpression)args.get(0);
            assertEquals("R2", tve.getTableName());
            assertEquals("C", tve.getColumnName());
            tve = (TupleValueExpression)args.get(1);
            assertEquals("R1", tve.getTableName());
            assertEquals("A", tve.getColumnName());
            assertEquals(2, se.getParameterIdxList().size());
            // Child query
            pn = se.getSubqueryNode();
            pred = ((AbstractScanPlanNode)pn).getPredicate();
            AbstractExpression le = pred.getLeft();
            assertEquals(ExpressionType.VALUE_PARAMETER, le.getRight().getExpressionType());
            AbstractExpression re = pred.getRight();
            assertEquals(ExpressionType.VALUE_PARAMETER, re.getRight().getExpressionType());
        }
    }

    public void testInJoin() {
        {
            // IN gets converted to EXISTS
            AbstractPlanNode pn = compile("select r1.d from r1, r2 where r1.a IN " +
                    "(select a from r3 where r3.c = r2.d);");
            pn = pn.getChild(0).getChild(0);
            assertTrue(pn instanceof NestLoopPlanNode);
            AbstractExpression pred = ((NestLoopPlanNode) pn).getJoinPredicate();
            assertTrue(pred != null);
            assertEquals(ExpressionType.OPERATOR_EXISTS, pred.getExpressionType());
            AbstractSubqueryExpression se = (AbstractSubqueryExpression) pred.getLeft();
            List<AbstractExpression> args = se.getArgs();
            assertEquals(2, args.size());
            TupleValueExpression tve = (TupleValueExpression)args.get(0);
            assertEquals("R2", tve.getTableName());
            assertEquals("D", tve.getColumnName());
            tve = (TupleValueExpression)args.get(1);
            assertEquals("R1", tve.getTableName());
            assertEquals("A", tve.getColumnName());
            assertEquals(2, se.getParameterIdxList().size());
            // Child query
            pn = se.getSubqueryNode();
            pred = ((AbstractScanPlanNode)pn).getPredicate();
            AbstractExpression le = pred.getLeft();
            assertEquals(ExpressionType.VALUE_PARAMETER, le.getRight().getExpressionType());
            AbstractExpression re = pred.getRight();
            assertEquals(ExpressionType.VALUE_PARAMETER, re.getLeft().getExpressionType());
        }
        {
            // OFFSET prevents In-to-EXISTS transformation
            AbstractPlanNode pn = compile("select r1.d from r1, r2 where r1.a IN " +
                    "(select a from r3 where r3.c = r2.d limit 1 offset 2);");
            /* enable to debug */ System.out.println(pn.toExplainPlanString());
            pn = pn.getChild(0).getChild(0);
            assertTrue(pn instanceof NestLoopPlanNode);
            AbstractExpression pred = ((NestLoopPlanNode) pn).getJoinPredicate();
            assertTrue(pred != null);
            assertEquals(ExpressionType.COMPARE_EQUAL, pred.getExpressionType());
            assertEquals(QuantifierType.ANY, ((ComparisonExpression) pred).getQuantifier());
            TupleValueExpression tve = (TupleValueExpression) pred.getLeft();
            assertEquals("R1", tve.getTableName());
            assertEquals("A", tve.getColumnName());
            // Child query
            AbstractSubqueryExpression se = (AbstractSubqueryExpression)pred.getRight();
            assertEquals(1, se.getParameterIdxList().size());
            List<AbstractExpression> args = se.getArgs();
            assertEquals(1, args.size());
            tve = (TupleValueExpression)args.get(0);
            assertEquals("R2", tve.getTableName());
            assertEquals("D", tve.getColumnName());
            pn = se.getSubqueryNode();
            assertEquals(PlanNodeType.SEQSCAN, pn.getPlanNodeType());
            pred = ((AbstractScanPlanNode)pn).getPredicate();
            tve = (TupleValueExpression) pred.getLeft();
            assertEquals("R3", tve.getTableName());
            assertEquals("C", tve.getColumnName());
            ParameterValueExpression pve = (ParameterValueExpression) pred.getRight();
            assertEquals(new Integer(0), pve.getParameterIndex());
        }
    }

    public void testInAggregated() {
        AbstractPlanNode pn = compile("select a, sum(c) as sc1 from r1 where (a, c) in " +
                "( SELECT a, count(c) as sc2 " +
                "from  r1  GROUP BY a ORDER BY a DESC) GROUP BY A;");
        pn = pn.getChild(0);
        assertTrue(pn instanceof AbstractScanPlanNode);

        AbstractExpression e = ((AbstractScanPlanNode)pn).getPredicate();
        assertEquals(ExpressionType.OPERATOR_EXISTS, e.getExpressionType());
        AbstractSubqueryExpression subExpr = (AbstractSubqueryExpression) e.getLeft();
        AbstractPlanNode sn = subExpr.getSubqueryNode();
        //* enable to debug */ System.out.println(sn.toExplainPlanString());
////        assertTrue(sn instanceof ProjectionPlanNode);
////        sn = sn.getChild(0);
////TODO: This ORDER BY serves no purpose?
        assertTrue(sn instanceof OrderByPlanNode);
        assertNotNull(sn.getInlinePlanNode(PlanNodeType.LIMIT));
        sn = sn.getChild(0);
////TODO: This PROJECTION (not even inline) serves no purpose?
        assertTrue(sn instanceof ProjectionPlanNode);
        sn = sn.getChild(0);
        assertTrue(sn instanceof SeqScanPlanNode);

        AggregatePlanNode aggNode = AggregatePlanNode.getInlineAggregationNode(sn);
        assertNotNull(aggNode.getPostPredicate());
    }

    public void testInHaving() {
        AbstractPlanNode pn = compile("select a from r1 " +
                " group by a having max(c) in (select c from r2 )");

        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof SeqScanPlanNode);
        AggregatePlanNode aggNode = AggregatePlanNode.getInlineAggregationNode(pn);
        assertNotNull(aggNode);
        NodeSchema ns = aggNode.getOutputSchema();
        assertEquals(2, ns.size());
        SchemaColumn aggColumn = ns.getColumns().get(1);
        assertEquals("$$_MAX_$$_1", aggColumn.getColumnAlias());
        AbstractExpression having = aggNode.getPostPredicate();
        assertEquals(ExpressionType.OPERATOR_EXISTS, having.getExpressionType());
        AbstractExpression se = having.getLeft();
        assertEquals(1, se.getArgs().size());
        assertTrue(se.getArgs().get(0) instanceof TupleValueExpression);
        TupleValueExpression argTve = (TupleValueExpression) se.getArgs().get(0);
        assertEquals(1, argTve.getColumnIndex());
        assertEquals("$$_MAX_$$_1", argTve.getColumnAlias());

    }

    public void testHavingInSubquery() {
        {
            // filter on agg of expression involving grand-parent tve
            AbstractPlanNode pn = compile("select a from r1 where exists " +
                    "(select 1 from r2 where exists  " +
                    " (select 1 from r3 group by c having min(a) > r1.d)) ");

            pn = pn.getChild(0);
            assertTrue(pn instanceof SeqScanPlanNode);
            AbstractExpression p = ((SeqScanPlanNode)pn).getPredicate();
            // child
            assertEquals(ExpressionType.OPERATOR_EXISTS, p.getExpressionType());
            AbstractSubqueryExpression se = (AbstractSubqueryExpression) p.getLeft();
            //* enable to debug */ System.out.println(se.explain(""));
            List<AbstractExpression> args = se.getArgs();
            assertEquals(1, args.size());
            assertEquals(1, se.getParameterIdxList().size());
            assertEquals("D", ((TupleValueExpression)args.get(0)).getColumnName());
            pn = se.getSubqueryNode();
            assertTrue(pn instanceof SeqScanPlanNode);
            p = ((SeqScanPlanNode)pn).getPredicate();
            // grand child
            assertEquals(ExpressionType.OPERATOR_EXISTS, p.getExpressionType());
            se = (AbstractSubqueryExpression) p.getLeft();
            pn = se.getSubqueryNode();
            pn = pn.getChild(0).getChild(0);
            AggregatePlanNode aggNode = AggregatePlanNode.getInlineAggregationNode(pn);
            assertNotNull(aggNode);
            AbstractExpression postExpr = aggNode.getPostPredicate();
            assertNotNull(postExpr);
            assertEquals(ExpressionType.COMPARE_GREATERTHAN, postExpr.getExpressionType());
            AbstractExpression re = postExpr.getRight();
            assertEquals(ExpressionType.VALUE_PARAMETER, re.getExpressionType());
            assertEquals(new Integer(0), ((ParameterValueExpression)re).getParameterIndex());
        }
        {
            // filter on agg of expression involving parent tve
            AbstractPlanNode pn = compile("select a from r1 where c in " +
                    " (select max(c) from r2 group by e having min(a) > r1.d) ");

            pn = pn.getChild(0);
            assertTrue(pn instanceof SeqScanPlanNode);
            AbstractExpression p = ((SeqScanPlanNode)pn).getPredicate();
            assertEquals(ExpressionType.OPERATOR_EXISTS, p.getExpressionType());
            AbstractSubqueryExpression se = (AbstractSubqueryExpression) p.getLeft();
            List<AbstractExpression> args = se.getArgs();
            assertEquals(2, args.size());
            assertEquals(2, se.getParameterIdxList().size());
            assertEquals("D", ((TupleValueExpression)args.get(0)).getColumnName());
            assertEquals("C", ((TupleValueExpression)args.get(1)).getColumnName());
            pn = se.getSubqueryNode();
            pn = pn.getChild(0);
            assertTrue(pn instanceof LimitPlanNode);
            pn = pn.getChild(0);
            assertTrue(pn instanceof SeqScanPlanNode);
            AggregatePlanNode aggNode = AggregatePlanNode.getInlineAggregationNode(pn);
            assertNotNull(aggNode);
            assertEquals(3, aggNode.getOutputSchema().size());
            AbstractExpression postExpr = aggNode.getPostPredicate();
            assertEquals(ExpressionType.CONJUNCTION_AND, postExpr.getExpressionType());
            AbstractExpression le = postExpr.getLeft();
            assertEquals(ExpressionType.COMPARE_GREATERTHAN, le.getExpressionType());
            assertEquals(new Integer(0), ((ParameterValueExpression)le.getRight()).getParameterIndex());
            AbstractExpression re = postExpr.getRight();
            assertEquals(ExpressionType.COMPARE_EQUAL, re.getExpressionType());
            assertEquals(new Integer(1), ((ParameterValueExpression)re.getLeft()).getParameterIndex());
        }
        {
            // filter on agg of expression involving user parameter ('?')
            AbstractPlanNode pn = compile("select a from r1 where c in " +
                    " (select max(c) from r2 group by e having min(a) > ?) ");

            pn = pn.getChild(0);
            assertTrue(pn instanceof SeqScanPlanNode);
            AbstractExpression p = ((SeqScanPlanNode)pn).getPredicate();
            assertEquals(ExpressionType.OPERATOR_EXISTS, p.getExpressionType());
            AbstractSubqueryExpression se = (AbstractSubqueryExpression)p.getLeft();
            assertEquals(1, se.getParameterIdxList().size());
            assertEquals(new Integer(1), se.getParameterIdxList().get(0));
            pn = ((AbstractSubqueryExpression)p.getLeft()).getSubqueryNode();
            pn = pn.getChild(0).getChild(0);
            assertEquals(PlanNodeType.SEQSCAN, pn.getPlanNodeType());
            AggregatePlanNode aggNode = AggregatePlanNode.getInlineAggregationNode(pn);
            assertNotNull(aggNode);
            AbstractExpression aggrExpr = aggNode.getPostPredicate();
            assertEquals(ExpressionType.CONJUNCTION_AND, aggrExpr.getExpressionType());
            // User PVE
            AbstractExpression le = aggrExpr.getLeft();
            assertEquals(ExpressionType.COMPARE_GREATERTHAN, le.getExpressionType());
            assertEquals(ExpressionType.VALUE_PARAMETER, le.getRight().getExpressionType());
            assertEquals(new Integer(0), ((ParameterValueExpression)le.getRight()).getParameterIndex());
            // Parent PVE
            AbstractExpression re = aggrExpr.getRight();
            assertEquals(ExpressionType.COMPARE_EQUAL, re.getExpressionType());
            assertEquals(ExpressionType.VALUE_PARAMETER, re.getLeft().getExpressionType());
            assertEquals(new Integer(1), ((ParameterValueExpression)re.getLeft()).getParameterIndex());
        }
        {
            // filter on agg of local tve
            AbstractPlanNode pn = compile("select a from r1 where c in " +
                    " (select max(c) from r2 group by e having min(a) > 0) ");

            pn = pn.getChild(0);
            assertTrue(pn instanceof SeqScanPlanNode);
            AbstractExpression p = ((SeqScanPlanNode)pn).getPredicate();
            assertEquals(ExpressionType.OPERATOR_EXISTS, p.getExpressionType());
            AbstractExpression subquery = p.getLeft();
            pn = ((AbstractSubqueryExpression)subquery).getSubqueryNode();
            pn = pn.getChild(0);
            assertTrue(pn instanceof LimitPlanNode);
            pn = pn.getChild(0);
            assertTrue(pn instanceof SeqScanPlanNode);
            AggregatePlanNode aggNode = AggregatePlanNode.getInlineAggregationNode(pn);
            assertNotNull(aggNode);
            assertEquals(3, aggNode.getOutputSchema().size());
            AbstractExpression aggrExpr = aggNode.getPostPredicate();
            assertEquals(ExpressionType.CONJUNCTION_AND, aggrExpr.getExpressionType());
        }
    }

    public void testSendReceiveInSubquery() {
        //      compileToFragments("select * from r1, (select * from r2 left join P1 on r2.a = p1.c left join r1 on p1.c = r1.a) t where r1.c = 1");
    }

    // HSQL failed to parse  these statement
    public void testHSQLFailed() {
        {
            failToCompile("select a from r1 where exists (" +
                    "select 1 from r2 group by r2.a having max(r1.a + r2.a) in (" +
                    " select max(a) from r3))",
                    "expression not in aggregate or GROUP BY columns");
        }
        {
            failToCompile("select a from r1 group by a " +
                    " having exists (select c from r2 where r2.c = max(r1.a))",
                    "subquery with WHERE expression with aggregates on parent columns are not supported");
        }
        {
            // parent correlated TVE in the aggregate expression. NulPointerExpression
            failToCompile("select max(c) from r1 group by a " +
                    " having count(*) = (select c from r2 where r2.c = r1.a)",
                    "");
        }
    }

    // Disabled for now
    //    public void testDeleteWhereIn() {
    //        List<AbstractPlanNode> lpn = compileToFragments("delete from r1 where a in (select a from r2 where r1.c = r2.c)");
    //        assertTrue(lpn.size() == 2);
    //        AbstractPlanNode n = lpn.get(1).getChild(0);
    //        assertTrue(n instanceof DeletePlanNode);
    //        n = n.getChild(0);
    //        assertTrue(n instanceof AbstractScanPlanNode);
    //        AbstractScanPlanNode spn = (AbstractScanPlanNode) n;
    //        AbstractExpression e = spn.getPredicate();
    //        assertEquals(ExpressionType.SUBQUERY, e.getExpressionType());
    //    }

    // Disabled for now
    //    public void testUpdateWhereIn() {
    //      List<AbstractPlanNode> lpn = compileToFragments("update r1 set c = 1 where a in (select a from r2 where r1.c = r2.c)");
    //      assertTrue(lpn.size() == 2);
    //      AbstractPlanNode n = lpn.get(1).getChild(0);
    //      assertTrue(n instanceof UpdatePlanNode);
    //      n = n.getChild(0);
    //      assertTrue(n instanceof AbstractScanPlanNode);
    //      AbstractScanPlanNode spn = (AbstractScanPlanNode) n;
    //      AbstractExpression e = spn.getPredicate();
    //      assertEquals(ExpressionType.SUBQUERY, e.getExpressionType());
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


    @Override
    protected void setUp() throws Exception {
        setupSchema(TestSubQueries.class.getResource("testplans-subqueries-ddl.sql"), "dd", false);
        AbstractPlanNode.enableVerboseExplainForDebugging();
        AbstractExpression.enableVerboseExplainForDebugging();
    }

}
