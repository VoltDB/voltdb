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

import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.AbstractSubqueryExpression;
import org.voltdb.expressions.ComparisonExpression;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.AggregatePlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.NodeSchema;
import org.voltdb.plannodes.SchemaColumn;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.PlanNodeType;
import org.voltdb.types.QuantifierType;

public class TestPlansScalarSubQueries extends PlannerTestCase {

    public void testSelectScalar() {
        AbstractPlanNode pn = compile("select r2.c, (select d from r1) scalar from r2");
        pn = pn.getChild(0);
        assertTrue(pn instanceof AbstractScanPlanNode);
        AbstractPlanNode proj = pn.getInlinePlanNode(PlanNodeType.PROJECTION);
        NodeSchema schema = proj.getOutputSchema();
        assertEquals(2, schema.size());
        SchemaColumn col = schema.getColumns().get(1);
        assertTrue(col != null);
        assertEquals("SCALAR", col.getColumnName());
    }

    public void testSelectCorrelatedScalar() {
        AbstractPlanNode pn = compile("select r2.c, (select d from r1 where r1.c = r2.c ) scalar from r2");
        pn = pn.getChild(0);
        assertTrue(pn instanceof AbstractScanPlanNode);
        AbstractPlanNode proj = pn.getInlinePlanNode(PlanNodeType.PROJECTION);
        NodeSchema schema = proj.getOutputSchema();
        assertEquals(2, schema.size());
        SchemaColumn col = schema.getColumns().get(1);
        assertTrue(col != null);
        assertEquals("SCALAR", col.getColumnName());
        AbstractExpression colExpr = col.getExpression();
        assertEquals(ExpressionType.VALUE_SCALAR, colExpr.getExpressionType());
        assertTrue(colExpr.getLeft() instanceof AbstractSubqueryExpression);
        AbstractSubqueryExpression subqueryExpr = (AbstractSubqueryExpression) colExpr.getLeft();
        List<Integer> params = subqueryExpr.getParameterIdxList();
        assertEquals(1, params.size());
        assertEquals(new Integer(0), params.get(0));
    }

    public void testSelectParameterScalar() {
        AbstractPlanNode pn = compile("select r2.c, (select d from r1 where r1.c = ? ) scalar from r2");
        pn = pn.getChild(0);
        assertTrue(pn instanceof AbstractScanPlanNode);
        AbstractPlanNode proj = pn.getInlinePlanNode(PlanNodeType.PROJECTION);
        NodeSchema schema = proj.getOutputSchema();
        assertEquals(2, schema.size());
        SchemaColumn col = schema.getColumns().get(1);
        assertTrue(col != null);
        assertEquals("SCALAR", col.getColumnName());
        AbstractExpression colExpr = col.getExpression();
        assertEquals(ExpressionType.VALUE_SCALAR, colExpr.getExpressionType());
        assertTrue(colExpr.getLeft() instanceof AbstractSubqueryExpression);
        AbstractSubqueryExpression subqueryExpr = (AbstractSubqueryExpression) colExpr.getLeft();
        AbstractPlanNode subquery = subqueryExpr.getSubqueryNode();
        assertEquals(PlanNodeType.SEQSCAN, subquery.getPlanNodeType());
        AbstractExpression pred = ((SeqScanPlanNode) subquery).getPredicate();
        assertEquals(ExpressionType.VALUE_PARAMETER, pred.getRight().getExpressionType());
    }

    public void testMultiColumnSelect() {
        failToCompile("select r2.c, (select d, c from r1) from r2",
                "Scalar subquery can have only one output column");
    }

    public void testWhereEqualScalar() {
        AbstractPlanNode pn = compile("select r2.c from r2 where (select r1.a from r1) = r2.c;");
        pn = pn.getChild(0);
        assertTrue(pn instanceof AbstractScanPlanNode);
        AbstractExpression pred = ((AbstractScanPlanNode) pn).getPredicate();
        assertEquals(ExpressionType.COMPARE_EQUAL, pred.getExpressionType());
        assertEquals(ExpressionType.VALUE_TUPLE, pred.getLeft().getExpressionType());
        assertEquals(ExpressionType.SELECT_SUBQUERY, pred.getRight().getExpressionType());
    }

    public void testWhereGreaterScalar() {
        AbstractPlanNode pn = compile("select r2.c from r2 where (select r1.a from r1) > r2.c;");
        pn = pn.getChild(0);
        assertTrue(pn instanceof AbstractScanPlanNode);
        AbstractExpression pred = ((AbstractScanPlanNode) pn).getPredicate();
        assertEquals(ExpressionType.COMPARE_LESSTHAN, pred.getExpressionType());
        assertEquals(ExpressionType.VALUE_TUPLE, pred.getLeft().getExpressionType());
        assertEquals(ExpressionType.SELECT_SUBQUERY, pred.getRight().getExpressionType());
    }

    public void testWhereParamScalar() {
        AbstractPlanNode pn = compile("select r2.c from r2 where r2.c = (select r1.a from r1 where r1.a = r2.c);");
        pn = pn.getChild(0);
        assertTrue(pn instanceof AbstractScanPlanNode);
        AbstractExpression pred = ((AbstractScanPlanNode) pn).getPredicate();
        assertEquals(ExpressionType.COMPARE_EQUAL, pred.getExpressionType());
        assertEquals(ExpressionType.VALUE_TUPLE, pred.getLeft().getExpressionType());
        assertEquals(ExpressionType.SELECT_SUBQUERY, pred.getRight().getExpressionType());
        assertEquals(1, pred.getRight().getArgs().size());
    }

    public void testWhereUserParamScalar() {
        AbstractPlanNode pn = compile("select r2.c from r2 where r2.c = (select r1.a from r1 where r1.a = ?);");
        pn = pn.getChild(0);
        assertTrue(pn instanceof AbstractScanPlanNode);
        AbstractExpression pred = ((AbstractScanPlanNode) pn).getPredicate();
        assertEquals(ExpressionType.COMPARE_EQUAL, pred.getExpressionType());
        assertEquals(ExpressionType.VALUE_TUPLE, pred.getLeft().getExpressionType());
        assertEquals(ExpressionType.SELECT_SUBQUERY, pred.getRight().getExpressionType());
        assertEquals(0, pred.getRight().getArgs().size());
    }

    public void testWhereIndexScalar() {
        {
            AbstractPlanNode pn = compile("select r5.c from r5 where r5.a = (select r1.a from r1 where r1.a = ?);");
            pn = pn.getChild(0);
            assertTrue(pn instanceof IndexScanPlanNode);
            AbstractExpression pred = ((IndexScanPlanNode) pn).getEndExpression();
            assertEquals(ExpressionType.COMPARE_EQUAL, pred.getExpressionType());
            assertEquals(ExpressionType.SELECT_SUBQUERY, pred.getRight().getExpressionType());
        }
        {
            AbstractPlanNode pn = compile("select r5.c from r5 where (select r1.a from r1 where r1.a = ?) < r5.a;");
            pn = pn.getChild(0);
            assertTrue(pn instanceof IndexScanPlanNode);
            List<AbstractExpression> exprs = ((IndexScanPlanNode) pn).getSearchKeyExpressions();
            assertEquals(1, exprs.size());
            assertEquals(ExpressionType.VALUE_SCALAR, exprs.get(0).getExpressionType());
        }
        {
            AbstractPlanNode pn = compile("select r5.c from r5 where r5.a IN (select r1.a from r1);");
            pn = pn.getChild(0);
            assertTrue(pn instanceof IndexScanPlanNode);
            AbstractExpression pred = ((IndexScanPlanNode) pn).getPredicate();
            assertEquals(ExpressionType.OPERATOR_EXISTS, pred.getExpressionType());
            assertEquals(ExpressionType.SELECT_SUBQUERY, pred.getLeft().getExpressionType());
        }
        {
            AbstractPlanNode pn = compile("select r5.c from r5 where  r5.a = ANY (select r1.a from r1);");
            pn = pn.getChild(0);
            assertTrue(pn instanceof IndexScanPlanNode);
            AbstractExpression expr = ((IndexScanPlanNode) pn).getPredicate();
            assertEquals(ExpressionType.OPERATOR_EXISTS, expr.getExpressionType());
            assertEquals(ExpressionType.SELECT_SUBQUERY, expr.getLeft().getExpressionType());
        }
        {
            AbstractPlanNode pn = compile("select r5.c from r5 where  r5.a = ANY (select r1.a from r1 limit 3 offset 4);");
            pn = pn.getChild(0);
            assertTrue(pn instanceof IndexScanPlanNode);
            AbstractExpression expr = ((IndexScanPlanNode) pn).getPredicate();
            assertEquals(ExpressionType.COMPARE_EQUAL, expr.getExpressionType());
            assertEquals(ExpressionType.SELECT_SUBQUERY, expr.getRight().getExpressionType());
        }
        {
            AbstractPlanNode pn = compile("select r5.c from r5 where  r5.a > ALL (select r1.a from r1);");
            pn = pn.getChild(0);
            assertTrue(pn instanceof IndexScanPlanNode);
            AbstractExpression expr = ((IndexScanPlanNode) pn).getPredicate();
            assertEquals(ExpressionType.SELECT_SUBQUERY, expr.getRight().getExpressionType());
        }
    }

    public void testWhereGreaterRow() {
        AbstractPlanNode pn = compile("select r5.c from r5 where (a,c) > (select r1.a, r1.c from r1);");
        pn = pn.getChild(0);
        assertTrue(pn instanceof AbstractScanPlanNode);
        AbstractExpression pred = ((AbstractScanPlanNode) pn).getPredicate();
        assertEquals(ExpressionType.COMPARE_GREATERTHAN, pred.getExpressionType());
        assertEquals(ExpressionType.ROW_SUBQUERY, pred.getLeft().getExpressionType());
        assertEquals(ExpressionType.SELECT_SUBQUERY, pred.getRight().getExpressionType());
    }

    public void testWhereEqualRow() {
        AbstractPlanNode pn = compile("select r2.c from r2 where (a,c) = (select r1.a, r1.c from r1 where r1.c = r2.c);");
        pn = pn.getChild(0);
        assertTrue(pn instanceof AbstractScanPlanNode);
        AbstractExpression pred = ((AbstractScanPlanNode) pn).getPredicate();
        assertEquals(ExpressionType.COMPARE_EQUAL, pred.getExpressionType());
        assertEquals(ExpressionType.ROW_SUBQUERY, pred.getLeft().getExpressionType());
        assertEquals(ExpressionType.SELECT_SUBQUERY, pred.getRight().getExpressionType());
        assertEquals(1, pred.getRight().getArgs().size());
    }

    public void testWhereRowMismatch() {
        failToCompile("select r2.c from r2 where (a,c) = (select a, a , 5 from r1);",
                "row column count mismatch");
    }

    public void testHavingScalar() {
        AbstractPlanNode pn = compile("select max(r2.c) from r2 group by r2.c having count(*) = (select a from r1);");
        pn = pn.getChild(0).getChild(0);
        assertEquals(PlanNodeType.SEQSCAN, pn.getPlanNodeType());
        AggregatePlanNode aggNode = AggregatePlanNode.getInlineAggregationNode(pn);
        AbstractExpression aggExpr = aggNode.getPostPredicate();
        assertEquals(ExpressionType.SELECT_SUBQUERY, aggExpr.getRight().getExpressionType());
    }

    public void testHavingRow() {
        AbstractPlanNode pn = compile("select max(r2.c) from r2 group by r2.c having (count(*), max(r2.c)) = (select a,c from r1);");
        pn = pn.getChild(0).getChild(0);
        assertEquals(PlanNodeType.SEQSCAN, pn.getPlanNodeType());
        AggregatePlanNode aggNode = AggregatePlanNode.getInlineAggregationNode(pn);
        AbstractExpression aggExpr = aggNode.getPostPredicate();
        assertEquals(ExpressionType.SELECT_SUBQUERY, aggExpr.getRight().getExpressionType());
    }

    public void testHavingRowMismatch() {
        failToCompile("select max(r2.c) from r2 group by r2.c having (count(*), max(r2.c)) = (select a,c, 5 from r1);",
                "row column count mismatch");
    }

    public void testWhereIndexRow() {
        {
            AbstractPlanNode pn = compile("select * from r5 where (a,c) = (select a, c from r1);");
            pn = pn.getChild(0);
            assertTrue(pn instanceof IndexScanPlanNode);
            AbstractExpression pred = ((IndexScanPlanNode) pn).getPredicate();
            assertEquals(ExpressionType.COMPARE_EQUAL, pred.getExpressionType());
            assertEquals(ExpressionType.ROW_SUBQUERY, pred.getLeft().getExpressionType());
            assertEquals(ExpressionType.SELECT_SUBQUERY, pred.getRight().getExpressionType());
        }
        {
            AbstractPlanNode pn = compile("select * from r5 where (select a, c from r1) >= (a,c);");
            pn = pn.getChild(0);
            assertTrue(pn instanceof IndexScanPlanNode);
            AbstractExpression pred = ((IndexScanPlanNode) pn).getPredicate();
            assertEquals(ExpressionType.COMPARE_GREATERTHANOREQUALTO, pred.getExpressionType());
            assertEquals(ExpressionType.SELECT_SUBQUERY, pred.getLeft().getExpressionType());
            assertEquals(ExpressionType.ROW_SUBQUERY, pred.getRight().getExpressionType());
        }
        {
            AbstractPlanNode pn = compile("select * from r5 where (a,c) IN (select a, c from r1);");
            pn = pn.getChild(0);
            assertTrue(pn instanceof IndexScanPlanNode);
            AbstractExpression pred = ((IndexScanPlanNode) pn).getPredicate();
            assertEquals(ExpressionType.OPERATOR_EXISTS, pred.getExpressionType());
            assertEquals(ExpressionType.SELECT_SUBQUERY, pred.getLeft().getExpressionType());
        }
        {
            AbstractPlanNode pn = compile("select * from r5 where (a,c) IN (select a, c from r1 limit 1 offset 4);");
            pn = pn.getChild(0);
            assertTrue(pn instanceof IndexScanPlanNode);
            AbstractExpression pred = ((IndexScanPlanNode) pn).getPredicate();
            assertEquals(ExpressionType.COMPARE_EQUAL, pred.getExpressionType());
            assertEquals(QuantifierType.ANY, ((ComparisonExpression) pred).getQuantifier());
            assertEquals(ExpressionType.ROW_SUBQUERY, pred.getLeft().getExpressionType());
        }

        {
            AbstractPlanNode pn = compile("select * from r5 where (a,c) > ALL (select a, c from r1);");
            pn = pn.getChild(0);
            assertTrue(pn instanceof IndexScanPlanNode);
            AbstractExpression pred = ((IndexScanPlanNode) pn).getPredicate();
            assertEquals(ExpressionType.COMPARE_GREATERTHAN, pred.getExpressionType());
            assertEquals(QuantifierType.ALL, ((ComparisonExpression) pred).getQuantifier());
            assertEquals(ExpressionType.ROW_SUBQUERY, pred.getLeft().getExpressionType());
        }
    }

    @Override
    protected void setUp() throws Exception {
        setupSchema(TestSubQueries.class.getResource("testplans-subqueries-ddl.sql"), "dd", false);
        //        AbstractPlanNode.enableVerboseExplainForDebugging();
        //        AbstractExpression.enableVerboseExplainForDebugging();
    }

}
