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
import org.voltdb.expressions.ParameterValueExpression;
import org.voltdb.expressions.SubqueryExpression;
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

public class TestPlansScalarSubQueries extends PlannerTestCase {

//    public void testInToExist() {
//        AbstractPlanNode pn = compile("select r2.c from r2 where r2.c in (select c from r1)");
//        AbstractPlanNode pn = compile("select r2.c from r2 where r2.c in (1,2)");
//    }

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
        assertTrue(colExpr instanceof SubqueryExpression);
        SubqueryExpression subqueryExpr = (SubqueryExpression) colExpr;
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
        assertTrue(colExpr instanceof SubqueryExpression);
        SubqueryExpression subqueryExpr = (SubqueryExpression) colExpr;
        AbstractPlanNode subquery = subqueryExpr.getSubqueryNode();
        assertEquals(PlanNodeType.SEQSCAN, subquery.getPlanNodeType());
        AbstractExpression pred = ((SeqScanPlanNode) subquery).getPredicate();
        assertEquals(ExpressionType.VALUE_PARAMETER, pred.getRight().getExpressionType());
    }

    public void testMultiColumnSelect() {
        failToCompile("select r2.c, (select d, c from r1) from r2", "warning");
    }

    public void testWhereScalar() {
        AbstractPlanNode pn = compile("select r2.c from r2 where r2.a = (select r1.a from r1 where a = 3);");
    }

    @Override
    protected void setUp() throws Exception {
        setupSchema(TestSubQueries.class.getResource("testplans-subqueries-ddl.sql"), "dd", false);
        //        AbstractPlanNode.enableVerboseExplainForDebugging();
        //        AbstractExpression.enableVerboseExplainForDebugging();
    }

}
