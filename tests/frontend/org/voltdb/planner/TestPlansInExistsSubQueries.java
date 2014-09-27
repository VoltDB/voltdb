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

public class TestPlansInExistsSubQueries extends PlannerTestCase {

    public void testInExistsGuard() {
        String errorMsg = "In/Exists are only supported in single partition procedure";

        String sql;
        sql = "select p2.c from p2 where p2.c > ? and exists (select c from r1 where r1.c = p2.c)";
        failToCompile(sql, errorMsg);

        sql = "select p2.c from p2 where p2.a in (select c from r1)";
        failToCompile(sql, errorMsg);


        errorMsg = "Unable to plan for statement. Error unknown";
        sql = "select r2.c from r2 where r2.c > ? and exists (select c from p1 where p1.c = r2.c)";
        failToCompile(sql, errorMsg);
    }

    public void testExistsWithUserParams() {
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
        assertEquals(ExpressionType.EXISTS_SUBQUERY, re.getExpressionType());
        assertEquals(1, re.getArgs().size());
        assertEquals(1, ((SubqueryExpression) re).getParameterIdxList().size());
        assertEquals(Integer.valueOf(1), ((SubqueryExpression) re).getParameterIdxList().get(0));
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
        assertEquals(ExpressionType.EXISTS_SUBQUERY, e.getExpressionType());
        SubqueryExpression subExpr = (SubqueryExpression) e;
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
        assertEquals(ExpressionType.EXISTS_SUBQUERY, l.getExpressionType());
        AbstractExpression r = e.getRight();
        assertEquals(ExpressionType.CONJUNCTION_AND, r.getExpressionType());
        l = r.getLeft();
        assertEquals(ExpressionType.IN_SUBQUERY, l.getExpressionType());
        r = r.getRight();
        assertEquals(ExpressionType.EXISTS_SUBQUERY, r.getExpressionType());
    }

    public void testInToExistWithOffset() {
        AbstractPlanNode pn = compile("select r2.c from r2 where r2.a in (select c from r1 limit 1 offset 3)");
        pn = pn.getChild(0);
        assertTrue(pn instanceof AbstractScanPlanNode);
        AbstractScanPlanNode spl = (AbstractScanPlanNode) pn;
        // Check param indexes
        AbstractExpression e = spl.getPredicate();
        assertEquals(ExpressionType.IN_SUBQUERY, e.getExpressionType());
    }

    public void testInToExistsComplex() {
        AbstractPlanNode pn = compile("select * from R1 where (A,C) in (select 2, C from r2 where r2.c > r1.c group by c)");
        pn = pn.getChild(0);
        assertTrue(pn instanceof AbstractScanPlanNode);
        AbstractScanPlanNode spn = (AbstractScanPlanNode) pn;
        AbstractExpression e = spn.getPredicate();
        assertEquals(ExpressionType.EXISTS_SUBQUERY, e.getExpressionType());
        SubqueryExpression subExpr = (SubqueryExpression) e;
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
        assertEquals(ExpressionType.EXISTS_SUBQUERY, le.getExpressionType());
        SubqueryExpression subExpr = (SubqueryExpression) le;
        assertEquals(1, subExpr.getArgs().size());
        assertEquals(1, subExpr.getParameterIdxList().size());
        assertEquals(Integer.valueOf(0), subExpr.getParameterIdxList().get(0));
    }

    public void testExistsJoin() {
        AbstractPlanNode pn = compile("select a from r1,r2 where r1.a = r2.a and " +
                "exists ( select 1 from r3 where r1.a = r3.a)");

        pn = pn.getChild(0).getChild(0);
        assertTrue(pn instanceof NestLoopIndexPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof SeqScanPlanNode);
        AbstractExpression pred = ((SeqScanPlanNode) pn).getPredicate();
        assertTrue(pred != null);
        assertEquals(ExpressionType.EXISTS_SUBQUERY, pred.getExpressionType());
    }

    public void testInAggeregated() {
        AbstractPlanNode pn = compile("select a, sum(c) as sc1 from r1 where (a, c) in " +
                "( SELECT a, count(c) as sc2 " +
                "from  r1  GROUP BY a ORDER BY a DESC) GROUP BY A;");
        pn = pn.getChild(0);
        assertTrue(pn instanceof AbstractScanPlanNode);

        AbstractExpression e = ((AbstractScanPlanNode)pn).getPredicate();
        assertEquals(ExpressionType.EXISTS_SUBQUERY, e.getExpressionType());
        SubqueryExpression subExpr = (SubqueryExpression) e;
        AbstractPlanNode sn = subExpr.getSubqueryNode();
        assertTrue(sn instanceof ProjectionPlanNode);
        sn = sn.getChild(0);
        assertTrue(sn instanceof OrderByPlanNode);
        assertNotNull(sn.getInlinePlanNode(PlanNodeType.LIMIT));
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
        assertEquals(ExpressionType.EXISTS_SUBQUERY, having.getExpressionType());
        assertEquals(1, having.getArgs().size());
        assertTrue(having.getArgs().get(0) instanceof TupleValueExpression);
        TupleValueExpression argTve = (TupleValueExpression) having.getArgs().get(0);
        assertEquals(1, argTve.getColumnIndex());
        assertEquals("$$_MAX_$$_1", argTve.getColumnAlias());

    }

    public void testHavingInSubqueryHaving() {
        AbstractPlanNode pn = compile("select a from r1 where c in " +
                " (select max(c) from r2 group by c having min(a) > 0) ");

        pn = pn.getChild(0);
        assertTrue(pn instanceof SeqScanPlanNode);
        AbstractExpression p = ((SeqScanPlanNode)pn).getPredicate();
        assertEquals(ExpressionType.EXISTS_SUBQUERY, p.getExpressionType());
        AbstractExpression subquery = p;
        pn = ((SubqueryExpression)subquery).getSubqueryNode();
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

    public void testSendReceiveInSubquery() {
        //      compileToFragments("select * from r1, (select * from r2 left join P1 on r2.a = p1.c left join r1 on p1.c = r1.a) t where r1.c = 1");
    }

    // HSQL failed to parse  these statement
    public void testHSQLFailed() {
        {    
            failToCompile("select a from r1,r2 where exists (" +
                    "select 1 from r3 where r1.a = r3.a and r2.a = r3.a)",
                    "-1");
        }
        {
            failToCompile("select a from r1 where exists (" +
                    "select 1 from r2 having max(r1.a + r2.a) in (" +
                    " select max(a) from r3))",
                    "expression not in aggregate or GROUP BY columns");
        }
        {
            failToCompile("select a from r1 group by a " +
                    " having exists (select c from r2 where r2.c = max(r1.a))", "");
        }
        {
            failToCompile("select r2.a from r2 where exists " +
                    "( SELECT 1 from R2 WHERE r2.c = ?)",
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
        //        AbstractPlanNode.enableVerboseExplainForDebugging();
        //        AbstractExpression.enableVerboseExplainForDebugging();
    }

}
