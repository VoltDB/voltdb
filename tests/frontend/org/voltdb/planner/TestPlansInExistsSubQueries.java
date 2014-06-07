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

public class TestPlansInExistsSubQueries extends PlannerTestCase {

    public void testExistsWithUserParams() {
        AbstractPlanNode pn = compile("select r2.c from r2 where r2.c > ? and exists (select c from r1 where r1.c = r2.c)");
        pn = pn.getChild(0);
        assertTrue(pn instanceof AbstractScanPlanNode);
        AbstractScanPlanNode nps = (AbstractScanPlanNode) pn;
        // Check param indexes
        AbstractExpression e = ((AbstractScanPlanNode) nps).getPredicate();
        AbstractExpression le = e.getLeft();
        assertEquals(ExpressionType.COMPARE_GREATERTHAN, le.getExpressionType());
        AbstractExpression pve = le.getRight();
        assertEquals(ExpressionType.VALUE_PARAMETER, pve.getExpressionType());
        assertEquals(new Integer(0), ((ParameterValueExpression)pve).getParameterIndex());
        AbstractExpression re = e.getRight();
        assertEquals(ExpressionType.OPERATOR_EXISTS, re.getExpressionType());
        assertEquals(ExpressionType.SUBQUERY, re.getLeft().getExpressionType());
        SubqueryExpression subExpr = (SubqueryExpression) re.getLeft();
        assertEquals(1, subExpr.getArgs().size());
        assertEquals(1, subExpr.getParameterIdxList().size());
        assertEquals(Integer.valueOf(1), subExpr.getParameterIdxList().get(0));
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
        assertEquals(ExpressionType.SUBQUERY, e.getLeft().getExpressionType());
        SubqueryExpression subExpr = (SubqueryExpression) e.getLeft();
        assertEquals(1, subExpr.getArgs().size());
        assertEquals(1, subExpr.getParameterIdxList().size());
        assertEquals(Integer.valueOf(0), subExpr.getParameterIdxList().get(0));
        AbstractExpression tve = subExpr.getArgs().get(0);
        assertTrue(tve instanceof TupleValueExpression);
        assertEquals("R2", ((TupleValueExpression)tve).getTableName());
        assertEquals("A", ((TupleValueExpression)tve).getColumnName());
    }

    public void testInToExistWithUnion() {
        AbstractPlanNode pn = compile("select r2.c from r2 where r2.a in (select c from r1 union select c from r3)");
        pn = pn.getChild(0);
        assertTrue(pn instanceof AbstractScanPlanNode);
        AbstractScanPlanNode spl = (AbstractScanPlanNode) pn;
        // Check param indexes
        AbstractExpression e = spl.getPredicate();
        assertEquals(ExpressionType.COMPARE_IN, e.getExpressionType());
    }

    public void testInToExistWithOffset() {
        AbstractPlanNode pn = compile("select r2.c from r2 where r2.a in (select c from r1 limit 1 offset 3)");
        pn = pn.getChild(0);
        assertTrue(pn instanceof AbstractScanPlanNode);
        AbstractScanPlanNode spl = (AbstractScanPlanNode) pn;
        // Check param indexes
        AbstractExpression e = spl.getPredicate();
        assertEquals(ExpressionType.COMPARE_IN, e.getExpressionType());
    }

    public void testInToExistsComplex() {
      AbstractPlanNode pn = compile("select * from R1 where (A,C) in (select 2, C from r2 where r2.c > r1.c group by c)");
      pn = pn.getChild(0);
      assertTrue(pn instanceof AbstractScanPlanNode);
      AbstractScanPlanNode spn = (AbstractScanPlanNode) pn;
      AbstractExpression e = spn.getPredicate();
      SubqueryExpression subExpr = (SubqueryExpression) e.getLeft();
      assertEquals(3, subExpr.getArgs().size());
      assertEquals(3, subExpr.getParameterIdxList().size());
    }

    public void testNotExistsW() {
        AbstractPlanNode pn = compile("select r2.c from r2 where not exists (select c from r1 where r1.c = r2.c)");
        pn = pn.getChild(0);
        assertTrue(pn instanceof AbstractScanPlanNode);
        AbstractScanPlanNode nps = (AbstractScanPlanNode) pn;
        AbstractExpression e = ((AbstractScanPlanNode) nps).getPredicate();
        assertEquals(ExpressionType.OPERATOR_NOT, e.getExpressionType());
        AbstractExpression le = e.getLeft();
        assertEquals(ExpressionType.OPERATOR_EXISTS, le.getExpressionType());
        assertEquals(ExpressionType.SUBQUERY, le.getLeft().getExpressionType());
        SubqueryExpression subExpr = (SubqueryExpression) le.getLeft();
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
        assertTrue(ExpressionType.OPERATOR_EXISTS == pred.getExpressionType());
    }

    public void testInAggeregated() {
        AbstractPlanNode pn = compile("select a, sum(c) as sc1 from r1 where (a, c) in " +
                "( SELECT a, count(c) as sc2 " +
                "from  r1  GROUP BY a ORDER BY a DESC) GROUP BY A;");
        pn = pn.getChild(0).getChild(0);
        assertTrue(pn instanceof AbstractScanPlanNode);
        AbstractExpression e = ((AbstractScanPlanNode)pn).getPredicate();
        assertEquals(ExpressionType.OPERATOR_EXISTS, e.getExpressionType());
        SubqueryExpression subExpr = (SubqueryExpression) e.getLeft();
        AbstractPlanNode sn = subExpr.getSubqueryNode();
        assertTrue(sn instanceof ProjectionPlanNode);
        sn = sn.getChild(0);
        assertTrue(sn instanceof LimitPlanNode);
        sn = sn.getChild(0);
        assertTrue(sn instanceof OrderByPlanNode);
        sn = sn.getChild(0);
        assertTrue(sn instanceof AggregatePlanNode);
        AbstractExpression expr = ((AggregatePlanNode)sn).getPostPredicate();
        assertTrue(expr != null);
      }

    public void testInHaving() {
        AbstractPlanNode pn = compile("select a from r1 " +
                    " group by a having max(c) in (select c from r2 )");
        pn = pn.getChild(0).getChild(0);
        assertTrue(pn instanceof AggregatePlanNode);
        AggregatePlanNode aggpn = (AggregatePlanNode) pn;
        NodeSchema ns = aggpn.getOutputSchema();
        assertEquals(2, ns.size());
        SchemaColumn aggColumn = ns.getColumns().get(1);
        assertEquals("$$_MAX_$$_1", aggColumn.getColumnAlias());
        AbstractExpression having = aggpn.getPostPredicate();
        assertEquals(ExpressionType.OPERATOR_EXISTS, having.getExpressionType());
        assertEquals(ExpressionType.SUBQUERY, having.getLeft().getExpressionType());
        AbstractExpression se = having.getLeft();
        assertEquals(1, se.getArgs().size());
        assertTrue(se.getArgs().get(0) instanceof TupleValueExpression);
        TupleValueExpression argTve = (TupleValueExpression) se.getArgs().get(0);
        assertEquals(1, argTve.getColumnIndex());
        assertEquals("$$_MAX_$$_1", argTve.getColumnAlias());

    }

    public void testHavingInSubqueryHaving() {
        AbstractPlanNode pn = compile("select a from r1 where c in " +
                " (select max(c) from r2 group by c having min(a) > 0) ");
        pn = pn.getChild(0);
        assertTrue(pn instanceof SeqScanPlanNode);
        AbstractExpression p = ((SeqScanPlanNode)pn).getPredicate();
        assertEquals(ExpressionType.OPERATOR_EXISTS, p.getExpressionType());
        AbstractExpression subquery = p.getLeft();
        assertEquals(ExpressionType.SUBQUERY, subquery.getExpressionType());
        pn = ((SubqueryExpression)subquery).getSubqueryNode();
        pn = pn.getChild(0);
        assertTrue(pn instanceof LimitPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof AggregatePlanNode);
        assertEquals(3, ((AggregatePlanNode)pn).getOutputSchema().size());
        AbstractExpression aggrExpr = ((AggregatePlanNode)pn).getPostPredicate();
        assertEquals(ExpressionType.CONJUNCTION_AND, aggrExpr.getExpressionType());
    }


    // HSQL failed to parse  these statement
    // commented out for now
//    public void testHSQLFailed() {
//        {
//            AbstractPlanNode pn = compile("select a from r1,r2 where exists (" +
//                "select 1 from r3 where r1.a = r3.a and r2.a = r3.a)");
//        }
//        {
//            AbstractPlanNode pn = compile("select a from r1 where exists (" +
//                "select 1 from r2 having max(r1.a + r2.a) in (" +
//                " select a from r3))");
//        }
//        {
//            AbstractPlanNode pn = compile("select a from r1 group by a " +
//                " having exists (select c from r2 where r2.c = max(r1.a))");
//        }
//    }


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

    public void testSendReceiveInSubquery() {
      failToCompile("select * from r1, (select * from r2 left join P1 on r2.a = p1.c left join r1 on p1.c = r1.a) t where r1.c = 1",
              "Subqueries on partitioned data are only supported in single partition stored procedures.");
    }

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

    //    public void testUpdateWhereIn() {
////        compileToFragments(" select  c from R1 \n" +
////                " where exists (select  c from r2 );");
//
////        compileToFragments(" select c from R1 \n" +
////                "    where c in (select c from r2 where "
////                + "  exists (select 1 from r3 where r3.c = r2.c));");
//
//        compileToFragments(" select c from R1 \n" +
//                "    where c in (select c from r2 where "
//                + "  a in (select a from r3 where r3.c = r1.c) limit 1 offset 1);");
//
////        compileToFragments(" select c from R1 \n" +
////                "    where c=? and a in (select a from r2 where r2.c = r1.c "
////                + " and c in (select c from r3 where r3.a = r1.a )"
////                + " );");
//
////        compileToFragments(" select c from R1 \n" +
////                "    where c=? and a in (select a from r2 where r2.c = r1.c "
////                + " and c in (select c from r3 where r3.a = r1.a limit 1 offset 2)"
////                + " limit 1 offset 2);");
//
////        compileToFragments(" select c from R1 \n" +
////                "    where c=? and a in (select a from r2 where r2.c = r1.c "
////                + " and exists (select c from r3 where r2.c=r3.c and r3.a = r1.a )"
////                + " limit 1 offset 2);");
//
////        compileToFragments(" select c from R1 \n" +
////                "    where c=? and exists (select a from r2 where r2.a = r1.a and r2.c = r1.c "
////                + " and exists (select c from r3 where r2.c=r3.c and r3.a = r1.a ));");
//// check the IN variant params seems don't work
//        //                "    where c=? and a in (select a from r2 where r2.c = r1.c "
////                + " and c in (select c from r3 where r3.a = r1.a ));");
////    "    group by c having max(a) in (select a from r2);");
////    "    where exists (select 1 from r2 where r2.c = r1.c);");
////                "                having max(a+c) > 2;");
//
//    }

    @Override
    protected void setUp() throws Exception {
        setupSchema(TestSubQueries.class.getResource("testplans-subqueries-ddl.sql"), "dd", false);
        AbstractPlanNode.enableVerboseExplainForDebugging();
        AbstractExpression.enableVerboseExplainForDebugging();
    }

}
