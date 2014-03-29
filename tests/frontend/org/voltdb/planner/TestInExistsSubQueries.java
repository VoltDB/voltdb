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
import org.voltdb.plannodes.DeletePlanNode;
import org.voltdb.plannodes.NestLoopPlanNode;
import org.voltdb.plannodes.NodeSchema;
import org.voltdb.plannodes.SchemaColumn;
import org.voltdb.plannodes.UpdatePlanNode;
import org.voltdb.types.ExpressionType;

public class TestInExistsSubQueries extends PlannerTestCase {

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
        assertEquals(ExpressionType.SUBQUERY, re.getExpressionType());
        SubqueryExpression subExpr = (SubqueryExpression) re;
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
        assertEquals(ExpressionType.SUBQUERY, e.getExpressionType());
        SubqueryExpression subExpr = (SubqueryExpression) e;
        assertEquals(1, subExpr.getArgs().size());
        assertEquals(1, subExpr.getParameterIdxList().size());
        assertEquals(Integer.valueOf(0), subExpr.getParameterIdxList().get(0));
        AbstractExpression tve = subExpr.getArgs().get(0);
        assertTrue(tve instanceof TupleValueExpression);
        assertEquals("R2", ((TupleValueExpression)tve).getTableName());
        assertEquals("A", ((TupleValueExpression)tve).getColumnName());
    }

    public void testInToExistsComplex() {
      AbstractPlanNode pn = compile("select * from R1 where (A,C) in (select 2, C from r2 where r2.c > r1.c group by c)");
      pn = pn.getChild(0);
      assertTrue(pn instanceof AbstractScanPlanNode);
      AbstractScanPlanNode spn = (AbstractScanPlanNode) pn;
      AbstractExpression e = spn.getPredicate();
      SubqueryExpression subExpr = (SubqueryExpression) e;
      assertEquals(3, subExpr.getArgs().size());
      assertEquals(3, subExpr.getParameterIdxList().size());
    }

    public void testDeleteWhereIn() {
        List<AbstractPlanNode> lpn = compileToFragments("delete from r1 where a in (select a from r2 where r1.c = r2.c)");
        assertTrue(lpn.size() == 2);
        AbstractPlanNode n = lpn.get(1).getChild(0);
        assertTrue(n instanceof DeletePlanNode);
        n = n.getChild(0);
        assertTrue(n instanceof AbstractScanPlanNode);
        AbstractScanPlanNode spn = (AbstractScanPlanNode) n;
        AbstractExpression e = spn.getPredicate();
        assertEquals(ExpressionType.SUBQUERY, e.getExpressionType());
    }

    public void testUpdateWhereIn() {
      List<AbstractPlanNode> lpn = compileToFragments("update r1 set c = 1 where a in (select a from r2 where r1.c = r2.c)");
      assertTrue(lpn.size() == 2);
      AbstractPlanNode n = lpn.get(1).getChild(0);
      assertTrue(n instanceof UpdatePlanNode);
      n = n.getChild(0);
      assertTrue(n instanceof AbstractScanPlanNode);
      AbstractScanPlanNode spn = (AbstractScanPlanNode) n;
      AbstractExpression e = spn.getPredicate();
      assertEquals(ExpressionType.SUBQUERY, e.getExpressionType());
    }

    public void testSendReceiveInSubquery() {
      failToCompile("select * from r1, (select * from r2 left join P1 on r2.a = p1.c left join r1 on p1.c = r1.a) t where r1.c = 1",
              "This special case join between an outer replicated table and " +
              "an inner partitioned table is too complex and is not supported.");
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

    @Override
    protected void setUp() throws Exception {
        setupSchema(TestSubQueries.class.getResource("testplans-subqueries-ddl.sql"), "dd", false);
    }

}
