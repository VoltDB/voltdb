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
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.NestLoopIndexPlanNode;
import org.voltdb.plannodes.NestLoopPlanNode;
import org.voltdb.plannodes.ReceivePlanNode;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.JoinType;

public class TestMultipleOuterJoinPlans  extends PlannerTestCase {

    public void testInnerOuterJoin() {
        AbstractPlanNode pn = compile("select * FROM R1 INNER JOIN R2 ON R1.A = R2.A LEFT JOIN R3 ON R3.C = R2.C");
        AbstractPlanNode n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        NestLoopPlanNode nlj = (NestLoopPlanNode) n;
        assertTrue(JoinType.LEFT == nlj.getJoinType());
        assertTrue(nlj.getJoinPredicate() != null);
        n = nlj.getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        nlj = (NestLoopPlanNode) n;
        assertTrue(JoinType.INNER == nlj.getJoinType());
        assertTrue(nlj.getJoinPredicate() != null);

        pn = compile("select * FROM R1, R2 LEFT JOIN R3 ON R3.C = R2.C WHERE R1.A = R2.A");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        nlj = (NestLoopPlanNode) n;
        assertTrue(JoinType.LEFT == nlj.getJoinType());
        assertTrue(nlj.getJoinPredicate() != null);
        n = nlj.getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        nlj = (NestLoopPlanNode) n;
        assertTrue(JoinType.INNER == nlj.getJoinType());
        assertTrue(nlj.getJoinPredicate() != null);
    }

    public void testOuterOuterJoin() {
        AbstractPlanNode pn = compile("select * FROM R1 LEFT JOIN R2 ON R1.A = R2.A LEFT JOIN R3 ON R3.C = R1.C");
        AbstractPlanNode n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        NestLoopPlanNode nlj = (NestLoopPlanNode) n;
        assertTrue(JoinType.LEFT == nlj.getJoinType());
        assertTrue(nlj.getJoinPredicate() != null);
        n = nlj.getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        nlj = (NestLoopPlanNode) n;
        assertTrue(JoinType.LEFT == nlj.getJoinType());
        assertTrue(nlj.getJoinPredicate() != null);

        pn = compile("select * FROM R1 LEFT JOIN R2 ON R1.A = R2.A RIGHT JOIN R3 ON R3.C = R1.C");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        nlj = (NestLoopPlanNode) n;
        assertTrue(JoinType.LEFT == nlj.getJoinType());
        assertTrue(nlj.getJoinPredicate() != null);
        n = nlj.getChild(1);
        assertTrue(n instanceof NestLoopPlanNode);
        nlj = (NestLoopPlanNode) n;
        assertTrue(JoinType.LEFT == nlj.getJoinType());
        assertTrue(nlj.getJoinPredicate() != null);

        pn = compile("select * FROM R1 RIGHT JOIN R2 ON R1.A = R2.A RIGHT JOIN R3 ON R3.C = R2.C");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        nlj = (NestLoopPlanNode) n;
        assertTrue(JoinType.LEFT == nlj.getJoinType());
        assertTrue(nlj.getJoinPredicate() != null);
        n = nlj.getChild(1);
        assertTrue(n instanceof NestLoopPlanNode);
        nlj = (NestLoopPlanNode) n;
        assertTrue(JoinType.LEFT == nlj.getJoinType());
        assertTrue(nlj.getJoinPredicate() != null);

        pn = compile("select * FROM R1 RIGHT JOIN R2 ON R1.A = R2.A LEFT JOIN R3 ON R3.C = R1.C");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        nlj = (NestLoopPlanNode) n;
        assertTrue(JoinType.LEFT == nlj.getJoinType());
        n = nlj.getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        nlj = (NestLoopPlanNode) n;
        assertTrue(JoinType.LEFT == nlj.getJoinType());

        pn = compile("select * FROM R1 RIGHT JOIN R2 ON R1.A = R2.A LEFT JOIN R3 ON R3.C = R1.C WHERE R1.A > 0");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        nlj = (NestLoopPlanNode) n;
        assertTrue(JoinType.LEFT == nlj.getJoinType());
        n = nlj.getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        nlj = (NestLoopPlanNode) n;
        assertTrue(JoinType.INNER == nlj.getJoinType());
    }

    public void testMultiTableJoinExpressions() {
        AbstractPlanNode pn = compile("select * FROM R1, R2 LEFT JOIN R3 ON R3.A = R2.C OR R3.A = R1.A WHERE R1.C = R2.C");
        AbstractPlanNode n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        NestLoopPlanNode nlj = (NestLoopPlanNode) n;
        assertTrue(JoinType.LEFT == nlj.getJoinType());
        assertTrue(nlj.getJoinPredicate() != null);
        AbstractExpression p = nlj.getJoinPredicate();
        assertEquals(ExpressionType.CONJUNCTION_OR, p.getExpressionType());
    }

    public void testPushDownExprJoin() {
        // R3.A > 0 gets pushed down all the way to the R3 scan node and used as an index
        AbstractPlanNode pn = compile("select * FROM R3, R2 LEFT JOIN R1 ON R1.C = R2.C WHERE R3.C = R2.C AND R3.A > 0");
        AbstractPlanNode n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        NestLoopPlanNode nlj = (NestLoopPlanNode) n;
        assertTrue(JoinType.LEFT == nlj.getJoinType());
        assertTrue(nlj.getJoinPredicate() != null);
        n = nlj.getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        nlj = (NestLoopPlanNode) n;
        assertTrue(JoinType.INNER == nlj.getJoinType());
        assertTrue(nlj.getJoinPredicate() != null);
        n = nlj.getChild(0);
        assertTrue(n instanceof IndexScanPlanNode);

        // R3.A > 0 is now outer join expresion and must stay at the LEF join
        pn = compile("select * FROM R3, R2 LEFT JOIN R1 ON R1.C = R2.C  AND R3.A > 0 WHERE R3.C = R2.C");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        nlj = (NestLoopPlanNode) n;
        assertTrue(JoinType.LEFT == nlj.getJoinType());
        assertTrue(nlj.getJoinPredicate() != null);
        n = nlj.getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        nlj = (NestLoopPlanNode) n;
        assertTrue(JoinType.INNER == nlj.getJoinType());
        assertTrue(nlj.getJoinPredicate() != null);
        n = nlj.getChild(0);
        assertTrue(n instanceof SeqScanPlanNode);

        pn = compile("select * FROM R3 JOIN R2 ON R3.C = R2.C RIGHT JOIN R1 ON R1.C = R2.C  AND R3.A > 0");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        nlj = (NestLoopPlanNode) n;
        assertTrue(JoinType.LEFT == nlj.getJoinType());
        assertTrue(nlj.getJoinPredicate() != null);
        n = nlj.getChild(1);
        assertTrue(n instanceof NestLoopPlanNode);
        nlj = (NestLoopPlanNode) n;
        assertTrue(JoinType.INNER == nlj.getJoinType());
        assertTrue(nlj.getJoinPredicate() != null);
        n = nlj.getChild(0);
        assertTrue(n instanceof SeqScanPlanNode);

        // R3.A > 0 gets pushed down all the way to the R3 scan node and used as an index
        pn = compile("select * FROM R2, R3 LEFT JOIN R1 ON R1.C = R2.C WHERE R3.C = R2.C AND R3.A > 0");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        nlj = (NestLoopPlanNode) n;
        assertTrue(JoinType.LEFT == nlj.getJoinType());
        assertTrue(nlj.getJoinPredicate() != null);
        n = nlj.getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        nlj = (NestLoopPlanNode) n;
        assertTrue(JoinType.INNER == nlj.getJoinType());
        assertTrue(nlj.getJoinPredicate() != null);
        n = nlj.getChild(1);
        assertTrue(n instanceof IndexScanPlanNode);

        // R3.A = R2.C gets pushed down to the R2, R3 join node scan node and used as an index
        pn = compile("select * FROM R2, R3 LEFT JOIN R1 ON R1.C = R2.C WHERE R3.A = R2.C");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        nlj = (NestLoopPlanNode) n;
        assertTrue(JoinType.LEFT == nlj.getJoinType());
        assertTrue(nlj.getJoinPredicate() != null);
        n = nlj.getChild(0);
        assertTrue(n instanceof NestLoopIndexPlanNode);
        NestLoopIndexPlanNode nlij = (NestLoopIndexPlanNode) n;
        assertTrue(JoinType.INNER == nlij.getJoinType());
    }

    public void testOuterSimplificationJoin() {
        // NULL_rejection simplification is the first transformation -
        // before the LEFT-to-RIGHT and the WHERE expressions push down

        AbstractPlanNode pn = compile("select * FROM R1, R3 RIGHT JOIN R2 ON R1.A = R2.A WHERE R3.C = R1.C");
        AbstractPlanNode n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        NestLoopPlanNode nlj = (NestLoopPlanNode) n;
        assertTrue(JoinType.INNER == nlj.getJoinType());
        assertTrue(nlj.getJoinPredicate() != null);
        n = nlj.getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        nlj = (NestLoopPlanNode) n;
        assertTrue(JoinType.INNER == nlj.getJoinType());
        assertTrue(nlj.getJoinPredicate() != null);

        // The second R3.C = R2.C join condition is NULL-rejecting for the first LEFT join
        pn = compile("select * FROM R1 LEFT JOIN R2 ON R1.A = R2.A LEFT JOIN R3 ON R3.C = R2.C");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        nlj = (NestLoopPlanNode) n;
        assertTrue(JoinType.LEFT == nlj.getJoinType());
        assertTrue(nlj.getJoinPredicate() != null);
        n = nlj.getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        nlj = (NestLoopPlanNode) n;
        assertTrue(JoinType.LEFT == nlj.getJoinType());
        assertTrue(nlj.getJoinPredicate() != null);

        // The second R3.C = R2.C join condition is NULL-rejecting for the first LEFT join
        pn = compile("select * FROM R1 LEFT JOIN R2 ON R1.A = R2.A RIGHT JOIN R3 ON R3.C = R2.C");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        nlj = (NestLoopPlanNode) n;
        assertTrue(JoinType.LEFT == nlj.getJoinType());
        assertTrue(nlj.getJoinPredicate() != null);
        n = nlj.getChild(1);
        assertTrue(n instanceof NestLoopPlanNode);
        nlj = (NestLoopPlanNode) n;
        assertTrue(JoinType.INNER == nlj.getJoinType());
        assertTrue(nlj.getJoinPredicate() != null);
    }

    public void testMultitableDistributedJoin() {
      // One distributed table
      List<AbstractPlanNode> lpn = compileToFragments("select *  FROM R3,R1 LEFT JOIN P2 ON R3.A = P2.A WHERE R3.A=R1.A ");
      assertTrue(lpn.size() == 2);
      AbstractPlanNode n = lpn.get(0).getChild(0).getChild(0);
      assertTrue(n instanceof NestLoopPlanNode);
      assertTrue(JoinType.LEFT == ((NestLoopPlanNode) n).getJoinType());
      AbstractPlanNode c = n.getChild(0);
      assertTrue(c instanceof NestLoopIndexPlanNode);

      // R3.A and P2.A have an index. P2,R1 is NLIJ/inlined IndexScan because it's an inner join even P2 is distributed
      lpn = compileToFragments("select *  FROM P2,R1 LEFT JOIN R3 ON R3.A = P2.A WHERE P2.A=R1.A ");
      assertTrue(lpn.size() == 2);
      n = lpn.get(0).getChild(0).getChild(0);
      assertTrue(n instanceof ReceivePlanNode);
      n = lpn.get(1).getChild(0);
      assertTrue(n instanceof NestLoopIndexPlanNode);
      assertTrue(JoinType.LEFT == ((NestLoopIndexPlanNode) n).getJoinType());
      c = n.getChild(0);
      assertTrue(c instanceof NestLoopIndexPlanNode);

      // R3.A has an index. R3,P2 is NLJ because it's an outer join and P2 is distributed
      lpn = compileToFragments("select *  FROM R3,R1 LEFT JOIN P2 ON R3.A = P2.A WHERE R3.A=R1.A ");
      assertTrue(lpn.size() == 2);
      // to debug */ System.out.println("DEBUG 0.0: " + lpn.get(0).toExplainPlanString());
      // to debug */ System.out.println("DEBUG 0.1: " + lpn.get(1).toExplainPlanString());
      n = lpn.get(0).getChild(0).getChild(0);
      assertTrue(n instanceof NestLoopPlanNode);
      assertTrue(JoinType.LEFT == ((NestLoopPlanNode) n).getJoinType());
      c = n.getChild(0);
      assertTrue(c instanceof NestLoopIndexPlanNode);
      assertTrue(JoinType.INNER == ((NestLoopIndexPlanNode) c).getJoinType());
      c = n.getChild(1);
      assertTrue(c instanceof ReceivePlanNode);
      n = lpn.get(1).getChild(0);
      // For determinism reason
      assertTrue(n instanceof IndexScanPlanNode);

      // R3.A has an index. P2,R1 is NLJ because P2 is distributed and it's an outer join
      lpn = compileToFragments("select *  FROM R1 LEFT JOIN P2 ON R1.A = P2.A, R3 WHERE R1.A=R3.A ");
      assertTrue(lpn.size() == 2);
      // to debug */ System.out.println("DEBUG 1.0: " + lpn.get(0).toExplainPlanString());
      // to debug */ System.out.println("DEBUG 1.1: " + lpn.get(1).toExplainPlanString());
      n = lpn.get(0).getChild(0).getChild(0);
      assertTrue(n instanceof NestLoopIndexPlanNode);
      assertTrue(JoinType.INNER == ((NestLoopIndexPlanNode) n).getJoinType());
      n = n.getChild(0);
      assertTrue(n instanceof NestLoopPlanNode);
      c = n.getChild(0);
      assertTrue(c instanceof SeqScanPlanNode);
      c = n.getChild(1);
      assertTrue(c instanceof ReceivePlanNode);
      n = lpn.get(1).getChild(0);
      // For determinism reason
      assertTrue(n instanceof IndexScanPlanNode);

      // Two distributed table
      lpn = compileToFragments("select *  FROM R3,P1 LEFT JOIN P2 ON R3.A = P2.A WHERE R3.A=P1.A ");
      assertTrue(lpn.size() == 2);
      n = lpn.get(0).getChild(0).getChild(0);
      assertTrue(n instanceof ReceivePlanNode);
      n = lpn.get(1).getChild(0);
      assertTrue(JoinType.LEFT == ((NestLoopIndexPlanNode) n).getJoinType());
      c = n.getChild(0);
      assertTrue(c instanceof NestLoopIndexPlanNode);
      assertTrue(JoinType.INNER == ((NestLoopIndexPlanNode) c).getJoinType());
    }

    @Override
    protected void setUp() throws Exception {
        setupSchema(TestJoinOrder.class.getResource("testplans-join-ddl.sql"), "testplansjoin", false);
    }


}
