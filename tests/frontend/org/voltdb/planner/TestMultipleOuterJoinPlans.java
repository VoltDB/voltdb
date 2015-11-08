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

import org.voltdb.expressions.AbstractExpression;
import org.voltdb.plannodes.AbstractJoinPlanNode;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.NestLoopPlanNode;
import org.voltdb.plannodes.ReceivePlanNode;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.JoinType;
import org.voltdb.types.PlanNodeType;

public class TestMultipleOuterJoinPlans  extends PlannerTestCase {

    private void verifyJoinNode(AbstractPlanNode n, PlanNodeType nodeType, JoinType joinType, boolean hasJoinPredicate,
            PlanNodeType outerNodeType, PlanNodeType innerNodeType,
            String outerTableAlias, String innerTableAlias) {
        assertEquals(nodeType, n.getPlanNodeType());
        AbstractJoinPlanNode jn = (AbstractJoinPlanNode) n;
        assertEquals(joinType, jn.getJoinType());
        assertEquals(hasJoinPredicate, jn.getJoinPredicate() != null);
        assertEquals(outerNodeType, jn.getChild(0).getPlanNodeType());
        if (outerTableAlias != null) {
            assertEquals(outerTableAlias, ((AbstractScanPlanNode) jn.getChild(0)).getTargetTableAlias());
        }
        if (nodeType == PlanNodeType.NESTLOOP) {
            assertEquals(innerNodeType, jn.getChild(1).getPlanNodeType());
        }
        if (innerTableAlias != null) {
            if (nodeType == PlanNodeType.NESTLOOP) {
                assertEquals(innerTableAlias, ((AbstractScanPlanNode) jn.getChild(1)).getTargetTableAlias());
            } else {
                IndexScanPlanNode sn = (IndexScanPlanNode) jn.getInlinePlanNode(PlanNodeType.INDEXSCAN);
                assertEquals(innerTableAlias, sn.getTargetTableAlias());
            }
        }
    }

    private void verifyJoinNode(AbstractPlanNode n, PlanNodeType nodeType, JoinType joinType, boolean hasJoinPredicate,
            PlanNodeType outerNodeType, PlanNodeType innerNodeType) {
        verifyJoinNode(n, nodeType, joinType, hasJoinPredicate, outerNodeType, innerNodeType, null, null);
    }

    public void testInnerOuterJoin() {
        AbstractPlanNode pn;
        AbstractPlanNode n;

        pn = compile("select * FROM R1 INNER JOIN R2 ON R1.A = R2.A LEFT JOIN R3 ON R3.C = R2.C");
        n = pn.getChild(0).getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.LEFT, true, PlanNodeType.NESTLOOP, PlanNodeType.SEQSCAN, null, "R3");
        n = n.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.INNER, true, PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN);

        pn = compile("select * FROM R1, R2 LEFT JOIN R3 ON R3.C = R2.C WHERE R1.A = R2.A");
        n = pn.getChild(0).getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.LEFT, true, PlanNodeType.NESTLOOP, PlanNodeType.SEQSCAN, null, "R3");
        n = n.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.INNER, true, PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN);
    }

    public void testOuterOuterJoin() {
        AbstractPlanNode pn;
        AbstractPlanNode n;

        pn = compile("select * FROM R1 LEFT JOIN R2 ON R1.A = R2.A LEFT JOIN R3 ON R3.C = R1.C");
        n = pn.getChild(0).getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.LEFT, true, PlanNodeType.NESTLOOP, PlanNodeType.SEQSCAN, null, "R3");
        n = n.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.LEFT, true, PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN, "R1", "R2");

        pn = compile("select * FROM R1 LEFT JOIN R2 ON R1.A = R2.A RIGHT JOIN R3 ON R3.C = R1.C");
        n = pn.getChild(0).getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.LEFT, true, PlanNodeType.SEQSCAN, PlanNodeType.NESTLOOP, "R3", null);
        n = n.getChild(1);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.LEFT, true, PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN, "R1", "R2");

        pn = compile("select * FROM R1 RIGHT JOIN R2 ON R1.A = R2.A RIGHT JOIN R3 ON R3.C = R2.C");
        n = pn.getChild(0).getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.LEFT, true, PlanNodeType.SEQSCAN, PlanNodeType.NESTLOOP, "R3", null);
        n = n.getChild(1);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.LEFT, true, PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN, "R2", "R1");

        pn = compile("select * FROM R1 RIGHT JOIN R2 ON R1.A = R2.A LEFT JOIN R3 ON R3.C = R1.C");
        n = pn.getChild(0).getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.LEFT, true, PlanNodeType.NESTLOOP, PlanNodeType.SEQSCAN, null, "R3");
        n = n.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.LEFT, true, PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN, "R2", "R1");

        pn = compile("select * FROM R1 RIGHT JOIN R2 ON R1.A = R2.A LEFT JOIN R3 ON R3.C = R1.C WHERE R1.A > 0");
        n = pn.getChild(0).getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.LEFT, true, PlanNodeType.NESTLOOP, PlanNodeType.SEQSCAN, null, "R3");
        n = n.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.INNER, true, PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN);
    }

    public void testMultiTableJoinExpressions() {
        AbstractPlanNode pn = compile("select * FROM R1, R2 LEFT JOIN R3 ON R3.A = R2.C OR R3.A = R1.A WHERE R1.C = R2.C");
        AbstractPlanNode n = pn.getChild(0).getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.LEFT, true, PlanNodeType.NESTLOOP, PlanNodeType.SEQSCAN, null, "R3");
        NestLoopPlanNode nlj = (NestLoopPlanNode) n;
        AbstractExpression p = nlj.getJoinPredicate();
        assertEquals(ExpressionType.CONJUNCTION_OR, p.getExpressionType());
    }

    public void testPushDownExprJoin() {
        AbstractPlanNode pn;
        AbstractPlanNode n;

        // R3.A > 0 gets pushed down all the way to the R3 scan node and used as an index
        pn = compile("select * FROM R3, R2 LEFT JOIN R1 ON R1.C = R2.C WHERE R3.C = R2.C AND R3.A > 0");
        n = pn.getChild(0).getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.LEFT, true, PlanNodeType.NESTLOOP, PlanNodeType.SEQSCAN, null, "R1");
        n = n.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.INNER, true, PlanNodeType.INDEXSCAN, PlanNodeType.SEQSCAN, "R3", "R2");

        // R3.A > 0 is now outer join expresion and must stay at the LEF join
        pn = compile("select * FROM R3, R2 LEFT JOIN R1 ON R1.C = R2.C  AND R3.A > 0 WHERE R3.C = R2.C");
        n = pn.getChild(0).getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.LEFT, true, PlanNodeType.NESTLOOP, PlanNodeType.SEQSCAN, null, "R1");
        n = n.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.INNER, true, PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN, "R3", "R2");

        pn = compile("select * FROM R3 JOIN R2 ON R3.C = R2.C RIGHT JOIN R1 ON R1.C = R2.C  AND R3.A > 0");
        n = pn.getChild(0).getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.LEFT, true, PlanNodeType.SEQSCAN, PlanNodeType.NESTLOOP, "R1", null);
        n = n.getChild(1);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.INNER, true, PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN, "R3", "R2");

        // R3.A > 0 gets pushed down all the way to the R3 scan node and used as an index
        pn = compile("select * FROM R2, R3 LEFT JOIN R1 ON R1.C = R2.C WHERE R3.C = R2.C AND R3.A > 0");
        n = pn.getChild(0).getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.LEFT, true, PlanNodeType.NESTLOOP, PlanNodeType.SEQSCAN, null, "R1");
        n = n.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.INNER, true, PlanNodeType.SEQSCAN, PlanNodeType.INDEXSCAN, "R2", "R3");

        // R3.A = R2.C gets pushed down to the R2, R3 join node scan node and used as an index
        pn = compile("select * FROM R2, R3 LEFT JOIN R1 ON R1.C = R2.C WHERE R3.A = R2.C");
        n = pn.getChild(0).getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.LEFT, true, PlanNodeType.NESTLOOPINDEX, PlanNodeType.SEQSCAN, null, "R1");
        n = n.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOPINDEX, JoinType.INNER, false, PlanNodeType.SEQSCAN, null, "R2", "R3");
    }

    public void testOuterSimplificationJoin() {
        // NULL_rejection simplification is the first transformation -
        // before the LEFT-to-RIGHT and the WHERE expressions push down
        AbstractPlanNode pn;
        AbstractPlanNode n;

        pn = compile("select * FROM R1, R3 RIGHT JOIN R2 ON R1.A = R2.A WHERE R3.C = R1.C");
        n = pn.getChild(0).getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.INNER, true, PlanNodeType.NESTLOOP, PlanNodeType.SEQSCAN);
        n = n.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.INNER, true, PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN);

        // The second R3.C = R2.C join condition is NULL-rejecting for the outer table
        // from the first LEFT join - can't simplify (not the inner table)
        pn = compile("select * FROM R1 LEFT JOIN R2 ON R1.A = R2.A LEFT JOIN R3 ON R3.C = R2.C");
        n = pn.getChild(0).getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.LEFT, true, PlanNodeType.NESTLOOP, PlanNodeType.SEQSCAN, null, "R3");
        n = n.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.LEFT, true, PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN, "R1", "R2");

        // The second R3.C = R2.C join condition is NULL-rejecting for the first LEFT join
        pn = compile("select * FROM R1 LEFT JOIN R2 ON R1.A = R2.A RIGHT JOIN R3 ON R3.C = R2.C");
        n = pn.getChild(0).getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.LEFT, true, PlanNodeType.SEQSCAN, PlanNodeType.NESTLOOP, "R3", null);
        n = n.getChild(1);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.INNER, true, PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN);

        // The R3.A = R1.A join condition is NULL-rejecting for the FULL join OUTER (R1) table
        // simplifying it to R1 LEFT JOIN R2
        pn = compile("select * FROM " +
                "R1 FULL JOIN R2 ON R1.A = R2.A " +
                "RIGHT JOIN R3 ON R3.A = R1.A");
        n = pn.getChild(0).getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.LEFT, true, PlanNodeType.SEQSCAN, PlanNodeType.NESTLOOP, "R3", null);
        n = n.getChild(1);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.LEFT, true, PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN, "R1", "R2");

        // The R3.A = R2.A join condition is NULL-rejecting for the FULL join INNER (R2) table
        // simplifying it to R1 RIGHT JOIN R2 which gets converted to R2 LEFT JOIN R1
        pn = compile("select * FROM " +
                "R1 FULL JOIN R2 ON R1.A = R2.A " +
                    "RIGHT JOIN R3 ON R3.A = R2.A");
        n = pn.getChild(0).getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.LEFT, true, PlanNodeType.SEQSCAN, PlanNodeType.NESTLOOP, "R3", null);
        n = n.getChild(1);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.LEFT, true, PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN, "R2", "R1");

        // The R3.A = R2.A AND R3.A = R1.A join condition is NULL-rejecting for the FULL join
        // OUTER (R1) and INNER (R1) tables simplifying it to R1 JOIN R2
        pn = compile("select * FROM " +
                "R1 FULL JOIN R2 ON R1.A = R2.A " +
                    "RIGHT JOIN R3 ON R3.A = R2.A AND R3.A = R1.A");
        n = pn.getChild(0).getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.LEFT, true, PlanNodeType.SEQSCAN, PlanNodeType.NESTLOOP, "R3", null);
        n = n.getChild(1);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.INNER, true, PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN);

        // R1 RIGHT JOIN R2 ON R1.A = R2.A                  R1 JOIN R3 ON R1.A = R3.A
        //      JOIN R3 ON R1.A = R3.A               ==>        JOIN  R2 ON R1.A = R2.A
        //          FULL JOIN R4 ON R1.A = R4.A                     LEFT JOIN R4 ON R1.A = R4.A
        //              FULL JOIN R5 ON R1.A = R5.A                     FULL JOIN R5 ON R1.A = R5.A
        pn = compile("select * FROM " +
                "R1 RIGHT JOIN R2 ON R1.A = R2.A " +
                    "JOIN R3 ON R1.A = R3.A " +
                    "FULL JOIN R4 ON R1.A = R4.A " +
                        "FULL JOIN R5 ON R1.A = R5.A");
        n = pn.getChild(0).getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOPINDEX, JoinType.FULL, false, PlanNodeType.NESTLOOPINDEX, PlanNodeType.INDEXSCAN, null, "R5");
        n = n.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOPINDEX, JoinType.LEFT, false, PlanNodeType.NESTLOOP, PlanNodeType.INDEXSCAN, null, "R4");
        n = n.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.INNER, true, PlanNodeType.NESTLOOPINDEX, PlanNodeType.SEQSCAN, null, "R2");
        n = n.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOPINDEX, JoinType.INNER, false, PlanNodeType.SEQSCAN, PlanNodeType.INDEXSCAN, "R1", "R3");
    }

    public void testMultitableDistributedJoin() {
        List<AbstractPlanNode> lpn;
        AbstractPlanNode n;

        // One distributed table
        lpn = compileToFragments("select *  FROM R3,R1 LEFT JOIN P2 ON R3.A = P2.A WHERE R3.A=R1.A ");
        assertTrue(lpn.size() == 2);
        n = lpn.get(0).getChild(0).getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.LEFT, true, PlanNodeType.NESTLOOPINDEX, PlanNodeType.RECEIVE);

        // R3.A and P2.A have an index. P2,R1 is NLIJ/inlined IndexScan because it's an inner join even P2 is distributed
        lpn = compileToFragments("select *  FROM P2,R1 LEFT JOIN R3 ON R3.A = P2.A WHERE P2.A=R1.A ");
        assertTrue(lpn.size() == 2);
        n = lpn.get(0).getChild(0).getChild(0);
        assertTrue(n instanceof ReceivePlanNode);
        n = lpn.get(1).getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOPINDEX, JoinType.LEFT, false, PlanNodeType.NESTLOOPINDEX, PlanNodeType.INDEXSCAN);

        // R3.A has an index. R3,P2 is NLJ because it's an outer join and P2 is distributed
        lpn = compileToFragments("select *  FROM R3,R1 LEFT JOIN P2 ON R3.A = P2.A WHERE R3.A=R1.A ");
        assertTrue(lpn.size() == 2);
        // to debug */ System.out.println("DEBUG 0.0: " + lpn.get(0).toExplainPlanString());
        // to debug */ System.out.println("DEBUG 0.1: " + lpn.get(1).toExplainPlanString());
        n = lpn.get(0).getChild(0).getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.LEFT, true, PlanNodeType.NESTLOOPINDEX, PlanNodeType.RECEIVE);
        n = n.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOPINDEX, JoinType.INNER, false, PlanNodeType.SEQSCAN, PlanNodeType.INDEXSCAN);
        n = lpn.get(1).getChild(0);
        // For determinism reason
        assertTrue(n instanceof IndexScanPlanNode);

        // R3.A has an index. P2,R1 is NLJ because P2 is distributed and it's an outer join
        lpn = compileToFragments("select *  FROM R1 LEFT JOIN P2 ON R1.A = P2.A, R3 WHERE R1.A=R3.A ");
        assertTrue(lpn.size() == 2);
        // to debug */ System.out.println("DEBUG 1.0: " + lpn.get(0).toExplainPlanString());
        // to debug */ System.out.println("DEBUG 1.1: " + lpn.get(1).toExplainPlanString());
        n = lpn.get(0).getChild(0).getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOPINDEX, JoinType.INNER, false, PlanNodeType.NESTLOOP, PlanNodeType.SEQSCAN);
        n = n.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.LEFT, true, PlanNodeType.SEQSCAN, PlanNodeType.RECEIVE);
        n = lpn.get(1).getChild(0);
        // For determinism reason
        assertTrue(n instanceof IndexScanPlanNode);

        // Two distributed table
        lpn = compileToFragments("select *  FROM R3,P1 LEFT JOIN P2 ON R3.A = P2.A WHERE R3.A=P1.A ");
        assertTrue(lpn.size() == 2);
        n = lpn.get(0).getChild(0).getChild(0);
        assertTrue(n instanceof ReceivePlanNode);
        n = lpn.get(1).getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOPINDEX, JoinType.LEFT, false, PlanNodeType.NESTLOOPINDEX, PlanNodeType.INDEXSCAN);
        n = n.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOPINDEX, JoinType.INNER, false, PlanNodeType.SEQSCAN, PlanNodeType.INDEXSCAN);
    }

    public void testFullJoinExpressions() {
        AbstractPlanNode pn;
        AbstractPlanNode n;
        NestLoopPlanNode nlj;
        AbstractExpression e;

        // WHERE outer and inner expressions stay at the FULL NLJ node
        pn = compile("select * FROM  " +
                "R1 FULL JOIN R2 ON R1.A = R2.A WHERE R2.C IS NULL AND R1.C is NULL");
        n = pn.getChild(0).getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.FULL, true, PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN);
        nlj = (NestLoopPlanNode) n;
        e = nlj.getWherePredicate();
        assertNotNull(e);
        assertEquals(ExpressionType.CONJUNCTION_AND, e.getExpressionType());

        // WHERE outer and inner expressions stay at the FULL NLJ node
        // The outer node is a join itself
        pn = compile("select * FROM  " +
                "R1 JOIN R2 ON R1.A = R2.A FULL JOIN R3 ON R3.C = R2.C WHERE R1.C is NULL");
        n = pn.getChild(0).getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.FULL, true, PlanNodeType.NESTLOOP, PlanNodeType.SEQSCAN);
        nlj = (NestLoopPlanNode) n;
        e = nlj.getWherePredicate();
        assertNotNull(e);
        assertEquals(ExpressionType.OPERATOR_IS_NULL, e.getExpressionType());

        // WHERE outer-inner expressions stay at the FULL NLJ node
        pn = compile("select * FROM  " +
                "R1 FULL JOIN R2 ON R1.A = R2.A WHERE R2.C IS NULL OR R1.C is NULL");
        n = pn.getChild(0).getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.FULL, true, PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN);
        nlj = (NestLoopPlanNode) n;
        e = nlj.getWherePredicate();
        assertNotNull(e);
        assertEquals(ExpressionType.CONJUNCTION_OR, e.getExpressionType());

        // WHERE outer and inner expressions push down process stops at the FULL join (R1,R2) node -
        // FULL join is itself an outer node
        pn = compile("select * FROM  " +
                "R1 FULL JOIN R2 ON R1.A = R2.A LEFT JOIN R3 ON R3.C = R2.C WHERE R1.C is NULL");
        n = pn.getChild(0).getChild(0).getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.FULL, true, PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN);
        nlj = (NestLoopPlanNode) n;
        e = nlj.getWherePredicate();
        assertNotNull(e);
        assertEquals(ExpressionType.OPERATOR_IS_NULL, e.getExpressionType());

        // NLJ JOIN outer expression is pre-join expression, NLJ JOIN inner expression together with
        // JOIN inner-outer one are part of the join predicate
        pn = compile("select * FROM  " +
                "R1 FULL JOIN R2 ON R1.A = R2.A AND R1.C = R2.C");
        n = pn.getChild(0).getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.FULL, true, PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN);
        nlj = (NestLoopPlanNode) n;
        e = nlj.getJoinPredicate();
        assertNotNull(e);
        assertEquals(ExpressionType.CONJUNCTION_AND, e.getExpressionType());

        // NLJ JOIN outer expression is pre-join expression, NLJ JOIN inner expression together with
        // JOIN inner-outer one are part of the join predicate
        pn = compile("select * FROM  " +
                "R1 FULL JOIN R2 ON R1.A = R2.A AND R1.C < 0 AND R2.C > 0");
        n = pn.getChild(0).getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.FULL, true, PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN);
        nlj = (NestLoopPlanNode) n;
        e = nlj.getWherePredicate();
        assertNull(e);
        e = nlj.getPreJoinPredicate();
        assertEquals(ExpressionType.COMPARE_LESSTHAN, e.getExpressionType());
        e = nlj.getJoinPredicate();
        assertEquals(ExpressionType.CONJUNCTION_AND, e.getExpressionType());

        // NLJ JOIN outer expression is pre-join expression, NLJ JOIN inner expression together with
        // JOIN inner-outer one are part of the join predicate
        // FULL join outer node is a join itself
        pn = compile("select * FROM  " +
                "R1 JOIN R2 ON R1.A = R2.A FULL JOIN R3 ON R1.A = R3.C AND R1.C is NULL");
        n = pn.getChild(0).getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.FULL, true, PlanNodeType.NESTLOOP, PlanNodeType.SEQSCAN);
        nlj = (NestLoopPlanNode) n;
        e = nlj.getPreJoinPredicate();
        assertNotNull(e);
        assertEquals(ExpressionType.OPERATOR_IS_NULL, e.getExpressionType());

    }

    public void testDistributedFullJoin() {
        List<AbstractPlanNode> lpn;
        AbstractPlanNode n;

        // FULL join on partition column
        lpn = compileToFragments("select * FROM  " +
                "P1 FULL JOIN R2 ON P1.A = R2.A ");
        assertEquals(2, lpn.size());
        n = lpn.get(0).getChild(0).getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.FULL, true, PlanNodeType.SEQSCAN, PlanNodeType.RECEIVE, "R2", null);

        // FULL join on partition column
        lpn = compileToFragments("select * FROM  " +
                "R2 FULL JOIN P1 ON P1.A = R2.A ");
        assertEquals(2, lpn.size());
        n = lpn.get(0).getChild(0).getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.FULL, true, PlanNodeType.SEQSCAN, PlanNodeType.RECEIVE, "R2", null);

        // FULL join on non-partition column
        lpn = compileToFragments("select * FROM  " +
                "P1 FULL JOIN R2 ON P1.C = R2.A ");
        assertEquals(2, lpn.size());
        n = lpn.get(0).getChild(0).getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.FULL, true, PlanNodeType.SEQSCAN, PlanNodeType.RECEIVE, "R2", null);

        // FULL join of two partition tables on partition column
        lpn = compileToFragments("select * FROM  " +
                "P1 FULL JOIN P4 ON P1.A = P4.A ");
        assertEquals(2, lpn.size());
        n = lpn.get(1).getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.FULL, true, PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN, "P1", "P4");

        // FULL join of two partition tables on non-partition column
        failToCompile("select * FROM  P1 FULL JOIN P4 ON P1.C = P4.A ",
                "Join of multiple partitioned tables has insufficient join criteria");
    }

    @Override
    protected void setUp() throws Exception {
        setupSchema(TestJoinOrder.class.getResource("testplans-join-ddl.sql"), "testplansjoin", false);
    }


}
