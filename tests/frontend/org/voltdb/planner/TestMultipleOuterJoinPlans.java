/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
import org.voltdb.plannodes.PlanNodeTree;
import org.voltdb.plannodes.ReceivePlanNode;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.IndexLookupType;
import org.voltdb.types.JoinType;
import org.voltdb.types.PlanNodeType;

public class TestMultipleOuterJoinPlans  extends PlannerTestCase {

    private void verifyJoinNode(AbstractPlanNode n, PlanNodeType nodeType, JoinType joinType,
            ExpressionType preJoinExpressionType, ExpressionType joinExpressionType, ExpressionType whereExpressionType,
            PlanNodeType outerNodeType, PlanNodeType innerNodeType,
            String outerTableAlias, String innerTableAlias) {
        assertEquals(nodeType, n.getPlanNodeType());
        AbstractJoinPlanNode jn = (AbstractJoinPlanNode) n;
        assertEquals(joinType, jn.getJoinType());
        if (preJoinExpressionType != null) {
            assertEquals(preJoinExpressionType, jn.getPreJoinPredicate().getExpressionType());
        } else {
            assertNull(jn.getPreJoinPredicate());
        }
        if (joinExpressionType != null) {
            assertEquals(joinExpressionType, jn.getJoinPredicate().getExpressionType());
        } else {
            assertNull(jn.getJoinPredicate());
        }
        if (whereExpressionType != null) {
            assertEquals(whereExpressionType, jn.getWherePredicate().getExpressionType());
        } else {
            assertNull(jn.getWherePredicate());
        }
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

    private void verifyJoinNode(AbstractPlanNode n, PlanNodeType nodeType, JoinType joinType,
            ExpressionType preJoinExpressionType, ExpressionType joinExpressionType, ExpressionType whereExpressionType,
            PlanNodeType outerNodeType, PlanNodeType innerNodeType) {
        verifyJoinNode(n, nodeType, joinType, preJoinExpressionType, joinExpressionType, whereExpressionType, outerNodeType, innerNodeType, null, null);
    }

    private void verifyIndexScanNode(AbstractPlanNode n, IndexLookupType lookupType, ExpressionType predExpressionType) {
        assertNotNull(n);
        assertEquals(PlanNodeType.INDEXSCAN, n.getPlanNodeType());
        IndexScanPlanNode isn = (IndexScanPlanNode) n;
        assertEquals(lookupType, isn.getLookupType());
        if (predExpressionType != null) {
            assertEquals(predExpressionType, isn.getPredicate().getExpressionType());
        } else {
            assertNull(isn.getPredicate());
        }
    }

    public void testInnerOuterJoin() {
        AbstractPlanNode pn;
        AbstractPlanNode n;

        pn = compile("select * FROM R1 INNER JOIN R2 ON R1.A = R2.A LEFT JOIN R3 ON R3.C = R2.C");
        n = pn.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.LEFT, null, ExpressionType.COMPARE_EQUAL, null, PlanNodeType.NESTLOOP, PlanNodeType.SEQSCAN, null, "R3");
        n = n.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.INNER, null, ExpressionType.COMPARE_EQUAL, null, PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN);

        pn = compile("select * FROM R1, R2 LEFT JOIN R3 ON R3.C = R2.C WHERE R1.A = R2.A");
        n = pn.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.LEFT, null, ExpressionType.COMPARE_EQUAL, null, PlanNodeType.NESTLOOP, PlanNodeType.SEQSCAN, null, "R3");
        n = n.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.INNER, null, ExpressionType.COMPARE_EQUAL, null, PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN);
    }

    public void testOuterOuterJoin() {
        AbstractPlanNode pn;
        AbstractPlanNode n;

        pn = compile("select * FROM R1 LEFT JOIN R2 ON R1.A = R2.A LEFT JOIN R3 ON R3.C = R1.C");
        assertEquals(PlanNodeType.SEND, pn.getPlanNodeType());
        pn = pn.getChild(0);
        verifyJoinNode(pn, PlanNodeType.NESTLOOP, JoinType.LEFT, null, ExpressionType.COMPARE_EQUAL, null, PlanNodeType.NESTLOOP, PlanNodeType.SEQSCAN, null, "R3");
        pn = pn.getChild(0);
        verifyJoinNode(pn, PlanNodeType.NESTLOOP, JoinType.LEFT, null, ExpressionType.COMPARE_EQUAL, null, PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN, "R1", "R2");

        pn = compile("select * FROM R1 LEFT JOIN R2 ON R1.A = R2.A RIGHT JOIN R3 ON R3.C = R1.C");
        assertEquals(PlanNodeType.SEND, pn.getPlanNodeType());
        pn = pn.getChild(0);

        assertEquals(PlanNodeType.PROJECTION, pn.getPlanNodeType());
        pn = pn.getChild(0);

        verifyJoinNode(pn, PlanNodeType.NESTLOOP, JoinType.LEFT, null, ExpressionType.COMPARE_EQUAL, null, PlanNodeType.SEQSCAN, PlanNodeType.NESTLOOP, "R3", null);
        pn = pn.getChild(1);
        verifyJoinNode(pn, PlanNodeType.NESTLOOP, JoinType.LEFT, null, ExpressionType.COMPARE_EQUAL, null, PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN, "R1", "R2");

        pn = compile("select * FROM R1 RIGHT JOIN R2 ON R1.A = R2.A RIGHT JOIN R3 ON R3.C = R2.C");
        assertEquals(PlanNodeType.SEND, pn.getPlanNodeType());
        pn = pn.getChild(0);
        assertEquals(PlanNodeType.PROJECTION, pn.getPlanNodeType());
        pn = pn.getChild(0);

        verifyJoinNode(pn, PlanNodeType.NESTLOOP, JoinType.LEFT, null, ExpressionType.COMPARE_EQUAL, null, PlanNodeType.SEQSCAN, PlanNodeType.NESTLOOP, "R3", null);
        pn = pn.getChild(1);
        verifyJoinNode(pn, PlanNodeType.NESTLOOP, JoinType.LEFT, null, ExpressionType.COMPARE_EQUAL, null, PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN, "R2", "R1");

        pn = compile("select * FROM R1 RIGHT JOIN R2 ON R1.A = R2.A LEFT JOIN R3 ON R3.C = R1.C");
        assertEquals(PlanNodeType.SEND, pn.getPlanNodeType());
        pn = pn.getChild(0);
        assertEquals(PlanNodeType.PROJECTION, pn.getPlanNodeType());
        pn = pn.getChild(0);

        verifyJoinNode(pn, PlanNodeType.NESTLOOP, JoinType.LEFT, null, ExpressionType.COMPARE_EQUAL, null, PlanNodeType.NESTLOOP, PlanNodeType.SEQSCAN, null, "R3");
        pn = pn.getChild(0);
        verifyJoinNode(pn, PlanNodeType.NESTLOOP, JoinType.LEFT, null, ExpressionType.COMPARE_EQUAL, null, PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN, "R2", "R1");

        pn = compile("select * FROM R1 RIGHT JOIN R2 ON R1.A = R2.A LEFT JOIN R3 ON R3.C = R1.C WHERE R1.A > 0");
        assertEquals(PlanNodeType.SEND, pn.getPlanNodeType());
        pn = pn.getChild(0);
        verifyJoinNode(pn, PlanNodeType.NESTLOOP, JoinType.LEFT, null, ExpressionType.COMPARE_EQUAL, null, PlanNodeType.NESTLOOP, PlanNodeType.SEQSCAN, null, "R3");
        pn = pn.getChild(0);
        verifyJoinNode(pn, PlanNodeType.NESTLOOP, JoinType.INNER, null, ExpressionType.COMPARE_EQUAL, null, PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN);
    }

    public void testMultiTableJoinExpressions() {
        AbstractPlanNode pn = compile("select * FROM R1, R2 LEFT JOIN R3 ON R3.A = R2.C OR R3.A = R1.A WHERE R1.C = R2.C");
        AbstractPlanNode n = pn.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.LEFT, null, ExpressionType.CONJUNCTION_OR, null, PlanNodeType.NESTLOOP, PlanNodeType.SEQSCAN, null, "R3");
        NestLoopPlanNode nlj = (NestLoopPlanNode) n;
        AbstractExpression p = nlj.getJoinPredicate();
        assertEquals(ExpressionType.CONJUNCTION_OR, p.getExpressionType());
    }

    private AbstractPlanNode requireProjection(AbstractPlanNode pn) {
        assertEquals(PlanNodeType.PROJECTION, pn.getPlanNodeType());
        return pn.getChild(0);
    }

    public void testPushDownExprJoin() {
        AbstractPlanNode pn;
        AbstractPlanNode n;

        // R3.A > 0 gets pushed down all the way to the R3 scan node and used as an index
        pn = compile("select * FROM R3, R2 LEFT JOIN R1 ON R1.C = R2.C WHERE R3.C = R2.C AND R3.A > 0");
        n = pn.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.LEFT, null, ExpressionType.COMPARE_EQUAL, null, PlanNodeType.NESTLOOP, PlanNodeType.SEQSCAN, null, "R1");
        n = n.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.INNER, null, ExpressionType.COMPARE_EQUAL, null, PlanNodeType.INDEXSCAN, PlanNodeType.SEQSCAN, "R3", "R2");

        // R3.A > 0 is now outer join expression and must stay at the LEFT join
        pn = compile("select * FROM R3, R2 LEFT JOIN R1 ON R1.C = R2.C  AND R3.A > 0 WHERE R3.C = R2.C");
        n = pn.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.LEFT, ExpressionType.COMPARE_GREATERTHAN, ExpressionType.COMPARE_EQUAL, null, PlanNodeType.NESTLOOP, PlanNodeType.SEQSCAN, null, "R1");
        n = n.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.INNER, null, ExpressionType.COMPARE_EQUAL, null, PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN, "R3", "R2");

        pn = compile("select * FROM R3 JOIN R2 ON R3.C = R2.C RIGHT JOIN R1 ON R1.C = R2.C  AND R3.A > 0");
        n = pn.getChild(0);
        n = requireProjection(n);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.LEFT, null, ExpressionType.CONJUNCTION_AND, null, PlanNodeType.SEQSCAN, PlanNodeType.NESTLOOP, "R1", null);
        n = n.getChild(1);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.INNER, null, ExpressionType.COMPARE_EQUAL, null, PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN, "R3", "R2");

        // R3.A > 0 gets pushed down all the way to the R3 scan node and used as an index
        pn = compile("select * FROM R2, R3 LEFT JOIN R1 ON R1.C = R2.C WHERE R3.C = R2.C AND R3.A > 0");
        n = pn.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.LEFT, null, ExpressionType.COMPARE_EQUAL, null, PlanNodeType.NESTLOOP, PlanNodeType.SEQSCAN, null, "R1");
        n = n.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.INNER, null, ExpressionType.COMPARE_EQUAL, null, PlanNodeType.SEQSCAN, PlanNodeType.INDEXSCAN, "R2", "R3");

        // R3.A = R2.C gets pushed down to the R2, R3 join node scan node and used as an index
        pn = compile("select * FROM R2, R3 LEFT JOIN R1 ON R1.C = R2.C WHERE R3.A = R2.C");
        n = pn.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.LEFT, null, ExpressionType.COMPARE_EQUAL, null, PlanNodeType.NESTLOOPINDEX, PlanNodeType.SEQSCAN, null, "R1");
        n = n.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOPINDEX, JoinType.INNER, null, null, null, PlanNodeType.SEQSCAN, null, "R2", "R3");
    }

    public void testOuterSimplificationJoin() {
        // NULL_rejection simplification is the first transformation -
        // before the LEFT-to-RIGHT and the WHERE expressions push down
        AbstractPlanNode pn;
        AbstractPlanNode n;

        pn = compile("select * FROM R1, R3 RIGHT JOIN R2 ON R1.A = R2.A WHERE R3.C = R1.C");
        n = pn.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.INNER, null, ExpressionType.COMPARE_EQUAL, null, PlanNodeType.NESTLOOP, PlanNodeType.SEQSCAN);
        n = n.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.INNER, null, ExpressionType.COMPARE_EQUAL, null, PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN);

        // The second R3.C = R2.C join condition is NULL-rejecting for the outer table
        // from the first LEFT join - can't simplify (not the inner table)
        pn = compile("select * FROM R1 LEFT JOIN R2 ON R1.A = R2.A LEFT JOIN R3 ON R3.C = R2.C");
        n = pn.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.LEFT, null, ExpressionType.COMPARE_EQUAL, null, PlanNodeType.NESTLOOP, PlanNodeType.SEQSCAN, null, "R3");
        n = n.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.LEFT, null, ExpressionType.COMPARE_EQUAL, null, PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN, "R1", "R2");

        // The second R3.C = R2.C join condition is NULL-rejecting for the first LEFT join
        pn = compile("select * FROM R1 LEFT JOIN R2 ON R1.A = R2.A RIGHT JOIN R3 ON R3.C = R2.C");
        n = pn.getChild(0);
        n = requireProjection(n);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.LEFT, null, ExpressionType.COMPARE_EQUAL, null, PlanNodeType.SEQSCAN, PlanNodeType.NESTLOOP, "R3", null);
        n = n.getChild(1);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.INNER, null, ExpressionType.COMPARE_EQUAL, null, PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN);

        // The R3.A = R1.A join condition is NULL-rejecting for the FULL join OUTER (R1) table
        // simplifying it to R1 LEFT JOIN R2
        pn = compile("select * FROM " +
                "R1 FULL JOIN R2 ON R1.A = R2.A " +
                "RIGHT JOIN R3 ON R3.A = R1.A");
        n = pn.getChild(0);
        n = requireProjection(n);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.LEFT, null, ExpressionType.COMPARE_EQUAL, null, PlanNodeType.SEQSCAN, PlanNodeType.NESTLOOP, "R3", null);
        n = n.getChild(1);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.LEFT, null, ExpressionType.COMPARE_EQUAL, null, PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN, "R1", "R2");

        // The R3.A = R2.A join condition is NULL-rejecting for the FULL join INNER (R2) table
        // simplifying it to R1 RIGHT JOIN R2 which gets converted to R2 LEFT JOIN R1
        pn = compile("select * FROM " +
                "R1 FULL JOIN R2 ON R1.A = R2.A " +
                    "RIGHT JOIN R3 ON R3.A = R2.A");
        n = pn.getChild(0);
        n = requireProjection(n);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.LEFT, null, ExpressionType.COMPARE_EQUAL, null, PlanNodeType.SEQSCAN, PlanNodeType.NESTLOOP, "R3", null);
        n = n.getChild(1);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.LEFT, null, ExpressionType.COMPARE_EQUAL, null, PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN, "R2", "R1");

        // The R1-R2 FULL join is an outer node in the top LEFT join - not simplified
        pn = compile("select * FROM " +
                "R1 FULL JOIN R2 ON R1.A = R2.A " +
                    "LEFT JOIN R3 ON R3.A = R2.A");
        n = pn.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOPINDEX, JoinType.LEFT, null, null, null, PlanNodeType.NESTLOOP, PlanNodeType.INDEXSCAN, null, "R3");
        n = n.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.FULL, null, ExpressionType.COMPARE_EQUAL, null, PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN, "R1", "R2");

        // The R3.A = R2.A AND R3.A = R1.A join condition is NULL-rejecting for the FULL join
        // OUTER (R1) and INNER (R1) tables simplifying it to R1 JOIN R2
        pn = compile("select * FROM " +
                "R1 FULL JOIN R2 ON R1.A = R2.A " +
                    "RIGHT JOIN R3 ON R3.A = R2.A AND R3.A = R1.A");
        n = pn.getChild(0);
        n = requireProjection(n);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.LEFT, null, ExpressionType.CONJUNCTION_AND, null, PlanNodeType.SEQSCAN, PlanNodeType.NESTLOOP, "R3", null);
        n = n.getChild(1);
        // HSQL doubles the join expression for the first join. Once it's corrected the join expression type
        // should be ExpressionType.COMPARE_EQUAL
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.INNER, null, ExpressionType.COMPARE_EQUAL, null, PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN);

        // The R4 FULL join is an outer node in the R5 FULL join and can not be simplified by the R1.A = R5.A ON expression
        // R1 RIGHT JOIN R2 ON R1.A = R2.A                  R1 JOIN R3 ON R1.A = R3.A
        //      JOIN R3 ON R1.A = R3.A               ==>        JOIN  R2 ON R1.A = R2.A
        //          FULL JOIN R4 ON R1.A = R4.A                     FULL JOIN R4 ON R1.A = R4.A
        //              FULL JOIN R5 ON R1.A = R5.A                     FULL JOIN R5 ON R1.A = R5.A
        pn = compile("select * FROM " +
                "R1 RIGHT JOIN R2 ON R1.A = R2.A " +
                    "JOIN R3 ON R1.A = R3.A " +
                    "FULL JOIN R4 ON R1.A = R4.A " +
                        "FULL JOIN R5 ON R1.A = R5.A");
        n = pn.getChild(0);
        n = requireProjection(n);
        verifyJoinNode(n, PlanNodeType.NESTLOOPINDEX, JoinType.FULL, null, null, null, PlanNodeType.NESTLOOPINDEX, PlanNodeType.INDEXSCAN, null, "R5");
        n = n.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOPINDEX, JoinType.FULL, null, null, null, PlanNodeType.NESTLOOP, PlanNodeType.INDEXSCAN, null, "R4");
        n = n.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.INNER, null, ExpressionType.COMPARE_EQUAL, null, PlanNodeType.NESTLOOPINDEX, PlanNodeType.SEQSCAN, null, "R2");
        n = n.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOPINDEX, JoinType.INNER, null, null, null, PlanNodeType.SEQSCAN, PlanNodeType.INDEXSCAN, "R1", "R3");

        // The R1-R2 LEFT JOIN belongs to the outer node of the top FULL join
        // and can't be simplified by the R2.A = R4.A ON join condition
        pn = compile("select * FROM " +
                "R1 LEFT JOIN R2 ON R1.A = R2.A " +
                    "JOIN R3 ON R1.A = R3.A " +
                    "FULL JOIN R4 ON R2.A = R4.A");
        n = pn.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOPINDEX, JoinType.FULL, null, null, null, PlanNodeType.NESTLOOPINDEX, PlanNodeType.INDEXSCAN, null, "R4");
        n = n.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOPINDEX, JoinType.INNER, null, null, null, PlanNodeType.NESTLOOP, PlanNodeType.SEQSCAN, null, "R3");
        n = n.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.LEFT, null, ExpressionType.COMPARE_EQUAL, null, PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN, "R1", "R2");

        // The R2.A > 0 WHERE expression is NULL rejecting for all outer joins
        pn = compile("select * FROM " +
                "R1 LEFT JOIN R2 ON R1.A = R2.A " +
                    "JOIN R3 ON R1.A = R3.A " +
                    "FULL JOIN R4 ON R1.A = R4.A WHERE R2.A > 0");
        n = pn.getChild(0);
        n = requireProjection(n);
        verifyJoinNode(n, PlanNodeType.NESTLOOPINDEX, JoinType.LEFT, null, null, null, PlanNodeType.NESTLOOP, PlanNodeType.INDEXSCAN, null, "R4");
        n = n.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.INNER, null, ExpressionType.COMPARE_EQUAL, null, PlanNodeType.NESTLOOPINDEX, PlanNodeType.SEQSCAN, null, "R2");
        n = n.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOPINDEX, JoinType.INNER, null, null, null, PlanNodeType.SEQSCAN, PlanNodeType.INDEXSCAN, "R1", "R3");

        // The R1-R2 RIGHT join is an outer node in the top FULL join - not simplified
        pn = compile("SELECT * FROM R1 RIGHT JOIN R2 ON R1.A = R2.A FULL JOIN R3 ON R3.A = R1.A");
        n = pn.getChild(0);
        n = requireProjection(n);
        verifyJoinNode(n, PlanNodeType.NESTLOOPINDEX, JoinType.FULL, null, null, null, PlanNodeType.NESTLOOP, PlanNodeType.INDEXSCAN, null, "R3");
        n = n.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.LEFT, null, ExpressionType.COMPARE_EQUAL, null, PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN, "R2", "R1");

        // The R1-R2 LEFT join is an outer node in the top FULL join - not simplified
        pn = compile("SELECT * FROM R1 LEFT JOIN R2 ON R1.A = R2.A FULL JOIN R3 ON R3.A = R2.A");
        n = pn.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOPINDEX, JoinType.FULL, null, null, null, PlanNodeType.NESTLOOP, PlanNodeType.INDEXSCAN, null, "R3");
        n = n.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.LEFT, null, ExpressionType.COMPARE_EQUAL, null, PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN, "R1", "R2");
    }

    public void testMultitableDistributedJoin() {
        List<AbstractPlanNode> lpn;
        AbstractPlanNode n;

        // One distributed table
        lpn = compileToFragments("select *  FROM R3,R1 LEFT JOIN P2 ON R3.A = P2.A WHERE R3.A=R1.A ");
        assertTrue(lpn.size() == 2);
        n = lpn.get(0);
        assertEquals(PlanNodeType.SEND, n.getPlanNodeType());
        n = n.getChild(0);
        n = requireProjection(n);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.LEFT, null, ExpressionType.COMPARE_EQUAL, null, PlanNodeType.NESTLOOPINDEX, PlanNodeType.RECEIVE);

        // R3.A and P2.A have an index. P2,R1 is NLIJ/inlined IndexScan because it's an inner join even P2 is distributed
        lpn = compileToFragments("select *  FROM P2,R1 LEFT JOIN R3 ON R3.A = P2.A WHERE P2.A=R1.A ");
        assertTrue(lpn.size() == 2);
        n = lpn.get(0);
        assertEquals(PlanNodeType.SEND, n.getPlanNodeType());
        n = n.getChild(0);
        n = requireProjection(n);
        assertTrue(n instanceof ReceivePlanNode);
        n = lpn.get(1);
        assertEquals(PlanNodeType.SEND, n.getPlanNodeType());
        n = n.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOPINDEX, JoinType.LEFT, null, null, null, PlanNodeType.NESTLOOPINDEX, PlanNodeType.INDEXSCAN);

        // R3.A has an index. R3,P2 is NLJ because it's an outer join and P2 is distributed
        lpn = compileToFragments("select *  FROM R3,R1 LEFT JOIN P2 ON R3.A = P2.A WHERE R3.A=R1.A ");
        assertTrue(lpn.size() == 2);
        // to debug */ System.out.println("DEBUG 0.0: " + lpn.get(0).toExplainPlanString());
        // to debug */ System.out.println("DEBUG 0.1: " + lpn.get(1).toExplainPlanString());
        n = lpn.get(0);
        assertEquals(PlanNodeType.SEND, n.getPlanNodeType());
        n = n.getChild(0);
        n = requireProjection(n);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.LEFT, null, ExpressionType.COMPARE_EQUAL, null, PlanNodeType.NESTLOOPINDEX, PlanNodeType.RECEIVE);
        n = n.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOPINDEX, JoinType.INNER, null, null, null, PlanNodeType.SEQSCAN, PlanNodeType.INDEXSCAN);
        n = lpn.get(1);
        assertEquals(PlanNodeType.SEND, n.getPlanNodeType());
        n = n.getChild(0);
        // For determinism reason
        assertTrue(n instanceof IndexScanPlanNode);

        // R3.A has an index. P2,R1 is NLJ because P2 is distributed and it's an outer join
        lpn = compileToFragments("select *  FROM R1 LEFT JOIN P2 ON R1.A = P2.A, R3 WHERE R1.A=R3.A ");
        assertTrue(lpn.size() == 2);
        // to debug */ System.out.println("DEBUG 1.0: " + lpn.get(0).toExplainPlanString());
        // to debug */ System.out.println("DEBUG 1.1: " + lpn.get(1).toExplainPlanString());
        n = lpn.get(0);
        assertEquals(PlanNodeType.SEND, n.getPlanNodeType());
        n = n.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOPINDEX, JoinType.INNER, null, null, null, PlanNodeType.NESTLOOP, PlanNodeType.SEQSCAN);
        n = n.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.LEFT, null, ExpressionType.COMPARE_EQUAL, null, PlanNodeType.SEQSCAN, PlanNodeType.RECEIVE);
        n = lpn.get(1);
        assertEquals(PlanNodeType.SEND, n.getPlanNodeType());
        n = n.getChild(0);
        // For determinism reason
        assertTrue(n instanceof IndexScanPlanNode);

        // Two distributed table
        lpn = compileToFragments("select *  FROM R3,P1 LEFT JOIN P2 ON R3.A = P2.A WHERE R3.A=P1.A ");
        assertTrue(lpn.size() == 2);
        n = lpn.get(0);
        assertEquals(PlanNodeType.SEND, n.getPlanNodeType());
        n = n.getChild(0);
        n = requireProjection(n);
        assertTrue(n instanceof ReceivePlanNode);
        n = lpn.get(1);
        assertEquals(PlanNodeType.SEND, n.getPlanNodeType());
        n = n.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOPINDEX, JoinType.LEFT, null, null, null, PlanNodeType.NESTLOOPINDEX, PlanNodeType.INDEXSCAN);
        n = n.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOPINDEX, JoinType.INNER, null, null, null, PlanNodeType.SEQSCAN, PlanNodeType.INDEXSCAN);
    }

    public void testFullJoinExpressions() {
        AbstractPlanNode pn;
        AbstractPlanNode n;

        // WHERE outer and inner expressions stay at the FULL NLJ node
        pn = compile("select * FROM  " +
                "R1 FULL JOIN R2 ON R1.A = R2.A WHERE R2.C IS NULL AND R1.C is NULL");
        n = pn.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.FULL, null, ExpressionType.COMPARE_EQUAL, ExpressionType.CONJUNCTION_AND, PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN);

        // WHERE outer and inner expressions stay at the FULL NLJ node
        // The outer node is a join itself
        pn = compile("select * FROM  " +
                "R1 JOIN R2 ON R1.A = R2.A FULL JOIN R3 ON R3.C = R2.C WHERE R1.C is NULL");
        n = pn.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.FULL, null, ExpressionType.COMPARE_EQUAL, ExpressionType.OPERATOR_IS_NULL, PlanNodeType.NESTLOOP, PlanNodeType.SEQSCAN);

        // WHERE outer-inner expressions stay at the FULL NLJ node
        pn = compile("select * FROM  " +
                "R1 FULL JOIN R2 ON R1.A = R2.A WHERE R2.C IS NULL OR R1.C is NULL");
        n = pn.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.FULL, null, ExpressionType.COMPARE_EQUAL, ExpressionType.CONJUNCTION_OR, PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN);

        // WHERE outer and inner expressions push down process stops at the FULL join (R1,R2) node -
        // FULL join is itself an outer node
        pn = compile("select * FROM  " +
                "R1 FULL JOIN R2 ON R1.A = R2.A LEFT JOIN R3 ON R3.C = R2.C WHERE R1.C is NULL");
        n = pn.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.LEFT, null, ExpressionType.COMPARE_EQUAL, null, PlanNodeType.NESTLOOP, PlanNodeType.SEQSCAN);
        n = n.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.FULL, null, ExpressionType.COMPARE_EQUAL, ExpressionType.OPERATOR_IS_NULL, PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN);

        // OUTER JOIN expression (R1.A > 0) is pre-predicate, inner and inner - outer expressions R3.C = R2.C AND R3.C < 0 are predicate
        pn = compile("select * FROM R1 JOIN R2 ON R1.A = R2.C FULL JOIN R3 ON R3.C = R2.C  AND R1.A > 0 AND R3.C < 0");
        n = pn.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.FULL, ExpressionType.COMPARE_GREATERTHAN, ExpressionType.CONJUNCTION_AND, null, PlanNodeType.NESTLOOP, PlanNodeType.SEQSCAN, null, "R3");
        n = n.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.INNER, null, ExpressionType.COMPARE_EQUAL, null, PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN, "R1", "R2");

        // NLJ JOIN outer expression is pre-join expression, NLJ JOIN inner expression together with
        // JOIN inner-outer one are part of the join predicate
        pn = compile("select * FROM  " +
                "R1 FULL JOIN R2 ON R1.A = R2.A AND R1.C = R2.C");
        n = pn.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.FULL, null, ExpressionType.CONJUNCTION_AND, null, PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN);

        // NLJ JOIN outer expression is pre-join expression, NLJ JOIN inner expression together with
        // JOIN inner-outer one are part of the join predicate
        pn = compile("select * FROM  " +
                "R1 FULL JOIN R2 ON R1.A = R2.A AND R1.C < 0 AND R2.C > 0");
        n = pn.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.FULL, ExpressionType.COMPARE_LESSTHAN, ExpressionType.CONJUNCTION_AND, null, PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN);

        // NLJ JOIN outer expression is pre-join expression, NLJ JOIN inner expression together with
        // JOIN inner-outer one are part of the join predicate
        pn = compile("select * FROM  " +
                "R1 JOIN R2 ON R1.A = R2.A FULL JOIN R3 ON R1.A = R3.C AND R1.C is NULL");
        n = pn.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.FULL, ExpressionType.OPERATOR_IS_NULL, ExpressionType.COMPARE_EQUAL, null, PlanNodeType.NESTLOOP, PlanNodeType.SEQSCAN);

    }

    public void testFullIndexJoinExpressions() {
        AbstractPlanNode pn;
        AbstractPlanNode n;

        // Simple FULL NLIJ.  Note that we verify
        // n but pn still points to the root.  Later we
        // will generate a plan string from pn and compare
        // it to another plan string, created from different
        // but equivalent SQL.  We are hoping for identical
        // plans.
        n = pn = compile("select * FROM  " +
                "R3 FULL JOIN R1 ON R3.A = R1.A WHERE R3.C IS NULL");
        assert(PlanNodeType.SEND == n.getPlanNodeType());
        n = n.getChild(0);
        n = requireProjection(n);
        verifyJoinNode(n, PlanNodeType.NESTLOOPINDEX, JoinType.FULL, null, null, ExpressionType.OPERATOR_IS_NULL, PlanNodeType.SEQSCAN, PlanNodeType.INDEXSCAN);
        String json = (new PlanNodeTree(pn)).toJSONString();

        // Same Join as above but using FULL OUTER JOIN syntax
        pn = compile("select * FROM  " +
                "R3 FULL OUTER JOIN R1 ON R3.A = R1.A WHERE R3.C IS NULL");
        String json1 = (new PlanNodeTree(pn)).toJSONString();
        assertEquals(json, json1);

        // FULL NLJ. R3.A is an index column but R3.A > 0 expression is used as a PREDICATE only
        pn = compile("select * FROM  " +
                "R1 FULL JOIN R3 ON R3.C = R1.A AND R3.A > 0");
        pn = pn.getChild(0);
        verifyJoinNode(pn, PlanNodeType.NESTLOOP, JoinType.FULL, null, ExpressionType.CONJUNCTION_AND, null, PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN, "R1", "R3");

        // FULL NLIJ, inner join R3.A > 0 is added as a post-predicate to the inline Index scan
        pn = compile("select * FROM R1 FULL JOIN R3 ON R3.A = R1.A AND R3.A > 55");
        pn = pn.getChild(0);
        verifyJoinNode(pn, PlanNodeType.NESTLOOPINDEX, JoinType.FULL, null, null, null, PlanNodeType.SEQSCAN, PlanNodeType.INDEXSCAN, "R1", "R3");
        verifyIndexScanNode(pn.getInlinePlanNode(PlanNodeType.INDEXSCAN), IndexLookupType.EQ, ExpressionType.COMPARE_GREATERTHAN);

        // FULL NLIJ, inner join L.A > 0 is added as a pre-predicate to the NLIJ
        pn = compile("select * FROM R3 L FULL JOIN R3 R ON L.A = R.A AND L.A > 55");
        pn = pn.getChild(0);
        verifyJoinNode(pn, PlanNodeType.NESTLOOPINDEX, JoinType.FULL, ExpressionType.COMPARE_GREATERTHAN, null, null, PlanNodeType.SEQSCAN, PlanNodeType.INDEXSCAN, "L", "R");
        verifyIndexScanNode(pn.getInlinePlanNode(PlanNodeType.INDEXSCAN), IndexLookupType.EQ, null);

        // FULL NLIJ, inner-outer join R3.c = R1.c is a post-predicate for the inline Index scan
        pn = compile("select * FROM R1 FULL JOIN R3 ON R3.A = R1.A AND R3.C = R1.C");
        pn = pn.getChild(0);
        verifyJoinNode(pn, PlanNodeType.NESTLOOPINDEX, JoinType.FULL, null, null, null, PlanNodeType.SEQSCAN, PlanNodeType.INDEXSCAN, "R1", "R3");
        verifyIndexScanNode(pn.getInlinePlanNode(PlanNodeType.INDEXSCAN), IndexLookupType.EQ, ExpressionType.COMPARE_EQUAL);

        // FULL NLIJ, outer join (R1, R2) expression R1.A > 0 is a pre-predicate
        pn = compile("select * FROM R1 JOIN R2 ON R1.A = R2.C FULL JOIN R3 ON R3.A = R2.C  AND R1.A > 0");
        pn = pn.getChild(0);
        verifyJoinNode(pn, PlanNodeType.NESTLOOPINDEX, JoinType.FULL, ExpressionType.COMPARE_GREATERTHAN, null, null, PlanNodeType.NESTLOOP, PlanNodeType.INDEXSCAN, null, "R3");
        verifyIndexScanNode(pn.getInlinePlanNode(PlanNodeType.INDEXSCAN), IndexLookupType.EQ, null);
        n = pn.getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.INNER, null, ExpressionType.COMPARE_EQUAL, null, PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN);

    }

    public void testDistributedFullJoin() {
        List<AbstractPlanNode> lpn;
        AbstractPlanNode n;

        // FULL join on partition column
        lpn = compileToFragments("select * FROM  " +
                "P1 FULL JOIN R2 ON P1.A = R2.A ");
        assertEquals(2, lpn.size());
        n = lpn.get(0).getChild(0);
        n = requireProjection(n);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.FULL, null, ExpressionType.COMPARE_EQUAL, null, PlanNodeType.SEQSCAN, PlanNodeType.RECEIVE, "R2", null);

        // FULL join on partition column
        lpn = compileToFragments("select * FROM  " +
                "R2 FULL JOIN P1 ON P1.A = R2.A ");
        assertEquals(2, lpn.size());
        n = lpn.get(0).getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.FULL, null, ExpressionType.COMPARE_EQUAL, null, PlanNodeType.SEQSCAN, PlanNodeType.RECEIVE, "R2", null);

        // FULL join on non-partition column
        lpn = compileToFragments("select * FROM  " +
                "P1 FULL JOIN R2 ON P1.C = R2.A ");
        assertEquals(2, lpn.size());
        n = lpn.get(0).getChild(0);
        n = requireProjection(n);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.FULL, null, ExpressionType.COMPARE_EQUAL, null, PlanNodeType.SEQSCAN, PlanNodeType.RECEIVE, "R2", null);

        // NLJ FULL join (R2, P2) on partition column  R2.A > 0 is a pre-predicate, P2.A = R2.A AND P2.E < 0 are join predicate
        // It can't be a NLIJ because P2 is partitioned - P2.A index is not used
        lpn = compileToFragments("select * FROM  " +
                "P2 FULL JOIN R2 ON P2.A = R2.A AND R2.A > 0 AND P2.E < 0");
        assertEquals(2, lpn.size());
        n = lpn.get(0).getChild(0);
        n = requireProjection(n);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.FULL, ExpressionType.COMPARE_GREATERTHAN, ExpressionType.CONJUNCTION_AND, null, PlanNodeType.SEQSCAN, PlanNodeType.RECEIVE, "R2", null);

        // NLJ FULL join (R2, P2) on partition column  P2.E = R2.A AND P2.A > 0 are join predicate
        // Inner join expression P2.A > 0 can't be used as index expression with NLJ
        lpn = compileToFragments("select * FROM  " +
                "P2 FULL JOIN R2 ON P2.E = R2.A AND P2.A > 0");
        assertEquals(2, lpn.size());
        n = lpn.get(0).getChild(0);
        n = requireProjection(n);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.FULL, null, ExpressionType.CONJUNCTION_AND, null, PlanNodeType.SEQSCAN, PlanNodeType.RECEIVE, "R2", null);

        // NLJ (R3, P2) on partition column P2.A. R3.A > 0 is a PRE_PREDICTAE
        // NLIJ (P2,R3) on partition column P2.A using index R3.A is an invalid plan for a FULL join
        lpn = compileToFragments("select * FROM  " +
                "P2 FULL JOIN R3 ON P2.A = R3.A AND R3.A > 0 AND P2.E < 0");
        assertEquals(2, lpn.size());
        n = lpn.get(0).getChild(0);
        n = requireProjection(n);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.FULL, ExpressionType.COMPARE_GREATERTHAN, ExpressionType.CONJUNCTION_AND, null, PlanNodeType.SEQSCAN, PlanNodeType.RECEIVE, "R3", null);

        // FULL NLJ join of two partition tables on partition column
        lpn = compileToFragments("select * FROM  P1 FULL JOIN P4 ON P1.A = P4.A ");
        assertEquals(2, lpn.size());
        n = lpn.get(1).getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOP, JoinType.FULL, null, ExpressionType.COMPARE_EQUAL, null, PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN, "P1", "P4");

        // FULL NLIJ (P1,P2) on partition column P2.A
        lpn = compileToFragments("select * FROM P2 FULL JOIN P1 ON P1.A = P2.A AND P2.A > 0");
        assertEquals(2, lpn.size());
        n = lpn.get(1).getChild(0);
        verifyJoinNode(n, PlanNodeType.NESTLOOPINDEX, JoinType.FULL, null, null, null, PlanNodeType.SEQSCAN, PlanNodeType.INDEXSCAN, "P1", "P2");
        verifyIndexScanNode(n.getInlinePlanNode(PlanNodeType.INDEXSCAN), IndexLookupType.EQ, ExpressionType.COMPARE_GREATERTHAN);

        // FULL join of two partition tables on non-partition column
        failToCompile("select * FROM  P1 FULL JOIN P4 ON P1.C = P4.A ",
                "The planner cannot guarantee that all rows would be in a single partition.");
}

    @Override
    protected void setUp() throws Exception {
        setupSchema(TestJoinOrder.class.getResource("testplans-join-ddl.sql"), "testplansjoin", false);
    }


}
