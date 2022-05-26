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

import java.util.ArrayList;
import java.util.List;

import org.voltdb.plannodes.AbstractJoinPlanNode;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.AggregatePlanNode;
import org.voltdb.plannodes.LimitPlanNode;
import org.voltdb.plannodes.MergeReceivePlanNode;
import org.voltdb.plannodes.OrderByPlanNode;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.plannodes.ReceivePlanNode;
import org.voltdb.types.JoinType;
import org.voltdb.types.PlanNodeType;

public class TestPlansLimit extends PlannerTestCase {
    @Override
    protected void setUp() throws Exception {
        setupSchema(getClass().getResource("testplans-groupby-ddl.sql"),
                    "testplanslimit", false);
    }

    public void testPushDownIntoScan() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT B1 FROM R1 WHERE A1 = ? LIMIT 1");
        checkPushedDownLimit(pn, false, true, false, false);
        pn = compileToFragments("SELECT B1 FROM R1 WHERE PKEY = ? LIMIT 1");
        checkPushedDownLimit(pn, false, true, false, false);
        pn = compileToFragments("SELECT * FROM R1 WHERE PKEY = ? LIMIT 1");
        checkPushedDownLimit(pn, false, true, false, false);
    }

    public void testPushDownIntoScanMultiPart() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT F_D1 FROM F LIMIT 1");
        checkPushedDownLimit(pn, true, true, false, false);
        pn = compileToFragments("SELECT * FROM F LIMIT 1");
        checkPushedDownLimit(pn, true, true, false, false);
    }

    public void testPushDownIntoJoin() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT D1.D1_PKEY FROM D1, D2 WHERE D1.D1_PKEY = D2.D2_PKEY LIMIT 2");
        checkPushedDownLimit(pn, false, false, true, false);
        pn = compileToFragments("SELECT D1.D1_NAME FROM D1, D2 WHERE D1.D1_PKEY = D2.D2_PKEY LIMIT 2");
        checkPushedDownLimit(pn, false, false, true, false);
        pn = compileToFragments("SELECT * FROM D1, D2 WHERE D1.D1_PKEY = D2.D2_PKEY LIMIT 2");
        checkPushedDownLimit(pn, false, false, true, false);
    }

    public void testPushDownIntoJoinMultiPart() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT T2.PKEY FROM T1, T2 WHERE T1.PKEY = T2.PKEY LIMIT 2");
        checkPushedDownLimit(pn, true, false, true, false);
        pn = compileToFragments("SELECT T2.I FROM T1, T2 WHERE T1.PKEY = T2.PKEY LIMIT 2");
        checkPushedDownLimit(pn, true, false, true, false);
        pn = compileToFragments("SELECT * FROM T1, T2 WHERE T1.PKEY = T2.PKEY LIMIT 2");
        checkPushedDownLimit(pn, true, false, true, false);
    }

    public void testPushDownIntoLeftJoin() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT D1.D1_PKEY FROM D1 LEFT JOIN D2 ON D1.D1_PKEY = D2.D2_PKEY LIMIT 2");
        checkPushedDownLimit(pn, false, false, true, true);
        pn = compileToFragments("SELECT D1.D1_NAME FROM D1 LEFT JOIN D2 ON D1.D1_PKEY = D2.D2_PKEY LIMIT 2");
        checkPushedDownLimit(pn, false, false, true, true);
        pn = compileToFragments("SELECT * FROM D1 LEFT JOIN D2 ON D1.D1_PKEY = D2.D2_PKEY LIMIT 2");
        checkPushedDownLimit(pn, false, false, true, true);
    }

    public void testPushDownIntoLeftJoinMultiPart() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT T1.PKEY FROM T1 LEFT JOIN T2 ON T1.PKEY = T2.PKEY LIMIT 2");
        checkPushedDownLimit(pn, true, false, true, true);
        pn = compileToFragments("SELECT T1.A1 FROM T1 LEFT JOIN T2 ON T1.PKEY = T2.PKEY LIMIT 2");
        checkPushedDownLimit(pn, true, false, true, true);
        pn = compileToFragments("SELECT * FROM T1 LEFT JOIN T2 ON T1.PKEY = T2.PKEY LIMIT 2");
        checkPushedDownLimit(pn, true, false, true, true);
    }

    /**
     * Check if the limit node is pushed-down in the given plan.
     *
     * @param np
     *            The generated plan
     * @param isMultiPart
     *            Whether or not the plan is distributed
     * @param downIntoScan
     *            limit node is pushed down into the scan node
     * @param downIntoJoin
     *            limit node is pushed down into the join node
     * @param isLeftJoin
     *            Whether or not the join node type is left outer join, TRUE when it's left outer join and downIntoJoin is TRUE
     */
    private void checkPushedDownLimit(List<AbstractPlanNode> pn, boolean isMultiPart, boolean downIntoScan, boolean downIntoJoin, boolean isLeftJoin) {
        assertTrue(pn.size() > 0);

        for ( AbstractPlanNode nd : pn) {
            System.out.println("PlanNode Explain string:\n" + nd.toExplainPlanString());
        }

        AbstractPlanNode p;
        if (isMultiPart) {
            assertTrue(pn.size() == 2);
            p = pn.get(0).getChild(0);
            if (p.getPlanNodeType() == PlanNodeType.PROJECTION) {
                p = p.getChild(0);
            }
            assertTrue(p instanceof LimitPlanNode);
            assertTrue(p.toJSONString().contains("\"LIMIT\""));
            checkPushedDownLimit(pn.get(1).getChild(0), downIntoScan, downIntoJoin, isLeftJoin);
        } else {
            p = pn.get(0).getChild(0);
            if (p.getPlanNodeType() == PlanNodeType.PROJECTION) {
                p = p.getChild(0);
            }
            checkPushedDownLimit(p, downIntoScan, downIntoJoin, isLeftJoin);
        }
    }

    private final boolean ENG5399fixed = false;
    private void checkPushedDownLimit(AbstractPlanNode p, boolean downIntoScan, boolean downIntoJoin, boolean isLeftJoin) {

        if (downIntoScan) {
            assertTrue(p instanceof AbstractScanPlanNode);
            assertTrue(p.getInlinePlanNode(PlanNodeType.LIMIT) != null);
        }

        if (downIntoJoin) {
            assertTrue(p instanceof AbstractJoinPlanNode);
            assertTrue(p.getInlinePlanNode(PlanNodeType.LIMIT) != null);
        }

        if (ENG5399fixed && isLeftJoin) {
            assertTrue(p instanceof AbstractJoinPlanNode);
            assertTrue(((AbstractJoinPlanNode)p).getJoinType() == JoinType.LEFT);
            assertTrue(p.getInlinePlanNode(PlanNodeType.LIMIT) != null);
            if (p.getChild(0) instanceof AbstractScanPlanNode || p.getChild(0) instanceof AbstractJoinPlanNode) {
                assertTrue(p.getChild(0).getInlinePlanNode(PlanNodeType.LIMIT) != null);
            } else {
                assertTrue(p.getChild(0).getPlanNodeType() == PlanNodeType.LIMIT);
            }
        }
    }


    public void testInlineLimitWithOrderBy() {
        List<AbstractPlanNode> pns = new ArrayList<AbstractPlanNode>();

        // no push down for aggregate nodes
        //@TODO LIMIT node is inline with aggragate
        pns = compileToFragments("select A1, count(*) as tag from T1 group by A1 order by A1 limit 1");
        checkInlineLimitWithOrderby(pns, true);

        pns = compileToFragments("select A1 from T1 order by A1 limit 1");
        checkInlineLimitAndOrderbyWithReceive(pns, true);

        pns = compileToFragments("select B3 from T3 order by B3 limit 1");
        checkInlineLimitAndOrderbyWithReceive(pns, true);

        // no push down
        pns = compileToFragments("select A1, count(*) as tag from T1 group by A1 order by tag limit 1");
        checkInlineLimitWithOrderby(pns, false);

        // Replicated table
        pns = compileToFragments("select A1 from R1 order by A1 limit 1");
        checkInlineLimitWithOrderby(pns, false);
    }


    private void checkInlineLimitWithOrderby(List<AbstractPlanNode> pns, boolean pushdown) {
        AbstractPlanNode p;

        p = pns.get(0).getChild(0);
        if (p instanceof ProjectionPlanNode) {
            p = p.getChild(0);
        }
        if (p instanceof MergeReceivePlanNode) {
            assertNotNull(p.getInlinePlanNode(PlanNodeType.ORDERBY));
            AbstractPlanNode aggr = AggregatePlanNode.getInlineAggregationNode(p);
            if (aggr != null) {
                assertNotNull(aggr.getInlinePlanNode(PlanNodeType.LIMIT));
            }
        } else {
            assertTrue(p instanceof OrderByPlanNode);
            assertNotNull(p.getInlinePlanNode(PlanNodeType.LIMIT));
        }

        if (pushdown) {
            assertEquals(2, pns.size());
            p = pns.get(1).getChild(0);
            assertTrue(p instanceof OrderByPlanNode);
            assertNotNull(p.getInlinePlanNode(PlanNodeType.LIMIT));
        } else if (pns.size() == 2) {
            p = pns.get(1).getChild(0);
            assertFalse(p.toExplainPlanString().toLowerCase().contains("limit"));
        }
    }

    private void checkInlineLimitAndOrderbyWithReceive(List<AbstractPlanNode> pns, boolean pushdown) {
        AbstractPlanNode p;

        p = pns.get(0).getChild(0);
        if (p instanceof ProjectionPlanNode) {
            p = p.getChild(0);
        }
        assertTrue(p instanceof MergeReceivePlanNode);
        assertNotNull(p.getInlinePlanNode(PlanNodeType.LIMIT));
        assertNotNull(p.getInlinePlanNode(PlanNodeType.ORDERBY));

        if (pushdown) {
            assertEquals(2, pns.size());
            p = pns.get(1).getChild(0);
            assertTrue(p instanceof OrderByPlanNode);
            assertNotNull(p.getInlinePlanNode(PlanNodeType.LIMIT));
        } else if (pns.size() == 2) {
            p = pns.get(1).getChild(0);
            assertFalse(p.toExplainPlanString().toLowerCase().contains("limit"));
        }
    }
}
