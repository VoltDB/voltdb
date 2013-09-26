/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.NestLoopPlanNode;
import org.voltdb.plannodes.SeqScanPlanNode;

public class TestJoinOrder extends PlannerTestCase {
    public void testBasicJoinOrder() {
        AbstractPlanNode pn = compileWithJoinOrder("select * FROM T1, T2, T3, T4, T5, T6, T7", "T7,T6,T5,T4,T3,T2,T1");
        AbstractPlanNode n = pn.getChild(0).getChild(0);
        for (int ii = 1; ii <= 7; ii++) {
            if (ii == 6) {
                assertTrue(((SeqScanPlanNode)n.getChild(0)).getTargetTableName().endsWith(Integer.toString(ii))
                        || ((SeqScanPlanNode)n.getChild(0)).getTargetTableName().endsWith(Integer.toString(ii + 1)));
                assertTrue(((SeqScanPlanNode)n.getChild(1)).getTargetTableName().endsWith(Integer.toString(ii))
                        || ((SeqScanPlanNode)n.getChild(1)).getTargetTableName().endsWith(Integer.toString(ii + 1)));
                break;
            } else {
                NestLoopPlanNode node = (NestLoopPlanNode)n;
                assertTrue(((SeqScanPlanNode)n.getChild(1)).getTargetTableName().endsWith(Integer.toString(ii)));
                n = node.getChild(0);
            }
        }

        pn = compileWithJoinOrder("select * FROM T1, T2, T3, T4, T5, T6, T7", "T1,T2,T3,T4,T5,T6,T7");
        n = pn.getChild(0).getChild(0);
        for (int ii = 7; ii > 0; ii--) {
            if (ii == 2) {
                assertTrue(((SeqScanPlanNode)n.getChild(0)).getTargetTableName().endsWith(Integer.toString(ii))
                        || ((SeqScanPlanNode)n.getChild(0)).getTargetTableName().endsWith(Integer.toString(ii - 1)));
                assertTrue(((SeqScanPlanNode)n.getChild(1)).getTargetTableName().endsWith(Integer.toString(ii))
                        || ((SeqScanPlanNode)n.getChild(1)).getTargetTableName().endsWith(Integer.toString(ii - 1)));
                break;
            } else {
                NestLoopPlanNode node = (NestLoopPlanNode)n;
                assertTrue(((SeqScanPlanNode)n.getChild(1)).getTargetTableName().endsWith(Integer.toString(ii)));
                n = node.getChild(0);
            }
        }
    }

    public void testOuterJoinOrder() {
        AbstractPlanNode pn = compileWithJoinOrder("select * FROM T1 LEFT JOIN T2 ON T1.A = T2.B", "T1, T2");
        AbstractPlanNode n = pn.getChild(0).getChild(0);
        assertTrue(((SeqScanPlanNode)n.getChild(0)).getTargetTableName().equals("T1"));
        assertTrue(((SeqScanPlanNode)n.getChild(1)).getTargetTableName().equals("T2"));

        try {
            compileWithInvalidJoinOrder("select * FROM T1 LEFT JOIN T2 ON T1.A = T2.B", "T2, T1");
            fail();
        } catch (Exception ex) {
            assertTrue("The specified join order is invalid for the given query".equals(ex.getMessage()));
        }
    }

    public void testMicroOptimizationJoinOrder() {
        AbstractPlanNode pn = compileWithJoinOrder("select * from J1, P2 where A=B", "J1, P2");
        AbstractPlanNode n = pn.getChild(0).getChild(0);
        assertTrue(((IndexScanPlanNode)n.getChild(0)).getTargetTableName().equals("J1"));
        assertTrue(((SeqScanPlanNode)n.getChild(1)).getTargetTableName().equals("P2"));
    }

    public void testInnerOuterJoinOrder() {
//      @TODO ENG_3038 Commented out until 2 table restriction for outer joins is there
        AbstractPlanNode pn = compileWithJoinOrder(
                "select * FROM T1, T2, T3 LEFT JOIN T4 ON T3.C = T4.D LEFT JOIN T5 ON T3.C = T5.E, T6,T7",
                "T2, T1, T3, T4, T5, T7, T6");
        AbstractPlanNode n = pn.getChild(0).getChild(0);
        String joinOrder[] = {"T2", "T1", "T3", "T4", "T5", "T7", "T6"};
        for (int i = 6; i > 0; i--) {
            assertTrue(n instanceof NestLoopPlanNode);
            assertTrue(n.getChild(1) instanceof SeqScanPlanNode);
            SeqScanPlanNode s = (SeqScanPlanNode) n.getChild(1);
            if (i == 1) {
                assertTrue(n.getChild(0) instanceof SeqScanPlanNode);
                assertTrue(joinOrder[i-1].equals(((SeqScanPlanNode) n.getChild(0)).getTargetTableName()));
            } else {
                assertTrue(n.getChild(0) instanceof NestLoopPlanNode);
                n = n.getChild(0);
            }
            assertTrue(joinOrder[i].equals(s.getTargetTableName()));
        }

    }

    @Override
    protected void setUp() throws Exception {
        setupSchema(TestJoinOrder.class.getResource("testjoinorder-ddl.sql"), "testjoinorder", true);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }
}
