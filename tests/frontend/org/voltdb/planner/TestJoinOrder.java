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


public class TestJoinOrder extends PlannerTestCase {
    /*public void testBasicJoinOrder() {
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
                assertTrue(((SeqScanPlanNode)n.getChild(0)).getTargetTableName().endsWith(Integer.toString(ii)));
                n = node.getChild(1);
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
                assertTrue(((SeqScanPlanNode)n.getChild(0)).getTargetTableName().endsWith(Integer.toString(ii)));
                n = node.getChild(1);
            }
        }
    }*/

    public void testENG4671() {
        String sql = "SELECT " +
                "  c1.name2," +
                "  c1.name1," +
                "  c2.ts," +
                "  count(*)," +
                "  sum(c2.value * c3.factor) " +
                "FROM " +
                "  c2, c4, c5, c1, c3 " +
                "WHERE " +
                "  c3.b = ? AND" +
                "  c2.ts BETWEEN ? AND ? AND" +
                "  c1.y = ? AND" +
                "  c4.f = ? AND" +
                "  c4.partitionkey = c5.partitionkey AND " +
                "  c2.partitionkey = c5.partitionkey AND " +
                "  c2.e = c1.e AND " +
                "  c2.a = c3.a AND" +
                "  c3.ts = c2.ts AND" +
                "  c3.g = c5.g " +
                "GROUP BY " +
                "  c1.name2," +
                "  c1.name1," +
                "  c2.ts;";

        AbstractPlanNode root = compile(sql, 5);
        System.out.println(root.toExplainPlanString());
    }

    @Override
    protected void setUp() throws Exception {
        setupSchema(TestJoinOrder.class.getResource("testjoinorder-ddl.sql"), "testjoinorder", true);
        forceReplication();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }
}
