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

import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.NestLoopPlanNode;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.plannodes.UnionPlanNode;

public class TestUnion extends PlannerTestCase {

    public void testUnion() {
        AbstractPlanNode pn = compile("select A from T1 UNION select B from T2 UNION select C from T3");
        assertTrue(pn.getChild(0) instanceof UnionPlanNode);
        UnionPlanNode unionPN = (UnionPlanNode) pn.getChild(0);
        assertTrue(unionPN.getUnionType() == ParsedUnionStmt.UnionType.UNION);
        assertTrue(unionPN.getChildCount() == 3);

        pn = compile("(select A from T1 UNION select B from T2) UNION select C from T3");
        unionPN = (UnionPlanNode) pn.getChild(0);
        assertTrue(unionPN.getUnionType() == ParsedUnionStmt.UnionType.UNION);
        assertTrue(unionPN.getChildCount() == 3);

        pn = compile("select A from T1 UNION (select B from T2 UNION select C from T3)");
        unionPN = (UnionPlanNode) pn.getChild(0);
        assertTrue(unionPN.getUnionType() == ParsedUnionStmt.UnionType.UNION);
        assertTrue(unionPN.getChildCount() == 3);
   }

    public void testPartitioningMixes() {
        // Sides are identically single-partitioned.
        AbstractPlanNode pn = compile("select DESC from T1 WHERE A = 1 UNION select TEXT from T5 WHERE E = 1");
        assertTrue(pn.getChild(0) instanceof UnionPlanNode);
        UnionPlanNode unionPN = (UnionPlanNode) pn.getChild(0);
        assertTrue(unionPN.getUnionType() == ParsedUnionStmt.UnionType.UNION);
        assertTrue(unionPN.getChildCount() == 2);

        // In the future, new capabilities like "pushdown of set ops into the collector fragment" and
        // "designation of coordinator execution sites for multi-partition (multi-fragment) plans"
        // may allow more liberal mixes of selects on partitioned tables.
    }

    public void testUnionAll() {
        AbstractPlanNode pn = compile("select A from T1 UNION ALL select B from T2");
        assertTrue(pn.getChild(0) instanceof UnionPlanNode);
        UnionPlanNode unionPN = (UnionPlanNode) pn.getChild(0);
        assertTrue(unionPN.getUnionType() == ParsedUnionStmt.UnionType.UNION_ALL);
        assertTrue(unionPN.getChildCount() == 2);
    }

    public void testExcept() {
        AbstractPlanNode pn = compile("select A from T1 EXCEPT select B from T2 EXCEPT select C from T3 EXCEPT select F from T6");
        assertTrue(pn.getChild(0) instanceof UnionPlanNode);
        UnionPlanNode unionPN = (UnionPlanNode) pn.getChild(0);
        assertTrue(unionPN.getUnionType() == ParsedUnionStmt.UnionType.EXCEPT);
        assertTrue(unionPN.getChildCount() == 4);

        pn = compile("select A from T1 EXCEPT (select B from T2 EXCEPT select C from T3) EXCEPT select F from T6");
        unionPN = (UnionPlanNode) pn.getChild(0);
        assertTrue(unionPN.getUnionType() == ParsedUnionStmt.UnionType.EXCEPT);
        assertTrue(unionPN.getChildCount() == 3);
        UnionPlanNode childPN = (UnionPlanNode) unionPN.getChild(1);
        assertTrue(childPN.getUnionType() == ParsedUnionStmt.UnionType.EXCEPT);
        assertTrue(childPN.getChildCount() == 2);
    }

    public void testExceptAll() {
        AbstractPlanNode pn = compile("select A from T1 EXCEPT ALL select B from T2");
        assertTrue(pn.getChild(0) instanceof UnionPlanNode);
        UnionPlanNode unionPN = (UnionPlanNode) pn.getChild(0);
        assertTrue(unionPN.getUnionType() == ParsedUnionStmt.UnionType.EXCEPT_ALL);
        assertTrue(unionPN.getChildCount() == 2);

        pn = compile("select A from T1 EXCEPT ALL (select B from T2 EXCEPT ALL select C from T3) EXCEPT ALL select F from T6");
        unionPN = (UnionPlanNode) pn.getChild(0);
        assertTrue(unionPN.getUnionType() == ParsedUnionStmt.UnionType.EXCEPT_ALL);
        assertTrue(unionPN.getChildCount() == 3);
        UnionPlanNode childPN = (UnionPlanNode) unionPN.getChild(1);
        assertTrue(childPN.getUnionType() == ParsedUnionStmt.UnionType.EXCEPT_ALL);
        assertTrue(childPN.getChildCount() == 2);
    }

    public void testIntersect() {
        AbstractPlanNode pn = compile("select A from T1 INTERSECT select B from T2 INTERSECT select C from T3");
        assertTrue(pn.getChild(0) instanceof UnionPlanNode);
        UnionPlanNode unionPN = (UnionPlanNode) pn.getChild(0);
        assertTrue(unionPN.getUnionType() == ParsedUnionStmt.UnionType.INTERSECT);
        assertTrue(unionPN.getChildCount() == 3);

        pn = compile("(select A from T1 INTERSECT select B from T2) INTERSECT select C from T3");
        assertTrue(pn.getChild(0) instanceof UnionPlanNode);
        unionPN = (UnionPlanNode) pn.getChild(0);
        assertTrue(unionPN.getUnionType() == ParsedUnionStmt.UnionType.INTERSECT);
        assertTrue(unionPN.getChildCount() == 3);

        pn = compile("select A from T1 INTERSECT (select B from T2 INTERSECT select C from T3)");
        assertTrue(pn.getChild(0) instanceof UnionPlanNode);
        unionPN = (UnionPlanNode) pn.getChild(0);
        assertTrue(unionPN.getUnionType() == ParsedUnionStmt.UnionType.INTERSECT);
        assertTrue(unionPN.getChildCount() == 3);
    }

    public void testIntersectAll() {
        AbstractPlanNode pn = compile("select A from T1 INTERSECT ALL select B from T2");
        assertTrue(pn.getChild(0) instanceof UnionPlanNode);
        UnionPlanNode unionPN = (UnionPlanNode) pn.getChild(0);
        assertTrue(unionPN.getUnionType() == ParsedUnionStmt.UnionType.INTERSECT_ALL);
        assertTrue(unionPN.getChildCount() == 2);
    }

    public void testMultipleSetOperations() {
        AbstractPlanNode pn = compile("select A from T1 UNION select B from T2 EXCEPT select C from T3");
        assertTrue(pn.getChild(0) instanceof UnionPlanNode);
        UnionPlanNode unionPN1 = (UnionPlanNode) pn.getChild(0);
        assertTrue(unionPN1.getUnionType() == ParsedUnionStmt.UnionType.EXCEPT);
        assertTrue(unionPN1.getChildCount() == 2);
        assertTrue(unionPN1.getChild(0) instanceof UnionPlanNode);
        UnionPlanNode unionPN2 = (UnionPlanNode) unionPN1.getChild(0);
        assertTrue(unionPN2.getUnionType() == ParsedUnionStmt.UnionType.UNION);
        assertTrue(unionPN2.getChildCount() == 2);
        assertTrue(unionPN1.getChild(1) instanceof SeqScanPlanNode);
    }

    public void testNonSupportedUnions()
    {
        // If both sides are multi-partitioned, there is no facility for pushing down the
        // union processing below the send/receive, so each child of the union requires
        // its own send/receive so the plan ends up as an unsupported 3-fragment plan.
        failToCompile("select DESC from T1 UNION select TEXT from T5");
        failToCompile("select A from T1 UNION select D from T4");

        // Query hangs from SQL coverage
        failToCompile("select A from T1 UNION select A from T1 INTERSECT select B from T2");

        // If ONE side is single-partitioned, it would theoretically be possible to satisfy
        // the query with a 2-fragment plan IFF the coordinator fragment could be forced to
        // execute on the designated single partition.
        // At this point, coordinator designation is only supported for single-fragment plans.
        // So, this case must also error out.
        failToCompile("select DESC from T1 WHERE A = 1 UNION select TEXT from T5");

        // If BOTH sides are single-partitioned, but for different partitions,
        // it would theoretically be possible to satisfy
        // the query with a 2-fragment plan IFF the coordinator fragment could be forced to
        // execute on one of the designated single partitions.
        // At this point, coordinator designation is only supported for single-fragment plans.
        failToCompile("select DESC from T1 WHERE A = 1 UNION select TEXT from T5 WHERE E = 2");

        // If both sides are multi-partitioned, there is no facility for pushing down the
        // union processing below the send/receive, so each child of the union requires
        // its own send/receive so the plan ends up as an unsupported 3-fragment plan.
        failToCompile("select DESC from T1 UNION select TEXT from T5");

        // If ONE side is single-partitioned, it would theoretically be possible to satisfy
        // the query with a 2-fragment plan IFF the coordinator fragment could be forced to
        // execute on the designated single partition.
        // At this point, coordinator designation is only supported for single-fragment plans.
        // So, this case must also error out.
        failToCompile("select DESC from T1 WHERE A = 1 UNION select TEXT from T5");

        // If BOTH sides are single-partitioned, but for different partitions,
        // it would theoretically be possible to satisfy
        // the query with a 2-fragment plan IFF the coordinator fragment could be forced to
        // execute on one of the designated single partitions.
        // At this point, coordinator designation is only supported for single-fragment plans.
        failToCompile("select DESC from T1 WHERE A = 1 UNION select TEXT from T5 WHERE E = 2");

        // Column types must match.
        failToCompile("select A, DESC from T1 UNION select B from T2");
        failToCompile("select B from T2 EXCEPT select A, DESC from T1");
        failToCompile("select B from T2 EXCEPT select F from T1");

        // nonsense syntax in place of union ops (trying various internal symbol names meaning n/a)
        failToCompile("select A from T1 NOUNION select B from T2");
        failToCompile("select A from T1 TERM select B from T2");
    }

    public void testSelfUnion() {
        AbstractPlanNode pn = compile("select B from T2 UNION select B from T2");
        assertTrue(pn.getChild(0) instanceof UnionPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn.getChildCount() == 2);
        assertTrue(pn.getChild(0) instanceof SeqScanPlanNode);
        assertTrue(pn.getChild(1) instanceof SeqScanPlanNode);

        // The same table/alias is repeated twice in the union but in the different selects
        pn = compile("select B from T2 A1, T2 A2 WHERE A1.B = A2.B UNION select B from T2 A1");
        assertTrue(pn.getChild(0) instanceof UnionPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn.getChildCount() == 2);
        assertTrue(pn.getChild(0) instanceof ProjectionPlanNode);
        assertTrue(pn.getChild(0).getChild(0) instanceof NestLoopPlanNode);
        assertTrue(pn.getChild(1) instanceof SeqScanPlanNode);

        // BOTH sides are single-partitioned  for the same partition
        pn = compile("select F from T1 WHERE T1.A = 2 UNION select F from T1 WHERE T1.A = 2");
        assertTrue(pn.getChild(0) instanceof UnionPlanNode);

        // If BOTH sides are single-partitioned, but for different partitions,
        // it would theoretically be possible to satisfy
        // the query with a 2-fragment plan IFF the coordinator fragment could be forced to
        // execute on one of the designated single partitions.
        // At this point, coordinator designation is only supported for single-fragment plans.
        failToCompile("select DESC from T1 WHERE A = 1 UNION select DESC from T1 WHERE A = 2");

        // If both sides are multi-partitioned, there is no facility for pushing down the
        // union processing below the send/receive, so each child of the union requires
        // its own send/receive so the plan ends up as an unsupported 3-fragment plan.
        failToCompile("select DESC from T1 UNION select DESC from T1");
    }

    @Override
    protected void setUp() throws Exception {
        setupSchema(TestUnion.class.getResource("testunion-ddl.sql"), "testunion", false);
    }
}
