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
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.LimitPlanNode;
import org.voltdb.plannodes.NestLoopPlanNode;
import org.voltdb.plannodes.OrderByPlanNode;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.plannodes.ReceivePlanNode;
import org.voltdb.plannodes.SchemaColumn;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.plannodes.SetOpPlanNode;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.PlanNodeType;
import org.voltdb.types.SetOpType;

public class TestUnion extends PlannerTestCase {

    public void testUnion() {
        AbstractPlanNode pn = compile("select A from T1 UNION select B from T2 UNION select C from T3");
        assertTrue(pn.getChild(0) instanceof SetOpPlanNode);
        SetOpPlanNode unionPN = (SetOpPlanNode) pn.getChild(0);
        assertTrue(unionPN.getSetOpType() == SetOpType.UNION);
        assertTrue(unionPN.getChildCount() == 3);

        pn = compile("(select A from T1 UNION select B from T2) UNION select C from T3");
        unionPN = (SetOpPlanNode) pn.getChild(0);
        assertTrue(unionPN.getSetOpType() == SetOpType.UNION);
        assertTrue(unionPN.getChildCount() == 3);

        pn = compile("select A from T1 UNION (select B from T2 UNION select C from T3)");
        unionPN = (SetOpPlanNode) pn.getChild(0);
        assertTrue(unionPN.getSetOpType() == SetOpType.UNION);
        assertTrue(unionPN.getChildCount() == 3);
   }

    public void testPartitioningMixes() {
        // Sides are identically single-partitioned.
        AbstractPlanNode pn = compile("select DESC from T1 WHERE A = 1 UNION select TEXT from T5 WHERE E = 1");
        assertTrue(pn.getChild(0) instanceof SetOpPlanNode);
        SetOpPlanNode unionPN = (SetOpPlanNode) pn.getChild(0);
        assertTrue(unionPN.getSetOpType() == SetOpType.UNION);
        assertTrue(unionPN.getChildCount() == 2);

        // In the future, new capabilities like "pushdown of set ops into the collector fragment" and
        // "designation of coordinator execution sites for multi-partition (multi-fragment) plans"
        // may allow more liberal mixes of selects on partitioned tables.
    }

    public void testUnionAll() {
        AbstractPlanNode pn = compile("select A from T1 UNION ALL select B from T2");
        assertTrue(pn.getChild(0) instanceof SetOpPlanNode);
        SetOpPlanNode unionPN = (SetOpPlanNode) pn.getChild(0);
        assertTrue(unionPN.getSetOpType() == SetOpType.UNION_ALL);
        assertTrue(unionPN.getChildCount() == 2);
    }

    public void testExcept() {
        AbstractPlanNode pn = compile("select A from T1 EXCEPT select B from T2 EXCEPT select C from T3 EXCEPT select F from T6");
        assertTrue(pn.getChild(0) instanceof SetOpPlanNode);
        SetOpPlanNode unionPN = (SetOpPlanNode) pn.getChild(0);
        assertTrue(unionPN.getSetOpType() == SetOpType.EXCEPT);
        assertTrue(unionPN.getChildCount() == 4);

        pn = compile("select A from T1 EXCEPT (select B from T2 EXCEPT select C from T3) EXCEPT select F from T6");
        unionPN = (SetOpPlanNode) pn.getChild(0);
        assertTrue(unionPN.getSetOpType() == SetOpType.EXCEPT);
        assertTrue(unionPN.getChildCount() == 3);
        SetOpPlanNode childPN = (SetOpPlanNode) unionPN.getChild(1);
        assertTrue(childPN.getSetOpType() == SetOpType.EXCEPT);
        assertTrue(childPN.getChildCount() == 2);
    }

    public void testExceptAll() {
        AbstractPlanNode pn = compile("select A from T1 EXCEPT ALL select B from T2");
        assertTrue(pn.getChild(0) instanceof SetOpPlanNode);
        SetOpPlanNode unionPN = (SetOpPlanNode) pn.getChild(0);
        assertTrue(unionPN.getSetOpType() == SetOpType.EXCEPT_ALL);
        assertTrue(unionPN.getChildCount() == 2);

        pn = compile("select A from T1 EXCEPT ALL (select B from T2 EXCEPT ALL select C from T3) EXCEPT ALL select F from T6");
        unionPN = (SetOpPlanNode) pn.getChild(0);
        assertTrue(unionPN.getSetOpType() == SetOpType.EXCEPT_ALL);
        assertTrue(unionPN.getChildCount() == 3);
        SetOpPlanNode childPN = (SetOpPlanNode) unionPN.getChild(1);
        assertTrue(childPN.getSetOpType() == SetOpType.EXCEPT_ALL);
        assertTrue(childPN.getChildCount() == 2);
    }

    public void testIntersect() {
        AbstractPlanNode pn = compile("select A from T1 INTERSECT select B from T2 INTERSECT select C from T3");
        assertTrue(pn.getChild(0) instanceof SetOpPlanNode);
        SetOpPlanNode unionPN = (SetOpPlanNode) pn.getChild(0);
        assertTrue(unionPN.getSetOpType() == SetOpType.INTERSECT);
        assertTrue(unionPN.getChildCount() == 3);

        pn = compile("(select A from T1 INTERSECT select B from T2) INTERSECT select C from T3");
        assertTrue(pn.getChild(0) instanceof SetOpPlanNode);
        unionPN = (SetOpPlanNode) pn.getChild(0);
        assertTrue(unionPN.getSetOpType() == SetOpType.INTERSECT);
        assertTrue(unionPN.getChildCount() == 3);

        pn = compile("select A from T1 INTERSECT (select B from T2 INTERSECT select C from T3)");
        assertTrue(pn.getChild(0) instanceof SetOpPlanNode);
        unionPN = (SetOpPlanNode) pn.getChild(0);
        assertTrue(unionPN.getSetOpType() == SetOpType.INTERSECT);
        assertTrue(unionPN.getChildCount() == 3);
    }

    public void testIntersectAll() {
        AbstractPlanNode pn = compile("select A from T1 INTERSECT ALL select B from T2");
        assertTrue(pn.getChild(0) instanceof SetOpPlanNode);
        SetOpPlanNode unionPN = (SetOpPlanNode) pn.getChild(0);
        assertTrue(unionPN.getSetOpType() == SetOpType.INTERSECT_ALL);
        assertTrue(unionPN.getChildCount() == 2);
    }

    public void testMultipleSetOperations() {
        AbstractPlanNode pn = compile("select A from T1 UNION select B from T2 EXCEPT select C from T3");
        assertTrue(pn.getChild(0) instanceof SetOpPlanNode);
        SetOpPlanNode unionPN1 = (SetOpPlanNode) pn.getChild(0);
        assertTrue(unionPN1.getSetOpType() == SetOpType.EXCEPT);
        assertTrue(unionPN1.getChildCount() == 2);
        assertTrue(unionPN1.getChild(0) instanceof SetOpPlanNode);
        SetOpPlanNode unionPN2 = (SetOpPlanNode) unionPN1.getChild(0);
        assertTrue(unionPN2.getSetOpType() == SetOpType.UNION);
        assertTrue(unionPN2.getChildCount() == 2);
        assertTrue(unionPN1.getChild(1) instanceof SeqScanPlanNode);
    }

    public void testNonSupportedUnions()
    {
        // If both sides are multi-partitioned, there is no facility for pushing down the
        // union processing below the send/receive, so each child of the union requires
        // its own send/receive so the plan ends up as an unsupported 3-fragment plan.
        failToCompile("select DESC from T1 UNION select TEXT from T5");
//        failToCompile("select A from T1 UNION select D from T4");

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

        // Multiple Set operations in a single statement with multiple partitioned tables
        failToCompile("select F from T1 UNION select G from T6 INTERSECT select F from T1");

        // Column types must match.
        failToCompile("select A, DESC from T1 UNION select B from T2");
        failToCompile("select B from T2 EXCEPT select A, DESC from T1");
        failToCompile("select B from T2 EXCEPT select F from T1");

        // nonsense syntax in place of union ops (trying various internal symbol names meaning n/a)
        failToCompile("select A from T1 NOUNION select B from T2");
        failToCompile("select A from T1 TERM select B from T2");

        // 1. one SP and two compatible MP - fail
        // one SP plus MP select with Partkey = 4 - fail
    }

    public void testSelfUnion() {
        AbstractPlanNode pn = compile("select B from T2 UNION select B from T2");
        assertTrue(pn.getChild(0) instanceof SetOpPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn.getChildCount() == 2);
        assertTrue(pn.getChild(0) instanceof SeqScanPlanNode);
        assertTrue(pn.getChild(1) instanceof SeqScanPlanNode);

        // The same table/alias is repeated twice in the union but in the different selects
        pn = compile("select A1.B from T2 A1, T2 A2 WHERE A1.B = A2.B UNION select B from T2 A1");
        assertTrue(pn.getChild(0) instanceof SetOpPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn.getChildCount() == 2);
        assertTrue(pn.getChild(0) instanceof ProjectionPlanNode);
        assertTrue(pn.getChild(0).getChild(0) instanceof NestLoopPlanNode);
        assertTrue(pn.getChild(1) instanceof SeqScanPlanNode);

        // BOTH sides are single-partitioned  for the same partition
        pn = compile("select F from T1 WHERE T1.A = 2 UNION select F from T1 WHERE T1.A = 2");
        assertTrue(pn.getChild(0) instanceof SetOpPlanNode);

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

    public void testSubqueryUnionWithParamENG7783() {
        AbstractPlanNode pn = compile(
                "SELECT B, ABS( B - ? ) AS distance FROM ( " +
                "( SELECT B FROM T2 WHERE B >=? ORDER BY B LIMIT ? " +
                ") UNION ALL ( " +
                "SELECT B FROM T2 WHERE B < ? ORDER BY B DESC LIMIT ? ) " +
                ") AS n ORDER BY distance LIMIT ?;"
                );
        assertTrue(pn.getChild(0) instanceof ProjectionPlanNode);
        assertTrue(pn.getChild(0).getChild(0) instanceof OrderByPlanNode);
        assertTrue(pn.getChild(0).getChild(0).getChild(0) instanceof SeqScanPlanNode);
        assertTrue(pn.getChild(0).getChild(0).getChild(0).getChild(0) instanceof SetOpPlanNode);

    }

    public void testUnionLimitOffset() {
        {
            AbstractPlanNode pn = compile(
                    "select C from T3 UNION select B from T2 limit 3 offset 2");
            checkLimitNode(pn.getChild(0), 3, 2);
            assertTrue(pn.getChild(0).getChild(0) instanceof SetOpPlanNode);
        }
        {
            AbstractPlanNode pn = compile(
                    "select C from T3 UNION (select B from T2 limit 3 offset 2) ");
            assertTrue(pn.getChild(0) instanceof SetOpPlanNode);
        }
        {
            AbstractPlanNode pn = compile(
                    "select C from T3 INTERSECT select B from T2 limit 3");
            checkLimitNode(pn.getChild(0), 3, 0);
            assertTrue(pn.getChild(0).getChild(0) instanceof SetOpPlanNode);
        }
        {
            AbstractPlanNode pn = compile(
                    "select C from T3 EXCEPT select B from T2 offset 2");
            checkLimitNode(pn.getChild(0), -1, 2);
            assertTrue(pn.getChild(0).getChild(0) instanceof SetOpPlanNode);
        }
        {
            AbstractPlanNode pn = compile(
                    "(select C from T3 EXCEPT select B from T2 offset 2) UNION select F from T6 limit 4 offset 5");
            checkLimitNode(pn.getChild(0), 4, 5);
            assertTrue(pn.getChild(0).getChild(0) instanceof SetOpPlanNode);
            SetOpPlanNode upn = (SetOpPlanNode) pn.getChild(0).getChild(0);
            checkLimitNode(upn.getChild(0), -1, 2);
            assertTrue(upn.getChild(0).getChild(0) instanceof SetOpPlanNode);
        }
        {
            // T1 is partitioned
            AbstractPlanNode pn = compile(
                    "select A from T1 EXCEPT select B from T2 offset 2");
            checkLimitNode(pn.getChild(0), -1, 2);
            assertTrue(pn.getChild(0).getChild(0) instanceof SetOpPlanNode);
        }
    }

    public void testUnionOrderby() {
        {
            AbstractPlanNode pn = compile("select B from T2 UNION select B from T2 order by B");
            pn = pn.getChild(0);
            String[] columnNames = {"B"};
            int[] idxs = {0};
            checkOrderByNode(pn, columnNames, idxs);
        }
        {
            AbstractPlanNode pn = compile("(select B as B1, B as B2 from T2 UNION select B as B1, B as B2 from T2) order by B1 asc, B2 desc");
            pn = pn.getChild(0);
            String[] columnNames = {"B1", "B2"};
            // We are selecting the same column twice from both sides of the union,
            // so it doesn't matter if the column indices are 0 or 1 here.
            int[] idxs = {0, 0};
            checkOrderByNode(pn, columnNames, idxs);
        }
        {
            AbstractPlanNode pn = compile("(select B as B1, B * -1 as B2 from T2 UNION select B as B1, B * -1 as B2 from T2) order by B1 asc, B2 desc");
            pn = pn.getChild(0);
            String[] columnNames = {"B1", "B2"};
            int[] idxs = {0, 1};
            checkOrderByNode(pn, columnNames, idxs);
        }
        {
            // T1 is partitioned
            AbstractPlanNode pn = compile("(select A from T1 UNION select B from T2) order by A");
            pn = pn.getChild(0);
            String[] columnNames = {"A"};
            int[] idxs = {0};
            checkOrderByNode(pn, columnNames, idxs);
        }
    }

    private boolean stmtIsDeterministic(String stmt) {
        CompiledPlan plan = compileAdHocPlan(stmt);
        return plan.isOrderDeterministic();
    }

    private void assertIsDeterministic(String stmt) {
        assertTrue("Expected stmt\n"
                + "   " + stmt + "\n"
                + "to be deterministic, but it was not.", stmtIsDeterministic(stmt));
    }

    private void assertIsNonDeterministic(String stmt) {
        assertFalse("Expected stmt\n"
                + "    " + stmt + "\n"
                + "to be non-deterministic, but it was."
                ,stmtIsDeterministic(stmt));
    }

    public void testUnionDeterminism() {

        // Not deterministic because no ordering on either statement.
        assertIsNonDeterministic("select B, DESC from T2 UNION select A, DESC from T1");

        // Not deterministic because ordering by just one column is not sufficient.
        assertIsNonDeterministic("(select B, DESC from T2 UNION select A, DESC from T1) order by B asc");

        // Ordering by all columns should be deterministic.
        assertIsDeterministic("(select B, DESC from T2 UNION select A, DESC from T1) order by B asc, DESC desc");

        // Should not be deterministic:
        //   Ordering by (a, b) makes a deterministic order on LHS, but
        //   RHS cannot be said to be deterministic
        assertIsNonDeterministic("(select a, b, c from t7 union  select a, b, c from t8) order by a, b");

        // This is deterministic: primary key on T7 (a, b) makes both sides of union deterministic.
        assertIsDeterministic("((select a, b, c from t7 order by a, b) union (select a, b, c from t7 order by a, b))");

        // As above, but add a non-deterministic sort to the top of the plan: no longer deterministic.
        assertIsNonDeterministic("((select a, b, c from t7 order by a, b) union (select a, b, c from t7 order by a, b)) order by a");

        // This is deterministic since the primary key on T7 (a, b) defines order on both sides,
        // And both sides are identical.  But our planner is not yet smart enough to figure this out.
        assertIsNonDeterministic("((select a, b, c from t7) union (select a, b, c from t7)) order by a, b");

        // This is query is correctly marked as non-deterministic even though there is a PK on
        // both sides that we are ordering by, because the third item on the select list is different.
        assertIsNonDeterministic("((select a, b, cast(c as bigint) from t7) union (select a, b, c + 1 from t7)) order by a, b");
    }

    public void testOtherSetOpDeterminism()
    {
        // Output of non-union set ops is considered to be non-deterministic,
        // since they use boost unordered containers in the EE.
        // This is true even if sub-selects are sorted.

        assertIsNonDeterministic("(select a from t1 order by a) intersect select b from t2");
        assertIsNonDeterministic("(select a from t1 order by a) intersect all select b from t2");
        assertIsNonDeterministic("(select a from t1 order by a) except select b from t2");
        assertIsNonDeterministic("(select a from t1 order by a) except all select b from t2");

        // A statement-level order by clause will the above statements deterministic.
        assertIsDeterministic("(select a from t1 intersect     select b from t2) order by a");
        assertIsDeterministic("(select a from t1 intersect all select b from t2) order by a");
        assertIsDeterministic("(select a from t1 except        select b from t2) order by a");
        assertIsDeterministic("(select a from t1 except all    select b from t2) order by a");

        // More examples composing the various set operators.

        // union on LHS of intersect
        assertIsNonDeterministic("((select a from t1 order by a) union (select b from t2 order by b)) "
                + "intersect select b from t2");

        assertIsDeterministic("(((select a from t1) union (select b from t2 order by b)) "
                + "intersect select b from t2) order by a");

        // Not deterministic, because outer ORDER BY does not make LHS of interestect
        // (the UNION) deterministic.
        assertIsNonDeterministic("(((select a, desc from t1) union (select b, desc from t2 order by b)) "
                + "intersect select b, desc from t2) order by a");

        // intersect on LHS of union
        // Not deterministic because LHS of union is not determinstic.
        assertIsNonDeterministic("((select a from t1) intersect (select b from t2)) "
                + "union (select b from t2 order by b)");

        // Deterministic because both sides of union are deterministic.
        assertIsDeterministic("((select a from t1) intersect (select b from t2) order by a) "
                + "union (select b from t2 order by b)");
    }

    public void testInvalidOrderBy() {
        String errorMsg = "invalid ORDER BY expression";
        // hsqldb 1.9 parser does not like ORDER BY expression operating on output columns
        failToCompile("select C+1, C as C2 from T3 UNION select B,B from T2 order by C+1", errorMsg);
        // Column B is not avaiable
        failToCompile("(select C from T3 UNION select B from T2) order by B", errorMsg);
        // ORDER BY is not at the end of the UNION SQL clause
        failToCompile("select C from T3 UNION select A from T1 order by A UNION select B from T2", errorMsg);

        // ORDER BY in the union has to be at the last part of the query
        failToCompile("select C from T3 UNION select A from T1 order by C UNION select B from T2", "unexpected token: UNION");

        // C is not available in ORDER BY clause "abs(C)"
        failToCompile("select abs(C) as tag, C as C2 from T3 UNION select B,B from T2 order by abs(C)", errorMsg);

        // hsqldb 1.9 parser does not like ORDER BY expression operating on output columns
        failToCompile("select C from T3 UNION select B from T2 order by C+1", errorMsg);

        // ORDER BY expression
        // voltdb has exception for type match on the output columns. expression that may change its type
        failToCompile("select C+1, C as C2 from T3 UNION select B,B from T2 order by 1", "Incompatible data types in UNION");
    }

    public void testMultiUnionOrderby() {
      {
          AbstractPlanNode pn = compile("select A from T1 union ((select B from T2 UNION select B from T2) order by B)");
          pn = pn.getChild(0);
          assertTrue(pn instanceof SetOpPlanNode);
          assertEquals(2, pn.getChildCount());
          // Left branch - SELECT FRM T1
          assertTrue(pn.getChild(0).getChild(0) instanceof ReceivePlanNode);
          // Right branch - union with order by
          assertTrue(pn.getChild(1) instanceof OrderByPlanNode);
          pn = pn.getChild(1);
          assertTrue(pn.getChild(0) instanceof SetOpPlanNode);
      }
      {
          AbstractPlanNode pn = compile("select A from T1 union (select B from T2 UNION select B from T2 limit 3)");
          pn = pn.getChild(0);
          assertTrue(pn instanceof SetOpPlanNode);
          assertEquals(2, pn.getChildCount());
          // Left branch - SELECT FRM T1
          assertTrue(pn.getChild(0).getChild(0) instanceof ReceivePlanNode);
          // Right branch - union with limit
          assertTrue(pn.getChild(1) instanceof LimitPlanNode);
          pn = pn.getChild(1);
          assertTrue(pn.getChild(0) instanceof SetOpPlanNode);
      }
      {
          AbstractPlanNode pn = compile("select A from T1 union (select B from T2 UNION select B from T2 offset 3)");
          pn = pn.getChild(0);
          assertTrue(pn instanceof SetOpPlanNode);
          assertEquals(2, pn.getChildCount());
          // Left branch - SELECT FRM T1
          assertTrue(pn.getChild(0).getChild(0) instanceof ReceivePlanNode);
          // Right branch - union with limit
          assertTrue(pn.getChild(1) instanceof LimitPlanNode);
          pn = pn.getChild(1);
          assertTrue(pn.getChild(0) instanceof SetOpPlanNode);
      }
      {
          AbstractPlanNode pn = compile("(select A from T1 union select B from T2 order by A) UNION select B from T2");
          pn = pn.getChild(0);
          assertTrue(pn instanceof SetOpPlanNode);
          assertEquals(2, pn.getChildCount());
          // Left branch - union with order by
          assertTrue(pn.getChild(0) instanceof OrderByPlanNode);
          assertTrue(pn.getChild(0).getChild(0) instanceof SetOpPlanNode);
          // Right branch - select from T2
          assertTrue(pn.getChild(1) instanceof SeqScanPlanNode);
      }
      {
          AbstractPlanNode pn = compile("(select A from T1 union select B from T2 offset 1) UNION select B from T2");
          pn = pn.getChild(0);
          assertTrue(pn instanceof SetOpPlanNode);
          assertEquals(2, pn.getChildCount());
          // Left branch - union with offset
          assertTrue(pn.getChild(0) instanceof LimitPlanNode);
          assertTrue(pn.getChild(0).getChild(0) instanceof SetOpPlanNode);
          // Right branch - select from T2
          assertTrue(pn.getChild(1) instanceof SeqScanPlanNode);
      }
      {
          AbstractPlanNode pn = compile("(select A from T1 union select B from T2 limit 1) UNION select B from T2");
          pn = pn.getChild(0);
          assertTrue(pn instanceof SetOpPlanNode);
          assertEquals(2, pn.getChildCount());
          // Left branch - union with offset
          assertTrue(pn.getChild(0) instanceof LimitPlanNode);
          assertTrue(pn.getChild(0).getChild(0) instanceof SetOpPlanNode);
          // Right branch - select from T2
          assertTrue(pn.getChild(1) instanceof SeqScanPlanNode);
      }
  }

    public void testUnionOrderByExpr() {
        {
            AbstractPlanNode pn = compile(
                    "select C, abs(C) as A from T3 UNION select B, B from T2 order by C, A");
            pn = pn.getChild(0);
            String[] columnNames = {"C", "A"};
            int[] idxs = {0, 1};
            checkOrderByNode(pn, columnNames, idxs);
        }
        {
            AbstractPlanNode pn = compile(
                    "select C, abs(C) as A from T3 UNION select B, B from T2 order by 1,2");
            pn = pn.getChild(0);
            String[] columnNames = {"C", "A"};
            int[] colIdx = { 0, 1};
            checkOrderByNode(pn, columnNames, colIdx);
        }
        {
            AbstractPlanNode pn = compile("select abs(C) as tag, C as C2 from T3 UNION select B,B from T2 order by tag, C2");
            pn = pn.getChild(0);
            String[] columnNames = {"TAG", "C2"};
            int[] colIdx = {0, 1};
            checkOrderByNode(pn, columnNames, colIdx);
        }
        {
            AbstractPlanNode pn = compile("select cast((C+1) as integer) TAG, C as C2 from T3 UNION select B,B from T2 order by TAG");
            pn = pn.getChild(0);
            String[] columnNames = {"TAG", "C2"};
            int[] colIdx = {0, 1};
            checkOrderByNode(pn, columnNames, colIdx);
        }
    }

    public void testUnionOrderByLimit() {
        // order by column name
        {
            AbstractPlanNode pn = compile(
                    "select C from T3 UNION select B from T2 order by C limit 3 offset 2");
            String[] columnNames = {"C"};
            pn = pn.getChild(0);
            checkOrderByNode(pn, columnNames, new int[]{0});
            assertTrue(pn.getChild(0) instanceof SetOpPlanNode);
            pn = pn.getInlinePlanNode(PlanNodeType.LIMIT);
            checkLimitNode(pn, 3, 2);
        }
        // order by alias
        {
            AbstractPlanNode pn = compile(
                    "select C as TAG from T3 UNION select B from T2 order by TAG limit 3 offset 2");
            String[] columnNames = {"TAG"};
            pn = pn.getChild(0);
            checkOrderByNode(pn, columnNames, new int[]{0});
            assertTrue(pn.getChild(0) instanceof SetOpPlanNode);
            pn = pn.getInlinePlanNode(PlanNodeType.LIMIT);
            checkLimitNode(pn, 3, 2);
        }
        // order by number
        {
            AbstractPlanNode pn = compile(
                    "select C as TAG from T3 UNION select B from T2 order by 1 limit 3 offset 2");
            String[] columnNames = {"TAG"};
            pn = pn.getChild(0);
            checkOrderByNode(pn, columnNames, new int[]{0});
            assertTrue(pn.getChild(0) instanceof SetOpPlanNode);
            pn = pn.getInlinePlanNode(PlanNodeType.LIMIT);
            checkLimitNode(pn, 3, 2);
        }
    }

    public void testUnionOrderByLimitParams() {
        AbstractPlanNode pn = compile(
                "select C from T3 where C = ? UNION select B from T2 order by C limit ? offset ?");
        String[] columnNames = {"C"};
        pn = pn.getChild(0);
        int[] idxs = {0};
        checkOrderByNode(pn, columnNames, idxs);
        assertTrue(pn.getChild(0) instanceof SetOpPlanNode);
        pn = pn.getInlinePlanNode(PlanNodeType.LIMIT);
        assert (pn instanceof LimitPlanNode);
        assertTrue(pn.toExplainPlanString().contains("LIMIT with parameter"));
    }

    public void testMultiPartitionedSetOpsPushDown() {
      List<AbstractPlanNode> pns;
      PlanNodeType[] coordinatorTypes;
      PlanNodeType[] setOpChildren;

      // Union of two distributed selects
      pns = compileToFragments("select AT9.A AA from T9 AT9 union select AT10.A A2 from T10 AT10");
      coordinatorTypes = new PlanNodeType[] {PlanNodeType.RECEIVE};
      setOpChildren = new PlanNodeType[] {PlanNodeType.INDEXSCAN, PlanNodeType.INDEXSCAN};
      checkPushedDownSetOp(pns, coordinatorTypes, SetOpType.UNION, setOpChildren);

      // EXCEPT of three distributed selects
      pns = compileToFragments("select A from T9 except select A from T10 except select A from T1");
      coordinatorTypes = new PlanNodeType[] {PlanNodeType.RECEIVE};
      setOpChildren = new PlanNodeType[] {PlanNodeType.INDEXSCAN, PlanNodeType.INDEXSCAN, PlanNodeType.SEQSCAN};
      checkPushedDownSetOp(pns, coordinatorTypes, SetOpType.EXCEPT, setOpChildren);

      // Missed optimization for Set Ops - Should be MERGERECEIVE since the partitions results are ordered by the primary key A
      pns = compileToFragments("(select A from T9 where A > 0 order by A) union (select A from T10 where A > 0 order by A) order by A");
      coordinatorTypes = new PlanNodeType[] {PlanNodeType.ORDERBY, PlanNodeType.RECEIVE};
      setOpChildren = new PlanNodeType[] {PlanNodeType.INDEXSCAN, PlanNodeType.INDEXSCAN};
      checkPushedDownSetOp(pns, coordinatorTypes, SetOpType.UNION, setOpChildren);

      // AT9.A AA2 and AT10.A A2 - partitioning column repeated twice
      pns = compileToFragments("select AT9.A AA1, AT9.A AA2 from T9 AT9 union select AT10.B AB1, AT10.A A2 from T10 AT10");
      coordinatorTypes = new PlanNodeType[] {PlanNodeType.RECEIVE};
      setOpChildren = new PlanNodeType[] {PlanNodeType.INDEXSCAN, PlanNodeType.INDEXSCAN};
      checkPushedDownSetOp(pns, coordinatorTypes, SetOpType.UNION, setOpChildren);

      // Union of two distributed joins - partitioning columns T9.A and T10.A are both in the same positions.
      pns = compileToFragments("select T9.A, T10.B from T9, T10 where T9.A = T10.A union select T10.A, T10.B from T9, T10 where T9.A = T10.A");
      coordinatorTypes = new PlanNodeType[] {PlanNodeType.RECEIVE};
      setOpChildren = new PlanNodeType[] {PlanNodeType.NESTLOOPINDEX, PlanNodeType.NESTLOOPINDEX};
      checkPushedDownSetOp(pns, coordinatorTypes, SetOpType.UNION, setOpChildren);

      // Multiple set ops with distributed tables
      pns = compileToFragments("select A from T9 union (select A from T10 except select A from T1)");
      coordinatorTypes = new PlanNodeType[] {PlanNodeType.RECEIVE};
      setOpChildren = new PlanNodeType[] {PlanNodeType.INDEXSCAN, PlanNodeType.SETOP};
      checkPushedDownSetOp(pns, coordinatorTypes, SetOpType.UNION, setOpChildren);

      // The output schema for both children is coming from the inline aggregates
      // T1.A and T4.D are partitioning columns
      pns = compileToFragments("SELECT A, COUNT(*) FROM T1 GROUP BY A UNION SELECT D, COUNT(*) FROM T4 GROUP BY D");
      coordinatorTypes = new PlanNodeType[] {PlanNodeType.RECEIVE};
      setOpChildren = new PlanNodeType[] {PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN};
      checkPushedDownSetOp(pns, coordinatorTypes, SetOpType.UNION, setOpChildren);

      // union with a distributed matview
      pns = compileToFragments("SELECT V_A, CNT FROM VT1 UNION SELECT D, COUNT(*) FROM T4 GROUP BY D");
      coordinatorTypes = new PlanNodeType[] {PlanNodeType.RECEIVE};
      setOpChildren = new PlanNodeType[] {PlanNodeType.INDEXSCAN, PlanNodeType.SEQSCAN};
      checkPushedDownSetOp(pns, coordinatorTypes, SetOpType.UNION, setOpChildren);

      // Union with a distributed join
      pns = compileToFragments("SELECT D FROM T4, T2 WHERE T4.D = T2.B UNION SELECT D FROM T4;");
      coordinatorTypes = new PlanNodeType[] {PlanNodeType.RECEIVE};
      setOpChildren = new PlanNodeType[] {PlanNodeType.NESTLOOP, PlanNodeType.SEQSCAN};
      checkPushedDownSetOp(pns, coordinatorTypes, SetOpType.UNION, setOpChildren);

      // FROM subquery - non-trivial coordinator fragment - SEQSCAN
      failToCompile("select A, B from (select T9.A, T10.B from T9, T10 where T9.A = T10.A limit 10) T union select A, B from T9",
              "Statements are too complex in set operation using multiple partitioned tables.");
      // No partioning columns in the output
      failToCompile("select T1.DESC from T1 union select T5.TEXT from T5",
              "Statements are too complex in set operation using multiple partitioned tables.");
      failToCompile("SELECT F, COUNT(*) FROM T1 GROUP BY F UNION SELECT D, COUNT(*) FROM T4 GROUP BY D",
              "Statements are too complex in set operation using multiple partitioned tables.");
      failToCompile("SELECT COUNT(*) FROM T1 UNION SELECT D FROM T4",
              "Statements are too complex in set operation using multiple partitioned tables.");
      // Partitioning columns position mismatch
      failToCompile("SELECT CNT, V_A FROM VT1 UNION SELECT D, COUNT(*) FROM T4 GROUP BY D",
              "Statements are too complex in set operation using multiple partitioned tables.");
      // Select from distributed T1 does not require a MP plan
      failToCompile("select T1.A from T1 where T1.A = 3 union select T5.E from T5",
              "Statements are too complex in set operation using multiple partitioned tables.");
      // Partitioning columns (T9.A and T10.A) position mismatch
      failToCompile("select T9.A, T9.B from T9 union select T10.B, T10.A from T10",
              "Statements are too complex in set operation using multiple partitioned tables.");
      // Union of three distributed selects. The output fo the last select doesn't have the partitioning column
      failToCompile("select A from T9 union select A from T10 union select F from T1",
              "Statements are too complex in set operation using multiple partitioned tables.");
      // Not all of the selects (T10) have partitioning column in the output
      failToCompile("select T9.A from T9 union select T10.B from T10",
              "Statements are too complex in set operation using multiple partitioned tables.");
      // Union of two distributed selects. One of them has a column expression that involved a partitioning column
      failToCompile("select AT9.A + 1 AA from T9 AT9 union select AT10.A A2 from T10 AT10",
              "Statements are too complex in set operation using multiple partitioned tables.");
    }

    private void checkPushedDownSetOp(List<AbstractPlanNode> pns, PlanNodeType[] coordinatorTypes, SetOpType setOpType, PlanNodeType[] setOpChildren) {
        assertEquals(2, pns.size());
        // Coordinator fragment
        AbstractPlanNode pn = pns.get(0);
        for (int i = 0; i < coordinatorTypes.length; ++i) {
            pn = pn.getChild(0);
            assertEquals(coordinatorTypes[i], pn.getPlanNodeType());
        }

        // Partition fragment
        pn = pns.get(1).getChild(0);
        assertEquals(PlanNodeType.SETOP, pn.getPlanNodeType());
        SetOpPlanNode setOpNode = (SetOpPlanNode) pn;
        assertEquals(setOpType, setOpNode.getSetOpType());
        assertEquals(setOpChildren.length, setOpNode.getChildCount());
        for (int i = 0; i < setOpChildren.length; ++i) {
            AbstractPlanNode childNode = setOpNode.getChild(i);
            if (childNode.getPlanNodeType() == PlanNodeType.PROJECTION) {
                childNode = childNode.getChild(0);
            }
            assertEquals(setOpChildren[i], childNode.getPlanNodeType());
        }
    }

    private void checkOrderByNode(AbstractPlanNode pn, String columns[], int[] idxs) {
        assertTrue(pn != null);
        assertTrue(pn instanceof OrderByPlanNode);
        OrderByPlanNode opn = (OrderByPlanNode) pn;
        assertEquals(columns.length, opn.getOutputSchema().size());
        for(int i = 0; i < columns.length; ++i) {
            SchemaColumn col = opn.getOutputSchema().getColumns().get(i);
            assertEquals(columns[i], col.getColumnAlias());
            AbstractExpression colExpr = col.getExpression();
            assertEquals(ExpressionType.VALUE_TUPLE, colExpr.getExpressionType());
            assertEquals(idxs[i], ((TupleValueExpression) colExpr).getColumnIndex());
        }
    }

    private void checkLimitNode(AbstractPlanNode pn, int limit, int offset) {
        assertTrue(pn instanceof LimitPlanNode);
        LimitPlanNode lpn = (LimitPlanNode) pn;
        assertEquals(limit, lpn.getLimit());
        assertEquals(offset, lpn.getOffset());
    }

    @Override
    protected void setUp() throws Exception {
        setupSchema(TestUnion.class.getResource("testplans-union-ddl.sql"), "testunion", false);
    }
}
