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
import org.voltdb.plannodes.UnionPlanNode;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.PlanNodeType;

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

        // Multiple Set operations in a single statement with multiple partitioned tables
        failToCompile("select F from T1 UNION select G from T6 INTERSECT select F from T1");

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
        assertTrue(pn.getChild(0).getChild(0).getChild(0).getChild(0) instanceof UnionPlanNode);

    }

    public void testUnionLimitOffset() {
        {
            AbstractPlanNode pn = compile(
                    "select C from T3 UNION select B from T2 limit 3 offset 2");
            checkLimitNode(pn.getChild(0), 3, 2);
            assertTrue(pn.getChild(0).getChild(0) instanceof UnionPlanNode);
        }
        {
            AbstractPlanNode pn = compile(
                    "select C from T3 UNION (select B from T2 limit 3 offset 2) ");
            assertTrue(pn.getChild(0) instanceof UnionPlanNode);
        }
        {
            AbstractPlanNode pn = compile(
                    "select C from T3 INTERSECT select B from T2 limit 3");
            checkLimitNode(pn.getChild(0), 3, 0);
            assertTrue(pn.getChild(0).getChild(0) instanceof UnionPlanNode);
        }
        {
            AbstractPlanNode pn = compile(
                    "select C from T3 EXCEPT select B from T2 offset 2");
            checkLimitNode(pn.getChild(0), -1, 2);
            assertTrue(pn.getChild(0).getChild(0) instanceof UnionPlanNode);
        }
        {
            AbstractPlanNode pn = compile(
                    "(select C from T3 EXCEPT select B from T2 offset 2) UNION select F from T6 limit 4 offset 5");
            checkLimitNode(pn.getChild(0), 4, 5);
            assertTrue(pn.getChild(0).getChild(0) instanceof UnionPlanNode);
            UnionPlanNode upn = (UnionPlanNode) pn.getChild(0).getChild(0);
            checkLimitNode(upn.getChild(0), -1, 2);
            assertTrue(upn.getChild(0).getChild(0) instanceof UnionPlanNode);
        }
        {
            // T1 is partitioned
            AbstractPlanNode pn = compile(
                    "select A from T1 EXCEPT select B from T2 offset 2");
            checkLimitNode(pn.getChild(0), -1, 2);
            assertTrue(pn.getChild(0).getChild(0) instanceof UnionPlanNode);
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
            int[] idxs = {1, 1};
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

    public void testUnionDeterminism() {
        {
            CompiledPlan plan = compileAdHocPlan("select B, DESC from T2 UNION select A, DESC from T1");
            boolean isDeterministic = plan.isOrderDeterministic();
            assertEquals(false, isDeterministic);
        }
        {
            CompiledPlan plan = compileAdHocPlan("(select B, DESC from T2 UNION select A, DESC from T1) order by B asc");
            boolean isDeterministic = plan.isOrderDeterministic();
            assertEquals(false, isDeterministic);
        }
        {
            CompiledPlan plan = compileAdHocPlan("(select B, DESC from T2 UNION select A, DESC from T1) order by B asc, DESC desc");
            boolean isDeterministic = plan.isOrderDeterministic();
            assertEquals(true, isDeterministic);
        }
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
          assertTrue(pn instanceof UnionPlanNode);
          assertEquals(2, pn.getChildCount());
          // Left branch - SELECT FRM T1
          assertTrue(pn.getChild(0).getChild(0) instanceof ReceivePlanNode);
          // Right branch - union with order by
          assertTrue(pn.getChild(1) instanceof OrderByPlanNode);
          pn = pn.getChild(1);
          assertTrue(pn.getChild(0) instanceof UnionPlanNode);
      }
      {
          AbstractPlanNode pn = compile("select A from T1 union (select B from T2 UNION select B from T2 limit 3)");
          pn = pn.getChild(0);
          assertTrue(pn instanceof UnionPlanNode);
          assertEquals(2, pn.getChildCount());
          // Left branch - SELECT FRM T1
          assertTrue(pn.getChild(0).getChild(0) instanceof ReceivePlanNode);
          // Right branch - union with limit
          assertTrue(pn.getChild(1) instanceof LimitPlanNode);
          pn = pn.getChild(1);
          assertTrue(pn.getChild(0) instanceof UnionPlanNode);
      }
      {
          AbstractPlanNode pn = compile("select A from T1 union (select B from T2 UNION select B from T2 offset 3)");
          pn = pn.getChild(0);
          assertTrue(pn instanceof UnionPlanNode);
          assertEquals(2, pn.getChildCount());
          // Left branch - SELECT FRM T1
          assertTrue(pn.getChild(0).getChild(0) instanceof ReceivePlanNode);
          // Right branch - union with limit
          assertTrue(pn.getChild(1) instanceof LimitPlanNode);
          pn = pn.getChild(1);
          assertTrue(pn.getChild(0) instanceof UnionPlanNode);
      }
      {
          AbstractPlanNode pn = compile("(select A from T1 union select B from T2 order by A) UNION select B from T2");
          pn = pn.getChild(0);
          assertTrue(pn instanceof UnionPlanNode);
          assertEquals(2, pn.getChildCount());
          // Left branch - union with order by
          assertTrue(pn.getChild(0) instanceof OrderByPlanNode);
          assertTrue(pn.getChild(0).getChild(0) instanceof UnionPlanNode);
          // Right branch - select from T2
          assertTrue(pn.getChild(1) instanceof SeqScanPlanNode);
      }
      {
          AbstractPlanNode pn = compile("(select A from T1 union select B from T2 offset 1) UNION select B from T2");
          pn = pn.getChild(0);
          assertTrue(pn instanceof UnionPlanNode);
          assertEquals(2, pn.getChildCount());
          // Left branch - union with offset
          assertTrue(pn.getChild(0) instanceof LimitPlanNode);
          assertTrue(pn.getChild(0).getChild(0) instanceof UnionPlanNode);
          // Right branch - select from T2
          assertTrue(pn.getChild(1) instanceof SeqScanPlanNode);
      }
      {
          AbstractPlanNode pn = compile("(select A from T1 union select B from T2 limit 1) UNION select B from T2");
          pn = pn.getChild(0);
          assertTrue(pn instanceof UnionPlanNode);
          assertEquals(2, pn.getChildCount());
          // Left branch - union with offset
          assertTrue(pn.getChild(0) instanceof LimitPlanNode);
          assertTrue(pn.getChild(0).getChild(0) instanceof UnionPlanNode);
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
            assertTrue(pn.getChild(0) instanceof UnionPlanNode);
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
            assertTrue(pn.getChild(0) instanceof UnionPlanNode);
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
            assertTrue(pn.getChild(0) instanceof UnionPlanNode);
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
        assertTrue(pn.getChild(0) instanceof UnionPlanNode);
        pn = pn.getInlinePlanNode(PlanNodeType.LIMIT);
        assert (pn instanceof LimitPlanNode);
        assertTrue(pn.toExplainPlanString().contains("LIMIT with parameter"));
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
