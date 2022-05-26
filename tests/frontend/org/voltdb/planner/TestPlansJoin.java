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

import org.apache.commons.lang3.StringUtils;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.AggregatePlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.NestLoopIndexPlanNode;
import org.voltdb.plannodes.NestLoopPlanNode;
import org.voltdb.plannodes.NodeSchema;
import org.voltdb.plannodes.OrderByPlanNode;
import org.voltdb.plannodes.SchemaColumn;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.IndexLookupType;
import org.voltdb.types.JoinType;
import org.voltdb.types.PlanNodeType;

public class TestPlansJoin extends PlannerTestCase {

    private static class JoinOp {
        private final String m_string;
        private final ExpressionType m_operator;
        private JoinOp(String string, ExpressionType operator) {
            m_string = string;
            m_operator = operator;
        }

        static JoinOp NOT_DISTINCT =
                new JoinOp(" IS NOT DISTINCT FROM ",
                        ExpressionType.COMPARE_NOTDISTINCT);
        static JoinOp EQUAL =
                new JoinOp("=", ExpressionType.COMPARE_EQUAL);

        static JoinOp[] JOIN_OPS = new JoinOp[] {EQUAL, NOT_DISTINCT};

        @Override
        public String toString() { return m_string; }
        ExpressionType toOperator() { return m_operator; }
    }

    public void testBasicInnerJoin() {
        String query;
        String pattern;
        AbstractPlanNode pn;

        // SELECT * with USING clause should contain only one column
        // for each column from the USING expression.
        query = "SELECT * FROM R1 JOIN R2 USING(C)";
        pn = compileToTopDownTree(query, 4, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);
        followAssertedLeftChain(pn,
                PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOP);
        assertEquals(4, pn.getOutputSchema().size());

        for (JoinOp joinOp : JoinOp.JOIN_OPS) {
            perJoinOpTestBasicInnerJoin(joinOp);
        }

        query = "SELECT R2.C FROM R1 JOIN R2 USING(X)";
        pattern = "object not found: X";
        failToCompile(query, pattern);

    }

    private void perJoinOpTestBasicInnerJoin(JoinOp joinOp) {
        String query;
        String pattern;
        AbstractPlanNode pn;
        NodeSchema selectColumns;

        // SELECT * with ON clause should return all columns from all tables
        query = "SELECT * FROM R1 JOIN R2 ON R1.C" +
                joinOp + "R2.C";
        pn = compileToTopDownTree(query, 5,
                true,
                PlanNodeType.SEND,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN);

        query = "SELECT R1.A, R1.C, D FROM R1 JOIN R2 ON R1.C" +
                joinOp + "R2.C";
        pn = compileToTopDownTree(query, 3,
                true,
                PlanNodeType.SEND,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN);

        query = "SELECT R1.A, C, R1.D FROM R1 JOIN R2 USING(C)";
        pn = compileToTopDownTree(query, 3,
                PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN);

        query = "SELECT R1.A, R2.C, R1.D FROM R1 JOIN R2 ON R1.C" +
                joinOp + "R2.C";
        pn = compileToTopDownTree(query, 3, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN);
        selectColumns = pn.getOutputSchema();
        assertEquals("R1", selectColumns.getColumn(0).getTableName());
        assertEquals("R2", selectColumns.getColumn(1).getTableName());

        // The output table for C can be either R1 or R2 because it's an INNER join
        query = "SELECT R1.A, C, R1.D FROM R1 JOIN R2 USING(C)";
        pn = compileToTopDownTree(query, 3, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN);
        selectColumns = pn.getOutputSchema();
        assertEquals("R1", selectColumns.getColumn(0).getTableName());
        String table = selectColumns.getColumn(1).getTableName();
        assertTrue("R2".equals(table) || "R1".equals(table));
        table = selectColumns.getColumn(2).getTableName();
        assertEquals("R1", table);

        query = "SELECT R2.C FROM R1 JOIN R2 ON R1.X" +
                joinOp + "R2.X";
        pattern = "object not found: R1.X";
        failToCompile(query, pattern);

        query = "SELECT * FROM R1 JOIN R2 ON R1.C" +
                joinOp + "R2.C AND 1";
        pattern = "data type of expression is not boolean";
        failToCompile(query, pattern);

        query = "SELECT * FROM R1 JOIN R2 ON R1.C" +
                joinOp + "R2.C AND MOD(3,1)=1";
        pattern = "Join with filters that do not depend on joined tables is not supported in VoltDB";
        failToCompile(query, pattern);
    }

    public void testBasicThreeTableInnerJoin() {
        String query;
        String pattern;

        for (JoinOp joinOp : JoinOp.JOIN_OPS) {
            perJoinOpTestBasicThreeTableInnerJoin(joinOp);
        }

        // Here C could be the C from USING(C), which would be R1.C or R2.C, or else
        // R3.C.  Either is possible, and this is ambiguous.
        query = "SELECT C FROM R1 INNER JOIN R2 USING (C), R3 " +
                " WHERE R1.A = R3.A";
        pattern = "Column \"C\" is ambiguous";
        failToCompile(query, pattern);
    }

    private void perJoinOpTestBasicThreeTableInnerJoin(JoinOp joinOp) {
        String query;
        AbstractPlanNode pn;
        AbstractPlanNode node;

        query = "SELECT * FROM R1 JOIN R2 ON R1.C" +
                joinOp + "R2.C JOIN R3 ON R3.C" +
                joinOp + "R2.C";
        pn = compileToTopDownTree(query, 7, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);

        query = "SELECT R1.C, R2.C R3.C FROM R1 INNER JOIN R2 ON R1.C" +
                joinOp + "R2.C INNER JOIN R3 ON R3.C" +
                joinOp + "R2.C";
        pn = compileToTopDownTree(query, 2, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOP,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);

        query = "SELECT C FROM R1 INNER JOIN R2 USING (C) INNER JOIN R3 USING(C)";
        pn = compileToTopDownTree(query, 1, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOP,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);

        query = "SELECT C FROM R1 INNER JOIN R2 USING (C), R3_NOC WHERE R1.A" +
                joinOp + "R3_NOC.A";
        pn = compileToTopDownTree(query, 1, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOP,
                null, // weakened. soon, replace with: NESTLOOPINDEX, SEQSCAN?
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOP);
        if (joinOp == JoinOp.EQUAL) { // weaken test for now
            node = followAssertedLeftChain(node, PlanNodeType.NESTLOOP,
                    PlanNodeType.NESTLOOPINDEX,
                    PlanNodeType.SEQSCAN);
        }
    }

    public void testScanJoinConditions() {
        String query;
        AbstractPlanNode pn;
        AbstractPlanNode node;
        AbstractScanPlanNode scan;
        AbstractExpression predicate;

        query = "SELECT * FROM R1 WHERE R1.C = 0";
        pn = compileToTopDownTree(query, 3, PlanNodeType.SEND,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.SEQSCAN);
        scan = (AbstractScanPlanNode) node;
        predicate = scan.getPredicate();
        assertExprTopDownTree(predicate, ExpressionType.COMPARE_EQUAL,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_CONSTANT);

        for (JoinOp joinOp : JoinOp.JOIN_OPS) {
            perJoinOpTestScanJoinConditions(joinOp);
        }
    }

    private void perJoinOpTestScanJoinConditions(JoinOp joinOp) {
        String query;
        AbstractPlanNode pn;
        AbstractPlanNode node;
        NestLoopPlanNode nlj;
        SeqScanPlanNode seqScan;
        AbstractExpression predicate;
        boolean theOpIsOnTheLeft;

        query = "SELECT * FROM R1, R2 WHERE R1.A" +
                joinOp + "R2.A AND R1.C > 0";
        pn = compileToTopDownTree(query, 5, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        assertNull(nlj.getPreJoinPredicate());
        predicate = nlj.getJoinPredicate();
        assertExprTopDownTree(predicate, joinOp.toOperator(),
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);
        assertNull(nlj.getWherePredicate());

        seqScan = (SeqScanPlanNode) node.getChild(0);
        assertEquals("R1", seqScan.getTargetTableName());
        predicate = seqScan.getPredicate();
        assertExprTopDownTree(predicate, ExpressionType.COMPARE_GREATERTHAN,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_CONSTANT);

        query = "SELECT * FROM R1, R2 WHERE R1.A" +
                joinOp + "R2.A AND R1.C > R2.C";
        pn = compileToTopDownTree(query, 5, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        assertNull(nlj.getPreJoinPredicate());
        predicate = nlj.getJoinPredicate();
        theOpIsOnTheLeft = (predicate != null) &&
                (predicate.getLeft() != null) &&
                predicate.getLeft().getExpressionType() == joinOp.toOperator();
        assertExprTopDownTree(predicate, ExpressionType.CONJUNCTION_AND,
                (theOpIsOnTheLeft ? joinOp.toOperator() : ExpressionType.COMPARE_LESSTHAN),
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE,
                (theOpIsOnTheLeft ? ExpressionType.COMPARE_LESSTHAN : joinOp.toOperator()),
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);
        assertNull(nlj.getWherePredicate());

        seqScan = (SeqScanPlanNode) nlj.getChild(0);
        assertNull(seqScan.getPredicate());

        seqScan = (SeqScanPlanNode) nlj.getChild(1);
        assertNull(seqScan.getPredicate());

        query = "SELECT * FROM R1 JOIN R2 ON R1.A" +
                joinOp + "R2.A WHERE R1.C > 0";
        pn = compileToTopDownTree(query, 5, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        assertNull(nlj.getPreJoinPredicate());
        predicate = nlj.getJoinPredicate();
        assertExprTopDownTree(predicate, joinOp.toOperator(),
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);
        assertNull(nlj.getWherePredicate());

        seqScan = (SeqScanPlanNode) nlj.getChild(0);
        assertEquals("R1", seqScan.getTargetTableName());
        predicate = seqScan.getPredicate();
        assertExprTopDownTree(predicate, ExpressionType.COMPARE_GREATERTHAN,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_CONSTANT);

        seqScan = (SeqScanPlanNode) nlj.getChild(1);
        assertEquals("R2", seqScan.getTargetTableName());
        assertNull(seqScan.getPredicate());

        query = "SELECT * FROM R1 JOIN R2 ON R1.A" +
                joinOp + "R2.A WHERE R1.C > R2.C";
        pn = compileToTopDownTree(query, 5, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        assertNull(nlj.getPreJoinPredicate());
        predicate = nlj.getJoinPredicate();
        theOpIsOnTheLeft = (predicate != null) &&
                (predicate.getLeft() != null) &&
                predicate.getLeft().getExpressionType() == joinOp.toOperator();
        assertExprTopDownTree(predicate, ExpressionType.CONJUNCTION_AND,
                (theOpIsOnTheLeft ? joinOp.toOperator() : ExpressionType.COMPARE_LESSTHAN),
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE,
                (theOpIsOnTheLeft ? ExpressionType.COMPARE_LESSTHAN : joinOp.toOperator()),
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);
        assertNull(nlj.getWherePredicate());

        seqScan = (SeqScanPlanNode) nlj.getChild(0);
        assertNull(seqScan.getPredicate());

        seqScan = (SeqScanPlanNode) nlj.getChild(1);
        assertNull(seqScan.getPredicate());

        query = "SELECT * FROM R1, R2, R3 WHERE R1.A" +
                joinOp + "R2.A AND R1.C" +
                joinOp + "R3.C AND R1.A > 0";
        pn = compileToTopDownTree(query, 7, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        assertNull(nlj.getPreJoinPredicate());
        predicate = nlj.getJoinPredicate();
        assertExprTopDownTree(predicate, joinOp.toOperator(),
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);
        assertNull(nlj.getWherePredicate());

        // Validate trivial child 1 before child 0 to free up local variable nlj.
        seqScan = (SeqScanPlanNode) nlj.getChild(1);
        assertEquals("R3", seqScan.getTargetTableName());
        assertNull(seqScan.getPredicate());

        nlj = (NestLoopPlanNode) nlj.getChild(0);
        assertNull(nlj.getPreJoinPredicate());
        predicate = nlj.getJoinPredicate();
        assertExprTopDownTree(predicate, joinOp.toOperator(),
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);
        assertNull(nlj.getWherePredicate());

        seqScan = (SeqScanPlanNode) nlj.getChild(0);
        assertEquals("R1", seqScan.getTargetTableName());
        predicate = seqScan.getPredicate();
        assertExprTopDownTree(predicate, ExpressionType.COMPARE_GREATERTHAN,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_CONSTANT);

        seqScan = (SeqScanPlanNode) nlj.getChild(1);
        assertEquals("R2", seqScan.getTargetTableName());
        assertNull(seqScan.getPredicate());

        query = "SELECT * FROM R1 JOIN R2 ON R1.A" +
                joinOp + "R2.A AND R1.C" +
                joinOp + "R2.C WHERE R1.A > 0";
        pn = compileToTopDownTree(query, 5, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        assertNull(nlj.getPreJoinPredicate());
        predicate = nlj.getJoinPredicate();
        assertExprTopDownTree(predicate, ExpressionType.CONJUNCTION_AND,
                joinOp.toOperator(),
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE,
                joinOp.toOperator(),
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);
        assertNull(nlj.getWherePredicate());

        seqScan = (SeqScanPlanNode) nlj.getChild(0);
        assertEquals("R1", seqScan.getTargetTableName());
        predicate = seqScan.getPredicate();
        assertExprTopDownTree(predicate, ExpressionType.COMPARE_GREATERTHAN,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_CONSTANT);

        seqScan = (SeqScanPlanNode) nlj.getChild(1);
        assertEquals("R2", seqScan.getTargetTableName());
        assertNull(seqScan.getPredicate());

        query = "SELECT A, C FROM R1 JOIN R2 USING (A, C)";
        pn = compileToTopDownTree(query, 2, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        assertNull(nlj.getPreJoinPredicate());
        predicate = nlj.getJoinPredicate();
        assertExprTopDownTree(predicate, ExpressionType.CONJUNCTION_AND,
                ExpressionType.COMPARE_EQUAL,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE,
                ExpressionType.COMPARE_EQUAL,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);
        assertNull(nlj.getWherePredicate());

        query = "SELECT A, C FROM R1 JOIN R2 USING (A, C) WHERE A > 0";
        pn = compileToTopDownTree(query, 2, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        assertNull(nlj.getPreJoinPredicate());
        predicate = nlj.getJoinPredicate();
        assertExprTopDownTree(predicate, ExpressionType.CONJUNCTION_AND,
                ExpressionType.COMPARE_EQUAL,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE,
                ExpressionType.COMPARE_EQUAL,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);
        assertNull(nlj.getWherePredicate());

        seqScan = (SeqScanPlanNode) nlj.getChild(0);
        predicate = seqScan.getPredicate();
        assertExprTopDownTree(predicate, ExpressionType.COMPARE_GREATERTHAN,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_CONSTANT);

        seqScan = (SeqScanPlanNode) nlj.getChild(1);
        assertNull(seqScan.getPredicate());

        query = "SELECT * FROM R1 JOIN R2 ON R1.A" +
                joinOp + "R2.A JOIN R3 ON R1.C" +
                joinOp + "R3.C WHERE R1.A > 0";
        pn = compileToTopDownTree(query, 7, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        assertNull(nlj.getPreJoinPredicate());
        predicate = nlj.getJoinPredicate();
        assertExprTopDownTree(predicate, joinOp.toOperator(),
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);
        assertNull(nlj.getWherePredicate());

        // Validate trivial child 1 before child 0 to free up local variable nlj.
        seqScan = (SeqScanPlanNode) nlj.getChild(1);
        assertEquals("R3", seqScan.getTargetTableName());
        assertNull(seqScan.getPredicate());

        nlj = (NestLoopPlanNode) nlj.getChild(0);
        assertNull(nlj.getPreJoinPredicate());
        predicate = nlj.getJoinPredicate();
        assertExprTopDownTree(predicate, joinOp.toOperator(),
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);
        assertNull(nlj.getWherePredicate());

        seqScan = (SeqScanPlanNode) nlj.getChild(0);
        assertEquals("R1", seqScan.getTargetTableName());
        predicate = seqScan.getPredicate();
        assertExprTopDownTree(predicate, ExpressionType.COMPARE_GREATERTHAN,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_CONSTANT);

        seqScan = (SeqScanPlanNode) nlj.getChild(1);
        assertEquals("R2", seqScan.getTargetTableName());
        assertNull(seqScan.getPredicate());
    }

    public void testDisplayColumnFromUsingCondition() {
        String query;
        List<AbstractPlanNode> lpn;
        AbstractPlanNode pn;
        AbstractPlanNode node;
        NestLoopPlanNode nlj;
        AbstractExpression predicate;
        SeqScanPlanNode seqScan;
        SchemaColumn sc0;
        NodeSchema selectColumns;

        query = "SELECT max(A) FROM R1 JOIN R2 USING(A)";
        pn = compileToTopDownTree(query, 1, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN);
        selectColumns = pn.getOutputSchema();
        for (SchemaColumn sc : selectColumns) {
            AbstractExpression e = sc.getExpression();
            assertTrue(e instanceof TupleValueExpression);
            TupleValueExpression tve = (TupleValueExpression) e;
            assertNotSame(-1, tve.getColumnIndex());
        }
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP);
        assertNotNull(AggregatePlanNode.getInlineAggregationNode(node));

        query = "SELECT distinct(A) FROM R1 JOIN R2 USING(A)";
        pn = compileToTopDownTree(query, 1, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN);
        selectColumns = pn.getOutputSchema();
        for (SchemaColumn sc : selectColumns) {
            AbstractExpression e = sc.getExpression();
            assertTrue(e instanceof TupleValueExpression);
            TupleValueExpression tve = (TupleValueExpression) e;
            assertNotSame(-1, tve.getColumnIndex());
        }

        query = "SELECT A FROM R1 JOIN R2 USING(A) ORDER BY A";
        pn = compileToTopDownTree(query, 1, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.ORDERBY,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);
        selectColumns = pn.getOutputSchema();
        for (SchemaColumn sc : selectColumns) {
            AbstractExpression e = sc.getExpression();
            assertTrue(e instanceof TupleValueExpression);
            TupleValueExpression tve = (TupleValueExpression) e;
            assertNotSame(-1, tve.getColumnIndex());
        }


        query = "SELECT * FROM P1 LABEL JOIN R2 USING(A) " +
                "WHERE A > 0 AND R2.C >= 5";
        lpn = compileToFragments(query);
        assertProjectingCoordinator(lpn);
        pn = lpn.get(1);
        assertTopDownTree(pn, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(lpn.get(1), PlanNodeType.SEND,
                PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        assertNull(nlj.getPreJoinPredicate());
        predicate = nlj.getJoinPredicate();
        assertExprTopDownTree(predicate, ExpressionType.COMPARE_EQUAL,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);
        assertNull(nlj.getWherePredicate());

        seqScan = (SeqScanPlanNode) nlj.getChild(0);
        predicate = seqScan.getPredicate();
        assertExprTopDownTree(predicate, ExpressionType.CONJUNCTION_AND,
                ExpressionType.COMPARE_GREATERTHANOREQUALTO,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_CONSTANT,
                ExpressionType.COMPARE_GREATERTHAN,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_CONSTANT);

        seqScan = (SeqScanPlanNode) nlj.getChild(1);
        assertNull(seqScan.getPredicate());

        query = "SELECT * FROM P1 LABEL LEFT JOIN R2 USING(A) WHERE A > 0";
        lpn = compileToFragments(query);
        node = followAssertedLeftChain(lpn.get(1), PlanNodeType.SEND,
                PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        assertTrue(JoinType.LEFT == nlj.getJoinType());
        assertNull(nlj.getPreJoinPredicate());
        predicate = nlj.getJoinPredicate();
        assertExprTopDownTree(predicate, ExpressionType.COMPARE_EQUAL,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);
        assertNull(nlj.getWherePredicate());

        seqScan = (SeqScanPlanNode) nlj.getChild(0);
        predicate = seqScan.getPredicate();
        assertExprTopDownTree(predicate, ExpressionType.COMPARE_GREATERTHAN,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_CONSTANT);

        query = "SELECT A FROM R2 LABEL RIGHT JOIN P1 AP1 USING(A) WHERE A > 0";
        lpn = compileToFragments(query);
        assertProjectingCoordinator(lpn);
        pn = lpn.get(0);
        selectColumns = pn.getOutputSchema();
        assertEquals(1, selectColumns.size());
        sc0 = selectColumns.getColumn(0);
        assertEquals("AP1", sc0.getTableAlias());
        assertEquals("P1", sc0.getTableName());

        pn = lpn.get(1);
        assertTopDownTree(pn, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(lpn.get(1), PlanNodeType.SEND,
                PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        assertEquals(JoinType.LEFT, nlj.getJoinType());
        assertNull(nlj.getPreJoinPredicate());
        predicate = nlj.getJoinPredicate();
        assertExprTopDownTree(predicate, ExpressionType.COMPARE_EQUAL,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);
        assertNull(nlj.getWherePredicate());

        seqScan = (SeqScanPlanNode) nlj.getChild(0);
        predicate = seqScan.getPredicate();
        assertExprTopDownTree(predicate, ExpressionType.COMPARE_GREATERTHAN,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_CONSTANT);
        selectColumns = seqScan.getOutputSchema();
        assertEquals(1, selectColumns.size());
        sc0 = selectColumns.getColumn(0);
        assertEquals("AP1", sc0.getTableAlias());
        assertEquals("P1", sc0.getTableName());
    }

    public void testTransitiveValueEquivalenceConditions() {
        for (JoinOp joinOp : JoinOp.JOIN_OPS) {
            perJoinOpTestTransitiveValueEquivalenceConditions(joinOp);
        }
    }

    private void perJoinOpTestTransitiveValueEquivalenceConditions(JoinOp joinOp) {
        String query;
        AbstractPlanNode pn;
        AbstractPlanNode node;
        NestLoopPlanNode nlj;
        SeqScanPlanNode seqScan;
        IndexScanPlanNode indexScan;
        AbstractExpression predicate;
        boolean theConstantIsOnTheLeft;

        // R1.A" + joinOp + "R2.A AND R2.A = 1 => R1.A = 1 AND R2.A = 1
        query = "SELECT * FROM R1 LEFT JOIN R2 ON R1.A" +
                joinOp + "R2.A AND R2.A = 1 ";
        pn = compileToTopDownTree(query, 5, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn,PlanNodeType.SEND,
                PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        predicate = nlj.getPreJoinPredicate();
        theConstantIsOnTheLeft = (predicate != null) &&
                (predicate.getLeft() != null) &&
                (predicate.getLeft().getExpressionType() ==
                    ExpressionType.VALUE_CONSTANT);
        if (theConstantIsOnTheLeft) {
            assertExprTopDownTree(predicate, joinOp.toOperator(),
                    ExpressionType.VALUE_CONSTANT, ExpressionType.VALUE_TUPLE);
        }
        else {
            assertExprTopDownTree(predicate, joinOp.toOperator(),
                    ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_CONSTANT);
        }
        assertNull(nlj.getJoinPredicate());
        assertNull(nlj.getWherePredicate());

        seqScan = (SeqScanPlanNode) nlj.getChild(0);
        assertNull(seqScan.getPredicate());

        seqScan = (SeqScanPlanNode) nlj.getChild(1);
        predicate = seqScan.getPredicate();
        assertExprTopDownTree(predicate, ExpressionType.COMPARE_EQUAL,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_CONSTANT);

        // Same test but now R2 is outer table R1.A " +
        // joinOp + "R2.A AND R2.A = 1 => R1.A = 1 AND R2.A = 1
        query = "SELECT * FROM R2 LEFT JOIN R1 ON R1.A" +
                joinOp + "R2.A AND R2.A = 1 ";
        pn = compileToTopDownTree(query, 5, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn,PlanNodeType.SEND,
                PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        predicate = nlj.getPreJoinPredicate();
        assertExprTopDownTree(predicate, ExpressionType.COMPARE_EQUAL,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_CONSTANT);
        assertNull(nlj.getJoinPredicate());
        assertNull(nlj.getWherePredicate());

        seqScan = (SeqScanPlanNode) nlj.getChild(0);
        assertNull(seqScan.getPredicate());

        seqScan = (SeqScanPlanNode) nlj.getChild(1);
        predicate = seqScan.getPredicate();
        assertExprTopDownTree(predicate, joinOp.toOperator(),
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_CONSTANT);

        // R1.A" + joinOp + "R2.A AND R2.C = 1 => R1.A " +
        // joinOp + "R2.A AND R2.C = 1
        query = "SELECT * FROM R1 LEFT JOIN R2 ON R1.A" +
                joinOp + "R2.A AND R2.C = 1 ";
        pn = compileToTopDownTree(query, 5, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn,PlanNodeType.SEND,
                PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        predicate = nlj.getJoinPredicate();
        assertExprTopDownTree(predicate, joinOp.toOperator(),
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);

        // R1.A" + joinOp + "R2.A AND ABS(R2.C) = 1 => R1.A " +
        // joinOp + "R2.A AND ABS(R2.C) = 1
        query = "SELECT * FROM R1 LEFT JOIN R2 ON R1.A" +
                joinOp + "R2.A AND ABS(R2.C) = 1 ";
        pn = compileToTopDownTree(query, 5,
                true,
                PlanNodeType.SEND,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn,PlanNodeType.SEND,
                PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        predicate = nlj.getJoinPredicate();
        assertExprTopDownTree(predicate, joinOp.toOperator(),
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);

        // R1.A" + joinOp + "R3.A - NLIJ
        query = "SELECT * FROM R1 LEFT JOIN R3 ON R1.A" +
                joinOp + "R3.A";
        pn = compileToTopDownTree(query, 5, PlanNodeType.SEND,
                null); // PlanNodeType.NESTLOOPINDEX,
                // PlanNodeType.SEQSCAN); weakened for now
        if (joinOp == JoinOp.EQUAL) { // weaken test for now
            node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                    PlanNodeType.NESTLOOPINDEX);
        }

        // R1.A" + joinOp + "R3.A AND R1.A = 4 =>  R3.A = 4 AND R1.A = 4  -- NLJ/IndexScan
        query = "SELECT * FROM R1 LEFT JOIN R3 ON R1.A" +
                joinOp + "R3.A AND R1.A = 4";
        pn = compileToTopDownTree(query, 5,
                true,
                PlanNodeType.SEND,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN, null); // weakened for now
        node = followAssertedLeftChain(pn, true,
                PlanNodeType.SEND,
                PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        predicate = nlj.getPreJoinPredicate();
        assertExprTopDownTree(predicate, ExpressionType.COMPARE_EQUAL,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_CONSTANT);
        assertNull(nlj.getJoinPredicate());
        assertNull(nlj.getWherePredicate());

        seqScan = (SeqScanPlanNode) nlj.getChild(0);
        assertEquals("R1", seqScan.getTargetTableName());
        assertNull(seqScan.getPredicate());

        if (joinOp == JoinOp.EQUAL) { // weakened for now
            indexScan = (IndexScanPlanNode) nlj.getChild(1);
            assertEquals(IndexLookupType.EQ, indexScan.getLookupType());
            predicate = indexScan.getEndExpression();
            assertExprTopDownTree(predicate, ExpressionType.COMPARE_EQUAL,
                    ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_CONSTANT);
            assertNull(indexScan.getPredicate());
            assertEquals("R3", indexScan.getTargetTableName());
        }

        // R1.A" + joinOp + "R3.A AND R3.A = 4 =>  R3.A = 4 AND R1.A = 4  -- NLJ/IndexScan
        query = "SELECT * FROM R1 LEFT JOIN R3 ON R1.A" +
                joinOp + "R3.A AND R3.A = 4";
        pn = compileToTopDownTree(query, 5,
                true,
                PlanNodeType.SEND,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN, PlanNodeType.INDEXSCAN);
        node = followAssertedLeftChain(pn, true,
                PlanNodeType.SEND,
                PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        predicate = nlj.getPreJoinPredicate();
        theConstantIsOnTheLeft = (predicate != null) &&
                (predicate.getLeft() != null) &&
                (predicate.getLeft().getExpressionType() ==
                    ExpressionType.VALUE_CONSTANT);
        if (theConstantIsOnTheLeft) {
            assertExprTopDownTree(predicate, joinOp.toOperator(),
                    ExpressionType.VALUE_CONSTANT, ExpressionType.VALUE_TUPLE);
        }
        else {
            assertExprTopDownTree(predicate, joinOp.toOperator(),
                    ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_CONSTANT);
        }
        assertNull(nlj.getJoinPredicate());
        assertNull(nlj.getWherePredicate());

        seqScan = (SeqScanPlanNode) nlj.getChild(0);
        assertEquals("R1", seqScan.getTargetTableName());
        assertNull(seqScan.getPredicate());
//        predicate = seqScan.getPredicate();
//        assertExprTopDownTree(predicate, ExpressionType.COMPARE_EQUAL,
//                ExpressionType.VALUE_TUPLE,
//                ExpressionType.VALUE_CONSTANT);

        indexScan = (IndexScanPlanNode) nlj.getChild(1);
        assertEquals("R3", indexScan.getTargetTableName());
        assertEquals(IndexLookupType.EQ, indexScan.getLookupType());
        predicate = indexScan.getEndExpression();
        assertExprTopDownTree(predicate, ExpressionType.COMPARE_EQUAL,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_CONSTANT);
        assertNull(indexScan.getPredicate());
    }

    public void testFunctionJoinConditions() {
        String query;
        String pattern;
        AbstractPlanNode pn;
        AbstractPlanNode node;
        NestLoopPlanNode nlj;
        AbstractExpression predicate;

        query = "SELECT * FROM R1, R2";
        pn = compileToTopDownTree(query, 5, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn,PlanNodeType.SEND,
                PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        assertNull(nlj.getJoinPredicate());

        for (JoinOp joinOp : JoinOp.JOIN_OPS) {
            perJoinOpTestFunctionJoinConditions(joinOp);
        }

        // USING expression can have only comma separated list of column names
        query = "SELECT * FROM R1 JOIN R2 USING (ABS(A))";
        pattern = "object not found: ABS";
        failToCompile(query, pattern);
    }

    private void perJoinOpTestFunctionJoinConditions(JoinOp joinOp) {
        String query;
        AbstractPlanNode pn;
        AbstractPlanNode node;
        NestLoopPlanNode nlj;
        AbstractExpression predicate;

        query = "SELECT * FROM R1 JOIN R2 ON ABS(R1.A) " +
                joinOp + " ABS(R2.A) ";
        pn = compileToTopDownTree(query, 5, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn,PlanNodeType.SEND,
                PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        predicate = nlj.getJoinPredicate();
        assertExprTopDownTree(predicate, joinOp.toOperator(),
                ExpressionType.FUNCTION,
                ExpressionType.VALUE_TUPLE,
                ExpressionType.FUNCTION,
                ExpressionType.VALUE_TUPLE);

        query = "SELECT * FROM R1, R2 WHERE ABS(R1.A) " +
                joinOp + " ABS(R2.A) ";
        pn = compileToTopDownTree(query, 5, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN, PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn,PlanNodeType.SEND,
                PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        predicate = nlj.getJoinPredicate();
        assertExprTopDownTree(predicate, joinOp.toOperator(),
                ExpressionType.FUNCTION,
                ExpressionType.VALUE_TUPLE,
                ExpressionType.FUNCTION,
                ExpressionType.VALUE_TUPLE);
    }

    public void testIndexJoinConditions() {
        String query;
        AbstractPlanNode pn;
        IndexScanPlanNode indexScan;
        AbstractExpression predicate;

        //TODO: These are not even join queries. They should
        // probably be moved to some other test class.

        query = "SELECT * FROM R3 WHERE R3.A = 0";
        pn = compileToTopDownTree(query, 2, PlanNodeType.SEND,
                PlanNodeType.INDEXSCAN);
        indexScan = (IndexScanPlanNode) pn.getChild(0);
        assertEquals(IndexLookupType.EQ, indexScan.getLookupType());
        predicate = indexScan.getEndExpression();
        assertExprTopDownTree(predicate, ExpressionType.COMPARE_EQUAL,
                ExpressionType.VALUE_TUPLE,
                ExpressionType.VALUE_CONSTANT);
        assertNull(indexScan.getPredicate());

        query = "SELECT * FROM R3 WHERE R3.A > 0 AND R3.A < 5 AND R3.C = 4";
        pn = compileToTopDownTree(query, 2, PlanNodeType.SEND,
                PlanNodeType.INDEXSCAN);
        indexScan = (IndexScanPlanNode) pn.getChild(0);
        assertEquals(IndexLookupType.GT, indexScan.getLookupType());
        predicate = indexScan.getEndExpression();
        assertExprTopDownTree(predicate, ExpressionType.COMPARE_LESSTHAN,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_CONSTANT);
        predicate = indexScan.getPredicate();
        assertExprTopDownTree(predicate, ExpressionType.COMPARE_EQUAL,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_CONSTANT);

        for (JoinOp joinOp : JoinOp.JOIN_OPS) {
            if (joinOp != JoinOp.EQUAL) { // weaken test for now
                continue;
            }
            perJoinOpTestIndexJoinConditions(joinOp);
        }
    }

    private void perJoinOpTestIndexJoinConditions(JoinOp joinOp) {
        String query;
        AbstractPlanNode pn;
        AbstractPlanNode node;
        NestLoopPlanNode nlj;
        NestLoopIndexPlanNode nlij;
        IndexScanPlanNode indexScan;
        AbstractExpression predicate;
        SeqScanPlanNode seqScan;

        query = "SELECT * FROM R3, R2 WHERE R3.A" +
                joinOp + "R2.A AND R3.C > 0 AND R2.C >= 5";
        pn = compileToTopDownTree(query, 4, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOPINDEX,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOPINDEX);
        nlij = (NestLoopIndexPlanNode) node;
        assertNull(nlij.getJoinPredicate());

        indexScan = nlij.getInlineIndexScan();
        assertEquals(IndexLookupType.EQ, indexScan.getLookupType());
        predicate = indexScan.getEndExpression();
        assertExprTopDownTree(predicate, joinOp.toOperator(),
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);

        predicate = indexScan.getPredicate();
        assertExprTopDownTree(predicate, ExpressionType.COMPARE_GREATERTHAN,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_CONSTANT);

        seqScan = (SeqScanPlanNode) nlij.getChild(0);
        predicate = seqScan.getPredicate();
        assertExprTopDownTree(predicate, ExpressionType.COMPARE_GREATERTHANOREQUALTO,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_CONSTANT);

        query = "SELECT * FROM R3 JOIN R2 ON R3.A" +
                joinOp + "R2.A WHERE R3.C > 0 AND R2.C >= 5";
        pn = compileToTopDownTree(query, 4, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOPINDEX,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOPINDEX);
        nlij = (NestLoopIndexPlanNode) node;
        assertNull(nlij.getJoinPredicate());

        indexScan = nlij.getInlineIndexScan();
        assertEquals(IndexLookupType.EQ, indexScan.getLookupType());
        predicate = indexScan.getEndExpression();
        assertExprTopDownTree(predicate, joinOp.toOperator(),
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);

        predicate = indexScan.getPredicate();
        assertExprTopDownTree(predicate, ExpressionType.COMPARE_GREATERTHAN,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_CONSTANT);

        seqScan = (SeqScanPlanNode) nlij.getChild(0);
        predicate = seqScan.getPredicate();
        assertExprTopDownTree(predicate, ExpressionType.COMPARE_GREATERTHANOREQUALTO,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_CONSTANT);

        query = "SELECT * FROM R3 JOIN R2 USING(A) WHERE R3.C > 0 AND R2.C >= 5";
        pn = compileToTopDownTree(query, 3, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOPINDEX,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOPINDEX);
        nlij = (NestLoopIndexPlanNode) node;
        assertNull(nlij.getJoinPredicate());

        indexScan = nlij.getInlineIndexScan();
        assertEquals(IndexLookupType.EQ, indexScan.getLookupType());
        predicate = indexScan.getEndExpression();
        assertExprTopDownTree(predicate, joinOp.toOperator(),
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);

        predicate = indexScan.getPredicate();
        assertExprTopDownTree(predicate, ExpressionType.COMPARE_GREATERTHAN,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_CONSTANT);

        seqScan = (SeqScanPlanNode) nlij.getChild(0);
        predicate = seqScan.getPredicate();
        assertExprTopDownTree(predicate, ExpressionType.COMPARE_GREATERTHANOREQUALTO,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_CONSTANT);

        query = "SELECT * FROM R3 JOIN R2 ON R3.A" +
                joinOp +     " R2.A JOIN R1 ON R2.A" +
                joinOp + "R1.A WHERE R3.C > 0 AND R2.C >= 5";
        pn = compileToTopDownTree(query, 7, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOP,
                PlanNodeType.NESTLOOPINDEX,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        assertNull(nlj.getPreJoinPredicate());
        predicate = nlj.getJoinPredicate();
        assertExprTopDownTree(predicate, joinOp.toOperator(),
                ExpressionType.VALUE_TUPLE,
                ExpressionType.VALUE_TUPLE);
        assertNull(nlj.getWherePredicate());

        nlij = (NestLoopIndexPlanNode) nlj.getChild(0);

        indexScan = nlij.getInlineIndexScan();
        assertEquals(IndexLookupType.EQ, indexScan.getLookupType());
        predicate = indexScan.getEndExpression();
        assertExprTopDownTree(predicate, joinOp.toOperator(),
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);

        predicate = indexScan.getPredicate();
        assertExprTopDownTree(predicate, ExpressionType.COMPARE_GREATERTHAN,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_CONSTANT);

        seqScan = (SeqScanPlanNode) nlij.getChild(0);
        predicate = seqScan.getPredicate();
        assertExprTopDownTree(predicate, ExpressionType.COMPARE_GREATERTHANOREQUALTO,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_CONSTANT);
    }

    public void testOpIndexInnerJoin() {
        for (JoinOp joinOp : JoinOp.JOIN_OPS) {
            if (joinOp != JoinOp.EQUAL) { // weaken test for now
                continue;
            }
            perJoinTestOpIndexInnerJoin(joinOp);
        }
    }

    private void perJoinTestOpIndexInnerJoin(JoinOp joinOp) {
        String query;
        AbstractPlanNode pn;
        AbstractPlanNode node;
        IndexScanPlanNode indexScan;

        query = "SELECT * FROM R3 JOIN R1 ON R1.C" +
                joinOp + "R3.A";
        pn = compileToTopDownTree(query, 5, true,
                PlanNodeType.SEND,
                PlanNodeType.NESTLOOPINDEX,
                PlanNodeType.SEQSCAN);

        // Test ORDER BY optimization on indexed self-join, ordering by LHS
        query = "SELECT X.A FROM R5 X, R5 Y WHERE X.A" +
                joinOp + "Y.A ORDER BY X.A";
        pn = compileToTopDownTree(query, 1, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOPINDEX,
                PlanNodeType.INDEXSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOPINDEX,
                PlanNodeType.INDEXSCAN);
        indexScan = (IndexScanPlanNode) node;
        assertEquals("X", indexScan.getTargetTableAlias());

        // Test ORDER BY optimization on indexed self-join, ordering by RHS
        query = "SELECT X.A FROM R5 X, R5 Y WHERE X.A" +
                joinOp + "Y.A ORDER BY Y.A";
        pn = compileToTopDownTree(query, 1, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOPINDEX,
                PlanNodeType.INDEXSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOPINDEX,
                PlanNodeType.INDEXSCAN);
        indexScan = (IndexScanPlanNode) node;
        assertEquals("Y", indexScan.getTargetTableAlias());

        // Test safety guarding misapplication of ORDER BY optimization on indexed self-join,
        // when ordering by combination of LHS and RHS columns.
        // These MAY become valid optimization cases when ENG-4728 is done,
        // using transitive equality to determine that the ORDER BY clause can be re-expressed
        // as being based on only one of the two table scans.
        query = "SELECT X.A, X.C FROM R4 X, R4 Y WHERE X.A" +
                joinOp + "Y.A ORDER BY X.A, Y.C";
        pn = compileToTopDownTree(query, 2, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.ORDERBY,
                PlanNodeType.NESTLOOPINDEX,
                PlanNodeType.SEQSCAN);

        query = "SELECT X.A FROM R4 X, R4 Y WHERE X.A" +
                joinOp + "Y.A ORDER BY Y.A, X.C";
        pn = compileToTopDownTree(query, 1, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.ORDERBY,
                PlanNodeType.NESTLOOPINDEX,
                PlanNodeType.SEQSCAN);
    }

    public void testMultiColumnJoin() {
        String query;
        AbstractPlanNode pn;
        AbstractPlanNode node;
        NestLoopPlanNode nlj;
        NestLoopIndexPlanNode nlij;
        IndexScanPlanNode indexScan;
        AbstractExpression predicate;

        // Test multi column condition on non index columns
        query = "SELECT A, C FROM R2 JOIN R1 USING(A, C)";
        pn = compileToTopDownTree(query, 2, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        predicate = nlj.getJoinPredicate();
        assertExprTopDownTree(predicate, ExpressionType.CONJUNCTION_AND,
                ExpressionType.COMPARE_EQUAL,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE,
                ExpressionType.COMPARE_EQUAL,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);

        query = "SELECT A, C FROM R3 JOIN R2 USING(A, C)";
        pn = compileToTopDownTree(query, 2, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOPINDEX,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOPINDEX);
        nlij = (NestLoopIndexPlanNode) node;

        indexScan = nlij.getInlineIndexScan();
        assertEquals(IndexLookupType.EQ, indexScan.getLookupType());
        predicate = indexScan.getEndExpression();
        assertExprTopDownTree(predicate, ExpressionType.COMPARE_EQUAL,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);

        predicate = indexScan.getPredicate();
        assertExprTopDownTree(predicate, ExpressionType.COMPARE_EQUAL,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);

        // Test multi column condition on index columns
        query = "SELECT A FROM R2 JOIN R3 USING(A)";
        pn = compileToTopDownTree(query, 1, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOPINDEX,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOPINDEX);
        nlij = (NestLoopIndexPlanNode) node;

        indexScan = nlij.getInlineIndexScan();
        assertEquals(IndexLookupType.EQ, indexScan.getLookupType());
        predicate = indexScan.getEndExpression();
        assertExprTopDownTree(predicate, ExpressionType.COMPARE_EQUAL,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);
        assertNull(indexScan.getPredicate());

        for (JoinOp joinOp : JoinOp.JOIN_OPS) {
            perJoinOpTestMultiColumnJoin(joinOp);
        }
    }

    private void perJoinOpTestMultiColumnJoin(JoinOp joinOp) {
        String query;
        AbstractPlanNode pn;
        AbstractPlanNode node;
        NestLoopPlanNode nlj;
        NestLoopIndexPlanNode nlij;
        IndexScanPlanNode indexScan;
        AbstractExpression predicate;

        query = "SELECT R1.A, R2.A FROM R2 JOIN R1 ON R1.A" +
                joinOp + "R2.A AND R1.C" +
                joinOp + "R2.C";
        pn = compileToTopDownTree(query, 2, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        predicate = nlj.getJoinPredicate();
        assertExprTopDownTree(predicate, ExpressionType.CONJUNCTION_AND,
                joinOp.toOperator(),
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE,
                joinOp.toOperator(),
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);

        if (joinOp != JoinOp.EQUAL) { // weaken test for now
            return;
        }

        query = "SELECT R3.A, R2.A FROM R2 JOIN R3 ON R3.A" +
                joinOp + "R2.A";
        pn = compileToTopDownTree(query, 2, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOPINDEX,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOPINDEX);
        nlij = (NestLoopIndexPlanNode) node;

        indexScan = nlij.getInlineIndexScan();
        assertEquals(IndexLookupType.EQ, indexScan.getLookupType());
        predicate = indexScan.getEndExpression();
        assertExprTopDownTree(predicate, joinOp.toOperator(),
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);
        assertNull(indexScan.getPredicate());

        query = "SELECT R3.A, R2.A FROM R3 JOIN R2 ON R3.A" +
                joinOp + "R2.A AND R3.C" +
                joinOp + "R2.C";
        pn = compileToTopDownTree(query, 2, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOPINDEX,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOPINDEX);
        nlij = (NestLoopIndexPlanNode) node;

        indexScan = nlij.getInlineIndexScan();
        assertEquals(IndexLookupType.EQ, indexScan.getLookupType());
        predicate = indexScan.getEndExpression();
        assertExprTopDownTree(predicate, joinOp.toOperator(),
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);

        predicate = indexScan.getPredicate();
        assertExprTopDownTree(predicate, joinOp.toOperator(),
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);
    }

    public void testDistributedInnerJoin() {
        for (JoinOp joinOp : JoinOp.JOIN_OPS) {
            perJoinOpTestDistributedInnerJoin(joinOp);
        }
    }

    private void perJoinOpTestDistributedInnerJoin(JoinOp joinOp) {
        String query;
        String pattern;
        List<AbstractPlanNode> lpn;

        // JOIN replicated and one distributed table
        query = "SELECT * FROM R1 JOIN P2 ON R1.C" +
                joinOp + "P2.A";
        lpn = compileToFragments(query);
        assertProjectingCoordinator(lpn);
        if (joinOp == JoinOp.EQUAL) {
            assertTopDownTree(lpn.get(1), PlanNodeType.SEND,
                    PlanNodeType.NESTLOOPINDEX,
                    PlanNodeType.SEQSCAN);
        }

        // Join multiple distributed tables on the partitioned column
        query = "SELECT * FROM P1 JOIN P2 USING(A)";
        lpn = compileToFragments(query);
        assertProjectingCoordinator(lpn);
        if (joinOp == JoinOp.EQUAL) {
            assertTopDownTree(lpn.get(1), PlanNodeType.SEND,
                    PlanNodeType.NESTLOOPINDEX,
                    PlanNodeType.SEQSCAN);
        }

        // Two Distributed tables join on non-partitioned column
        query = "SELECT * FROM P1 JOIN P2 ON P1.C" +
                joinOp + "P2.E";
        pattern = "This query is not plannable.  The planner cannot guarantee that all rows would be in a single partition.";
        failToCompile(query, pattern);

        // Two Distributed tables join on boolean constant
        query = "SELECT * FROM P1 JOIN P2 ON 1=1";
        pattern = "This query is not plannable.  The planner cannot guarantee that all rows would be in a single partition.";
        failToCompile(query, pattern);
    }

    public void testBasicOuterJoin() {
        for (JoinOp joinOp : JoinOp.JOIN_OPS) {
            perJoinOpTestBasicOuterJoin(joinOp);
        }
    }

    private void perJoinOpTestBasicOuterJoin(JoinOp joinOp) {
        String query;
        AbstractPlanNode pn;
        AbstractPlanNode node;
        NestLoopPlanNode nlj;
        SeqScanPlanNode seqScan;
        AbstractExpression predicate;

        // SELECT * with ON clause should return all columns from all tables
        query = "SELECT * FROM R1 LEFT JOIN R2 ON R1.C" +
                joinOp + "R2.C";
        pn = compileToTopDownTree(query, 5, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        assertEquals(JoinType.LEFT, nlj.getJoinType());
        assertNull(nlj.getPreJoinPredicate());
        predicate = nlj.getJoinPredicate();
        assertExprTopDownTree(predicate, joinOp.toOperator(),
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);
        assertNull(nlj.getWherePredicate());

        seqScan = (SeqScanPlanNode) nlj.getChild(0);
        assertEquals("R1", seqScan.getTargetTableName());

        seqScan = (SeqScanPlanNode) nlj.getChild(1);
        assertEquals("R2", seqScan.getTargetTableName());

        query = "SELECT * FROM R1 LEFT JOIN R2 ON R1.C" +
                joinOp + "R2.C AND R1.A = 5";
        pn = compileToTopDownTree(query, 5, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        assertEquals(JoinType.LEFT, nlj.getJoinType());
        predicate = nlj.getPreJoinPredicate();
        assertExprTopDownTree(predicate, ExpressionType.COMPARE_EQUAL,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_CONSTANT);

        predicate = nlj.getJoinPredicate();
        assertExprTopDownTree(predicate, joinOp.toOperator(),
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);
        assertNull(nlj.getWherePredicate());

        seqScan = (SeqScanPlanNode) nlj.getChild(0);
        assertEquals("R1", seqScan.getTargetTableName());

        seqScan = (SeqScanPlanNode) nlj.getChild(1);
        assertEquals("R2", seqScan.getTargetTableName());
    }

    public void testRightOuterJoin() {
        for (JoinOp joinOp : JoinOp.JOIN_OPS) {
            perJoinOpTestRightOuterJoin(joinOp);
        }
    }

    private void perJoinOpTestRightOuterJoin(JoinOp joinOp) {
        String query;
        List<AbstractPlanNode> lpn;
        AbstractPlanNode pn;
        AbstractPlanNode node;
        NestLoopPlanNode nlj;
        SeqScanPlanNode seqScan;
        AbstractExpression predicate;

        // SELECT * FROM R1 RIGHT JOIN R2 ON R1.C " +
        // joinOp + "R2.C => SELECT * FROM R2 LEFT JOIN R1 ON R1.C" + joinOp + "R2.C
        query = "SELECT * FROM R1 RIGHT JOIN R2 ON R1.C" +
                joinOp + "R2.C";
        pn = compileToTopDownTree(query, 5, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        assertEquals(JoinType.LEFT, nlj.getJoinType());
        assertNull(nlj.getPreJoinPredicate());
        predicate = nlj.getJoinPredicate();
        assertExprTopDownTree(predicate, joinOp.toOperator(),
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);
        assertNull(nlj.getWherePredicate());

        seqScan = (SeqScanPlanNode) nlj.getChild(0);
        assertEquals("R2", seqScan.getTargetTableName());
        seqScan = (SeqScanPlanNode) nlj.getChild(1);
        assertEquals("R1", seqScan.getTargetTableName());

        // Same but with distributed table
        query = "SELECT * FROM P1 RIGHT JOIN R2 ON P1.C" + joinOp + "R2.C";
        lpn = compileToFragments(query);
        assertReplicatedLeftJoinCoordinator(lpn, "R2");

        pn = lpn.get(1);
        assertTopDownTree(pn, PlanNodeType.SEND,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.SEQSCAN);
        seqScan = (SeqScanPlanNode) node;
        assertEquals("P1", seqScan.getTargetTableName());
    }

    public void testSeqScanOuterJoinCondition() {
        for (JoinOp joinOp : JoinOp.JOIN_OPS) {
            perJoinOpTestSeqScanOuterJoinCondition(joinOp);
        }
    }

    private void perJoinOpTestSeqScanOuterJoinCondition(JoinOp joinOp) {
        String query;
        AbstractPlanNode pn;
        AbstractPlanNode node;
        NestLoopPlanNode nlj;
        AbstractExpression predicate;
        SeqScanPlanNode seqScan;
        IndexScanPlanNode indexScan;

        // R1.C" + joinOp + "R2.C Inner-Outer join Expr stays at the NLJ as Join predicate
        query = "SELECT * FROM R1 LEFT JOIN R2 ON R1.C" +
                joinOp + "R2.C";
        pn = compileToTopDownTree(query, 5, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        assertNull(nlj.getPreJoinPredicate());
        predicate = nlj.getJoinPredicate();
        assertExprTopDownTree(predicate, joinOp.toOperator(),
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);
        assertNull(nlj.getWherePredicate());

        seqScan = (SeqScanPlanNode) nlj.getChild(0);
        assertNull(seqScan.getPredicate());

        seqScan = (SeqScanPlanNode) nlj.getChild(1);
        assertNull(seqScan.getPredicate());

        // R1.C" + joinOp + "R2.C Inner-Outer join Expr stays at the NLJ as Join predicate
        // R1.A > 0 Outer Join Expr stays at the the NLJ as pre-join predicate
        // R2.A < 0 Inner Join Expr is pushed down to the inner SeqScan node
        query = "SELECT * FROM R1 LEFT JOIN R2 ON R1.C" +
                joinOp + "R2.C AND R1.A > 0 AND R2.A < 0";
        pn = compileToTopDownTree(query, 5, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        predicate = nlj.getPreJoinPredicate();
        assertExprTopDownTree(predicate, ExpressionType.COMPARE_GREATERTHAN,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_CONSTANT);

        predicate = nlj.getJoinPredicate();
        assertExprTopDownTree(predicate, joinOp.toOperator(),
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);
        assertNull(nlj.getWherePredicate());

        seqScan = (SeqScanPlanNode) nlj.getChild(0);
        assertNull(seqScan.getPredicate());

        seqScan = (SeqScanPlanNode) nlj.getChild(1);
        predicate = seqScan.getPredicate();
        assertExprTopDownTree(predicate, ExpressionType.COMPARE_LESSTHAN,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_CONSTANT);

        // R1.C" + joinOp + "R2.C Inner-Outer join Expr stays at the NLJ as Join predicate
        // (R1.A > 0 OR R2.A < 0) Inner-Outer join Expr stays at the NLJ as Join predicate
        query = "SELECT * FROM R1 LEFT JOIN R2 ON R1.C" +
                joinOp + "R2.C AND (R1.A > 0 OR R2.A < 0)";
        pn = compileToTopDownTree(query, 5, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        assertNull(nlj.getPreJoinPredicate());
        predicate = nlj.getJoinPredicate();
        boolean theOrIsOnTheLeft = (predicate != null) &&
                (predicate.getLeft() != null) &&
                (ExpressionType.CONJUNCTION_OR ==
                predicate.getLeft().getExpressionType());
        if (theOrIsOnTheLeft) {
            assertExprTopDownTree(predicate, ExpressionType.CONJUNCTION_AND,
                    ExpressionType.CONJUNCTION_OR,
                    ExpressionType.COMPARE_GREATERTHAN,
                    ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_CONSTANT,
                    ExpressionType.COMPARE_LESSTHAN,
                    ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_CONSTANT,
                    joinOp.toOperator(),
                    ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);
        }
        else {
            assertExprTopDownTree(predicate, ExpressionType.CONJUNCTION_AND,
                    joinOp.toOperator(),
                    ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE,
                    ExpressionType.CONJUNCTION_OR,
                    ExpressionType.COMPARE_GREATERTHAN,
                    ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_CONSTANT,
                    ExpressionType.COMPARE_LESSTHAN,
                    ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_CONSTANT);
        }
        assertNull(nlj.getWherePredicate());

        seqScan = (SeqScanPlanNode) nlj.getChild(0);
        assertNull(seqScan.getPredicate());

        seqScan = (SeqScanPlanNode) nlj.getChild(1);
        assertNull(seqScan.getPredicate());

        // R1.C" + joinOp + "R2.C Inner-Outer join Expr stays at the NLJ as Join predicate
        // R1.A > 0 Outer Where Expr is pushed down to the outer SeqScan node
        // R2.A IS NULL Inner Where Expr stays at the the NLJ as post join (where) predicate
        // (R1.C > R2.C OR R2.C IS NULL) Inner-Outer Where stays at the the NLJ as post join (where) predicate
        query = "SELECT * FROM R1 LEFT JOIN R2 ON R1.C" +
                joinOp + "R2.C WHERE R1.A > 0 AND R2.A IS NULL AND (R1.C > R2.C OR R2.C IS NULL)";
        pn = compileToTopDownTree(query, 5, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        assertEquals(JoinType.LEFT, nlj.getJoinType());
        assertNull(nlj.getPreJoinPredicate());
        predicate = nlj.getJoinPredicate();
        assertExprTopDownTree(predicate, joinOp.toOperator(),
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);

        predicate = nlj.getWherePredicate();
        assertExprTopDownTree(predicate, ExpressionType.CONJUNCTION_AND,
                ExpressionType.CONJUNCTION_OR,
                ExpressionType.COMPARE_GREATERTHAN,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE,
                ExpressionType.OPERATOR_IS_NULL,
                ExpressionType.VALUE_TUPLE,
                ExpressionType.OPERATOR_IS_NULL,
                ExpressionType.VALUE_TUPLE);

        seqScan = (SeqScanPlanNode) nlj.getChild(0);
        predicate = seqScan.getPredicate();
        assertExprTopDownTree(predicate, ExpressionType.COMPARE_GREATERTHAN,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_CONSTANT);

        seqScan = (SeqScanPlanNode) nlj.getChild(1);
        assertNull(seqScan.getPredicate());

        // R3.A" + joinOp + "R2.A Inner-Outer index join Expr. NLJ predicate.
        // R3.A > 3 Index Outer where expr pushed down to IndexScanPlanNode
        // R3.C < 0 non-index Outer where expr pushed down to IndexScanPlanNode as a predicate
        query = "SELECT * FROM R3 LEFT JOIN R2 ON R3.A" +
                joinOp + "R2.A WHERE R3.A > 3 AND R3.C < 0";
        pn = compileToTopDownTree(query, 4, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP,
                PlanNodeType.INDEXSCAN,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        assertEquals(JoinType.LEFT, nlj.getJoinType());
        assertNull(nlj.getPreJoinPredicate());
        predicate = nlj.getJoinPredicate();
        assertExprTopDownTree(predicate, joinOp.toOperator(),
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);
        assertNull(nlj.getWherePredicate());

        indexScan = (IndexScanPlanNode) nlj.getChild(0);
        assertEquals(IndexLookupType.GT, indexScan.getLookupType());
        assertNull(indexScan.getEndExpression());
        predicate = indexScan.getPredicate();
        assertExprTopDownTree(predicate, ExpressionType.COMPARE_LESSTHAN,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_CONSTANT);

        // R3.C" + joinOp + "R2.C Inner-Outer non-index join Expr. NLJ predicate.
        // R3.A > 3 Index null rejecting inner where expr pushed down to IndexScanPlanNode
        // NLJ is simplified to be INNER
        query = "SELECT * FROM R2 LEFT JOIN R3 ON R3.C" +
                joinOp + "R2.C WHERE R3.A > 3";
        pn = compileToTopDownTree(query, 4, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.INDEXSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        assertEquals(JoinType.INNER, nlj.getJoinType());
        assertNull(nlj.getPreJoinPredicate());
        predicate = nlj.getJoinPredicate();
        assertExprTopDownTree(predicate, joinOp.toOperator(),
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);
        assertNull(nlj.getWherePredicate());

        seqScan = (SeqScanPlanNode) nlj.getChild(0);
        assertEquals("R2", seqScan.getTargetTableName());
        assertNull(seqScan.getPredicate());

        indexScan = (IndexScanPlanNode) nlj.getChild(1);
        assertEquals(IndexLookupType.GT, indexScan.getLookupType());
        assertNull(indexScan.getEndExpression());
        assertNull(indexScan.getPredicate());

        if (joinOp != JoinOp.EQUAL) { // weaken test for now
            return;
        }
        query = "SELECT * FROM R2 LEFT JOIN R3 ON R3.A" +
                joinOp + "R2.C WHERE R3.A > 3";
        pn = compileToTopDownTree(query, 4, PlanNodeType.SEND,
                PlanNodeType.NESTLOOPINDEX,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.NESTLOOPINDEX);
        NestLoopIndexPlanNode nlij = (NestLoopIndexPlanNode) node;
        assertEquals(JoinType.INNER, nlij.getJoinType());
   }

    public void testDistributedSeqScanOuterJoinCondition() {
        for (JoinOp joinOp : JoinOp.JOIN_OPS) {
            perJoinOpTestDistributedSeqScanOuterJoinCondition(joinOp);
        }
    }

    private void perJoinOpTestDistributedSeqScanOuterJoinCondition(JoinOp joinOp) {
        // Distributed Outer table
        String query;
        String pattern;
        List<AbstractPlanNode> lpn;
        AbstractPlanNode pn;

        query = "SELECT * FROM P1 LEFT JOIN R2 ON P1.C" +
                joinOp + "R2.C";
        lpn = compileToFragments(query);
        assertEquals(2, lpn.size());
        assertProjectingCoordinator(lpn);
        pn = lpn.get(1);
        assertTopDownTree(pn, PlanNodeType.SEND,
        PlanNodeType.NESTLOOP,
        PlanNodeType.SEQSCAN,
        PlanNodeType.SEQSCAN);

        // Distributed Inner table
        query = "SELECT * FROM R2 LEFT JOIN P1 ON P1.C" +
                joinOp + "R2.C";
        lpn = compileToFragments(query);
        assertEquals(2, lpn.size());
        assertReplicatedLeftJoinCoordinator(lpn, "R2");
        pn = lpn.get(1);
        assertTopDownTree(pn, PlanNodeType.SEND,
                PlanNodeType.SEQSCAN);

        // Distributed Inner and Outer table joined on the partition column
        query = "SELECT * FROM P1 LEFT JOIN P4 ON P1.A" +
                joinOp + "P4.A";
        lpn = compileToFragments(query);
        assertEquals(2, lpn.size());
        assertTopDownTree(lpn.get(1), PlanNodeType.SEND,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);

        // Distributed Inner and Outer table joined on the non-partition column
        query = "SELECT * FROM P1 LEFT JOIN P4 ON P1.A" +
                joinOp + "P4.E";
        pattern = "This query is not plannable.  The planner cannot guarantee that all rows would be in a single partition";
        failToCompile(query, pattern);
    }

    public void testBasicIndexOuterJoin() {
        for (JoinOp joinOp : JoinOp.JOIN_OPS) {
            perJoinOpTestBasicIndexOuterJoin(joinOp);
        }
    }

    private void perJoinOpTestBasicIndexOuterJoin(JoinOp joinOp) {
        // R3 is indexed but it's the outer table and the join expression
        // must stay at the NLJ so the index can't be used
        String query;
        AbstractPlanNode pn;
        AbstractPlanNode node;
        NestLoopIndexPlanNode nlij;
        NestLoopPlanNode nlj;
        SeqScanPlanNode seqScan;
        IndexScanPlanNode indexScan;
        AbstractExpression predicate;

        query = "SELECT * FROM R3 LEFT JOIN R2 ON R3.A" +
                joinOp + "R2.C";
        pn = compileToTopDownTree(query, 4, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        assertEquals(JoinType.LEFT, nlj.getJoinType());
        assertNull(nlj.getPreJoinPredicate());
        predicate = nlj.getJoinPredicate();
        assertExprTopDownTree(predicate, joinOp.toOperator(),
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);
        assertNull(nlj.getWherePredicate());

        seqScan = (SeqScanPlanNode) nlj.getChild(0);
        assertEquals("R3", seqScan.getTargetTableName());

        seqScan = (SeqScanPlanNode) nlj.getChild(1);
        assertEquals("R2", seqScan.getTargetTableName());

        // R3 is indexed but it's the outer table so index can't be used
        query = "SELECT * FROM R2 RIGHT JOIN R3 ON R3.A" +
                joinOp + "R2.C";
        pn = compileToTopDownTree(query, 4, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        assertEquals(JoinType.LEFT, nlj.getJoinType());
        assertNull(nlj.getPreJoinPredicate());
        predicate = nlj.getJoinPredicate();
        assertExprTopDownTree(predicate, joinOp.toOperator(),
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);
        assertNull(nlj.getWherePredicate());

        seqScan = (SeqScanPlanNode) nlj.getChild(0);
        assertEquals("R3", seqScan.getTargetTableName());

        seqScan = (SeqScanPlanNode) nlj.getChild(1);
        assertEquals("R2", seqScan.getTargetTableName());

        if (joinOp != JoinOp.EQUAL) { // weaken test for now
            return;
        }
        query = "SELECT * FROM R2 LEFT JOIN R3 ON R2.C" +
                joinOp + "R3.A";
        pn = compileToTopDownTree(query, 4, PlanNodeType.SEND,
                PlanNodeType.NESTLOOPINDEX,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.NESTLOOPINDEX);
        nlij = (NestLoopIndexPlanNode) node;
        assertEquals(JoinType.LEFT, nlij.getJoinType());
        seqScan = (SeqScanPlanNode) nlij.getChild(0);
        assertEquals("R2", seqScan.getTargetTableName());
        indexScan = nlij.getInlineIndexScan();
        assertEquals(IndexLookupType.EQ, indexScan.getLookupType());
        predicate = indexScan.getEndExpression();
        assertExprTopDownTree(predicate, joinOp.toOperator(),
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);
        assertNull(indexScan.getPredicate());
        assertEquals("R3", indexScan.getTargetTableName());
    }

    public void testIndexOuterJoinConditions() {
        for (JoinOp joinOp : JoinOp.JOIN_OPS) {
            if (joinOp != JoinOp.EQUAL) { // weaken test for now
                continue;
            }
            perJoinOpTestIndexOuterJoinConditions(joinOp);
        }
    }

    private void perJoinOpTestIndexOuterJoinConditions(JoinOp joinOp) {
        String query;
        AbstractPlanNode pn;
        AbstractPlanNode node;
        NestLoopIndexPlanNode nlij;
        NestLoopPlanNode nlj;
        IndexScanPlanNode indexScan;
        AbstractExpression predicate;
        SeqScanPlanNode seqScan;

        // R1.C" + joinOp + "R3.A Inner-Outer index join Expr. NLIJ/Inlined IndexScan
        // R3.C > 0 Inner Join Expr is pushed down to the inlined IndexScan node as a predicate
        // R2.A < 6 Outer Join Expr is a pre-join predicate for NLIJ
        query = "SELECT * FROM R2 LEFT JOIN R3 ON R3.A" +
                joinOp + "R2.A AND R3.C > 0 AND R2.A < 6";
        pn = compileToTopDownTree(query, 4, PlanNodeType.SEND,
                PlanNodeType.NESTLOOPINDEX,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.NESTLOOPINDEX);
        nlij = (NestLoopIndexPlanNode) node;
        assertEquals(JoinType.LEFT, nlij.getJoinType());
        predicate = nlij.getPreJoinPredicate();
        assertExprTopDownTree(predicate, ExpressionType.COMPARE_LESSTHAN,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_CONSTANT);
        assertNull(nlij.getJoinPredicate());
        assertNull(nlij.getWherePredicate());

        seqScan = (SeqScanPlanNode) node.getChild(0);
        assertNull(seqScan.getPredicate());

        indexScan = nlij.getInlineIndexScan();
        assertEquals(IndexLookupType.EQ, indexScan.getLookupType());
        predicate = indexScan.getEndExpression();
        assertExprTopDownTree(predicate, joinOp.toOperator(),
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);

        predicate = indexScan.getPredicate();
        assertExprTopDownTree(predicate, ExpressionType.COMPARE_GREATERTHAN,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_CONSTANT);

        // R1.C" + joinOp + "R3.A Inner-Outer non-index join Expr. NLJ/IndexScan
        // R3.A > 0 Inner index Join Expr is pushed down to the inner IndexScan node as an index
        // R3.C != 0 Non-index Inner Join Expression is pushed down to the inner IndexScan node as a predicate
        // R2.A < 6 Outer Join Expr is a pre-join predicate for NLJ
        query = "SELECT * FROM R2 LEFT JOIN R3 ON R3.C" +
                joinOp + "R2.A AND R3.A > 0 AND R3.C != 0 AND R2.A < 6";
        pn = compileToTopDownTree(query, 4, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.INDEXSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        assertEquals(JoinType.LEFT, nlj.getJoinType());
        predicate = nlj.getPreJoinPredicate();
        assertExprTopDownTree(predicate, ExpressionType.COMPARE_LESSTHAN,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_CONSTANT);

        predicate = nlj.getJoinPredicate();
        assertExprTopDownTree(predicate, joinOp.toOperator(),
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);
        assertNull(nlj.getWherePredicate());

        seqScan = (SeqScanPlanNode) nlj.getChild(0);
        assertNull(seqScan.getPredicate());
        indexScan = (IndexScanPlanNode) nlj.getChild(1);
        assertEquals(IndexLookupType.GT, indexScan.getLookupType());
        assertNull(indexScan.getEndExpression());
        predicate = indexScan.getPredicate();
        assertExprTopDownTree(predicate, ExpressionType.COMPARE_NOTEQUAL,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_CONSTANT);

        // R2.A" + joinOp + "R3.A Inner-Outer index join Expr. NLIJ/Inlined IndexScan
        // R3.A IS NULL Inner where expr - part of the NLIJ where predicate
        // R2.A < 6 OR R3.C IS NULL Inner-Outer where expr - part of the NLIJ where predicate
        // R2.A > 3 Outer where expr - pushed down to the outer node
        query = "SELECT * FROM R2 LEFT JOIN R3 ON R3.A" +
                joinOp + "R2.A WHERE R3.A IS NULL AND R2.A > 3 AND (R2.A < 6 OR R3.C IS NULL)";
        pn = compileToTopDownTree(query, 4, PlanNodeType.SEND,
                PlanNodeType.NESTLOOPINDEX,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.NESTLOOPINDEX);
        nlij = (NestLoopIndexPlanNode) node;
        assertEquals(nlij.getJoinType(), JoinType.LEFT);
        assertNull(nlij.getPreJoinPredicate());
        assertNull(nlij.getJoinPredicate());
        predicate = nlij.getWherePredicate();
        assertExprTopDownTree(predicate, ExpressionType.CONJUNCTION_AND,
                ExpressionType.CONJUNCTION_OR,
                ExpressionType.COMPARE_LESSTHAN,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_CONSTANT,
                ExpressionType.OPERATOR_IS_NULL,
                ExpressionType.VALUE_TUPLE,
                ExpressionType.OPERATOR_IS_NULL,
                ExpressionType.VALUE_TUPLE);

        seqScan = (SeqScanPlanNode) nlij.getChild(0);
        predicate = seqScan.getPredicate();
        assertExprTopDownTree(predicate, ExpressionType.COMPARE_GREATERTHAN,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_CONSTANT);

        indexScan = nlij.getInlineIndexScan();
        assertEquals(IndexLookupType.EQ, indexScan.getLookupType());
        predicate = indexScan.getEndExpression();
        assertExprTopDownTree(predicate, joinOp.toOperator(),
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);
        assertNull(indexScan.getPredicate());
    }

    public void testDistributedInnerOuterTable() {
        for (JoinOp joinOp : JoinOp.JOIN_OPS) {
            perJoinOpTestDistributedInnerOuterTable(joinOp);
        }
    }

    private void perJoinOpTestDistributedInnerOuterTable(JoinOp joinOp) {
        // Distributed Outer table
        String query;
        String pattern;
        List<AbstractPlanNode> lpn;
        AbstractPlanNode pn;

        query = "SELECT * FROM P1 LEFT JOIN R2 ON P1.C" +
                joinOp + "R2.C";
        lpn = compileToFragments(query);
        assertProjectingCoordinator(lpn);
        assertEquals(2, lpn.size());
        pn = lpn.get(1);
        assertTopDownTree(pn, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);

        // Distributed Inner table
        query = "SELECT * FROM R2 LEFT JOIN P1 ON P1.C" +
                joinOp + "R2.C";
        lpn = compileToFragments(query);
        assertReplicatedLeftJoinCoordinator(lpn, "R2");
        pn = lpn.get(1);
        assertTopDownTree(pn, PlanNodeType.SEND,
                PlanNodeType.SEQSCAN);

        // Distributed Inner and Outer table joined on the partition column
        query = "SELECT * FROM P1 LEFT JOIN P4 ON P1.A" +
                joinOp + "P4.A";
        lpn = compileToFragments(query);
        assertProjectingCoordinator(lpn);
        assertEquals(2, lpn.size());
        pn = lpn.get(1);
        assertTopDownTree(pn, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);

        // Distributed Inner and Outer table joined on the non-partition column
        query = "SELECT * FROM P1 LEFT JOIN P4 ON P1.A" +
                joinOp + "P4.E";
        pattern = "This query is not plannable.  The planner cannot guarantee that all rows would be in a single partition";
        failToCompile(query, pattern);
    }

    public void testDistributedIndexJoinConditions() {
        for (JoinOp joinOp : JoinOp.JOIN_OPS) {
            if (joinOp != JoinOp.EQUAL) { // weaken test for now
                continue;
            }
            perJoinOpDistributedIndexJoinConditions(joinOp);
        }
    }

    private void perJoinOpDistributedIndexJoinConditions(JoinOp joinOp) {
        // Distributed outer table, replicated inner -NLIJ/inlined IndexScan
        String query;
        List<AbstractPlanNode> lpn;
        AbstractPlanNode pn;
        AbstractPlanNode node;
        NestLoopPlanNode nlj;
        NestLoopIndexPlanNode nlij;
        IndexScanPlanNode indexScan;
        AbstractExpression predicate;

//        query = "SELECT * FROM P1 LEFT JOIN R3 ON P1.C" +
//                joinOp + "R3.A";
//        lpn = compileToFragments(query);
//        assertEquals(2, lpn.size());
//        pn = lpn.get(1).getChild(0);
//        assertTrue(node instanceof NestLoopIndexPlanNode);
//        assertEquals(1, pn.getChildCount());
//        assertTrue(n.getChild(0) instanceof SeqScanPlanNode);

        // Distributed inner and replicated outer tables -NLJ/IndexScan
        query = "SELECT * FROM R3 LEFT JOIN P2 ON R3.A" +
                joinOp + "P2.A AND P2.A < 0 AND P2.E > 3 WHERE P2.A IS NULL";
        lpn = compileToFragments(query);
        assertEquals(2, lpn.size());
        //*enable to debug*/printExplainPlan(lpn);
        assertReplicatedLeftJoinCoordinator(lpn, "R3");
        pn = lpn.get(0);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        assertNull(nlj.getPreJoinPredicate());
        predicate = nlj.getJoinPredicate();
        assertExprTopDownTree(predicate, joinOp.toOperator(),
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);

        predicate = nlj.getWherePredicate();
        assertExprTopDownTree(predicate, ExpressionType.OPERATOR_IS_NULL,
                ExpressionType.VALUE_TUPLE);

        pn = lpn.get(1);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.INDEXSCAN);
        indexScan = (IndexScanPlanNode) node;
        assertEquals(IndexLookupType.LT, indexScan.getLookupType());
        assertNull(indexScan.getEndExpression());
        predicate = indexScan.getPredicate();
        assertExprTopDownTree(predicate, ExpressionType.CONJUNCTION_AND,
                ExpressionType.COMPARE_GREATERTHAN,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_CONSTANT,
                ExpressionType.OPERATOR_NOT,
                ExpressionType.OPERATOR_IS_NULL,
                ExpressionType.VALUE_TUPLE);

        // Distributed inner and outer tables -NLIJ/inlined IndexScan
        query = "SELECT * FROM P2 RIGHT JOIN P3 ON P3.A" +
                joinOp + "P2.A AND P2.A < 0 WHERE P2.A IS NULL";
        lpn = compileToFragments(query);
        assertProjectingCoordinator(lpn);
        pn = lpn.get(1);
        assertTopDownTree(pn, PlanNodeType.SEND,
                PlanNodeType.NESTLOOPINDEX,
                PlanNodeType.INDEXSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.NESTLOOPINDEX);
        nlij = (NestLoopIndexPlanNode) node;
        assertEquals(JoinType.LEFT, nlij.getJoinType());
        assertNull(nlij.getPreJoinPredicate());
        assertNull(nlij.getJoinPredicate());
        predicate = nlij.getWherePredicate();
        assertExprTopDownTree(predicate, ExpressionType.OPERATOR_IS_NULL,
                ExpressionType.VALUE_TUPLE);

        indexScan = nlij.getInlineIndexScan();
        assertEquals(IndexLookupType.EQ, indexScan.getLookupType());
        predicate = indexScan.getEndExpression();
        assertExprTopDownTree(predicate, joinOp.toOperator(),
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);

        predicate = indexScan.getPredicate();
        assertExprTopDownTree(predicate, ExpressionType.COMPARE_LESSTHAN,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_CONSTANT);
    }

    public void testNonSupportedJoin() {
        for (JoinOp joinOp : JoinOp.JOIN_OPS) {
            perJoinOpTestNonSupportedJoin(joinOp);
        }
    }

    private void perJoinOpTestNonSupportedJoin(JoinOp joinOp) {
        String query;
        String pattern;
        // JOIN with parentheses (HSQL limitation)
        query = "SELECT R2.C FROM (R1 JOIN R2 ON R1.C" +
                joinOp + "R2.C) JOIN R3 ON R1.C" +
                joinOp + "R3.C";
        pattern = "object not found: R1.C";
        failToCompile(query, pattern);

        // JOIN with join hierarchy (HSQL limitation)
        query = "SELECT * FROM R1 JOIN R2 JOIN R3 ON R1.C" +
                joinOp + "R2.C ON R1.C" +
                joinOp + "R3.C";
        pattern = "unexpected token";
        failToCompile(query, pattern);
    }

    public void testOuterJoinSimplification() {
        for (JoinOp joinOp : JoinOp.JOIN_OPS) {
            perJoinOpTestOuterJoinSimplification(joinOp);
        }
    }

    private void perJoinOpTestOuterJoinSimplification(JoinOp joinOp) {
        String query;
        AbstractPlanNode pn;
        AbstractPlanNode node;
        NestLoopPlanNode nlj;
        SeqScanPlanNode seqScan;
        IndexScanPlanNode indexScan;
        AbstractExpression predicate;
        NestLoopIndexPlanNode nlij;

        query = "SELECT * FROM R1 LEFT JOIN R2 ON R1.C" +
                joinOp + "R2.C WHERE R2.C IS NOT NULL";
        pn = compileToTopDownTree(query, 5,
                true,
                PlanNodeType.SEND,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn,
                true,
                PlanNodeType.SEND,
                PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        assertEquals(nlj.getJoinType(), JoinType.INNER);

        query = "SELECT * FROM R1 LEFT JOIN R2 ON R1.C" +
                joinOp + "R2.C WHERE R2.C > 0";
        pn = compileToTopDownTree(query, 5, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                 PlanNodeType.PROJECTION,
                 PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        assertEquals(nlj.getJoinType(), JoinType.INNER);

        query = "SELECT * FROM R1 RIGHT JOIN R2 ON R1.C" +
                joinOp + "R2.C WHERE R1.C > 0";
        pn = compileToTopDownTree(query, 5, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                 PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        assertEquals(nlj.getJoinType(), JoinType.INNER);

        query = "SELECT * FROM R1 LEFT JOIN R3 ON R1.C" +
                joinOp + "R3.C WHERE R3.A > 0";
        pn = compileToTopDownTree(query, 5,
                true,
                PlanNodeType.SEND,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.INDEXSCAN);
        node = followAssertedLeftChain(pn,
                    true,
                    PlanNodeType.SEND,
                    PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        assertEquals(nlj.getJoinType(), JoinType.INNER);

        query = "SELECT * FROM R1 LEFT JOIN R3 ON R1.C" +
                joinOp + "R3.A WHERE R3.A > 0";
        if (joinOp == JoinOp.EQUAL) { // weaken test for now
           pn = compileToTopDownTree(query, 5,
                   PlanNodeType.SEND,
                   PlanNodeType.NESTLOOPINDEX,
                   PlanNodeType.SEQSCAN);
           node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                   PlanNodeType.NESTLOOPINDEX);
            nlij = (NestLoopIndexPlanNode) node;
            assertEquals(nlij.getJoinType(), JoinType.INNER);
        }

        query = "SELECT * FROM R1 LEFT JOIN R2 ON R1.C" +
                joinOp + "R2.C WHERE ABS(R2.C) < 10";
        pn = compileToTopDownTree(query, 5, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                 PlanNodeType.PROJECTION,
                 PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        assertEquals(nlj.getJoinType(), JoinType.INNER);

        query = "SELECT * FROM R1 RIGHT JOIN R2 ON R1.C" +
                joinOp + "R2.C WHERE ABS(R1.C) < 10";
        pn = compileToTopDownTree(query, 5,
                    true,
                    PlanNodeType.SEND,
                    PlanNodeType.NESTLOOP,
                    PlanNodeType.SEQSCAN,
                    PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn,
                    true,
                    PlanNodeType.SEND,
                    PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        assertEquals(nlj.getJoinType(), JoinType.INNER);

        query = "SELECT * FROM R1 LEFT JOIN R2 ON R1.C" +
                joinOp + "R2.C WHERE ABS(R1.C) < 10";
        pn = compileToTopDownTree(query, 5,
                true,
                PlanNodeType.SEND,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn,
                true,
                PlanNodeType.SEND,
                PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        assertEquals(nlj.getJoinType(), JoinType.LEFT);

        query = "SELECT * FROM R1 RIGHT JOIN R2 ON R1.C" +
                joinOp + "R2.C WHERE ABS(R2.C) < 10";
        pn = compileToTopDownTree(query, 5, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                 PlanNodeType.PROJECTION,
                 PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        assertEquals(nlj.getJoinType(), JoinType.LEFT);

        query = "SELECT * FROM R1 LEFT JOIN R2 ON R1.C" +
                joinOp + "R2.C WHERE ABS(R2.C) < 10 AND R1.C = 3";
        pn = compileToTopDownTree(query, 5,
                true,
                PlanNodeType.SEND,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn,
                 true,
                 PlanNodeType.SEND,
                 PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        assertEquals(nlj.getJoinType(), JoinType.INNER);

        query = "SELECT * FROM R1 LEFT JOIN R2 ON R1.C" +
                joinOp + "R2.C WHERE ABS(R2.C) <  10 OR R2.C IS NOT NULL";
        pn = compileToTopDownTree(query, 5, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                 PlanNodeType.PROJECTION,
                 PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        assertEquals(nlj.getJoinType(), JoinType.INNER);

        query = "SELECT * FROM R1 LEFT JOIN R2 ON R1.C" +
                joinOp + "R2.C WHERE ABS(R1.C) <  10 AND R1.C > 3";
        pn = compileToTopDownTree(query, 5, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                 PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        assertEquals(nlj.getJoinType(), JoinType.LEFT);

        query = "SELECT * FROM R1 LEFT JOIN R2 ON R1.C" +
                joinOp + "R2.C WHERE ABS(R1.C) <  10 OR R2.C IS NOT NULL";
        pn = compileToTopDownTree(query, 5, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                 PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        assertEquals(nlj.getJoinType(), JoinType.LEFT);

        // Test with seqscan with different filers.
        query = "SELECT R2.A, R1.* FROM R1 LEFT OUTER JOIN R2 ON R2.A" +
                joinOp + "R1.A WHERE R2.A > 3";
        pn = compileToTopDownTree(query, 4,
                true,
                PlanNodeType.SEND,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);
        //* enable for debug */ System.out.println(pn.toExplainPlanString());
        node = followAssertedLeftChain(pn,
                    true,
                    PlanNodeType.SEND,
                    PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        assertEquals(nlj.getJoinType(), JoinType.INNER);
        assertNull(nlj.getPreJoinPredicate());
        predicate = nlj.getJoinPredicate();
        assertExprTopDownTree(predicate, joinOp.toOperator(),
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);
        assertNull(nlj.getWherePredicate());

        query = "SELECT R2.A, R1.* FROM R1 LEFT OUTER JOIN R2 ON R2.A" +
                joinOp + "R1.A WHERE R2.A IS NULL";
        pn = compileToTopDownTree(query, 4, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                 PlanNodeType.PROJECTION,
                 PlanNodeType.NESTLOOP);
        nlj = (NestLoopPlanNode) node;
        assertEquals(nlj.getJoinType(), JoinType.LEFT);
        assertNull(nlj.getPreJoinPredicate());
        predicate = nlj.getJoinPredicate();
        assertExprTopDownTree(predicate, joinOp.toOperator(),
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);

        predicate = nlj.getWherePredicate();
        assertExprTopDownTree(predicate, ExpressionType.OPERATOR_IS_NULL,
                ExpressionType.VALUE_TUPLE);

        seqScan = (SeqScanPlanNode) nlj.getChild(0);
        assertNull(seqScan.getPredicate());

        seqScan = (SeqScanPlanNode) nlj.getChild(1);
        assertNull(seqScan.getPredicate());

        if (joinOp != JoinOp.EQUAL) { // weaken test for now
            return;
        }
        query = "SELECT b.A, a.* FROM R1 a LEFT OUTER JOIN R4 b ON b.A" +
                joinOp + "a.A AND b.C " +
                joinOp + " a.C AND a.D " +
                joinOp + " b.D WHERE b.A IS NULL";
        pn = compileToTopDownTree(query, 4, PlanNodeType.SEND,
                 PlanNodeType.PROJECTION,
                 PlanNodeType.NESTLOOPINDEX,
                 PlanNodeType.SEQSCAN);
        //* enable for debug */ System.out.println(pn.toExplainPlanString());
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOPINDEX);
        nlij = (NestLoopIndexPlanNode) node;
        assertEquals(nlij.getJoinType(), JoinType.LEFT);
        assertNull(nlij.getPreJoinPredicate());
        assertNull(nlij.getJoinPredicate());
        predicate = nlij.getWherePredicate();
        assertExprTopDownTree(predicate, ExpressionType.OPERATOR_IS_NULL,
                ExpressionType.VALUE_TUPLE);

        indexScan = nlij.getInlineIndexScan();
        assertEquals(IndexLookupType.EQ, indexScan.getLookupType());
        predicate = indexScan.getEndExpression();
        assertExprTopDownTree(predicate, ExpressionType.CONJUNCTION_AND,
                ExpressionType.CONJUNCTION_AND,
                ExpressionType.COMPARE_EQUAL,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE,
                ExpressionType.COMPARE_EQUAL,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE,
                ExpressionType.COMPARE_EQUAL,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);
        assertNull(indexScan.getPredicate());


        query = "SELECT b.A, a.* FROM R1 a LEFT OUTER JOIN R4 b ON b.A" +
                joinOp + "a.A AND b.C " +
                joinOp + " a.C AND a.D " +
                joinOp + " b.D WHERE b.B + b.A IS NULL";
        pn = compileToTopDownTree(query, 4, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOPINDEX,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOPINDEX);
        nlij = (NestLoopIndexPlanNode) node;
        assertEquals(nlij.getJoinType(), JoinType.LEFT);
        assertNull(nlij.getPreJoinPredicate());
        assertNull(nlij.getJoinPredicate());
        predicate = nlij.getWherePredicate();
        assertExprTopDownTree(predicate, ExpressionType.OPERATOR_IS_NULL,
                ExpressionType.OPERATOR_PLUS,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);

        query = "SELECT a.* FROM R1 a LEFT OUTER JOIN R5 b ON b.A" +
                joinOp + "a.A WHERE b.A IS NULL";
        pn = compileToTopDownTree(query, 3, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOPINDEX,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOPINDEX);
        nlij = (NestLoopIndexPlanNode) node;
        assertEquals(nlij.getJoinType(), JoinType.LEFT);
        assertNull(nlij.getPreJoinPredicate());
        assertNull(nlij.getJoinPredicate());
        predicate = nlij.getWherePredicate();
        assertExprTopDownTree(predicate, ExpressionType.OPERATOR_IS_NULL,
                ExpressionType.VALUE_TUPLE);
    }

    public void testMoreThan5TableJoins() {
       for (JoinOp joinOp : JoinOp.JOIN_OPS) {
           if (joinOp != JoinOp.EQUAL) { // weaken test for now
               continue;
           }
           perJoinOpTestMoreThan5TableJoins(joinOp);
       }
    }

    private void perJoinOpTestMoreThan5TableJoins(JoinOp joinOp) {
        String query;
        List<AbstractPlanNode> lpn;

        // INNER JOIN with >5 tables.
        query = "SELECT R1.C FROM R3, R2, P1, P2, P3, R1 WHERE R3.A" +
                joinOp + "R2.A AND R2.A" +
                joinOp + "P1.A AND P1.A" +
                joinOp + "P2.A AND P3.A" +
                joinOp + "P2.A AND R1.C" +
                joinOp + "R2.C";
        lpn = compileToFragments(query);
        assertProjectingCoordinator(lpn);

        assertTopDownTree(lpn.get(1), PlanNodeType.SEND,
                PlanNodeType.NESTLOOP,
                PlanNodeType.NESTLOOPINDEX,
                PlanNodeType.NESTLOOPINDEX,
                PlanNodeType.NESTLOOP,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);

        // OUTER JOIN with >5 tables.
        query = "SELECT R1.C FROM R3, R2, P1, P2, P3 LEFT OUTER JOIN R1 ON R1.C" +
                joinOp + "R2.C WHERE R3.A" +
                joinOp + "R2.A AND R2.A" +
                joinOp + "P1.A AND P1.A" +
                joinOp + "P2.A AND P3.A" +
                joinOp + "P2.A";
        lpn = compileToFragments(query);
        assertProjectingCoordinator(lpn);

        assertTopDownTree(lpn.get(1), PlanNodeType.SEND,
                PlanNodeType.NESTLOOP,
                PlanNodeType.NESTLOOPINDEX,
                PlanNodeType.NESTLOOPINDEX,
                PlanNodeType.NESTLOOP,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);

    }

    public void testAmbigousIdentifierInSelectList() throws Exception {
        String query;
        String pattern;

        // Since A is in the using list, lr.a and rr.a are the same.
        // This is not ambiguous.  The two aliases reference the same column.
        query = "SELECT R1.A, A FROM R1 WHERE A > 0;";
        compileToTopDownTree(query, 2, PlanNodeType.SEND,
                PlanNodeType.SEQSCAN);

        query = "SELECT lr.a FROM r1 lr, r1 rr ORDER BY a;";
        compileToTopDownTree(query, 1, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.ORDERBY,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);

        query = "SELECT lr.a alias, lr.a, a, lr.a+1 aliasexp, lr.a+1, a+1 " +
                "FROM r1 lr ORDER BY a;";
        compileToTopDownTree(query, 6, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.ORDERBY,
                PlanNodeType.SEQSCAN);

        query = "SELECT lr.a a, a FROM r1 lr JOIN r1 rr using (a) ORDER BY a;";
        compileToTopDownTree(query, 2, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.ORDERBY,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);

        query = "SELECT lr.a a, rr.a FROM r1 lr JOIN r1 rr using (a) ORDER BY a;";
        compileToTopDownTree(query, 2, PlanNodeType.SEND,
                PlanNodeType.ORDERBY,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);

        // This is not actually an ambiguous query.  This is actually ok.
        query = "SELECT * FROM R2 WHERE A IN (SELECT A FROM R1);";
        compileToTopDownTree(query, 2, PlanNodeType.SEND,
                PlanNodeType.SEQSCAN);

        query = "SELECT R3.C, C FROM R1 INNER JOIN R2 USING(C) " +
                " INNER JOIN R3 USING(C);";
        compileToTopDownTree(query, 2, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOP,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);

        // This one is ok too. There are several common columns in R2, R1.
        // But they are fully qualified as R1.A, R2.A and so forth when *
        // is expanded.
        query = "SELECT * FROM R2, R1";
        compileToTopDownTree(query, 5, PlanNodeType.SEND,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);

        query = "SELECT R1.C FROM R1 INNER JOIN R2 USING (C), R3";
        compileToTopDownTree(query, 1, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOP,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);

        query = "SELECT R2.C FROM R1 INNER JOIN R2 USING (C), R3";
        compileToTopDownTree(query, 1, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOP,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);

        query = "SELECT R3.C, R1.C FROM R1 INNER JOIN R2 USING(C), R3;";
        compileToTopDownTree(query, 2, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOP,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);

        query = "SELECT C, C FROM R1 GROUP BY C ORDER BY C;";
        compileToTopDownTree(query, 2, PlanNodeType.SEND,
                PlanNodeType.ORDERBY,
                PlanNodeType.PROJECTION,
                PlanNodeType.SEQSCAN);

        query = "SELECT lr.a a, rr.a a FROM r1 lr, r2 rr ORDER BY a;";
        pattern = "The name \"A\" in an order by expression is ambiguous.  It's in columns: LR.A, RR.A";
        failToCompile(query, pattern);

        // Simple ambiguous column reference.
        query = "SELECT A, C FROM R1, R2;";
        pattern = "Column \"A\" is ambiguous.  It's in tables: R1, R2";
        failToCompile(query, pattern);

        // Ambiguous reference in an arithmetic expression.
        query = "SELECT A + C FROM R1, R2;";
        pattern = "Column \"A\" is ambiguous.  It's in tables: R1, R2";
        failToCompile(query, pattern);

        query = "SELECT sqrt(A) FROM R1, R2;";
        pattern = "Column \"A\" is ambiguous.  It's in tables: R1, R2";
        failToCompile(query, pattern);

        // Ambiguous reference in a WHERE clause.
        query = "SELECT NOTC FROM R1, R3_NOC WHERE A > 100;";
        pattern = "Column \"A\" is ambiguous.  It's in tables: R1, R3_NOC";
        failToCompile(query, pattern);

        query = "SELECT NOTC FROM R1, R3_NOC WHERE A > sqrt(NOTC);";
        pattern = "Column \"A\" is ambiguous.  It's in tables: R1, R3_NOC";
        failToCompile(query, pattern);

        query = "SELECT NOTC FROM R1, R3_NOC WHERE sqrt(A) > sqrt(NOTC);";
        pattern = "Column \"A\" is ambiguous.  It's in tables: R1, R3_NOC";
        failToCompile(query, pattern);

        query = "SELECT NOTC FROM R1 JOIN R3_NOC ON sqrt(A) > sqrt(NOTC);";
        pattern = "Column \"A\" is ambiguous.  It's in tables: R1, R3_NOC";
        failToCompile(query, pattern);

        // Ambiguous reference to an unconstrained column in a join.  That is,
        // C is in both R1 and R3, R1 and R3 are joined together, but not on C.
        // Note that we test above for a similar case, with three joined tables.
        query = "SELECT C FROM R1 INNER JOIN R3 USING(A);";
        pattern = "Column \"C\" is ambiguous.  It's in tables: R1, R3";
        failToCompile(query, pattern);

        query = "SELECT C FROM R1 INNER JOIN R3 using(C), R2;";
        pattern = "Column \"C\" is ambiguous.  It's in tables: USING(C), R2";
        failToCompile(query, pattern);

        // Ambiguous references in GROUP BY expressions.
        query = "SELECT NOTC FROM R1, R3_NOC GROUP BY A;";
        pattern = "Column \"A\" is ambiguous.  It's in tables: R1, R3_NOC";
        failToCompile(query, pattern);

        query = "SELECT NOTC FROM R1, R3_NOC GROUP BY sqrt(A);";
        pattern = "Column \"A\" is ambiguous.  It's in tables: R1, R3_NOC";
        failToCompile(query, pattern);

        query = "SELECT sqrt(R1.A) FROM R1, R3_NOC GROUP BY R1.A having count(A) + 2 * sum(A) > 2;";
        pattern = "Column \"A\" is ambiguous.  It's in tables: R1, R3_NOC";
        failToCompile(query, pattern);

        // Ambiguous references in subqueries.
        query = "SELECT ALPHA FROM (SELECT SQRT(A) AS ALPHA FROM R1) AS S1, (SELECT SQRT(C) AS ALPHA FROM R1) AS S2;";
        pattern = "Column \"ALPHA\" is ambiguous.  It's in tables: S1, S2";
        failToCompile(query, pattern);

        query = "SELECT ALPHA FROM (SELECT SQRT(A), SQRT(C) FROM R1, R3) AS S1, (SELECT SQRT(C) AS ALPHA FROM R1) AS S2;";
        pattern = "Column \"A\" is ambiguous.  It's in tables: R1, R3";
        failToCompile(query, pattern);

        query = "select R3.C, C from R1 inner join R2 using(C) inner join R3 on C=R3.A;";
        pattern = "Column \"C\" is ambiguous.  It's in tables: USING(C), R3";
        failToCompile(query, pattern);

        // Ambiguous columns in an ORDER BY expression.

        query = "SELECT LR.A, RR.A FROM R1 LR, R1 RR ORDER BY A;";
        pattern = "The name \"A\" in an order by expression is ambiguous.  It's in columns: LR.A, RR.A.";
        failToCompile(query, pattern);

        // Note that LT.A and RT.A are not considered here.
        query = "SELECT LT.A AS LA, RT.A AS RA FROM R1 AS LT, R1 AS RT ORDER BY A;";
        pattern = "Column \"A\" is ambiguous.  It's in tables: LT, RT";
        failToCompile(query, pattern);

        // Two columns in the SELECT list with the same name.  This complicates
        // checking for ORDER BY aliases.
        query = "SELECT LT.A AS LA, RT.A AS LA FROM R1 AS LT, R1 AS RT ORDER BY LA;";
        pattern = "The name \"LA\" in an order by expression is ambiguous.  It's in columns: LA(0), LA(1)";
        failToCompile(query, pattern);

        query = "SELECT NOTC FROM R1, R3_NOC ORDER BY A;";
        pattern = "Column \"A\" is ambiguous.  It's in tables: R1, R3_NOC";
        failToCompile(query, pattern);

        query = "SELECT NOTC FROM R1, R3_NOC ORDER BY sqrt(A);";
        pattern = "Column \"A\" is ambiguous.  It's in tables: R1, R3_NOC";
        failToCompile(query, pattern);

        // Ambiguous columns in an ORDER BY expression.
        query = "SELECT LR.A, RR.A FROM R1 LR, R1 RR ORDER BY A;";
        pattern = "The name \"A\" in an order by expression is ambiguous.  It's in columns: LR.A, RR.A";
        failToCompile(query, pattern);

        // Note that LT.A and RT.A are not considered here.
        query = "SELECT LT.A AS LA, RT.A AS RA FROM R1 AS LT, R1 AS RT ORDER BY A;";
        pattern = "Column \"A\" is ambiguous.  It's in tables: LT, RT";
        failToCompile(query, pattern);

        query = "SELECT LT.A, RT.A FROM R1 AS LT, R1 AS RT ORDER BY A";
        pattern = "The name \"A\" in an order by expression is ambiguous.  It's in columns: LT.A, RT.A";
        failToCompile(query, pattern);

        // Two columns in the SELECT list with the same name.  This complicates
        // checking for ORDER BY aliases.
        query = "SELECT (R1.A + 1) A, A FROM R1 ORDER BY A";
        pattern = "The name \"A\" in an order by expression is ambiguous.  It's in columns: A(0), R1.A.";
        failToCompile(query, pattern);

        query = "SELECT LT.A AS LA, RT.A AS LA FROM R1 AS LT, R1 AS RT ORDER BY LA;";
        pattern = "The name \"LA\" in an order by expression is ambiguous.  It's in columns: LA(0), LA(1)";
        failToCompile(query, pattern);

        query = "SELECT NOTC FROM R1, R3_NOC ORDER BY A;";
        pattern = "Column \"A\" is ambiguous.  It's in tables: R1, R3_NOC";
        failToCompile(query, pattern);

        query = "SELECT NOTC FROM R1, R3_NOC ORDER BY sqrt(A);";
        pattern = "Column \"A\" is ambiguous.  It's in tables: R1, R3_NOC";
        failToCompile(query, pattern);

        for (JoinOp joinOp : JoinOp.JOIN_OPS) {
            perJoinOpTestAmbigousIdentifierInSelectList(joinOp);
        }
    }

    private void perJoinOpTestAmbigousIdentifierInSelectList(JoinOp joinOp) throws Exception {
        String query;
        String pattern;

        // R1 JOIN R2 ON R1.A" + joinOp + "R2.A is not R1 JOIN R2 using(A).
        query = "SELECT lr.a a, rr.a a FROM r1 lr JOIN r1 rr ON lr.a " +
                joinOp + "rr.a ORDER BY a;";
        pattern = "The name \"A\" in an order by expression is ambiguous.  It's in columns: LR.A, RR.A";
        failToCompile(query, pattern);

        query = "SELECT R1.C FROM R1 INNER JOIN R2 USING (C), R3 WHERE R1.A" +
                joinOp + "R3.A";
        compileToTopDownTree(query, 1, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOP,
                null,
                PlanNodeType.SEQSCAN);

        query = "SELECT C FROM R1 INNER JOIN R2 using(C), R3 WHERE R1.A" +
                joinOp + "R3.A;";
        pattern = "Column \"C\" is ambiguous.  It's in tables: USING(C), R3";
        failToCompile(query, pattern);
    }

    public void testUsingColumns() {
        String query;
        AbstractPlanNode pn;
        OrderByPlanNode orderBy;
        NestLoopPlanNode nlj;
        AggregatePlanNode aggr;
        NodeSchema selectColumns;
        SchemaColumn col;
        AbstractExpression colExp;
        AbstractExpression predicate;

        // Test USING column
        query = "SELECT MAX(R1.A), C FROM R1 FULL JOIN R2 USING (C) " +
                "WHERE C > 0 GROUP BY C ORDER BY C";
        pn = compileToTopDownTree(query, 2, PlanNodeType.SEND,
                PlanNodeType.ORDERBY,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);

        // ORDER BY column
        orderBy = (OrderByPlanNode) pn.getChild(0);
        List<AbstractExpression> s = orderBy.getSortExpressions();
        assertEquals(1, s.size());
        assertEquals(ExpressionType.VALUE_TUPLE, s.get(0).getExpressionType());

        // WHERE
        nlj = (NestLoopPlanNode) orderBy.getChild(0);
        assertNull(nlj.getPreJoinPredicate());
        predicate = nlj.getJoinPredicate();
        assertExprTopDownTree(predicate, ExpressionType.COMPARE_EQUAL,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);
        predicate = nlj.getWherePredicate();
        assertExprTopDownTree(predicate, ExpressionType.COMPARE_GREATERTHAN,
                ExpressionType.OPERATOR_CASE_WHEN,
                ExpressionType.OPERATOR_IS_NULL,
                ExpressionType.VALUE_TUPLE,
                ExpressionType.OPERATOR_ALTERNATIVE,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE,
                ExpressionType.VALUE_CONSTANT);

        // GROUP BY
        aggr = (AggregatePlanNode) nlj.getInlinePlanNode(PlanNodeType.HASHAGGREGATE);
        assertNotNull(aggr);
        List<AbstractExpression> g = aggr.getGroupByExpressions();
        assertEquals(1, g.size());
        assertExprTopDownTree(g.get(0), ExpressionType.OPERATOR_CASE_WHEN,
                ExpressionType.OPERATOR_IS_NULL,
                ExpressionType.VALUE_TUPLE,
                ExpressionType.OPERATOR_ALTERNATIVE,
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);

        // Test three table full join
        query = "SELECT C FROM R1 FULL JOIN R2 USING (C) FULL JOIN R3 USING (C)";
        pn = compileToTopDownTree(query, 1, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOP,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);
        selectColumns = pn.getOutputSchema();
        col = selectColumns.getColumn(0);
        assertEquals("C", col.getColumnAlias());
        colExp = col.getExpression();
        assertEquals(ExpressionType.VALUE_TUPLE, colExp.getExpressionType());

        // Test three table INNER join. USING C column should be resolved
        query = "SELECT C FROM R1 JOIN R2 USING (C) JOIN R3 USING (C)";
        pn = compileToTopDownTree(query, 1, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOP,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);
        selectColumns = pn.getOutputSchema();
        assertEquals(1, selectColumns.size());
        col = selectColumns.getColumn(0);
        assertEquals("C", col.getColumnAlias());
        colExp = col.getExpression();
        assertEquals(ExpressionType.VALUE_TUPLE, colExp.getExpressionType());

        // Test two table LEFT join. USING C column should be resolved
        query = "SELECT C FROM R1 LEFT JOIN R2 USING (C)";
        pn = compileToTopDownTree(query, 1, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);
        selectColumns = pn.getOutputSchema();
        assertEquals(1, selectColumns.size());
        col = selectColumns.getColumn(0);
        assertEquals("C", col.getColumnAlias());
        colExp = col.getExpression();
        assertEquals(ExpressionType.VALUE_TUPLE, colExp.getExpressionType());

        // Test two table RIGHT join. USING C column should be resolved
        query = "SELECT C FROM R1 RIGHT JOIN R2 USING (C)";
        pn = compileToTopDownTree(query, 1, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);
        selectColumns = pn.getOutputSchema();
        assertEquals(1, selectColumns.size());
        col = selectColumns.getColumn(0);
        assertEquals("C", col.getColumnAlias());
        colExp = col.getExpression();
        assertEquals(ExpressionType.VALUE_TUPLE, colExp.getExpressionType());
    }

    public void testJoinOrders() {
        String query;
        AbstractPlanNode pn;
        AbstractPlanNode node;
        AbstractScanPlanNode sn;

        // R1 is an outer node - has one filter
        query = "SELECT * FROM R2 JOIN R1 USING (C) WHERE R1.A > 0";
        pn = compileToTopDownTree(query, 4, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN);
        sn = (AbstractScanPlanNode) node;
        assertEquals("R1", sn.getTargetTableName());

        // R2 is an outer node - R2.A = 3 filter is discounter more than R1.A > 0
        query = "SELECT * FROM R1 JOIN R2 USING (C) WHERE R1.A > 0 AND R2.A = 3";
        pn = compileToTopDownTree(query, 4, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN);
        sn = (AbstractScanPlanNode) node;
        assertEquals("R2", sn.getTargetTableName());

        // R2 is an outer node - R2.A = 3 filter is discounter more than two non-EQ filters
        query = "SELECT * FROM R1 JOIN R2 USING (C) WHERE R1.A > 0 AND R1.A < 3 AND R2.A = 3";
        pn = compileToTopDownTree(query, 4, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN);
        sn = (AbstractScanPlanNode) node;
        assertEquals("R2", sn.getTargetTableName());

        // R1 is an outer node - EQ + non-EQ overweight EQ
        query = "SELECT * FROM R1 JOIN R2 USING (C) WHERE R1.A = 0 AND R1.D < 3 AND R2.A = 3";
        pn = compileToTopDownTree(query, 4, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOP,
                PlanNodeType.SEQSCAN);
        sn = (AbstractScanPlanNode) node;
        assertEquals("R1", sn.getTargetTableName());

        for (JoinOp joinOp : JoinOp.JOIN_OPS) {
            perJoinOpTestJoinOrders(joinOp);
        }
    }

    private void perJoinOpTestJoinOrders(JoinOp joinOp) {
        String query;
        AbstractPlanNode pn;
        AbstractPlanNode node;
        SeqScanPlanNode seqScan;
        NestLoopIndexPlanNode nlij;
        IndexScanPlanNode indexScan;
        AbstractExpression predicate;

        if (joinOp != JoinOp.EQUAL) { // weaken test for now
            return;
        }
        // Index Join (R3.A) still has a lower cost compare to a Loop Join
        // despite the R3.C = 0 equality filter on the inner node
        query = "SELECT * FROM R1 JOIN R3 ON R3.A" +
                joinOp + "R1.A WHERE R3.C = 0";
        pn = compileToTopDownTree(query, 5, PlanNodeType.SEND,
                PlanNodeType.NESTLOOPINDEX,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.NESTLOOPINDEX,
                PlanNodeType.SEQSCAN);
        seqScan = (SeqScanPlanNode) node;
        assertEquals("R1", seqScan.getTargetTableName());

        // R3.A is an INDEX. Both children are IndexScans. With everything being equal,
        // the Left table (L) has fewer filters and should be an inner node
        query = "SELECT L.A, R.A FROM R3 L JOIN R3 R ON L.A" +
                joinOp + "R.A WHERE R.A > 3 AND R.C  = 3 AND L.A > 2 ;";
        pn = compileToTopDownTree(query, 2, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOPINDEX,
                PlanNodeType.INDEXSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.PROJECTION,
                PlanNodeType.NESTLOOPINDEX,
                PlanNodeType.INDEXSCAN);
        indexScan = (IndexScanPlanNode) node;
        assertEquals("R", indexScan.getTargetTableAlias());

        // NLIJ with inline inner IndexScan over R2 using its partial index is a winner
        // over the NLJ with R2 on the outer side
        query = "SELECT * FROM R3 JOIN R2 ON R3.C" +
                joinOp + "R2.C WHERE R2.C > 100;";
        pn = compileToTopDownTree(query, 4, PlanNodeType.SEND,
                PlanNodeType.NESTLOOPINDEX,
                PlanNodeType.SEQSCAN);
        node = followAssertedLeftChain(pn, PlanNodeType.SEND,
                PlanNodeType.NESTLOOPINDEX);
        nlij = (NestLoopIndexPlanNode) node;
        indexScan = nlij.getInlineIndexScan();
        assertEquals(IndexLookupType.EQ, indexScan.getLookupType());
        predicate = indexScan.getEndExpression();
        assertExprTopDownTree(predicate, joinOp.toOperator(),
                ExpressionType.VALUE_TUPLE, ExpressionType.VALUE_TUPLE);
        assertNull(indexScan.getPredicate());
        assertEquals("PARTIAL_IND2", indexScan.getTargetIndexName());

        seqScan = (SeqScanPlanNode) nlij.getChild(0);
        assertEquals("R3", seqScan.getTargetTableName());

    }

    public void testExplainHighlights() {
        // These tests of critical aspects of join-related @Explain output were
        // migrated from the regression suite where they really did not belong.
        // They MAY be somewhat redundant with other less stringly tests in this
        // suite, but they do have the advantage of covering some key aspects of
        // explain string generation in an informal easily-maintained way that
        // does not get bogged down in the precise explain string syntax.

        String query;
        String explained;
        for (JoinOp joinOp : JoinOp.JOIN_OPS) {
            int notDistinctCount = joinOp == JoinOp.NOT_DISTINCT ? 1 : 0;
            query = "SELECT P1.A, P1.C, P3.A, P3.F " +
                    "FROM P1 FULL JOIN P3 ON P1.A" +
                    joinOp + "P3.A AND P1.A = ? AND P3.F = 1 " +
                    "ORDER BY P1.A, P1.C, P3.A, P3.F";
            explained = buildExplainPlan(compileToFragments(query));
            assertTrue(explained.contains("NESTLOOP INDEX FULL JOIN"));
            assertEquals(notDistinctCount, StringUtils.countMatches(explained, "NOT DISTINCT"));
            query = "SELECT R1.A, R1.C, R3.A, R3.C " +
                    "FROM R1 FULL JOIN R3 ON R3.A" +
                    joinOp + "R1.A AND R3.A < 2 " +
                    "ORDER BY R1.A, R1.D, R3.A, R3.C";
            explained = buildExplainPlan(compileToFragments(query));
            //* enable to debug */ System.out.println("DEBUG: " + explained);
            assertTrue(explained.contains("NESTLOOP INDEX FULL JOIN"));
            assertEquals(notDistinctCount, StringUtils.countMatches(explained, "NOT DISTINCT"));
            query = "SELECT LHS.A, LHS.C, RHS.A, RHS.C " +
                    "FROM R3 LHS FULL JOIN R3 RHS ON LHS.A" +
                    joinOp + "RHS.A AND LHS.A < 2 " +
                    "ORDER BY 1, 2, 3, 4";
            explained = buildExplainPlan(compileToFragments(query));
            //* enable to debug */ System.out.println("DEBUG: " + explained);
            assertTrue(explained.contains("NESTLOOP INDEX FULL JOIN"));
            assertEquals(notDistinctCount, StringUtils.countMatches(explained, "NOT DISTINCT"));
            query = "SELECT * " +
                    "FROM R1 FULL JOIN R2 " +
                    "ON R1.A" +
                    joinOp + "R2.A RIGHT JOIN P2 " +
                    "ON P2.A" +
                    joinOp + "R1.A " +
                    "ORDER BY P2.A";
            explained = buildExplainPlan(compileToFragments(query));
            //* enable to debug */ System.out.println("DEBUG: " + explained);
            // Account for how IS NOT DISTINCT FROM does not reject all nulls.
            if (joinOp == JoinOp.EQUAL) { // weaken test for now
              assertFalse(explained.contains("FULL"));
              assertEquals(2, StringUtils.countMatches(explained, "LEFT"));
            }
            else {
                assertEquals(1, StringUtils.countMatches(explained, "FULL"));
                assertEquals(1, StringUtils.countMatches(explained, "LEFT"));
            }
            query = "SELECT * " +
                    "FROM R1 FULL JOIN R2 " +
                    "ON R1.A" +
                    joinOp + "R2.A LEFT JOIN P2 " +
                    "ON P2.A" +
                    joinOp + "R2.A " +
                    "ORDER BY P2.A";
            explained = buildExplainPlan(compileToFragments(query));
            assertTrue(explained.contains("FULL"));
            query = "SELECT * " +
                    "FROM R1 RIGHT JOIN R2 " +
                    "ON R1.A" +
                    joinOp + "R2.A FULL JOIN P2 " +
                    "ON R1.A" +
                    joinOp + "P2.A " +
                    "ORDER BY P2.A";
            explained = buildExplainPlan(compileToFragments(query));
            assertTrue(explained.contains("LEFT"));
            query = "SELECT * " +
                    "FROM R1 FULL JOIN R2 " +
                    "ON R1.A" +
                    joinOp + "R2.A FULL JOIN P2 " +
                    "ON R1.A" +
                    joinOp + "P2.A " +
                    "ORDER BY P2.A";
            explained = buildExplainPlan(compileToFragments(query));
            assertEquals(2, StringUtils.countMatches(explained, "FULL"));
            query = "SELECT MAX(R1.C), A " +
                    "FROM R1 FULL JOIN R2 USING (A) " +
                    "WHERE A > 0 GROUP BY A ORDER BY A";
            explained = buildExplainPlan(compileToFragments(query));
            assertEquals(1, StringUtils.countMatches(explained, "FULL"));
            query = "SELECT A " +
                    "FROM R1 FULL JOIN R2 USING (A) " +
                    "FULL JOIN R3 USING(A) " +
                    "WHERE A > 0 ORDER BY A";
            explained = buildExplainPlan(compileToFragments(query));
            assertEquals(2, StringUtils.countMatches(explained, "FULL"));
            query = "SELECT L.A " +
                    "FROM R3 L FULL JOIN R3 R " +
                    "ON L.C" +
                    joinOp + "R.C " +
                    "ORDER BY A";
            explained = buildExplainPlan(compileToFragments(query));
            assertEquals(1, StringUtils.countMatches(explained, "FULL"));
            assertEquals(1, StringUtils.countMatches(explained, "SORT"));
            query = "SELECT L.A, SUM(L.C) " +
                    "FROM R3 L FULL JOIN R3 R " +
                    "ON L.C" +
                    joinOp + "R.C " +
                    "GROUP BY L.A ORDER BY 1";
            explained = buildExplainPlan(compileToFragments(query));
            assertEquals(1, StringUtils.countMatches(explained, "FULL"));
            assertEquals(1, StringUtils.countMatches(explained, "SORT"));
            assertEquals(1, StringUtils.countMatches(explained, "Serial AGGREGATION"));
        }
    }

    @Override
    protected void setUp() throws Exception {
        setupSchema(TestJoinOrder.class.getResource("testplans-join-ddl.sql"),
                "testplansjoin", false);
    }

}
