/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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
import org.voltdb.expressions.ComparisonExpression;
import org.voltdb.expressions.ParameterValueExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AggregatePlanNode;
import org.voltdb.plannodes.HashAggregatePlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.LimitPlanNode;
import org.voltdb.plannodes.NestLoopIndexPlanNode;
import org.voltdb.plannodes.NestLoopPlanNode;
import org.voltdb.plannodes.NodeSchema;
import org.voltdb.plannodes.OrderByPlanNode;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.plannodes.ReceivePlanNode;
import org.voltdb.plannodes.SchemaColumn;
import org.voltdb.plannodes.SendPlanNode;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.plannodes.UnionPlanNode;
import org.voltdb.types.JoinType;
import org.voltdb.types.PlanNodeType;

public class TestSubQueries extends PlannerTestCase {

    public void testUnsupportedSyntax() {
        failToCompile("DELETE FROM R1 WHERE A IN (SELECT A A1 FROM R1 WHERE A>1)", "Unsupported subquery syntax");
    }

    private void checkOutputSchema(AbstractPlanNode planNode, String... columns) {
        if (columns.length > 0) {
            checkOutputSchema(null, planNode, columns);
        }
    }

    private void checkOutputSchema(String tableAlias, AbstractPlanNode planNode, String... columns) {
        NodeSchema schema = planNode.getOutputSchema();
        List<SchemaColumn> schemaColumn = schema.getColumns();
        assertEquals(columns.length, schemaColumn.size());

        for (int i = 0; i < schemaColumn.size(); ++i) {
            SchemaColumn col = schemaColumn.get(i);
            if (tableAlias != null) {
                assertTrue(col.getTableAlias().contains(tableAlias));
            }
            // Try to check column. If not available, check its column alias instead.
            if (col.getColumnName() == null || col.getColumnName().equals("")) {
                assertNotNull(col.getColumnAlias());
                assertEquals(columns[i], col.getColumnAlias());
            } else {
                assertEquals(columns[i], col.getColumnName());
            }
        }
    }

    private void checkSeqScanSubSelects(AbstractPlanNode scanNode, String tableAlias, String... columns) {
        assertTrue(scanNode instanceof SeqScanPlanNode);
        SeqScanPlanNode snode = (SeqScanPlanNode) scanNode;
        if (tableAlias != null) {
            assertEquals(tableAlias, snode.getTargetTableAlias());
        }

        checkOutputSchema(snode, columns);
    }

    private void checkPredicateComparisonExpression(AbstractPlanNode pn, String tableAlias) {
        AbstractExpression expr = ((SeqScanPlanNode) pn).getPredicate();
        assertTrue(expr instanceof ComparisonExpression);
        expr = expr.getLeft();
        assertTrue(expr instanceof TupleValueExpression);
        assertEquals(tableAlias, ((TupleValueExpression) expr).getTableAlias());
    }

    private void checkIndexedSubSelects(AbstractPlanNode indexNode, String tableName, String indexName, String... columns) {
        assertTrue(indexNode instanceof IndexScanPlanNode);
        IndexScanPlanNode idxNode = (IndexScanPlanNode) indexNode;
        if (tableName != null) {
            assertEquals(tableName, idxNode.getTargetTableName());
        }
        if (indexName != null) {
            String actualIndexName = idxNode.getTargetIndexName();
            assertTrue(actualIndexName.contains(indexName));
        }

        checkOutputSchema(idxNode, columns);
    }

    public void testSubSelects_Simple() {
        AbstractPlanNode pn;
        String tbName = "T1";

        pn = compile("select A, C FROM (SELECT A, C FROM R1) T1");
        pn = pn.getChild(0);

        checkSeqScanSubSelects(pn, tbName,  "A", "C");
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, "R1", "A", "C");


        pn = compile("select A, C FROM (SELECT A, C FROM R1) T1 WHERE A > 0");
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, tbName,  "A", "C");
        checkPredicateComparisonExpression(pn, tbName);
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, "R1", "A", "C");


        pn = compile("select A, C FROM (SELECT A, C FROM R1) T1 WHERE T1.A < 0");
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, tbName,  "A", "C");
        checkPredicateComparisonExpression(pn, tbName);
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, "R1", "A", "C");


        pn = compile("select A1, C1 FROM (SELECT A A1, C C1 FROM R1) T1 WHERE T1.A1 < 0");
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, tbName,  "A1", "C1");
        checkPredicateComparisonExpression(pn, tbName);
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, "R1", "A", "C");

        // With projection.
        pn = compile("select C1 FROM (SELECT A A1, C C1 FROM R1) T1 WHERE T1.A1 < 0");
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, tbName,  "C1");
        assertEquals(((SeqScanPlanNode) pn).getInlinePlanNodes().size(), 1);
        assertNotNull(((SeqScanPlanNode) pn).getInlinePlanNode(PlanNodeType.PROJECTION));
        checkPredicateComparisonExpression(pn, tbName);
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, "R1", "A", "C");

        // Complex columns in sub selects
        pn = compile("select C1 FROM (SELECT A+3 A1, C C1 FROM R1) T1 WHERE T1.A1 < 0");
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, tbName,  "C1");
        assertEquals(((SeqScanPlanNode) pn).getInlinePlanNodes().size(), 1);
        assertNotNull(((SeqScanPlanNode) pn).getInlinePlanNode(PlanNodeType.PROJECTION));

        checkPredicateComparisonExpression(pn, tbName);
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, "R1", "A1", "C");

        pn = compile("select C1 FROM (SELECT A+3, C C1 FROM R1) T1 WHERE T1.C1 < 0");
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, tbName,  "C1");
        assertEquals(((SeqScanPlanNode) pn).getInlinePlanNodes().size(), 1);
        assertNotNull(((SeqScanPlanNode) pn).getInlinePlanNode(PlanNodeType.PROJECTION));
        checkPredicateComparisonExpression(pn, tbName);
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, "R1",  "C1", "C");


        // select *
        pn = compile("select A, C FROM (SELECT * FROM R1) T1 WHERE T1.A < 0");
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, tbName,  "A", "C");
        checkPredicateComparisonExpression(pn, tbName);
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, "R1", "A", "C", "D");


        pn = compile("select * FROM (SELECT A, D FROM R1) T1 WHERE T1.A < 0");
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, tbName,  "A", "D");
        checkPredicateComparisonExpression(pn, tbName);
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, "R1", "A", "D");


        pn = compile("select A, C FROM (SELECT * FROM R1 where D > 3) T1 WHERE T1.A < 0");
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, tbName,  "A", "C");
        checkPredicateComparisonExpression(pn, tbName);
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, "R1", "A", "C", "D");
    }

    public void testSubSelects_Three_Levels() {
        AbstractPlanNode pn;

        // Three levels selects
        pn = compile("select A2 FROM " +
                "(SELECT A1 AS A2 FROM (SELECT A AS A1 FROM R1 WHERE A < 3) T1 WHERE T1.A1 > 0) T2  WHERE T2.A2 = 3");
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, "T2",  "A2");
        checkPredicateComparisonExpression(pn, "T2");
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, "T1",  "A1");
        checkPredicateComparisonExpression(pn, "T1");
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, "R1",  "A");
        checkPredicateComparisonExpression(pn, "R1");
    }

    public void testSubSelects_Function() {
        AbstractPlanNode pn;
        String tbName = "T1";

        // Function expression
        pn = compile("select ABS(C) FROM (SELECT A, C FROM R1) T1");
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, tbName,  "C1" );
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, "R1",  "A", "C" );

        // Should this really be supported ?
        failToCompile("select A, ABS(C) FROM (SELECT A A1, C FROM R1) T1",
                "user lacks privilege or object not found: A");
        failToCompile("select A+1, ABS(C) FROM (SELECT A A1, C FROM R1) T1",
                "user lacks privilege or object not found: A");

        // Use alias column from sub select instead.
        pn = compile("select A1, ABS(C) FROM (SELECT A A1, C FROM R1) T1");
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, tbName,  "A1", "C2" ); // hsql auto generated column alias C2.
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, "R1",  "A", "C" );

        pn = compile("select A1 + 3, ABS(C) FROM (SELECT A A1, C FROM R1) T1");
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, tbName,  "C1", "C2" );
        assertEquals(((SeqScanPlanNode) pn).getInlinePlanNodes().size(), 1);
        assertNotNull(((SeqScanPlanNode) pn).getInlinePlanNode(PlanNodeType.PROJECTION));
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, "R1",  "A", "C" );


        pn = compile("select A1 + 3, ABS(C) FROM (SELECT A A1, C FROM R1) T1 WHERE ABS(A1) > 3");
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, tbName,  "C1", "C2" );
        assertEquals(((SeqScanPlanNode) pn).getInlinePlanNodes().size(), 1);
        assertNotNull(((SeqScanPlanNode) pn).getInlinePlanNode(PlanNodeType.PROJECTION));
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, "R1",  "A", "C" );
    }

    public void testSubSelects_Aggregation_Groupby() {
        AbstractPlanNode pn;

        pn = compile("select A, C FROM (SELECT * FROM R1 WHERE A > 3 Limit 3) T1 ");
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, "T1",  "A", "C" );
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, "R1",  "A", "C", "D");
        checkPredicateComparisonExpression(pn, "R1");
        assertEquals(((SeqScanPlanNode) pn).getInlinePlanNodes().size(), 2);
        assertNotNull(((SeqScanPlanNode) pn).getInlinePlanNode(PlanNodeType.PROJECTION));
        assertNotNull(((SeqScanPlanNode) pn).getInlinePlanNode(PlanNodeType.LIMIT));

        // inline limit and projection node.
        pn = compile("select A, SUM(D) FROM (SELECT A, D FROM R1 WHERE A > 3 Limit 3 ) T1 Group by A");
        pn = pn.getChild(0);
        assertTrue(pn instanceof HashAggregatePlanNode);
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, "T1",  "A", "D" );
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, "R1",  "A", "D" );
        checkPredicateComparisonExpression(pn, "R1");
        assertEquals(((SeqScanPlanNode) pn).getInlinePlanNodes().size(), 2);
        assertNotNull(((SeqScanPlanNode) pn).getInlinePlanNode(PlanNodeType.PROJECTION));
        assertNotNull(((SeqScanPlanNode) pn).getInlinePlanNode(PlanNodeType.LIMIT));

        // add order by node, wihtout inline limit and projection node.
        pn = compile("select A, SUM(D) FROM (SELECT A, D FROM R1 WHERE A > 3 ORDER BY D Limit 3 ) T1 Group by A");
        pn = pn.getChild(0);
        assertTrue(pn instanceof HashAggregatePlanNode);
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, "T1",  "A", "D" );
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof LimitPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof OrderByPlanNode);
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, "R1",  "A", "D" );
        checkPredicateComparisonExpression(pn, "R1");
        assertEquals(((SeqScanPlanNode) pn).getInlinePlanNodes().size(), 1);
        assertNotNull(((SeqScanPlanNode) pn).getInlinePlanNode(PlanNodeType.PROJECTION));

        pn = compile("select A, SUM(D) FROM (SELECT A, D FROM R1 WHERE A > 3 ORDER BY D Limit 3 ) T1 Group by A HAVING SUM(D) < 3");
        pn = pn.getChild(0);
        assertTrue(pn instanceof HashAggregatePlanNode);
        assertNotNull(((HashAggregatePlanNode)pn).getPostPredicate());
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, "T1",  "A", "D" );
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof LimitPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof OrderByPlanNode);
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, "R1",  "A", "D" );
        checkPredicateComparisonExpression(pn, "R1");
        assertEquals(((SeqScanPlanNode) pn).getInlinePlanNodes().size(), 1);
        assertNotNull(((SeqScanPlanNode) pn).getInlinePlanNode(PlanNodeType.PROJECTION));


        pn = compile("select A, SUM(D)*COUNT(*) FROM (SELECT A, D FROM R1 WHERE A > 3 ORDER BY D Limit 3 ) T1 Group by A HAVING SUM(D) < 3");
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode); // complex aggregation
        pn = pn.getChild(0);
        assertTrue(pn instanceof HashAggregatePlanNode);
        assertNotNull(((HashAggregatePlanNode)pn).getPostPredicate());
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, "T1",  "A", "D" );
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof LimitPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof OrderByPlanNode);
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, "R1",  "A", "D" );
        checkPredicateComparisonExpression(pn, "R1");
        assertEquals(((SeqScanPlanNode) pn).getInlinePlanNodes().size(), 1);
        assertNotNull(((SeqScanPlanNode) pn).getInlinePlanNode(PlanNodeType.PROJECTION));



        pn = compile("select A, SUM(D) FROM (SELECT A, D FROM R1 WHERE A > 3 ORDER BY D Limit 3 ) T1 Group by A HAVING AVG(D) < 3");
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode); // complex aggregation
        pn = pn.getChild(0);
        assertTrue(pn instanceof HashAggregatePlanNode);
        assertNotNull(((HashAggregatePlanNode)pn).getPostPredicate());
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, "T1",  "A", "D" );
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof LimitPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof OrderByPlanNode);
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, "R1",  "A", "D" );
        checkPredicateComparisonExpression(pn, "R1");
        assertEquals(((SeqScanPlanNode) pn).getInlinePlanNodes().size(), 1);
        assertNotNull(((SeqScanPlanNode) pn).getInlinePlanNode(PlanNodeType.PROJECTION));



        // Aggregation inside of the from clause
        pn = compile("select A FROM (SELECT A, SUM(C) FROM R1 WHERE A > 3 GROUP BY A ORDER BY A Limit 3) T1 ");
        pn = pn.getChild(0);
        assertTrue(pn instanceof SeqScanPlanNode);
        checkSeqScanSubSelects(pn, "T1", "A");
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof LimitPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof OrderByPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof HashAggregatePlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof SeqScanPlanNode);
        checkSeqScanSubSelects(pn, "R1", "A", "C");


        pn = compile("select SC, SUM(A) as SA FROM (SELECT A, SUM(C) as SC, MAX(D) as MD FROM R1 " +
                "WHERE A > 3 GROUP BY A ORDER BY A Limit 3) T1  " +
                "Group by SC");

        pn = pn.getChild(0);
        assertTrue(pn instanceof HashAggregatePlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof SeqScanPlanNode);
        checkSeqScanSubSelects(pn, "T1", "A", "SC");
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof LimitPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof OrderByPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof HashAggregatePlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof SeqScanPlanNode);
        checkSeqScanSubSelects(pn, "R1", "A", "C", "D");
    }

    public void testSubSelects_Distributed() {
        // force it to be single partitioned.
        AbstractPlanNode pn;
        pn = compileForSinglePartition("select A FROM (SELECT A, C FROM P1 WHERE A > 3) T1");
        System.out.println(pn.toExplainPlanString());
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, "T1",  "A" );
        pn = pn.getChild(0);
        checkIndexedSubSelects(pn, "P1", "SYS_IDX_P1_PK_TREE", "A", "C");

        // AdHoc multiple partitioned sub-select queries.
        List<AbstractPlanNode> planNodes;
        planNodes = compileToFragments("select A, C FROM (SELECT A, C FROM P1) T1 ");
        assertTrue(planNodes.size() == 2);
        pn = planNodes.get(0).getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ReceivePlanNode);
        pn = planNodes.get(1).getChild(0);
        checkSeqScanSubSelects(pn, "T1",  "A", "C" );
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode); // This sounds it could be optimized
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, "P1",  "A", "C" );

        planNodes = compileToFragments("select A FROM (SELECT A, C FROM P1 WHERE A > 3) T1 ");
        assertTrue(planNodes.size() == 2);
        pn = planNodes.get(0).getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ReceivePlanNode);
        pn = planNodes.get(1).getChild(0);
        checkSeqScanSubSelects(pn, "T1",  "A" );
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        checkIndexedSubSelects(pn, "P1", "SYS_IDX_P1_PK_TREE", "A", "C");


        // Single partition detection : single table
        // Multiple joins are in Join tests.
        planNodes = compileToFragments("select A FROM (SELECT A FROM P1 WHERE A = 3) T1 ");
        assertTrue(planNodes.size() == 1);
        pn = planNodes.get(0);
        assertTrue(pn instanceof SendPlanNode);
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, "T1",  "A");
        pn = pn.getChild(0);
        checkIndexedSubSelects(pn, "P1", "SYS_IDX_P1_PK_TREE", "A");
        assertEquals(((IndexScanPlanNode) pn).getInlinePlanNodes().size(), 1);
        assertNotNull(((IndexScanPlanNode) pn).getInlinePlanNode(PlanNodeType.PROJECTION));

        planNodes = compileToFragments("select A, C FROM (SELECT A, C FROM P1 WHERE A = 3) T1 ");
        assertTrue(planNodes.size() == 1);
        pn = planNodes.get(0);
        assertTrue(pn instanceof SendPlanNode);
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, "T1",  "A", "C");
        pn = pn.getChild(0);
        checkIndexedSubSelects(pn, "P1", "SYS_IDX_P1_PK_TREE", "A", "C");
        assertEquals(((IndexScanPlanNode) pn).getInlinePlanNodes().size(), 1);
        assertNotNull(((IndexScanPlanNode) pn).getInlinePlanNode(PlanNodeType.PROJECTION));

        // Group by
        planNodes = compileToFragments("select C, SD FROM " +
                "(SELECT C, SUM(D) as SD FROM P1 GROUP BY C) T1 ");
        assertTrue(planNodes.size() == 2);

        planNodes = compileToFragments("select C, SD FROM " +
                "(SELECT C, SUM(D) as SD FROM P1 GROUP BY C) T1, R1 Where T1.C = R1.C ");
        assertTrue(planNodes.size() == 2);

        // Group by Partitioned column
        planNodes = compileToFragments("select C, SD FROM " +
                "(SELECT A, C, SUM(D) as SD FROM P1 WHERE A > 3 GROUP BY A, C) T1 ");
        assertTrue(planNodes.size() == 2);

        planNodes = compileToFragments("select C, SD FROM " +
                "(SELECT A, C, SUM(D) as SD FROM P1 WHERE A = 3 GROUP BY A, C) T1 ");
        assertTrue(planNodes.size() == 1);

        planNodes = compileToFragments("select C, SD FROM " +
                "(SELECT A, C, SUM(D) as SD FROM P1 WHERE A = 3 GROUP BY A, C) T1, R1 WHERE T1.C = R1.C ");
        assertTrue(planNodes.size() == 1);


        // Limit
        planNodes = compileToFragments("select C FROM (SELECT C FROM P1 WHERE A > 3 ORDER BY C LIMIT 5) T1 ");
        assertTrue(planNodes.size() == 2);

        planNodes = compileToFragments("select T1.C FROM (SELECT C FROM P1 WHERE A > 3 ORDER BY C LIMIT 5) T1, " +
                "R1 WHERE T1.C > R1.C ");
        assertTrue(planNodes.size() == 2);

        planNodes = compileToFragments("select C FROM (SELECT C FROM P1 WHERE A = 3 ORDER BY C LIMIT 5) T1 ");
        assertTrue(planNodes.size() == 1);

        planNodes = compileToFragments("select T1.C FROM (SELECT C FROM P1 WHERE A = 3 ORDER BY C LIMIT 5) T1, " +
                "R1 WHERE T1.C > R1.C ");
        assertTrue(planNodes.size() == 1);


        // Group by & LIMIT 5
        planNodes = compileToFragments("select C, SD FROM " +
                "(SELECT C, SUM(D) as SD FROM P1 GROUP BY C ORDER BY C LIMIT 5) T1 ");
        assertTrue(planNodes.size() == 2);

        planNodes = compileToFragments("select C, SD FROM " +
                "(SELECT C, SUM(D) as SD FROM P1 WHERE A = 3 GROUP BY C ORDER BY C LIMIT 5) T1 ");
        assertTrue(planNodes.size() == 1);
    }

    public void testSubSelects_Unsupported_Cases() {
        String errorMessage = "Every derived table must have its own alias.";
        failToCompile("select C FROM (SELECT C FROM R1)  ", errorMessage);

        // Unsupported joins.
        String joinErrorMsg = "Join of multiple partitioned tables has insufficient join criteria.";
        failToCompile("select A, C FROM (SELECT A FROM P1) T1, (SELECT C FROM P2) T2 WHERE T1.A = T2.C ",
                joinErrorMsg);
        failToCompile("select D1, D2 FROM (SELECT A, D D1 FROM P1 ) T1, (SELECT A, D D2 FROM P2 ) T2 WHERE T1.A = T2.A",
                joinErrorMsg);
        failToCompile("select D1, P2.D FROM (SELECT A, D D1 FROM P1 WHERE A=1) T1, P2 WHERE T1.A = P2.A AND P2.A = 1",
                joinErrorMsg);

        // Join of a single partitioned sub-queries. The partitions are different
        failToCompile("select D1, D2 FROM (SELECT A, D D1 FROM P1 WHERE A=2) T1, (SELECT A, D D2 FROM P2 WHERE A=2) T2",
                joinErrorMsg);
        failToCompile("select D1, D2 FROM " +
                "(SELECT A, D D1 FROM P1) T1, (SELECT A, D D2 FROM P2) T2 WHERE T1.A = 1 AND T2.A = 2", joinErrorMsg);

        // parent partition table join with subselect partitioned temp table
        failToCompile("select * from (SELECT A, D D1 FROM P1) T1, P2 where p2.D = T1.D1",
                joinErrorMsg);

        failToCompile("select * from p2, (select * from (SELECT A, D D1 FROM P1) T1) T2 where p2.D= T2.D1",
                joinErrorMsg);

        // In future, this query may be supported.
        failToCompile("select * from p2, (select * from (SELECT A, D D1 FROM P1 WHERE A=2) T1) T2 where p2.D = T2.D1",
                joinErrorMsg);

        failToCompile("select * from p2, (select * from (SELECT A, D D1 FROM P1 WHERE A=2) T1) T2 " +
                "where p2.A = T2.A", joinErrorMsg);


        failToCompile("select T1.A, T1.C, T1.SD FROM " +
                "(SELECT A, C, SUM(D) as SD FROM P1 WHERE A > 3 GROUP BY A, C) T1, P2 WHERE T1.C = P2.C ",
                joinErrorMsg);

    }

    public void testSubSelects_Edge_Cases() {
        AbstractPlanNode pn;

        pn = compile("select T1.A FROM (SELECT A FROM R1) T1, (SELECT A FROM R2)T2 ");
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        checkOutputSchema("T1", pn, "A");
        pn = pn.getChild(0);
        assertTrue(pn instanceof NestLoopPlanNode);
        checkOutputSchema(pn, "A", "A");
        checkSeqScanSubSelects(pn.getChild(0), "T1", "A");
        checkSeqScanSubSelects(pn.getChild(0).getChild(0), "R1", "A");
        checkSeqScanSubSelects(pn.getChild(1), "T2", "A");
        checkSeqScanSubSelects(pn.getChild(1).getChild(0), "R2", "A");

        pn = compile("select T2.A FROM (SELECT A FROM R1) T1, (SELECT A FROM R2)T2 ");
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        checkOutputSchema("T2", pn, "A");
        pn = pn.getChild(0);
        assertTrue(pn instanceof NestLoopPlanNode);
        checkOutputSchema(pn, "A", "A");
        checkSeqScanSubSelects(pn.getChild(0), "T1", "A");
        checkSeqScanSubSelects(pn.getChild(0).getChild(0), "R1", "A");
        checkSeqScanSubSelects(pn.getChild(1), "T2", "A");
        checkSeqScanSubSelects(pn.getChild(1).getChild(0), "R2", "A");

        // TODO(xin): hsql does not complain about the ambiguous column A, but use 'T1' as default.
        // FIX(xin): throw compiler exception for this query.
        pn = compile("select A FROM (SELECT A FROM R1) T1, (SELECT A FROM R2) T2 ");
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        checkOutputSchema("T1", pn, "A");
        pn = pn.getChild(0);
        assertTrue(pn instanceof NestLoopPlanNode);
        checkOutputSchema(pn, "A", "A");
        checkSeqScanSubSelects(pn.getChild(0), "T1", "A");
        checkSeqScanSubSelects(pn.getChild(0).getChild(0), "R1", "A");
        checkSeqScanSubSelects(pn.getChild(1), "T2", "A");
        checkSeqScanSubSelects(pn.getChild(1).getChild(0), "R2", "A");

        // Quick tests of some past spectacular planner failures that sqlcoverage uncovered.

        pn = compile("SELECT 1, * FROM (select * from R1) T1, R2 T2 WHERE T2.A < 3737632230784348203");
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);

        pn = compile("SELECT 2, * FROM (select * from R1) T1, R2 T2 WHERE CASE WHEN T2.A > 44 THEN T2.C END < 44 + 10");
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);

        pn = compile("SELECT -8, T2.C FROM (select * from R1) T1, R1 T2 WHERE (T2.C + 5 ) > 44");
        pn = pn.getChild(0);
        System.out.println(pn.toExplainPlanString());
        assertTrue(pn instanceof ProjectionPlanNode);
    }

    public void testSubSelects_Simple_Joins() {
        AbstractPlanNode pn;
        AbstractPlanNode nlpn;

        pn = compile("select A, C FROM (SELECT A FROM R1) T1, (SELECT C FROM R2) T2 WHERE T1.A = T2.C");
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);

        nlpn = pn.getChild(0);
        assertTrue(nlpn instanceof NestLoopPlanNode);
        assertEquals(2, nlpn.getChildCount());
        pn = nlpn.getChild(0);
        checkSeqScanSubSelects(pn, "T1",  "A");
        pn= pn.getChild(0);
        checkSeqScanSubSelects(pn, "R1",  "A");

        pn = nlpn.getChild(1);
        checkSeqScanSubSelects(pn, "T2",  "C");
        pn= pn.getChild(0);
        checkSeqScanSubSelects(pn, "R2",  "C");


        // sub-selected table joins
        pn = compile("select A, C FROM (SELECT A FROM R1) T1, (SELECT C FROM R2) T2 WHERE A = C");
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);

        nlpn = pn.getChild(0);
        assertTrue(nlpn instanceof NestLoopPlanNode);
        assertEquals(2, nlpn.getChildCount());
        pn = nlpn.getChild(0);
        checkSeqScanSubSelects(pn, "T1",  "A");
        pn= pn.getChild(0);
        checkSeqScanSubSelects(pn, "R1",  "A");

        pn = nlpn.getChild(1);
        checkSeqScanSubSelects(pn, "T2",  "C");
        pn= pn.getChild(0);
        checkSeqScanSubSelects(pn, "R2",  "C");
    }

    public void testSubSelects_Joins() {
        AbstractPlanNode pn;
        List<AbstractPlanNode> planNodes;
        AbstractPlanNode nlpn;

        // Left Outer join
        planNodes = compileToFragments("SELECT A, C FROM R1 LEFT JOIN (SELECT A, C FROM R2) T1 ON T1.C = R1.C ");
        assertTrue(planNodes.size() == 1);
        pn = planNodes.get(0).getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        nlpn = pn.getChild(0);
        assertTrue(nlpn instanceof NestLoopPlanNode);
        assertEquals(JoinType.LEFT, ((NestLoopPlanNode) nlpn).getJoinType());
        pn = nlpn.getChild(0);
        checkSeqScanSubSelects(pn, "R1", "A", "C");
        pn = nlpn.getChild(1);
        checkSeqScanSubSelects(pn, "T1", "C");
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, "R2", "A", "C");


        // Join with partitioned tables

        // Join on coordinator: LEFT OUTER JOIN, replicated table on left side
        planNodes = compileToFragments("SELECT A, C FROM R1 LEFT JOIN (SELECT A, C FROM P1) T1 ON T1.C = R1.C ");
        assertTrue(planNodes.size() == 2);
        pn = planNodes.get(0).getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        nlpn = pn.getChild(0);
        assertTrue(nlpn instanceof NestLoopPlanNode);
        assertEquals(JoinType.LEFT, ((NestLoopPlanNode) nlpn).getJoinType());
        pn = nlpn.getChild(0);
        checkSeqScanSubSelects(pn, "R1", "A", "C");
        pn = nlpn.getChild(1);
        assertTrue(pn instanceof ReceivePlanNode);

        pn = planNodes.get(1);
        assertTrue(pn instanceof SendPlanNode);
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, "T1", "C");
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, "P1", "A", "C");


        // Join locally: inner join case for subselects
        planNodes = compileToFragments("SELECT A, C FROM R1 INNER JOIN (SELECT A, C FROM P1) T1 ON T1.C = R1.C ");
        assertTrue(planNodes.size() == 2);
        pn = planNodes.get(0).getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ReceivePlanNode);

        pn = planNodes.get(1);
        assertTrue(pn instanceof SendPlanNode);
        nlpn = pn.getChild(0);
        assertTrue(nlpn instanceof NestLoopPlanNode);
        assertEquals(JoinType.INNER, ((NestLoopPlanNode) nlpn).getJoinType());
        pn = nlpn.getChild(0);
        checkSeqScanSubSelects(pn, "R1", "A", "C");
        pn = nlpn.getChild(1);
        checkSeqScanSubSelects(pn, "T1", "C");
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, "P1", "A", "C");


        // Two sub-queries. One is partitioned and the other one is replicated
        planNodes = compileToFragments("select A, C FROM (SELECT A FROM R1) T1, (SELECT C FROM P1) T2 WHERE T1.A = T2.C ");
        assertTrue(planNodes.size() == 2);
        pn = planNodes.get(0).getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ReceivePlanNode);

        pn = planNodes.get(1);
        assertTrue(pn instanceof SendPlanNode);
        nlpn = pn.getChild(0);
        assertTrue(nlpn instanceof NestLoopPlanNode);
        assertEquals(JoinType.INNER, ((NestLoopPlanNode) nlpn).getJoinType());
        pn = nlpn.getChild(0);
        checkSeqScanSubSelects(pn, "T1", "A");
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, "R1", "A");

        pn = nlpn.getChild(1);
        checkSeqScanSubSelects(pn, "T2", "C");
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, "P1", "C");

        // This is a single fragment plan because planner can detect "A = 3".
        // Join locally
        planNodes = compileToFragments("select A, C FROM (SELECT A FROM R1) T1, (SELECT C FROM P1 where A = 3) T2 " +
                "WHERE T1.A = T2.C ");
        assertTrue(planNodes.size() == 1);
        pn = planNodes.get(0);
        assertTrue(pn instanceof SendPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        nlpn = pn.getChild(0);
        assertTrue(nlpn instanceof NestLoopPlanNode);
        pn = nlpn.getChild(0);
        checkSeqScanSubSelects(pn, "T1", "A");
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, "R1", "A");
        pn = nlpn.getChild(1);
        checkSeqScanSubSelects(pn, "T2", "C");
        pn = pn.getChild(0);
        checkIndexedSubSelects(pn, "P1", "SYS_IDX_P1_PK_TREE", "C");
        assertEquals(((IndexScanPlanNode) pn).getInlinePlanNodes().size(), 1);
        assertNotNull(((IndexScanPlanNode) pn).getInlinePlanNode(PlanNodeType.PROJECTION));


        // More single partition detection
        planNodes = compileToFragments("select C FROM (SELECT P1.C FROM P1, P2 " +
                "WHERE P1.A = P2.A AND P1.A = 3) T1 ");
        assertTrue(planNodes.size() == 1);

        planNodes = compileToFragments("select T1.C FROM (SELECT P1.C FROM P1, P2 " +
                "WHERE P1.A = P2.A AND P1.A = 3) T1, R1 where T1.C > R1.C ");
        assertTrue(planNodes.size() == 1);

        planNodes = compileToFragments("select T1.C FROM (SELECT P1.C FROM P1, P2 " +
                "WHERE P1.A = P2.A AND P1.A = 3) T1, (select C FROM R1) T2 where T1.C > T2.C ");
        assertTrue(planNodes.size() == 1);

    }

    public void testSubSelects_FromAllReplicated() {
        AbstractPlanNode pn;
        List<AbstractPlanNode> planNodes;
        AbstractPlanNode nlpn;

        planNodes = compileToFragments("select T1.A, P1.C FROM (SELECT A FROM R1) T1, P1 " +
                "WHERE T1.A = P1.C AND P1.A = 3 ");
        assertTrue(planNodes.size() == 1);
        pn = planNodes.get(0);
        assertTrue(pn instanceof SendPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        nlpn = pn.getChild(0);
        assertTrue(nlpn instanceof NestLoopPlanNode);
        pn = nlpn.getChild(0);
        checkSeqScanSubSelects(pn, "T1", "A");
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, "R1", "A");
        pn = nlpn.getChild(1);
        checkIndexedSubSelects(pn, "P1", "SYS_IDX_P1_PK_TREE", "A", "C");


        planNodes = compileToFragments("select T1.A FROM (SELECT A FROM R1) T1, P1 " +
                "WHERE T1.A = P1.A AND P1.A = 3 ");
        assertTrue(planNodes.size() == 1);
        pn = planNodes.get(0);
        assertTrue(pn instanceof SendPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        nlpn = pn.getChild(0);
        assertTrue(nlpn instanceof NestLoopPlanNode);
        pn = nlpn.getChild(0);
        checkSeqScanSubSelects(pn, "T1", "A");
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, "R1", "A");
        pn = nlpn.getChild(1);
        checkIndexedSubSelects(pn, "P1", "SYS_IDX_P1_PK_TREE", "A");


        planNodes = compileToFragments("select T1.A FROM (SELECT A FROM R1) T1, P1 " +
                "WHERE T1.A = P1.A AND T1.A = 3 ");
        assertTrue(planNodes.size() == 1);
        pn = planNodes.get(0);
        assertTrue(pn instanceof SendPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        nlpn = pn.getChild(0);
        assertTrue(nlpn instanceof NestLoopPlanNode);
        pn = nlpn.getChild(0);
        checkSeqScanSubSelects(pn, "T1", "A");
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, "R1", "A");
        pn = nlpn.getChild(1);
        checkIndexedSubSelects(pn, "P1", "SYS_IDX_P1_PK_TREE", "A");


        planNodes = compileToFragments("select T1.A, P1.C FROM (SELECT A FROM R1) T1, P1 " +
                "WHERE T1.A = P1.C ");
        assertTrue(planNodes.size() == 2);

        pn = planNodes.get(0).getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ReceivePlanNode);

        pn = planNodes.get(1);
        assertTrue(pn instanceof SendPlanNode);
        System.out.println(pn.toExplainPlanString());
        nlpn = pn.getChild(0);
        assertTrue(nlpn instanceof NestLoopPlanNode);
        pn = nlpn.getChild(0);
        checkSeqScanSubSelects(pn, "T1", "A");
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, "R1", "A");
        pn = nlpn.getChild(1);
        assertTrue(pn instanceof SeqScanPlanNode);


        // Three table joins
        planNodes = compileToFragments("select T1.A, P1.A FROM (SELECT A FROM R1) T1, P1, P2 " +
                "WHERE P2.A = P1.A and T1.A = P1.C ");
        assertTrue(planNodes.size() == 2);

        pn = planNodes.get(0).getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ReceivePlanNode);

        pn = planNodes.get(1);
        assertTrue(pn instanceof SendPlanNode);

        nlpn = pn.getChild(0);
        assertTrue(nlpn instanceof NestLoopPlanNode);

        pn = nlpn.getChild(1);
        checkSeqScanSubSelects(pn, "T1", "A");
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, "R1", "A");

        nlpn = nlpn.getChild(0);
        assertTrue(nlpn instanceof NestLoopIndexPlanNode);
        pn = nlpn.getChild(0);
        checkSeqScanSubSelects(pn, "P1", "A", "C");

        assertEquals(nlpn.getInlinePlanNodes().size(), 1);
        pn = nlpn.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        checkIndexedSubSelects(pn, "P2", "SYS_IDX_P2_PK_TREE", "A");
    }

    public void testSubSelects_With_Unions() {
        AbstractPlanNode pn;
        pn = compile("select A, C FROM (SELECT A, C FROM R1 UNION SELECT A, C FROM R2 UNION SELECT A, C FROM R3) T1 order by A ");
        System.out.println(pn.toExplainPlanString());

        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof OrderByPlanNode);
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, "T1",  "A", "C");
        AbstractPlanNode upn = pn.getChild(0);
        assertTrue(upn instanceof UnionPlanNode);

        pn = upn.getChild(0);
        checkSeqScanSubSelects(pn, "R1", "A", "C");
        pn = upn.getChild(1);
        checkSeqScanSubSelects(pn, "R2", "A", "C");
        pn = upn.getChild(2);
        checkSeqScanSubSelects(pn, "R3", "A", "C");
    }

    public void testSubSelects_Parameters() {
        AbstractPlanNode pn = compile("select A1 FROM (SELECT A A1 FROM R1 WHERE A > ?) TEMP WHERE A1 < ?");
        pn = pn.getChild(0);
        assertTrue(pn instanceof SeqScanPlanNode);
        AbstractExpression p = ((SeqScanPlanNode) pn).getPredicate();
        assertTrue(p != null);
        assertTrue(p instanceof ComparisonExpression);
        AbstractExpression cp = p.getLeft();
        assertTrue(cp instanceof TupleValueExpression);
        cp = p.getRight();
        assertTrue(cp instanceof ParameterValueExpression);
        assertEquals(1, ((ParameterValueExpression)cp).getParameterIndex().intValue());
        assertTrue(pn.getChildCount() == 1);
        assertTrue(pn.getChild(0) instanceof SeqScanPlanNode);
        SeqScanPlanNode sc = (SeqScanPlanNode) pn.getChild(0);
        assertTrue(sc.getPredicate() != null);
        p = sc.getPredicate();
        assertTrue(p instanceof ComparisonExpression);
        cp = p.getRight();
        assertTrue(cp instanceof ParameterValueExpression);
        assertEquals(0, ((ParameterValueExpression)cp).getParameterIndex().intValue());
    }

    public void testSubSelects_GroupBy_NonPartitionKeys() {
        AbstractPlanNode pn;
        List<AbstractPlanNode> planNodes;
        AbstractPlanNode nlpn;

        // Top aggregation node on coordinator
        planNodes = compileToFragments(
                "SELECT -8, T1.NUM FROM R4 T0, " +
                "(select max(RATIO) RATIO, sum(NUM) NUM, DESC from P4 group by DESC) T1 " +
                "WHERE (T1.NUM + 5 ) > 44");

        assertTrue(planNodes.size() == 2);
        pn = planNodes.get(0);
        assertTrue(pn instanceof SendPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        nlpn = pn.getChild(0);
        assertTrue(nlpn instanceof NestLoopPlanNode);
        assertEquals(JoinType.INNER, ((NestLoopPlanNode) nlpn).getJoinType());
        pn = nlpn.getChild(0);
        checkIndexedSubSelects(pn, "R4", null);
        pn = nlpn.getChild(1);
        checkSeqScanSubSelects(pn, "T1", "NUM");
        pn = pn.getChild(0);
        assertTrue(pn instanceof AggregatePlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ReceivePlanNode);

        pn = planNodes.get(1);
        assertTrue(pn instanceof SendPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof AggregatePlanNode);
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, "P4");
    }

    public void testSubSelects_LimitOffset() {
        AbstractPlanNode pn;
        List<AbstractPlanNode> planNodes;
        AbstractPlanNode nlpn;

        // Top aggregation node on coordinator
        planNodes = compileToFragments(
                "SELECT -8, T1.NUM " +
                "FROM R4 T0, (select RATIO, NUM, DESC from P4 order by DESC, NUM, RATIO limit 1 offset 1) T1 " +
                "WHERE (T1.NUM + 5 ) > 44");

        assertTrue(planNodes.size() == 2);
        pn = planNodes.get(0);
        assertTrue(pn instanceof SendPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        nlpn = pn.getChild(0);
        assertTrue(nlpn instanceof NestLoopPlanNode);
        assertEquals(JoinType.INNER, ((NestLoopPlanNode) nlpn).getJoinType());
        pn = nlpn.getChild(0);
        checkIndexedSubSelects(pn, "R4", null);
        pn = nlpn.getChild(1);
        checkSeqScanSubSelects(pn, "T1", "NUM");
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof LimitPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof OrderByPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ReceivePlanNode);

        pn = planNodes.get(1);
        assertTrue(pn instanceof SendPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof LimitPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof OrderByPlanNode);
        pn = pn.getChild(0);
        checkSeqScanSubSelects(pn, "P4");
    }

    @Override
    protected void setUp() throws Exception {
        setupSchema(TestSubQueries.class.getResource("testplans-subqueries-ddl.sql"), "dd", false);
        AbstractPlanNode.enableVerboseExplainForDebugging();
        AbstractExpression.enableVerboseExplainForDebugging();
    }

}
