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

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.hsqldb_voltpatches.HSQLInterface;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ComparisonExpression;
import org.voltdb.expressions.ConstantValueExpression;
import org.voltdb.expressions.ParameterValueExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.AggregatePlanNode;
import org.voltdb.plannodes.HashAggregatePlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.MergeReceivePlanNode;
import org.voltdb.plannodes.NestLoopIndexPlanNode;
import org.voltdb.plannodes.NestLoopPlanNode;
import org.voltdb.plannodes.NodeSchema;
import org.voltdb.plannodes.OrderByPlanNode;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.plannodes.ReceivePlanNode;
import org.voltdb.plannodes.SchemaColumn;
import org.voltdb.plannodes.SendPlanNode;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.plannodes.TableCountPlanNode;
import org.voltdb.plannodes.UnionPlanNode;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.JoinType;
import org.voltdb.types.PlanNodeType;

public class TestPlansSubQueries extends PlannerTestCase {
    @Override
    protected void setUp() throws Exception {
        setupSchema(TestPlansSubQueries.class.getResource("testplans-subqueries-ddl.sql"), "ddl", false);
    }

    public void testSelectOnlyGuard() {
        // Can only have expression subqueries in SELECT statements

        failToCompile("INSERT INTO R1 (A, C, D) VALUES ((SELECT MAX(A) FROM R1), 32, 32)",
                "Subquery expressions are only supported in SELECT statements");

        failToCompile("INSERT INTO R1 (A, C, D) SELECT (SELECT MAX(A) FROM R1), 32, 32 FROM R1",
                "Subquery expressions are only supported in SELECT statements");

        failToCompile("UPDATE R1 SET A = (SELECT MAX(A) FROM R1)",
                "Subquery expressions are only supported in SELECT statements");

        failToCompile("UPDATE R1 SET A = 37 WHERE A = (SELECT MAX(A) FROM R1)",
                "Subquery expressions are only supported in SELECT statements");

        failToCompile("DELETE FROM R1 WHERE A IN (SELECT A A1 FROM R1 WHERE A>1)",
                "Subquery expressions are only supported in SELECT statements");

        failToCompile("SELECT * FROM R1 WHERE A IN (32, 33) "
                + "UNION SELECT * FROM R1 WHERE A = (SELECT MAX(A) FROM R1)",
                "Subquery expressions are only supported in SELECT statements");
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

    private void checkSeqScan(AbstractPlanNode scanNode, String tableAlias, String... columns) {
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

    private void checkIndexScan(AbstractPlanNode indexNode, String tableName, String indexName, String... columns) {
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

    private void checkPrimaryKeyIndexScan(AbstractPlanNode indexNode, String tableName, String... columns) {
        // DDL use this patten to define primary key
        // "CONSTRAINT P1_PK_TREE PRIMARY KEY"
        String primaryKeyIndexName = HSQLInterface.AUTO_GEN_CONSTRAINT_WRAPPER_PREFIX + tableName + "_PK_TREE";

        checkIndexScan(indexNode, tableName, primaryKeyIndexName, columns);
    }

    public void testSimple() {
        AbstractPlanNode pn;
        String tbName = "T1";

        pn = compile("select A, C FROM (SELECT A, C FROM R1) T1");
        pn = pn.getChild(0);

        checkSeqScan(pn, tbName,  "A", "C");
        pn = pn.getChild(0);
        checkSeqScan(pn, "R1", "A", "C");


        pn = compile("select A, C FROM (SELECT A, C FROM R1) T1 WHERE A > 0");
        pn = pn.getChild(0);
        checkSeqScan(pn, tbName,  "A", "C");
        checkPredicateComparisonExpression(pn, tbName);
        pn = pn.getChild(0);
        checkSeqScan(pn, "R1", "A", "C");


        pn = compile("select A, C FROM (SELECT A, C FROM R1) T1 WHERE T1.A < 0");
        pn = pn.getChild(0);
        checkSeqScan(pn, tbName,  "A", "C");
        checkPredicateComparisonExpression(pn, tbName);
        pn = pn.getChild(0);
        checkSeqScan(pn, "R1", "A", "C");


        pn = compile("select A1, C1 FROM (SELECT A A1, C C1 FROM R1) T1 WHERE T1.A1 < 0");
        pn = pn.getChild(0);
        checkSeqScan(pn, tbName,  "A1", "C1");
        checkPredicateComparisonExpression(pn, tbName);
        pn = pn.getChild(0);
        checkSeqScan(pn, "R1", "A", "C");

        // With projection.
        pn = compile("select C1 FROM (SELECT A A1, C C1 FROM R1) T1 WHERE T1.A1 < 0");
        pn = pn.getChild(0);
        checkSeqScan(pn, tbName,  "C1");
        assertEquals(((SeqScanPlanNode) pn).getInlinePlanNodes().size(), 1);
        assertNotNull(((SeqScanPlanNode) pn).getInlinePlanNode(PlanNodeType.PROJECTION));
        checkPredicateComparisonExpression(pn, tbName);
        pn = pn.getChild(0);
        checkSeqScan(pn, "R1", "A", "C");

        // Complex columns in sub selects
        pn = compile("select C1 FROM (SELECT A+3 A1, C C1 FROM R1) T1 WHERE T1.A1 < 0");
        pn = pn.getChild(0);
        checkSeqScan(pn, tbName,  "C1");
        assertEquals(((SeqScanPlanNode) pn).getInlinePlanNodes().size(), 1);
        assertNotNull(((SeqScanPlanNode) pn).getInlinePlanNode(PlanNodeType.PROJECTION));

        checkPredicateComparisonExpression(pn, tbName);
        pn = pn.getChild(0);
        checkSeqScan(pn, "R1", "A1", "C");

        pn = compile("select COL1 FROM (SELECT A+3, C COL1 FROM R1) T1 WHERE T1.COL1 < 0");
        pn = pn.getChild(0);
        checkSeqScan(pn, tbName,  "COL1");
        assertEquals(((SeqScanPlanNode) pn).getInlinePlanNodes().size(), 1);
        assertNotNull(((SeqScanPlanNode) pn).getInlinePlanNode(PlanNodeType.PROJECTION));
        checkPredicateComparisonExpression(pn, tbName);
        pn = pn.getChild(0);
        checkSeqScan(pn, "R1",  "C1", "C");


        // select *
        pn = compile("select A, C FROM (SELECT * FROM R1) T1 WHERE T1.A < 0");
        pn = pn.getChild(0);
        checkSeqScan(pn, tbName,  "A", "C");
        checkPredicateComparisonExpression(pn, tbName);
        pn = pn.getChild(0);
        checkSeqScan(pn, "R1", "A", "C", "D");


        pn = compile("select * FROM (SELECT A, D FROM R1) T1 WHERE T1.A < 0");
        pn = pn.getChild(0);
        checkSeqScan(pn, tbName,  "A", "D");
        checkPredicateComparisonExpression(pn, tbName);
        pn = pn.getChild(0);
        checkSeqScan(pn, "R1", "A", "D");


        pn = compile("select A, C FROM (SELECT * FROM R1 where D > 3) T1 WHERE T1.A < 0");
        pn = pn.getChild(0);
        checkSeqScan(pn, tbName,  "A", "C");
        checkPredicateComparisonExpression(pn, tbName);
        pn = pn.getChild(0);
        checkSeqScan(pn, "R1", "A", "C", "D");
    }

    public void testMultipleLevelsNested() {
        AbstractPlanNode pn;
        List<AbstractPlanNode> planNodes;

        // Three levels selects
        pn = compile("select A2 FROM " +
                "(SELECT A1 AS A2 FROM (SELECT A AS A1 FROM R1 WHERE A < 3) T1 WHERE T1.A1 > 0) T2  WHERE T2.A2 = 3");
        pn = pn.getChild(0);
        checkSeqScan(pn, "T2",  "A2");
        checkPredicateComparisonExpression(pn, "T2");
        pn = pn.getChild(0);
        checkSeqScan(pn, "T1",  "A1");
        checkPredicateComparisonExpression(pn, "T1");
        pn = pn.getChild(0);
        checkSeqScan(pn, "R1",  "A");
        checkPredicateComparisonExpression(pn, "R1");

        //
        // Crazy fancy sub-query:
        // Multiple nested levels + partitioned table + partition detecting
        //
        planNodes = compileToFragments(
                "select P3.A, T3.C " +
                "FROM (select * from " +
                "               (select T1.A, P1.C from P1, " +
                "                             (select P2.A from R1, P2 " +
                "                               where p2.A = R1.C and R1.D = 3) T1 " +
                "               where P1.A = T1.A ) T2 ) T3, " +
                "     P3 " +
                "where P3.A = T3.A ");
        assertEquals(2, planNodes.size());


        planNodes = compileToFragments(
                "select P3.A, T3.C " +
                "FROM (select * from " +
                "               (select T1.A, P1.C from P1, " +
                "                             (select P2.A from R1, P2 " +
                "                               where p2.A = R1.C and p2.A = 3) T1 " +
                "               where P1.A = T1.A ) T2 ) T3, " +
                "     P3 " +
                "where P3.A = T3.A ");
        assertEquals(1, planNodes.size());

        // LIMIT
        String sql = "select A_count, count(*) " +
                    "from (select A, count(*) as A_count " +
                    "       from (select A, C from P1 ORDER BY A LIMIT 6) T1 group by A) T2 " +
                    "group by A_count order by A_count";
        planNodes = compileToFragments(sql);
        // send node
        pn = planNodes.get(1).getChild(0);
        checkPrimaryKeyIndexScan(pn, "P1");
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.LIMIT));

        // LIMIT with GROUP BY, no limit push down
        sql = "select A_count, count(*) " +
                "from (select A, count(*) as A_count " +
                "       from (select C, COUNT(*) A from P1 GROUP BY C ORDER BY A LIMIT 6) T1 group by A) T2 " +
                "group by A_count order by A_count";
        planNodes = compileToFragments(sql);
        // send node
        pn = planNodes.get(1).getChild(0);
        // P1 has PRIMARY KEY INDEX on column A: GROUP BY C should not use its INDEX to speed up.
        checkSeqScan(pn, "P1", "C", "A");
        assertNotNull(AggregatePlanNode.getInlineAggregationNode(pn));
        assertNull(pn.getInlinePlanNode(PlanNodeType.LIMIT));
    }

    public void testFunctions() {
        AbstractPlanNode pn;
        String tbName = "T1";

        // Function expression
        pn = compile("select ABS(C) FROM (SELECT A, C FROM R1) T1");
        pn = pn.getChild(0);
        checkSeqScan(pn, tbName,  "C1" );
        pn = pn.getChild(0);
        checkSeqScan(pn, "R1",  "A", "C" );

        // Use alias column from sub select instead.
        pn = compile("select A1, ABS(C) FROM (SELECT A A1, C FROM R1) T1");
        pn = pn.getChild(0);
        checkSeqScan(pn, tbName,  "A1", "C2" ); // hsql auto generated column alias C2.
        pn = pn.getChild(0);
        checkSeqScan(pn, "R1",  "A", "C" );

        pn = compile("select A1 + 3, ABS(C) FROM (SELECT A A1, C FROM R1) T1");
        pn = pn.getChild(0);
        checkSeqScan(pn, tbName,  "C1", "C2" );
        assertEquals(((SeqScanPlanNode) pn).getInlinePlanNodes().size(), 1);
        assertNotNull(((SeqScanPlanNode) pn).getInlinePlanNode(PlanNodeType.PROJECTION));
        pn = pn.getChild(0);
        checkSeqScan(pn, "R1",  "A", "C" );

        pn = compile("select A1 + 3, ABS(C) FROM (SELECT A A1, C FROM R1) T1 WHERE ABS(A1) > 3");
        pn = pn.getChild(0);
        checkSeqScan(pn, tbName,  "C1", "C2" );
        assertEquals(((SeqScanPlanNode) pn).getInlinePlanNodes().size(), 1);
        assertNotNull(((SeqScanPlanNode) pn).getInlinePlanNode(PlanNodeType.PROJECTION));
        pn = pn.getChild(0);
        checkSeqScan(pn, "R1",  "A", "C" );
    }

    public void testReplicated() {
        AbstractPlanNode pn;
        List<AbstractPlanNode> planNodes;
        AbstractPlanNode nlpn;

        planNodes = compileToFragments("select T1.A, P1.C FROM (SELECT A FROM R1) T1, P1 " +
                "WHERE T1.A = P1.C AND P1.A = 3 ");
        assertEquals(1, planNodes.size());
        pn = planNodes.get(0);
        assertTrue(pn instanceof SendPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        nlpn = pn.getChild(0);
        assertTrue(nlpn instanceof NestLoopPlanNode);
        pn = nlpn.getChild(0);
        checkSeqScan(pn, "T1", "A");
        pn = pn.getChild(0);
        checkSeqScan(pn, "R1", "A");
        pn = nlpn.getChild(1);
        checkPrimaryKeyIndexScan(pn, "P1", "A", "C");


        planNodes = compileToFragments("select T1.A FROM (SELECT A FROM R1) T1, P1 " +
                "WHERE T1.A = P1.A AND P1.A = 3 ");
        assertEquals(1, planNodes.size());
        pn = planNodes.get(0);
        assertTrue(pn instanceof SendPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        nlpn = pn.getChild(0);
        assertTrue(nlpn instanceof NestLoopPlanNode);
        pn = nlpn.getChild(0);
        checkSeqScan(pn, "T1", "A");
        pn = pn.getChild(0);
        checkSeqScan(pn, "R1", "A");
        pn = nlpn.getChild(1);
        checkPrimaryKeyIndexScan(pn, "P1", "A");


        planNodes = compileToFragments("select T1.A FROM (SELECT A FROM R1) T1, P1 " +
                "WHERE T1.A = P1.A AND T1.A = 3 ");
        assertEquals(1, planNodes.size());
        pn = planNodes.get(0);
        assertTrue(pn instanceof SendPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        nlpn = pn.getChild(0);
        assertTrue(nlpn instanceof NestLoopPlanNode);
        pn = nlpn.getChild(0);
        checkSeqScan(pn, "T1", "A");
        pn = pn.getChild(0);
        checkSeqScan(pn, "R1", "A");
        pn = nlpn.getChild(1);
        checkPrimaryKeyIndexScan(pn, "P1", "A");

        // Uncomment next test cases when ENG-6371 is fixed
//        planNodes = compileToFragments("select T1.A FROM (SELECT A FROM R1 where R1.A = 3) T1, P1 " +
//                "WHERE T1.A = P1.A ");
//        assertEquals(1, planNodes.size());
//        pn = planNodes.get(0);
//        assertTrue(pn instanceof SendPlanNode);
//        pn = pn.getChild(0);
//        assertTrue(pn instanceof ProjectionPlanNode);
//        nlpn = pn.getChild(0);
//        assertTrue(nlpn instanceof NestLoopPlanNode);
//        pn = nlpn.getChild(0);
//        checkSeqScanSubSelects(pn, "T1", "A");
//        pn = pn.getChild(0);
//        checkSeqScanSubSelects(pn, "R1", "A");
//        pn = nlpn.getChild(1);
//        checkPrimaryKeySubSelect(pn, "P1", "A");


        planNodes = compileToFragments("select T1.A, P1.C FROM (SELECT A FROM R1) T1, P1 " +
                "WHERE T1.A = P1.C ");
        assertEquals(2, planNodes.size());

        pn = planNodes.get(0).getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ReceivePlanNode);

        pn = planNodes.get(1);
        assertTrue(pn instanceof SendPlanNode);
        //* enable to debug */ System.out.println(pn.toExplainPlanString());
        nlpn = pn.getChild(0);
        assertTrue(nlpn instanceof NestLoopPlanNode);
        pn = nlpn.getChild(0);
        checkSeqScan(pn, "T1", "A");
        pn = pn.getChild(0);
        checkSeqScan(pn, "R1", "A");
        pn = nlpn.getChild(1);
        assertTrue(pn instanceof AbstractScanPlanNode);


        // Three table joins
        planNodes = compileToFragments("select T1.A, P1.A FROM (SELECT A FROM R1) T1, P1, P2 " +
                "WHERE P2.A = P1.A and T1.A = P1.C ");
        assertEquals(2, planNodes.size());

        pn = planNodes.get(0).getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ReceivePlanNode);

        pn = planNodes.get(1);
        assertTrue(pn instanceof SendPlanNode);

        nlpn = pn.getChild(0);
        assertTrue(nlpn instanceof NestLoopPlanNode);

        pn = nlpn.getChild(1);
        checkSeqScan(pn, "T1", "A");
        pn = pn.getChild(0);
        checkSeqScan(pn, "R1", "A");

        nlpn = nlpn.getChild(0);
        assertTrue(nlpn instanceof NestLoopIndexPlanNode);
        pn = nlpn.getChild(0);
        checkPrimaryKeyIndexScan(pn, "P1", "A", "C");

        assertEquals(nlpn.getInlinePlanNodes().size(), 1);
        pn = nlpn.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        checkPrimaryKeyIndexScan(pn, "P2", "A");
    }

    public void testReplicatedGroupbyLIMIT() {
        AbstractPlanNode pn;

        pn = compile("select A, C FROM (SELECT * FROM R1 WHERE A > 3 Limit 3) T1 ");
        pn = pn.getChild(0);
        checkSeqScan(pn, "T1",  "A", "C" );
        pn = pn.getChild(0);
        checkSeqScan(pn, "R1",  "A", "C", "D");
        checkPredicateComparisonExpression(pn, "R1");
        assertEquals(((SeqScanPlanNode) pn).getInlinePlanNodes().size(), 2);
        assertNotNull(((SeqScanPlanNode) pn).getInlinePlanNode(PlanNodeType.PROJECTION));
        assertNotNull(((SeqScanPlanNode) pn).getInlinePlanNode(PlanNodeType.LIMIT));

        // inline limit and projection node.
        pn = compile("select A, SUM(D) FROM (SELECT A, D FROM R1 WHERE A > 3 Limit 3 ) T1 Group by A");
        pn = pn.getChild(0);
        assertTrue(pn instanceof SeqScanPlanNode);
        assertTrue(pn.getInlinePlanNode(PlanNodeType.HASHAGGREGATE) != null);

        pn = pn.getChild(0);
        checkSeqScan(pn, "R1",  "A", "D" );
        checkPredicateComparisonExpression(pn, "R1");
        assertEquals(((SeqScanPlanNode) pn).getInlinePlanNodes().size(), 2);
        assertNotNull(((SeqScanPlanNode) pn).getInlinePlanNode(PlanNodeType.PROJECTION));
        assertNotNull(((SeqScanPlanNode) pn).getInlinePlanNode(PlanNodeType.LIMIT));

        // add order by node, wihtout inline limit and projection node.
        pn = compile("select A, SUM(D) FROM (SELECT A, D FROM R1 WHERE A > 3 ORDER BY D Limit 3 ) T1 Group by A");
        pn = pn.getChild(0);
        assertTrue(pn.getInlinePlanNode(PlanNodeType.HASHAGGREGATE) != null);
        checkSeqScan(pn, "T1" );
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        // inline limit with order by
        assertTrue(pn instanceof OrderByPlanNode);
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.LIMIT));
        pn = pn.getChild(0);
        checkSeqScan(pn, "R1",  "A", "D" );
        checkPredicateComparisonExpression(pn, "R1");
        assertEquals(((SeqScanPlanNode) pn).getInlinePlanNodes().size(), 1);
        assertNotNull(((SeqScanPlanNode) pn).getInlinePlanNode(PlanNodeType.PROJECTION));

        AbstractPlanNode aggNode;

        pn = compile("select A, SUM(D) FROM (SELECT A, D FROM R1 WHERE A > 3 ORDER BY D Limit 3 ) T1 Group by A HAVING SUM(D) < 3");
        pn = pn.getChild(0);
        assertTrue(pn.getInlinePlanNode(PlanNodeType.HASHAGGREGATE) != null);
        aggNode = pn.getInlinePlanNode(PlanNodeType.HASHAGGREGATE);
        assertNotNull(((HashAggregatePlanNode)aggNode).getPostPredicate());
        checkSeqScan(pn, "T1" );
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        // inline limit with order by
        assertTrue(pn instanceof OrderByPlanNode);
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.LIMIT));
        pn = pn.getChild(0);
        checkSeqScan(pn, "R1",  "A", "D" );
        checkPredicateComparisonExpression(pn, "R1");
        assertEquals(((SeqScanPlanNode) pn).getInlinePlanNodes().size(), 1);
        assertNotNull(((SeqScanPlanNode) pn).getInlinePlanNode(PlanNodeType.PROJECTION));


        pn = compile("select A, SUM(D)*COUNT(*) FROM (SELECT A, D FROM R1 WHERE A > 3 ORDER BY D Limit 3 ) T1 Group by A HAVING SUM(D) < 3");
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode); // complex aggregation
        pn = pn.getChild(0);
        assertTrue(pn.getInlinePlanNode(PlanNodeType.HASHAGGREGATE) != null);
        aggNode = pn.getInlinePlanNode(PlanNodeType.HASHAGGREGATE);
        assertNotNull(((HashAggregatePlanNode)aggNode).getPostPredicate());

        checkSeqScan(pn, "T1");
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        // inline limit with order by
        assertTrue(pn instanceof OrderByPlanNode);
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.LIMIT));
        pn = pn.getChild(0);
        checkSeqScan(pn, "R1",  "A", "D" );
        checkPredicateComparisonExpression(pn, "R1");
        assertEquals(((SeqScanPlanNode) pn).getInlinePlanNodes().size(), 1);
        assertNotNull(((SeqScanPlanNode) pn).getInlinePlanNode(PlanNodeType.PROJECTION));



        pn = compile("select A, SUM(D) FROM (SELECT A, D FROM R1 WHERE A > 3 ORDER BY D Limit 3 ) T1 Group by A HAVING AVG(D) < 3");
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode); // complex aggregation
        pn = pn.getChild(0);
        assertTrue(pn.getInlinePlanNode(PlanNodeType.HASHAGGREGATE) != null);
        aggNode = pn.getInlinePlanNode(PlanNodeType.HASHAGGREGATE);
        assertNotNull(((HashAggregatePlanNode)aggNode).getPostPredicate());

        checkSeqScan(pn, "T1");
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        // inline limit with order by
        assertTrue(pn instanceof OrderByPlanNode);
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.LIMIT));
        pn = pn.getChild(0);
        checkSeqScan(pn, "R1",  "A", "D" );
        checkPredicateComparisonExpression(pn, "R1");
        assertEquals(((SeqScanPlanNode) pn).getInlinePlanNodes().size(), 1);
        assertNotNull(((SeqScanPlanNode) pn).getInlinePlanNode(PlanNodeType.PROJECTION));



        // Aggregation inside of the from clause
        pn = compile("select A FROM (SELECT A, SUM(C) FROM R1 WHERE A > 3 GROUP BY A ORDER BY A Limit 3) T1 ");
        pn = pn.getChild(0);
        assertTrue(pn instanceof SeqScanPlanNode);
        checkSeqScan(pn, "T1", "A");
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        // inline limit with order by
        assertTrue(pn instanceof OrderByPlanNode);
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.LIMIT));
        pn = pn.getChild(0);
        assertTrue(pn.getInlinePlanNode(PlanNodeType.HASHAGGREGATE) != null);
        assertTrue(pn instanceof SeqScanPlanNode);
        checkSeqScan(pn, "R1");


        pn = compile("select SC, SUM(A) as SA FROM (SELECT A, SUM(C) as SC, MAX(D) as MD FROM R1 " +
                "WHERE A > 3 GROUP BY A ORDER BY A Limit 3) T1  " +
                "Group by SC");

        pn = pn.getChild(0);
        assertTrue(pn.getInlinePlanNode(PlanNodeType.HASHAGGREGATE) != null);
        assertTrue(pn instanceof SeqScanPlanNode);
        checkSeqScan(pn, "T1");
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        // inline limit with order by
        assertTrue(pn instanceof OrderByPlanNode);
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.LIMIT));
        pn = pn.getChild(0);
        assertTrue(pn.getInlinePlanNode(PlanNodeType.HASHAGGREGATE) != null);
        assertTrue(pn instanceof SeqScanPlanNode);
        checkSeqScan(pn, "R1");
    }

    public void testPartitionedSameLevel() {
        // force it to be single partitioned.
        AbstractPlanNode pn;
        List<AbstractPlanNode> planNodes;

        //
        // Single partition detection : single table
        //
        planNodes = compileToFragments("select A FROM (SELECT A FROM P1 WHERE A = 3) T1 ");
        assertEquals(1, planNodes.size());
        pn = planNodes.get(0);
        assertTrue(pn instanceof SendPlanNode);
        pn = pn.getChild(0);
        checkSeqScan(pn, "T1",  "A");
        pn = pn.getChild(0);
        checkPrimaryKeyIndexScan(pn, "P1", "A");
        assertEquals(((IndexScanPlanNode) pn).getInlinePlanNodes().size(), 1);
        assertNotNull(((IndexScanPlanNode) pn).getInlinePlanNode(PlanNodeType.PROJECTION));

        planNodes = compileToFragments("select A, C FROM (SELECT A, C FROM P1 WHERE A = 3) T1 ");
        assertEquals(1, planNodes.size());
        pn = planNodes.get(0);
        assertTrue(pn instanceof SendPlanNode);
        pn = pn.getChild(0);
        checkSeqScan(pn, "T1",  "A", "C");
        pn = pn.getChild(0);
        checkPrimaryKeyIndexScan(pn, "P1", "A", "C");
        assertEquals(((IndexScanPlanNode) pn).getInlinePlanNodes().size(), 1);
        assertNotNull(((IndexScanPlanNode) pn).getInlinePlanNode(PlanNodeType.PROJECTION));

        // Single partition query without selecting partition column from sub-query
        planNodes = compileToFragments("select C FROM (SELECT A, C FROM P1 WHERE A = 3) T1 ");
        assertEquals(1, planNodes.size());
        planNodes = compileToFragments("select C FROM (SELECT C FROM P1 WHERE A = 3) T1 ");
        assertEquals(1, planNodes.size());

        //
        // AdHoc multiple partitioned sub-select queries.
        //
        planNodes = compileToFragments("select A, C FROM (SELECT A, C FROM P1) T1 ");
        assertEquals(2, planNodes.size());
        pn = planNodes.get(0).getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ReceivePlanNode);
        pn = planNodes.get(1).getChild(0);
        checkSeqScan(pn, "T1",  "A", "C" );
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode); // This sounds it could be optimized
        pn = pn.getChild(0);
        checkPrimaryKeyIndexScan(pn, "P1", "A", "C");

        planNodes = compileToFragments("select A FROM (SELECT A, C FROM P1 WHERE A > 3) T1 ");
        assertEquals(2, planNodes.size());
        pn = planNodes.get(0).getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ReceivePlanNode);
        pn = planNodes.get(1).getChild(0);
        checkSeqScan(pn, "T1",  "A" );
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        checkPrimaryKeyIndexScan(pn, "P1", "A", "C");


        //
        // Group by
        //
        planNodes = compileToFragments("select C, SD FROM " +
                "(SELECT C, SUM(D) as SD FROM P1 GROUP BY C) T1 ");
        assertEquals(2, planNodes.size());
        pn = planNodes.get(0).getChild(0);
        checkSeqScan(pn, "T1", "C", "SD");
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.PROJECTION));
        pn = pn.getChild(0);
        assertTrue(pn instanceof HashAggregatePlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ReceivePlanNode);

        pn = planNodes.get(1);
        assertTrue(pn instanceof SendPlanNode);
        pn = pn.getChild(0);
        checkPrimaryKeyIndexScan(pn, "P1", "C", "SD");
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.PROJECTION));
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.HASHAGGREGATE));

        // rename group by column
        planNodes = compileToFragments("select X, SD FROM " +
                "(SELECT C AS X, SUM(D) as SD FROM P1 GROUP BY C) T1 ");
        assertEquals(2, planNodes.size());
        pn = planNodes.get(0).getChild(0);
        checkSeqScan(pn, "T1", "X", "SD");
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.PROJECTION));
        pn = pn.getChild(0);
        assertTrue(pn instanceof HashAggregatePlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ReceivePlanNode);

        pn = planNodes.get(1);
        assertTrue(pn instanceof SendPlanNode);
        pn = pn.getChild(0);
        checkPrimaryKeyIndexScan(pn, "P1", "C", "SD");
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.PROJECTION));
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.HASHAGGREGATE));


        AbstractPlanNode nlpn;
        //
        // Partitioned Joined tests
        //
        failToCompile("select * FROM " +
                "(SELECT C, SUM(D) as SD FROM P1 GROUP BY C) T1, P2 where T1.C = P2.A ",
                joinErrorMsg);

        planNodes = compileToFragments("select T1.C, T1.SD FROM " +
                "(SELECT C, SUM(D) as SD FROM P1 GROUP BY C) T1, R1 Where T1.C = R1.C ");
        assertEquals(2, planNodes.size());

        pn = planNodes.get(0).getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        nlpn = pn.getChild(0);
        assertTrue(nlpn instanceof NestLoopPlanNode);
        pn = nlpn.getChild(1);
        checkSeqScan(pn, "R1");
        pn = nlpn.getChild(0);
        checkSeqScan(pn, "T1", "C", "SD");
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.PROJECTION));
        pn = pn.getChild(0);
        assertTrue(pn instanceof HashAggregatePlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ReceivePlanNode);

        pn = planNodes.get(1);
        assertTrue(pn instanceof SendPlanNode);
        pn = pn.getChild(0);
        checkPrimaryKeyIndexScan(pn, "P1", "C", "SD");
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.PROJECTION));
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.HASHAGGREGATE));

        // Group by Partitioned column
        planNodes = compileToFragments("select C, SD FROM " +
                "(SELECT A, C, SUM(D) as SD FROM P1 WHERE A > 3 GROUP BY A, C) T1 ");
        assertEquals(2, planNodes.size());

        planNodes = compileToFragments("select C, SD FROM " +
                "(SELECT A, C, SUM(D) as SD FROM P1 WHERE A = 3 GROUP BY A, C) T1 ");
        assertEquals(1, planNodes.size());

        planNodes = compileToFragments("select T1.C, T1.SD FROM " +
                "(SELECT A, C, SUM(D) as SD FROM P1 WHERE A = 3 GROUP BY A, C) T1, R1 WHERE T1.C = R1.C ");
        assertEquals(1, planNodes.size());

        //
        // Limit
        //
        planNodes = compileToFragments("select C FROM (SELECT C FROM P1 WHERE A > 3 ORDER BY C LIMIT 5) T1 ");
        assertEquals(2, planNodes.size());

        planNodes = compileToFragments("select T1.C FROM (SELECT C FROM P1 WHERE A > 3 ORDER BY C LIMIT 5) T1, " +
                "R1 WHERE T1.C > R1.C ");
        assertEquals(2, planNodes.size());

        planNodes = compileToFragments("select C FROM (SELECT A, C FROM P1 WHERE A = 3 ORDER BY C LIMIT 5) T1 ");
        assertEquals(1, planNodes.size());
        // Without selecting partition column from sub-query
        planNodes = compileToFragments(("select C FROM (SELECT C FROM P1 WHERE A = 3 ORDER BY C LIMIT 5) T1 "));
        assertEquals(1, planNodes.size());

        planNodes = compileToFragments("select T1.C FROM (SELECT A, C FROM P1 WHERE A = 3 ORDER BY C LIMIT 5) T1, " +
                "R1 WHERE T1.C > R1.C ");
        assertEquals(1, planNodes.size());
        // Without selecting partition column from sub-query
        planNodes = compileToFragments("select T1.C FROM (SELECT C FROM P1 WHERE A = 3 ORDER BY C LIMIT 5) T1, " +
                "R1 WHERE T1.C > R1.C ");
        assertEquals(1, planNodes.size());

        //
        // Group by & LIMIT 5
        //
        planNodes = compileToFragments("select C, SD FROM " +
                "(SELECT C, SUM(D) as SD FROM P1 GROUP BY C ORDER BY C LIMIT 5) T1 ");
        assertEquals(2, planNodes.size());

        // Without selecting partition column from sub-query
        planNodes = compileToFragments("select C, SD FROM " +
                "(SELECT C, SUM(D) as SD FROM P1 WHERE A = 3 GROUP BY C ORDER BY C LIMIT 5) T1 ");
        assertEquals(1, planNodes.size());
    }

    public void testPartitionedCrossLevel() {
        AbstractPlanNode pn;
        List<AbstractPlanNode> planNodes;
        AbstractPlanNode nlpn;

        planNodes = compileToFragments("SELECT T1.A, T1.C, P2.D FROM P2, (SELECT A, C FROM P1) T1 " +
                "where T1.A = P2.A ");
        assertEquals(2, planNodes.size());
        pn = planNodes.get(0).getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ReceivePlanNode);

        pn = planNodes.get(1);
        assertTrue(pn instanceof SendPlanNode);
        nlpn = pn.getChild(0);
        assertTrue(nlpn instanceof NestLoopIndexPlanNode);
        assertEquals(JoinType.INNER, ((NestLoopIndexPlanNode) nlpn).getJoinType());
        pn = nlpn.getChild(0);
        checkSeqScan(pn, "T1", "A", "C");
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        checkPrimaryKeyIndexScan(pn, "P1", "A", "C");
        // Check inlined index scan
        pn = ((NestLoopIndexPlanNode) nlpn).getInlinePlanNode(PlanNodeType.INDEXSCAN);
        checkPrimaryKeyIndexScan(pn, "P2", "A", "D");


        planNodes = compileToFragments("SELECT P2.A, P2.C FROM P2, (SELECT A, C FROM P1) T1 " +
                "where T1.A = P2.A and P2.A = 1");
        assertEquals(1, planNodes.size());

        planNodes = compileToFragments("SELECT P2.A, P2.C FROM P2, (SELECT A, C FROM P1) T1 " +
                "where T1.A = P2.A and T1.A = 1");
        assertEquals(1, planNodes.size());

        planNodes = compileToFragments("SELECT P2.A, P2.C FROM P2, (SELECT A, C FROM P1 where P1.A = 3) T1 " +
                "where T1.A = P2.A ");
        assertEquals(1, planNodes.size());

        // Distributed join
        planNodes = compileToFragments("select D1, D2 " +
                "FROM (SELECT A, D D1 FROM P1 ) T1, (SELECT A, D D2 FROM P2 ) T2 " +
                "WHERE T1.A = T2.A");
        assertEquals(2, planNodes.size());

        planNodes = compileToFragments("select D1, P2.D " +
                "FROM (SELECT A, D D1 FROM P1 WHERE A=1) T1, P2 " +
                "WHERE T1.A = P2.A AND P2.A = 1");
        assertEquals(1, planNodes.size());


        // TODO (xin): Make it compile in future
//        planNodes = compileToFragments("select T1.A, T1.C, T1.SD FROM " +
//                "(SELECT A, C, SUM(D) as SD FROM P1 WHERE A > 3 GROUP BY A, C) T1, P2 WHERE T1.A = P2.A");


        // (1) Multiple level subqueries (recursive) partition detecting
        planNodes = compileToFragments("select * from p2, " +
                "(select * from (SELECT A, D D1 FROM P1) T1) T2 where p2.A = T2.A");
        assertEquals(2, planNodes.size());

        planNodes = compileToFragments("select * from p2, " +
                "(select * from (SELECT A, D D1 FROM P1 WHERE A=2) T1) T2 " +
                "where p2.A = T2.A ");
        assertEquals(1, planNodes.size());

        planNodes = compileToFragments("select * from p2, " +
                "(select * from (SELECT P1.A, P1.D FROM P1, P3 where P1.A = P3.A) T1) T2 " +
                "where p2.A = T2.A");
        assertEquals(2, planNodes.size());

        planNodes = compileToFragments("select * from p2, " +
                "(select * from (SELECT P1.A, P1.D FROM P1, P3 where P1.A = P3.A) T1) T2 " +
                "where p2.A = T2.A and P2.A = 1");
        assertEquals(1, planNodes.size());


        // (2) Multiple subqueries on the same level partition detecting
        planNodes = compileToFragments("select D1, D2 FROM " +
                "(SELECT A, D D1 FROM P1 WHERE A=2) T1, " +
                "(SELECT A, D D2 FROM P2 WHERE A=2) T2");
        assertEquals(1, planNodes.size());


        planNodes = compileToFragments("select D1, D2 FROM " +
                "(SELECT A, D D1 FROM P1 WHERE A=2) T1, " +
                "(SELECT A, D D2 FROM P2) T2 where T2.A = 2");
        assertEquals(1, planNodes.size());

        planNodes = compileToFragments("select D1, D2 FROM " +
                "(SELECT A, D D1 FROM P1) T1, " +
                "(SELECT A, D D2 FROM P2 WHERE A=2) T2 where T1.A = 2");
        assertEquals(1, planNodes.size());


        // partitioned column renaming tests
        planNodes = compileToFragments("select D1, D2 FROM " +
                "(SELECT A A1, D D1 FROM P1) T1, " +
                "(SELECT A, D D2 FROM P2 WHERE A=2) T2 where T1.A1 = 2");
        assertEquals(1, planNodes.size());

        planNodes = compileToFragments("select D1, D2 FROM " +
                "(SELECT A, D D1 FROM P1 WHERE A=2) T1, " +
                "(SELECT A A2, D D2 FROM P2 ) T2 where T2.A2 = 2");
        assertEquals(1, planNodes.size());


        planNodes = compileToFragments("select A1, A2, D1, D2 " +
                "FROM (SELECT A A1, D D1 FROM P1 WHERE A=2) T1, " +
                "(SELECT A A2, D D2 FROM P2) T2 where T2.A2=2");
        assertEquals(1, planNodes.size());

        planNodes = compileToFragments("select A1, A2, D1, D2 " +
                "FROM (SELECT A A1, D D1 FROM P1 WHERE A=2) T1, " +
                "(SELECT A A2, D D2 FROM P2) T2 where T2.A2=2");
        assertEquals(1, planNodes.size());


        // Test with LIMIT
        failToCompile("select A1, A2, D1, D2 " +
                "FROM (SELECT A A1, D D1 FROM P1 WHERE A=2) T1, " +
                "(SELECT A A2, D D2 FROM P2 ORDER BY D LIMIT 3) T2 where T2.A2=2",
                joinErrorMsg);
    }

    public void testPartitionedGroupByWithoutAggregate() {
        AbstractPlanNode pn;
        List<AbstractPlanNode> planNodes;

        // group by non-partition column, no pushed down
        planNodes = compileToFragments(
                "SELECT * FROM (SELECT C FROM P1 GROUP BY C) T1");
        assertEquals(2, planNodes.size());
        pn = planNodes.get(0).getChild(0);
        checkSeqScan(pn, "T1");
        pn = pn.getChild(0);
        assertTrue(pn instanceof HashAggregatePlanNode);

        pn = planNodes.get(1).getChild(0);
        checkPrimaryKeyIndexScan(pn, "P1");

        // count(*), no pushed down
        planNodes = compileToFragments(
                "SELECT count(*) FROM (SELECT c FROM P1 GROUP BY c) T1");
        assertEquals(2, planNodes.size());
        pn = planNodes.get(0).getChild(0);
        assertTrue(pn instanceof TableCountPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof HashAggregatePlanNode);

        pn = planNodes.get(1).getChild(0);
        checkPrimaryKeyIndexScan(pn, "P1");


        // group by partition column, pushed down
        planNodes = compileToFragments(
                "SELECT * FROM (SELECT A FROM P1 GROUP BY A) T1");
        assertEquals(2, planNodes.size());
        pn = planNodes.get(0).getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        assertTrue(pn.getChild(0) instanceof ReceivePlanNode);

        pn = planNodes.get(1).getChild(0);
        checkSeqScan(pn, "T1");
        pn = pn.getChild(0);
        checkPrimaryKeyIndexScan(pn, "P1");

        planNodes = compileToFragments(
                "SELECT count(*) FROM (SELECT A FROM P1 GROUP BY A) T1");
        assertEquals(2, planNodes.size());
        pn = planNodes.get(0).getChild(0);
        assertTrue(pn.getChild(0) instanceof ReceivePlanNode);

        pn = planNodes.get(1).getChild(0);
        assertTrue(pn instanceof TableCountPlanNode);
        pn = pn.getChild(0);
        checkPrimaryKeyIndexScan(pn, "P1");
    }

    public void testPartitionedGroupBy() {
        AbstractPlanNode pn;
        List<AbstractPlanNode> planNodes;
        AbstractPlanNode nlpn;

        // (1) Single partition query, filter on outer query.
        planNodes = compileToFragments(
                "SELECT * FROM (SELECT A, C FROM P1 GROUP BY A, C) T1 " +
                "where T1.A = 1 ");
        assertEquals(1, planNodes.size());
        pn = planNodes.get(0);
        assertTrue(pn instanceof SendPlanNode);
        pn = pn.getChild(0);
        checkSeqScan(pn, "T1", "A", "C");
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.PROJECTION));
        // Because it group by the partition column, we can drop the group by column on coordinator

        pn = pn.getChild(0);
        checkPrimaryKeyIndexScan(pn, "P1");
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.PROJECTION));
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.PARTIALAGGREGATE));


        // (2) Single partition query, filter in inner sub-query.
        planNodes = compileToFragments(
                "SELECT * FROM (SELECT A, C FROM P1 WHERE A = 1 GROUP BY A, C) T1");
        assertEquals(1, planNodes.size());
        pn = planNodes.get(0);
        assertTrue(pn instanceof SendPlanNode);
        pn = pn.getChild(0);
        checkSeqScan(pn, "T1", "A", "C");
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.PROJECTION));

        pn = pn.getChild(0);
        checkPrimaryKeyIndexScan(pn, "P1");
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.PROJECTION));
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.PARTIALAGGREGATE));


        // (3) Sub-query with replicated table group by
        planNodes = compileToFragments(
                "SELECT * FROM (SELECT A, C FROM R1 GROUP BY A, C) T1, P1 " +
                "where T1.A = P1.A ");
        assertEquals(2, planNodes.size());
        pn = planNodes.get(0);
        assertTrue(pn instanceof SendPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ReceivePlanNode);

        pn = planNodes.get(1);
        assertTrue(pn instanceof SendPlanNode);
        nlpn = pn.getChild(0);
        assertTrue(nlpn instanceof NestLoopIndexPlanNode);
        assertEquals(JoinType.INNER, ((NestLoopIndexPlanNode) nlpn).getJoinType());
        pn = nlpn.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        checkPrimaryKeyIndexScan(pn, "P1");

        pn = nlpn.getChild(0);
        checkSeqScan(pn, "T1");
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.PROJECTION));
        pn = pn.getChild(0);
        checkSeqScan(pn, "R1");
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.PROJECTION));
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.HASHAGGREGATE));


        // Top aggregation node on coordinator
        planNodes = compileToFragments(
                "SELECT -8, T1.NUM FROM SR4 T0, " +
                "(select max(RATIO) RATIO, sum(NUM) NUM, DESC from SP4 group by DESC) T1 " +
                "WHERE (T1.NUM + 5 ) > 44");

        assertEquals(2, planNodes.size());
        pn = planNodes.get(0);
        assertTrue(pn instanceof SendPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        nlpn = pn.getChild(0);
        assertTrue(nlpn instanceof NestLoopPlanNode);
        assertEquals(JoinType.INNER, ((NestLoopPlanNode) nlpn).getJoinType());
        pn = nlpn.getChild(1);
        checkPrimaryKeyIndexScan(pn, "SR4");
        pn = nlpn.getChild(0);
        checkSeqScan(pn, "T1", "NUM");
        pn = pn.getChild(0);
        assertTrue(pn instanceof AggregatePlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ReceivePlanNode);

        pn = planNodes.get(1);
        assertTrue(pn instanceof SendPlanNode);
        pn = pn.getChild(0);
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.HASHAGGREGATE));
        checkPrimaryKeyIndexScan(pn, "SP4");

        //
        // (4) Sub-query with partitioned table group by
        //

        // optimize the group by case to join on distributed node.
        planNodes = compileToFragments(
                "SELECT * FROM (SELECT A, C FROM P1 GROUP BY A, C) T1, P2 " +
                "where T1.A = P2.A");
        assertEquals(2, planNodes.size());
        pn = planNodes.get(0);
        assertTrue(pn instanceof SendPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ReceivePlanNode);

        pn = planNodes.get(1);
        assertTrue(pn instanceof SendPlanNode);
        nlpn = pn.getChild(0);
        assertTrue(nlpn instanceof NestLoopIndexPlanNode);
        assertEquals(JoinType.INNER, ((NestLoopIndexPlanNode) nlpn).getJoinType());
        pn = nlpn.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        checkPrimaryKeyIndexScan(pn, "P2");

        pn = nlpn.getChild(0);
        checkSeqScan(pn, "T1");
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.PROJECTION));
        pn = pn.getChild(0);
        checkPrimaryKeyIndexScan(pn, "P1");
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.PROJECTION));
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.PARTIALAGGREGATE));


        // Add aggregate inside of subquery
        planNodes = compileToFragments(
                "SELECT * FROM (SELECT A, COUNT(*) CT FROM P1 GROUP BY A, C) T1, P2 " +
                "where T1.A = P2.A");
        assertEquals(2, planNodes.size());
        pn = planNodes.get(0);
        assertTrue(pn instanceof SendPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ReceivePlanNode);

        pn = planNodes.get(1);
        assertTrue(pn instanceof SendPlanNode);
        nlpn = pn.getChild(0);
        assertTrue(nlpn instanceof NestLoopIndexPlanNode);
        assertEquals(JoinType.INNER, ((NestLoopIndexPlanNode) nlpn).getJoinType());
        pn = nlpn.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        checkPrimaryKeyIndexScan(pn, "P2");

        pn = nlpn.getChild(0);
        checkSeqScan(pn, "T1");
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.PROJECTION));
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        checkPrimaryKeyIndexScan(pn, "P1");
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.PROJECTION));
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.PARTIALAGGREGATE));

        // Add distinct option to aggregate inside of subquery
        planNodes = compileToFragments(
                "SELECT * FROM (SELECT A, C, SUM(distinct D) FROM P1 GROUP BY A, C) T1, P2 " +
                "where T1.A = P2.A ");
        assertEquals(2, planNodes.size());
        pn = planNodes.get(0);
        assertTrue(pn instanceof SendPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ReceivePlanNode);

        pn = planNodes.get(1);
        assertTrue(pn instanceof SendPlanNode);
        nlpn = pn.getChild(0);
        assertTrue(nlpn instanceof NestLoopIndexPlanNode);
        assertEquals(JoinType.INNER, ((NestLoopIndexPlanNode) nlpn).getJoinType());
        pn = nlpn.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        checkPrimaryKeyIndexScan(pn, "P2");

        pn = nlpn.getChild(0);
        checkSeqScan(pn, "T1");
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.PROJECTION));
        pn = pn.getChild(0);
        checkPrimaryKeyIndexScan(pn, "P1");
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.PROJECTION));
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.PARTIALAGGREGATE));

        // single partition filter inside subquery
        planNodes = compileToFragments(
                "SELECT * FROM (SELECT A, C FROM P1 WHERE A = 3 GROUP BY A, C) T1, P2 " +
                "where T1.A = P2.A ");
        assertEquals(1, planNodes.size());

        pn = planNodes.get(0);
        assertTrue(pn instanceof SendPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        nlpn = pn.getChild(0);
        assertTrue(nlpn instanceof NestLoopIndexPlanNode);
        assertEquals(JoinType.INNER, ((NestLoopIndexPlanNode) nlpn).getJoinType());
        pn = nlpn.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        checkPrimaryKeyIndexScan(pn, "P2");

        pn = nlpn.getChild(0);
        checkSeqScan(pn, "T1");
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.PROJECTION));
        pn = pn.getChild(0);
        checkPrimaryKeyIndexScan(pn, "P1");
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.PROJECTION));
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.PARTIALAGGREGATE));

        // single partition filter outside subquery
        planNodes = compileToFragments(
                "SELECT * FROM (SELECT A, C FROM P1 GROUP BY A, C) T1, P2 " +
                "where T1.A = P2.A and P2.A = 3");
        assertEquals(1, planNodes.size());

        pn = planNodes.get(0);
        assertTrue(pn instanceof SendPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        nlpn = pn.getChild(0);
        assertTrue(nlpn instanceof NestLoopPlanNode);
        assertEquals(JoinType.INNER, ((NestLoopPlanNode) nlpn).getJoinType());
        pn = nlpn.getChild(0);
        checkSeqScan(pn, "T1");
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.PROJECTION));
        pn = pn.getChild(0);
        checkPrimaryKeyIndexScan(pn, "P1");
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.PROJECTION));
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.PARTIALAGGREGATE));

        pn = nlpn.getChild(1);
        checkPrimaryKeyIndexScan(pn, "P2");


        planNodes = compileToFragments(
                "SELECT * FROM (SELECT A, C FROM P1 GROUP BY A, C) T1, P2 " +
                "where T1.A = P2.A and T1.A = 3");
        assertEquals(1, planNodes.size());

        pn = planNodes.get(0);
        assertTrue(pn instanceof SendPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        nlpn = pn.getChild(0);
        assertTrue(nlpn instanceof NestLoopPlanNode);
        assertEquals(JoinType.INNER, ((NestLoopPlanNode) nlpn).getJoinType());
        pn = nlpn.getChild(0);
        checkSeqScan(pn, "T1");
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.PROJECTION));
        pn = pn.getChild(0);
        checkPrimaryKeyIndexScan(pn, "P1");
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.PROJECTION));
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.PARTIALAGGREGATE));

        pn = nlpn.getChild(1);
        checkPrimaryKeyIndexScan(pn, "P2");


        // Group by C, A instead of A, C
        planNodes = compileToFragments(
                "SELECT * FROM (SELECT A, C FROM P1 GROUP BY C, A) T1, P2 " +
                "where T1.A = P2.A and T1.A = 3");
        assertEquals(1, planNodes.size());

        pn = planNodes.get(0);
        assertTrue(pn instanceof SendPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        nlpn = pn.getChild(0);
        assertTrue(nlpn instanceof NestLoopPlanNode);
        assertEquals(JoinType.INNER, ((NestLoopPlanNode) nlpn).getJoinType());
        pn = nlpn.getChild(0);
        checkSeqScan(pn, "T1");
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.PROJECTION));
        pn = pn.getChild(0);
        checkPrimaryKeyIndexScan(pn, "P1");
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.PROJECTION));
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.PARTIALAGGREGATE));

        pn = nlpn.getChild(1);
        checkPrimaryKeyIndexScan(pn, "P2");

    }

    public void testTableAggSubquery() {
        AbstractPlanNode pn;
        List<AbstractPlanNode> planNodes;
        AbstractPlanNode nlpn;

        planNodes = compileToFragments(
                "SELECT * FROM (SELECT sum(C) AS SC FROM P1) T1");
        assertEquals(2, planNodes.size());
        pn = planNodes.get(0).getChild(0);
        checkSeqScan(pn, "T1", "SC");
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.PROJECTION));
        pn = pn.getChild(0);
        assertTrue(pn instanceof AggregatePlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ReceivePlanNode);

        pn = planNodes.get(1);
        assertTrue(pn instanceof SendPlanNode);
        pn = pn.getChild(0);
        checkSeqScan(pn, "P1");
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.PROJECTION));
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.AGGREGATE));


        failToCompile("SELECT * FROM (SELECT sum(C) AS SC FROM P1) T1, P2 " +
                "where P2.A = T1.SC", joinErrorMsg);

        failToCompile("SELECT * FROM (SELECT count(A) as A FROM P1) T1, P2 " +
                "where P2.A = T1.A", joinErrorMsg);

        // Special non-push-down-able join case where the join must follow the
        // agg which must follow the send/receive.
        planNodes = compileToFragments(
                "SELECT * FROM (SELECT sum(C) AS SC FROM P1) T1, R1 " +
                "where R1.A = T1.SC");
        assertEquals(2, planNodes.size());
        //* enable to debug */ System.out.println(planNodes.get(0).toExplainPlanString());
        //* enable to debug */ System.out.println(planNodes.get(1).toExplainPlanString());
        pn = planNodes.get(0).getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        nlpn = pn.getChild(0);
        assertTrue(nlpn instanceof NestLoopPlanNode);
        assertEquals(JoinType.INNER, ((NestLoopPlanNode) nlpn).getJoinType());

        pn = nlpn.getChild(1);
        checkSeqScan(pn, "R1");

        pn = nlpn.getChild(0);
        checkSeqScan(pn, "T1");
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.PROJECTION));
        pn = pn.getChild(0);
        assertTrue(pn instanceof AggregatePlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ReceivePlanNode);

        pn = planNodes.get(1).getChild(0);
        checkPrimaryKeyIndexScan(pn, "P1");
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.PROJECTION));
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.AGGREGATE));
    }

    /*
     * LIMIT/OFFSET/DISTINCT/GROUP BY are not always bad guys.
     * When they apply on the replicated table only, the subquery that
     * contains them should be able to drop the receive node at the top
     * of subqueries' partitioned tables.
     */
    public void testFineGrainedCases() {
        // LIMIT comes from replicated table which has no receive node
        checkPushedDownJoins(3,
                "SELECT * FROM (SELECT P1.A, R1.C FROM R1, P1,  " +
                "                (SELECT A, C FROM R2 LIMIT 5) T0 where R1.A = T0.A ) T1, " +
                "              P2 " +
                "where T1.A = P2.A");
        // Distinct apply on replicated table only
        checkPushedDownJoins(3,
                "SELECT * FROM (SELECT P1.A, R1.C FROM R1, P1,  " +
                "                (SELECT Distinct A, C FROM R2 where A > 3) T0 where R1.A = T0.A ) T1, " +
                "              P2 " +
                "where T1.A = P2.A");
        // table count
        checkPushedDownJoins(3,
                "SELECT * FROM (SELECT P1.A, R1.C FROM R1, P1,  " +
                "                (SELECT COUNT(*) AS A FROM R2 where C > 3) T0 where R1.A = T0.A ) T1, " +
                "              P2 " +
                "where T1.A = P2.A");
        // group by
        checkPushedDownJoins(3,
                "SELECT * FROM (SELECT P1.A, R1.C FROM R1, P1,  " +
                "                (SELECT A, COUNT(*) C FROM R2 where C > 3 GROUP BY A) T0 where R1.A = T0.A ) T1, " +
                "              P2 " +
                "where T1.A = P2.A");
        //
        checkPushedDownJoins(3,
                "SELECT * FROM (SELECT P1.A, R1.C FROM R1, P1,  " +
                "                (SELECT A, C FROM R2 where C > 3 ) T0 where R1.A = T0.A ) T1, " +
                "              P2 " +
                "where T1.A = P2.A");
    }

    private void checkJoinNode(AbstractPlanNode root, PlanNodeType type, int num) {
        List<AbstractPlanNode> nodes = root.findAllNodesOfType(type);
        if (num > 0) {
            assertEquals(num, nodes.size());
        }
    }

    private void checkPushedDownJoins(int nestLoopCount, String joinQuery) {
        List<AbstractPlanNode> planNodes = compileToFragments(joinQuery);
        assertEquals(2, planNodes.size());
        //* enable to debug */ System.out.println(planNodes.get(0).toExplainPlanString());
        checkJoinNode(planNodes.get(0), PlanNodeType.NESTLOOP, 0);
        checkJoinNode(planNodes.get(0), PlanNodeType.NESTLOOPINDEX, 0);
        // Join on distributed node
        //* enable to debug */ System.out.println(planNodes.get(1).toExplainPlanString());
        checkJoinNode(planNodes.get(1), PlanNodeType.NESTLOOP, nestLoopCount);
    }

    public void testPartitionedLimitOffset() {
        AbstractPlanNode pn;
        List<AbstractPlanNode> planNodes;
        AbstractPlanNode nlpn;

        // Top aggregation node on coordinator
        planNodes = compileToFragments(
                "SELECT -8, T1.NUM " +
                "FROM SR4 T0, (select RATIO, NUM, DESC from SP4 order by DESC, NUM, RATIO limit 1 offset 1) T1 " +
                "WHERE (T1.NUM + 5 ) > 44");

        assertEquals(2, planNodes.size());
        pn = planNodes.get(0);
        assertTrue(pn instanceof SendPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        nlpn = pn.getChild(0);
        assertTrue(nlpn instanceof NestLoopPlanNode);
        assertEquals(JoinType.INNER, ((NestLoopPlanNode) nlpn).getJoinType());
        pn = nlpn.getChild(1);
        checkPrimaryKeyIndexScan(pn, "SR4");
        pn = nlpn.getChild(0);
        checkSeqScan(pn, "T1", "NUM");
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        // inline limit with order by
        assertTrue(pn instanceof MergeReceivePlanNode);
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.LIMIT));
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.ORDERBY));

        pn = planNodes.get(1).getChild(0);
        // inline limit with order by
        assertTrue(pn instanceof OrderByPlanNode);
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.LIMIT));
        pn = pn.getChild(0);
        checkPrimaryKeyIndexScan(pn, "SP4");


        planNodes = compileToFragments(
                "SELECT * FROM (SELECT A, C FROM P1 LIMIT 3) T1 " +
                "where T1.A = 1 ");
        assertEquals(2, planNodes.size());
    }

    public void testPartitionedAlias() {
        List<AbstractPlanNode> planNodes;
        planNodes = compileToFragments("SELECT * FROM P1 X, P2 Y where X.A = Y.A");
        assertEquals(2, planNodes.size());


        // Rename partition columns in sub-query
        planNodes = compileToFragments(
                "SELECT * FROM " +
                "   (select P1.A P1A, P2.A P2A from P1, P2 where p1.a=p2.a and p1.a = 1) T1," +
                "   P3, P4 " +
                "WHERE P3.A = P4.A and T1.P1A = P3.A");
        assertEquals(1, planNodes.size());

        planNodes = compileToFragments(
                "SELECT * FROM " +
                "   (select P1.A P1A, P2.A P2A from P1, P2 where p1.a=p2.a and p1.a = 1) T1," +
                "   P3, P4 " +
                "WHERE P3.A = P4.A and T1.P1A = P4.A");
        assertEquals(1, planNodes.size());

        planNodes = compileToFragments(
                "SELECT * FROM " +
                "   (select P1.A P1A, P2.A P2A from P1, P2 where p1.a=p2.a and p1.a = 1) T1," +
                "   P3, P4 " +
                "WHERE T1.P1A = P4.A and T1.P1A = P3.A");
        assertEquals(1, planNodes.size());


        planNodes = compileToFragments(
                "SELECT * FROM " +
                "   (select P1.A P1A, P2.A P2A from P1, P2 where p1.a=p2.a and p1.a = 1) T1," +
                "   P3, P4 " +
                "WHERE T1.P2A = P4.A and T1.P2A = P3.A");
        assertEquals(1, planNodes.size());

        planNodes = compileToFragments(
                "SELECT * FROM " +
                "   (select P1.A P1A, P2.A P2A from P1, P2 where p1.a=p2.a and p1.a = 1) T1," +
                "   P3, P4 " +
                "WHERE P3.A = P4.A and T1.P2A = P3.A");
        assertEquals(1, planNodes.size());


        // Rename partition columns in sub-query
        planNodes = compileToFragments(
                "SELECT * FROM " +
                "   (select P1.A P1A, P2.A P2A from P1, P2 where p1.a=p2.a and p1.a = 1) T1," +
                "   P3 X, P4 Y " +
                "WHERE X.A = Y.A and T1.P1A = X.A");
        assertEquals(1, planNodes.size());

    }

    private final String joinErrorMsg = ".";
    public void testUnsupportedCases() {
        // (1)
        // sub-selected table must have an alias
        //
        failToCompile("select A, ABS(C) FROM (SELECT A A1, C FROM R1) T1",
                "user lacks privilege or object not found: A");
        failToCompile("select A+1, ABS(C) FROM (SELECT A A1, C FROM R1) T1",
                "user lacks privilege or object not found: A");

        // (2)
        // sub-selected table must have an alias
        //
        String errorMessage = "Every derived table must have its own alias.";
        failToCompile("select C FROM (SELECT C FROM R1)  ", errorMessage);

        // (3)
        // sub-selected table must have an valid join criteria.
        //

        // Joined on different columns (not on their partitioned columns)
        failToCompile("select * from (SELECT A, D D1 FROM P1) T1, P2 where p2.D = T1.D1",
                joinErrorMsg);

        failToCompile("select T1.A, T1.C, T1.SD FROM " +
                "(SELECT A, C, SUM(D) as SD FROM P1 WHERE A > 3 GROUP BY A, C) T1, P2 WHERE T1.C = P2.C ",
                joinErrorMsg);

        // Nested subqueries
        failToCompile("select * from p2, (select * from (SELECT A, D D1 FROM P1) T1) T2 where p2.D= T2.D1",
                joinErrorMsg);
        failToCompile("select * from p2, (select * from (SELECT A, D D1 FROM P1 WHERE A=2) T1) T2 where p2.D = T2.D1",
                joinErrorMsg);

        // Multiple subqueries on same level
        failToCompile("select A, C FROM (SELECT A FROM P1) T1, (SELECT C FROM P2) T2 WHERE T1.A = T2.C ",
                joinErrorMsg);

        failToCompile("select D1, D2 FROM (SELECT A, D D1 FROM P1 WHERE A=1) T1, " +
                "(SELECT A, D D2 FROM P2 WHERE A=2) T2", joinErrorMsg);

        failToCompile("select D1, D2 FROM " +
                "(SELECT A, D D1 FROM P1) T1, (SELECT A, D D2 FROM P2) T2 " +
                "WHERE T1.A = 1 AND T2.A = 2", joinErrorMsg);

        // (4)
        // invalid partition
        //
        failToCompile("select * from (SELECT A, D D1 FROM P1) T1, P2 where p2.A = T1.A + 1",
                joinErrorMsg);

        failToCompile("select * from (SELECT D D1 FROM P1) T1, P2 where P2.A = 1",
                joinErrorMsg);

        failToCompile("select * FROM " +
                "(SELECT C, SUM(D) as SD FROM P1 GROUP BY C) T1, P2 where T1.C = P2.A ",
                joinErrorMsg);


        // (5)
        // ambiguous columns referencing
        //
        failToCompile(
                "SELECT * FROM " +
                "   (select * from P1, P2 where p1.a=p2.a and p1.a = 1) T1," +
                "   P3 X, P4 Y " +
                "WHERE X.A = Y.A and T1.A = X.A",  "T1.A");

        //
        // (6) Subquery with partition table join with partition table on outer level
        //
        failToCompile("SELECT * FROM (SELECT A, C FROM P1 GROUP BY A, C LIMIT 5) T1, P2 " +
                "where T1.A = P2.A", joinErrorMsg);

        failToCompile("SELECT * FROM (SELECT A, C FROM P1 GROUP BY A, C LIMIT 5 OFFSET 1) T1, P2 " +
                "where T1.A = P2.A", joinErrorMsg);

        // Without GROUP BY.
        failToCompile("SELECT * FROM (SELECT COUNT(*) FROM P1) T1, P2 ", joinErrorMsg);
        failToCompile("SELECT * FROM (SELECT MAX(C) FROM P1) T1, P2 ", joinErrorMsg);
        failToCompile("SELECT * FROM (SELECT SUM(A) FROM P1) T1, P2 ", joinErrorMsg);

        failToCompile("SELECT * FROM (SELECT A, C FROM P1 LIMIT 5) T1, P2 " +
                "where T1.A = P2.A", joinErrorMsg);



        // Nested LIMIT/OFFSET
        failToCompile("SELECT * FROM (SELECT R1.A, R1.C FROM R1, " +
                "                     (SELECT A, C FROM P1 LIMIT 5) T0 where R1.A = T0.A ) T1, P2 " +
                "where T1.A = P2.A", joinErrorMsg);


        // Invalid LIMIT/OFFSET on parent subquery with partitoned nested subquery
        failToCompile(
                "SELECT * FROM (SELECT T0.A, R1.C FROM R1, " +
                "                 (SELECT P1.A, C FROM P1,R2 where P1.A = R2.A) T0 " +
                "                 where R1.A = T0.A  ORDER BY T0.A LIMTI 5) T1, " +
                "              P2 " +
                "where T1.A = P2.A");

        // Invalid on the same level
        failToCompile("SELECT * FROM (SELECT A, C FROM P1 GROUP BY A, C LIMIT 5) T1, " +
                "                    (SELECT A, C FROM P2) T2 " +
                "where T1.A = T2.A", joinErrorMsg);

        failToCompile("SELECT * FROM (SELECT A, C FROM P1 GROUP BY A, C LIMIT 5) T1, " +
                "                    (SELECT A, C FROM P2) T2, P3 " +
                "where T1.A = T2.A AND P3.A = T2.A", joinErrorMsg);

        failToCompile("SELECT * FROM (SELECT A, C FROM P1 GROUP BY A, C LIMIT 5) T1, " +
                "                    (SELECT A, C FROM R2) T2, P3 " +
                "where T1.A = T2.A AND P3.A = T2.A", joinErrorMsg);

        // Error in one of the sub-queries and return exception directly for the whole statement
        String sql = "SELECT * FROM (SELECT A, C FROM P1 GROUP BY A, C) T1, " +
                "                    (SELECT P1.A, P1.C FROM P1, P3) T2, P3 " +
                "where T1.A = T2.A AND P3.A = T2.A";
        failToCompile(sql, joinErrorMsg);
        failToCompile(sql, "Subquery statement for table T2 has error");
    }

    /**
     * MANY of these DISTINCT use cases could be supported, some quite easily.
     * The cases that we can not support are those that require a join on
     * partition key AFTER a global distinct operation.
     * Other cases where the DISTINCT can be executed locally -- because it
     * contains the partition key are the most trivial.
     * TODO: make the planner smarter to plan these kind of sub-queries.
     */
    public void testDistinct() {
        AbstractPlanNode pn;
        List<AbstractPlanNode> planNodes;

        planNodes = compileToFragments(
                "SELECT * FROM (SELECT A, C, SUM(distinct D) FROM P2 GROUP BY A, C) T1, R1 where T1.A = R1.A ");
        assertEquals(2, planNodes.size());
        pn = planNodes.get(0).getChild(0);

        assertFalse(pn.toExplainPlanString().contains("DISTINCT"));

        pn = planNodes.get(1).getChild(0);
        // this join can be pushed down.
        //* enable to debug */ System.out.println(pn.toExplainPlanString());
        assertTrue(pn.toExplainPlanString().contains("LOOP INNER JOIN"));
        pn = pn.getChild(0);
        // This is a trivial subquery result scan.
        assertTrue(pn instanceof SeqScanPlanNode);
        pn = pn.getChild(0);
        // This is the subquery plan.
        checkPrimaryKeyIndexScan(pn, "P2");
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.PROJECTION));
        assertTrue(pn.toExplainPlanString().contains("SUM DISTINCT(P2.D"));

        // verify the optimized plan without sub-query like the one above
        planNodes = compileToFragments(
                "SELECT P2.A, P2.C, SUM(distinct P2.D) FROM P2, R1 WHERE P2.A = R1.A GROUP BY P2.A, P2.C");
        assertEquals(2, planNodes.size());
        pn = planNodes.get(0);
        assertTrue(pn instanceof SendPlanNode);
        assertTrue(pn.getChild(0) instanceof ReceivePlanNode);

        pn = planNodes.get(1).getChild(0);
        assertTrue(pn instanceof NestLoopIndexPlanNode);
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.HASHAGGREGATE));

        assertNotNull(pn.getInlinePlanNode(PlanNodeType.INDEXSCAN));
        assertTrue(pn.getInlinePlanNode(PlanNodeType.INDEXSCAN).
                toExplainPlanString().contains("INDEX SCAN of \"P2\" using its primary key index"));

        assertTrue(pn.getChild(0) instanceof SeqScanPlanNode);
        assertTrue(pn.getChild(0).toExplainPlanString().contains("SEQUENTIAL SCAN of \"R1\""));

        // T
        planNodes = compileToFragments(
                "SELECT * FROM (SELECT DISTINCT A FROM P1) T1, P2 where T1.A = P2.A");
        assertEquals(2, planNodes.size());
        //* enable to debug */ System.out.println(planNodes.get(1).toExplainPlanString());
        assertFalse(planNodes.get(0).toExplainPlanString().contains("AGGREGATION"));
        assertFalse(planNodes.get(0).toExplainPlanString().contains("DISTINCT"));
        assertFalse(planNodes.get(0).toExplainPlanString().contains("JOIN"));

        assertTrue(planNodes.get(1).toExplainPlanString().contains("AGGREGATION"));
        assertTrue(planNodes.get(1).toExplainPlanString().contains("INDEX INNER JOIN"));

        // Distinct with GROUP BY
        // TODO: group by partition column cases can be supported
        String errorMessage = "This query is not plannable.  It has a subquery which needs cross-partition access.";
        failToCompile(
                "SELECT * FROM (SELECT DISTINCT A, C FROM P1 GROUP BY A, C) T1, P2 " +
                "where T1.A = P2.A", errorMessage);

        failToCompile(
                "SELECT * FROM (SELECT DISTINCT A FROM P1 GROUP BY A, C) T1, P2 " +
                "where T1.A = P2.A", errorMessage);

        planNodes = compileToFragments(
                "SELECT * " +
                "FROM   (   SELECT T0.A, R1.C " +
                "           FROM   R1, " +
                "                  (   SELECT DISTINCT P1.A " +
                "                      FROM P1, R2 " +
                "                      WHERE P1.A = R2.A) " +
                "                  T0 " +
                "           WHERE  R1.A = T0.A ) " +
                "       T1, " +
                "       P2 " +
                "WHERE T1.A = P2.A");
        assertEquals(2, planNodes.size());
        //* enable to debug */ System.out.println(planNodes.get(1).toExplainPlanString());
        assertFalse(planNodes.get(0).toExplainPlanString().contains("AGGREGATION"));
        assertFalse(planNodes.get(0).toExplainPlanString().contains("DISTINCT"));
        assertFalse(planNodes.get(0).toExplainPlanString().contains("JOIN"));

        assertTrue(planNodes.get(1).toExplainPlanString().contains("AGGREGATION"));
        assertTrue(planNodes.get(1).toExplainPlanString().contains("INDEX INNER JOIN"));
        assertTrue(planNodes.get(1).toExplainPlanString().contains("LOOP INNER JOIN"));

        // Distinct without GROUP BY
        String sql1, sql2;
        sql1 = "SELECT * FROM (SELECT DISTINCT A, C FROM P1) T1, P2 where T1.A = P2.A";
        sql2 = "SELECT * FROM (SELECT A, C FROM P1 GROUP BY A, C) T1, P2 where T1.A = P2.A";
        checkQueriesPlansAreTheSame(sql1, sql2);

        sql1 =  "SELECT * FROM (SELECT T0.A, R1.C FROM R1, " +
                "                (SELECT Distinct P1.A, P1.C FROM P1,R2 where P1.A = R2.A) T0 where R1.A = T0.A ) T1, " +
                "              P2 " +
                "where T1.A = P2.A";
        sql2 =  "SELECT * FROM (SELECT T0.A, R1.C FROM R1, " +
                "                (SELECT P1.A, P1.C FROM P1,R2 where P1.A = R2.A group by P1.A, P1.C) T0 where R1.A = T0.A ) T1, " +
                "              P2 " +
                "where T1.A = P2.A";
        checkQueriesPlansAreTheSame(sql1, sql2);

        planNodes = compileToFragments(
                "SELECT * FROM (SELECT DISTINCT T0.A FROM R1, " +
                "                (SELECT P1.A, P1.C FROM P1,R2 where P1.A = R2.A) T0 where R1.A = T0.A ) T1, " +
                "              P2 " +
                "where T1.A = P2.A");
        assertEquals(2, planNodes.size());
        //* enable to debug */ System.out.println(planNodes.get(1).toExplainPlanString());
        assertFalse(planNodes.get(0).toExplainPlanString().contains("AGGREGATION"));
        assertFalse(planNodes.get(0).toExplainPlanString().contains("DISTINCT"));
        assertFalse(planNodes.get(0).toExplainPlanString().contains("JOIN"));

        assertTrue(planNodes.get(1).toExplainPlanString().contains("AGGREGATION"));
        assertTrue(planNodes.get(1).toExplainPlanString().contains("INDEX INNER JOIN"));
        assertTrue(planNodes.get(1).toExplainPlanString().contains("LOOP INNER JOIN"));

        failToCompile(
                "SELECT * FROM (SELECT DISTINCT A FROM P1 GROUP BY A, C) T1, P2 " +
                "where T1.A = P2.A");

        sql1 =  "SELECT * FROM (SELECT DISTINCT T0.A, R1.C FROM R1, " +
                "                (SELECT P1.A, P1.C FROM P1,R2 where P1.A = R2.A) T0 where R1.A = T0.A ) T1, " +
                "              P2 " +
                "where T1.A = P2.A";
        sql2 =  "SELECT * FROM (SELECT T0.A, R1.C FROM R1, " +
                "                (SELECT P1.A, P1.C FROM P1,R2 where P1.A = R2.A) T0 where R1.A = T0.A GROUP BY T0.A, R1.C) T1, " +
                "              P2 " +
                "where T1.A = P2.A";
        checkQueriesPlansAreTheSame(sql1, sql2);
    }

    public void testEdgeCases() {
        AbstractPlanNode pn;

        pn = compile("select T1.A FROM (SELECT A FROM R1) T1, (SELECT A FROM R2)T2 ");
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        checkOutputSchema("T1", pn, "A");
        pn = pn.getChild(0);
        assertTrue(pn instanceof NestLoopPlanNode);
        checkOutputSchema(pn, "A", "A");
        checkSeqScan(pn.getChild(0), "T1", "A");
        checkSeqScan(pn.getChild(0).getChild(0), "R1", "A");
        checkSeqScan(pn.getChild(1), "T2", "A");
        checkSeqScan(pn.getChild(1).getChild(0), "R2", "A");

        pn = compile("select T2.A FROM (SELECT A FROM R1) T1, (SELECT A FROM R2)T2 ");
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        checkOutputSchema("T2", pn, "A");
        pn = pn.getChild(0);
        assertTrue(pn instanceof NestLoopPlanNode);
        checkOutputSchema(pn, "A", "A");
        checkSeqScan(pn.getChild(0), "T1", "A");
        checkSeqScan(pn.getChild(0).getChild(0), "R1", "A");
        checkSeqScan(pn.getChild(1), "T2", "A");
        checkSeqScan(pn.getChild(1).getChild(0), "R2", "A");

        pn = compile("select T1.A FROM (SELECT A FROM R1) T1, (SELECT A FROM R2) T2 ");
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        checkOutputSchema("T1", pn, "A");
        pn = pn.getChild(0);
        assertTrue(pn instanceof NestLoopPlanNode);
        checkOutputSchema(pn, "A", "A");
        checkSeqScan(pn.getChild(0), "T1", "A");
        checkSeqScan(pn.getChild(0).getChild(0), "R1", "A");
        checkSeqScan(pn.getChild(1), "T2", "A");
        checkSeqScan(pn.getChild(1).getChild(0), "R2", "A");

        // Quick tests of some past spectacular planner failures that sqlcoverage uncovered.

        pn = compile("SELECT 1, * FROM (select * from R1) T1, R2 T2 WHERE T2.A < 3737632230784348203");
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);

        pn = compile("SELECT 2, * FROM (select * from R1) T1, R2 T2 WHERE CASE WHEN T2.A > 44 THEN T2.C END < 44 + 10");
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);

        pn = compile("SELECT -8, T2.C FROM (select * from R1) T1, R1 T2 WHERE (T2.C + 5 ) > 44");
        pn = pn.getChild(0);
        //* enable to debug */ System.out.println(pn.toExplainPlanString());
        assertTrue(pn instanceof ProjectionPlanNode);
    }

    public void testJoinsSimple() {
        AbstractPlanNode pn;
        AbstractPlanNode nlpn;

        pn = compile("select A, C FROM (SELECT A FROM R1) T1, (SELECT C FROM R2) T2 WHERE T1.A = T2.C");
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);

        nlpn = pn.getChild(0);
        assertTrue(nlpn instanceof NestLoopPlanNode);
        assertEquals(2, nlpn.getChildCount());
        pn = nlpn.getChild(0);
        checkSeqScan(pn, "T1",  "A");
        pn= pn.getChild(0);
        checkSeqScan(pn, "R1",  "A");

        pn = nlpn.getChild(1);
        checkSeqScan(pn, "T2",  "C");
        pn= pn.getChild(0);
        checkSeqScan(pn, "R2",  "C");


        // sub-selected table joins
        pn = compile("select A, C FROM (SELECT A FROM R1) T1, (SELECT C FROM R2) T2 WHERE A = C");
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);

        nlpn = pn.getChild(0);
        assertTrue(nlpn instanceof NestLoopPlanNode);
        assertEquals(2, nlpn.getChildCount());
        pn = nlpn.getChild(0);
        checkSeqScan(pn, "T1",  "A");
        pn= pn.getChild(0);
        checkSeqScan(pn, "R1",  "A");

        pn = nlpn.getChild(1);
        checkSeqScan(pn, "T2",  "C");
        pn= pn.getChild(0);
        checkSeqScan(pn, "R2",  "C");
    }

    public void testJoins() {
        AbstractPlanNode pn;
        List<AbstractPlanNode> planNodes;
        AbstractPlanNode nlpn;

        // Left Outer join
        planNodes = compileToFragments("SELECT R1.A, R1.C FROM R1 LEFT JOIN (SELECT A, C FROM R2) T1 ON T1.C = R1.C ");
        assertEquals(1, planNodes.size());
        pn = planNodes.get(0).getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        nlpn = pn.getChild(0);
        assertTrue(nlpn instanceof NestLoopPlanNode);
        assertEquals(JoinType.LEFT, ((NestLoopPlanNode) nlpn).getJoinType());
        pn = nlpn.getChild(0);
        checkSeqScan(pn, "R1", "A", "C");
        pn = nlpn.getChild(1);
        checkSeqScan(pn, "T1", "C");
        pn = pn.getChild(0);
        checkSeqScan(pn, "R2", "A", "C");

        // Join with partitioned tables

        // Join on coordinator: LEFT OUTER JOIN, replicated table on left side
        planNodes = compileToFragments("SELECT R1.A, R1.C FROM R1 LEFT JOIN (SELECT A, C FROM P1) T1 ON T1.C = R1.C ");
        assertEquals(2, planNodes.size());
        pn = planNodes.get(0).getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        nlpn = pn.getChild(0);
        assertTrue(nlpn instanceof NestLoopPlanNode);
        assertEquals(JoinType.LEFT, ((NestLoopPlanNode) nlpn).getJoinType());
        pn = nlpn.getChild(0);
        checkSeqScan(pn, "R1", "A", "C");
        pn = nlpn.getChild(1);
        assertTrue(pn instanceof ReceivePlanNode);

        pn = planNodes.get(1);
        assertTrue(pn instanceof SendPlanNode);
        pn = pn.getChild(0);
        checkSeqScan(pn, "T1", "C");
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        checkPrimaryKeyIndexScan(pn, "P1", "A", "C");

        // Group by inside of the subquery
        // whether it contains group by or not does not matter, because we check it by whether inner side is partitioned or not
        planNodes = compileToFragments("SELECT R1.A, R1.C FROM R1 LEFT JOIN (SELECT A, count(*) C FROM P1 GROUP BY A) T1 ON T1.C = R1.C ");
        assertEquals(2, planNodes.size());
        pn = planNodes.get(0).getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        nlpn = pn.getChild(0);
        assertTrue(nlpn instanceof NestLoopPlanNode);
        assertEquals(JoinType.LEFT, ((NestLoopPlanNode) nlpn).getJoinType());
        pn = nlpn.getChild(0);
        checkSeqScan(pn, "R1", "A", "C");
        pn = nlpn.getChild(1);
        assertTrue(pn instanceof ReceivePlanNode);

        pn = planNodes.get(1);
        assertTrue(pn instanceof SendPlanNode);
        pn = pn.getChild(0);
        checkSeqScan(pn, "T1", "C");
        pn = pn.getChild(0);
        checkPrimaryKeyIndexScan(pn, "P1", "A", "C");
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.PROJECTION));
        // Using index scan for group by only: use serial aggregate instead hash aggregate
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.AGGREGATE));

        // LEFT partition table
        planNodes = compileToFragments("SELECT T1.CC FROM P1 LEFT JOIN (SELECT A, count(*) CC FROM P2 GROUP BY A) T1 ON T1.A = P1.A ");
        assertEquals(2, planNodes.size());
        pn = planNodes.get(0).getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ReceivePlanNode);

        pn = planNodes.get(1);
        assertTrue(pn instanceof SendPlanNode);
        nlpn = pn.getChild(0);
        assertTrue(nlpn instanceof NestLoopPlanNode);
        assertEquals(JoinType.LEFT, ((NestLoopPlanNode) nlpn).getJoinType());

        pn = nlpn.getChild(0);
        checkPrimaryKeyIndexScan(pn, "P1");
        pn = nlpn.getChild(1);
        checkSeqScan(pn, "T1");
        pn = pn.getChild(0);
        checkPrimaryKeyIndexScan(pn, "P2");
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.PROJECTION));
        // Using index scan for group by only: use serial aggregate instead hash aggregate
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.AGGREGATE));


        // Right outer join
        planNodes = compileToFragments("SELECT R1.A, R1.C FROM R1 RIGHT JOIN (SELECT A, count(*) C FROM P1 GROUP BY A) T1 ON T1.C = R1.C ");
        assertEquals(2, planNodes.size());
        pn = planNodes.get(0).getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ReceivePlanNode);

        pn = planNodes.get(1);
        assertTrue(pn instanceof SendPlanNode);
        nlpn = pn.getChild(0);
        assertTrue(nlpn instanceof NestLoopPlanNode);
        assertEquals(JoinType.LEFT, ((NestLoopPlanNode) nlpn).getJoinType());

        pn = nlpn.getChild(1);
        checkSeqScan(pn, "R1", "A", "C");
        pn = nlpn.getChild(0);
        checkSeqScan(pn, "T1", "C");
        pn = pn.getChild(0);
        checkPrimaryKeyIndexScan(pn, "P1", "A", "C");
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.PROJECTION));
        // Using index scan for group by only: use serial aggregate instead hash aggregate
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.AGGREGATE));

        // RIGHT partition table
        planNodes = compileToFragments("SELECT T1.CC FROM P1 RIGHT JOIN (SELECT A, count(*) CC FROM P2 GROUP BY A) T1 ON T1.A = P1.A ");
        assertEquals(2, planNodes.size());
        pn = planNodes.get(0).getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ReceivePlanNode);

        pn = planNodes.get(1);
        assertTrue(pn instanceof SendPlanNode);
        nlpn = pn.getChild(0);
        assertTrue(nlpn instanceof NestLoopIndexPlanNode);
        assertEquals(JoinType.LEFT, ((NestLoopIndexPlanNode) nlpn).getJoinType());

        pn = nlpn.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        checkPrimaryKeyIndexScan(pn, "P1");
        pn = nlpn.getChild(0);
        checkSeqScan(pn, "T1");
        pn = pn.getChild(0);
        checkPrimaryKeyIndexScan(pn, "P2");
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.PROJECTION));
        // Using index scan for group by only: use serial aggregate instead hash aggregate
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.AGGREGATE));

        // Join locally: inner join case for subselects
        planNodes = compileToFragments("SELECT R1.A, R1.C FROM R1 INNER JOIN (SELECT A, C FROM P1) T1 ON T1.C = R1.C ");
        assertEquals(2, planNodes.size());
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
        checkSeqScan(pn, "R1", "A", "C");
        pn = nlpn.getChild(1);
        checkSeqScan(pn, "T1", "C");
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        checkPrimaryKeyIndexScan(pn, "P1", "A", "C");


        // Two sub-queries. One is partitioned and the other one is replicated
        planNodes = compileToFragments("select A, C FROM (SELECT A FROM R1) T1, (SELECT C FROM P1) T2 WHERE T1.A = T2.C ");
        assertEquals(2, planNodes.size());
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
        checkSeqScan(pn, "T1", "A");
        pn = pn.getChild(0);
        checkSeqScan(pn, "R1", "A");

        pn = nlpn.getChild(1);
        checkSeqScan(pn, "T2", "C");
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        checkPrimaryKeyIndexScan(pn, "P1", "C");

        // This is a single fragment plan because planner can detect "A = 3".
        // Join locally
        planNodes = compileToFragments("select A, C FROM (SELECT A FROM R1) T1, (SELECT C FROM P1 where A = 3) T2 " +
                "WHERE T1.A = T2.C ");
        assertEquals(1, planNodes.size());
        pn = planNodes.get(0);
        assertTrue(pn instanceof SendPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        nlpn = pn.getChild(0);
        assertTrue(nlpn instanceof NestLoopPlanNode);
        pn = nlpn.getChild(0);
        checkSeqScan(pn, "T1", "A");
        pn = pn.getChild(0);
        checkSeqScan(pn, "R1", "A");
        pn = nlpn.getChild(1);
        checkSeqScan(pn, "T2", "C");
        pn = pn.getChild(0);
        checkPrimaryKeyIndexScan(pn, "P1", "C");

        assertEquals(((IndexScanPlanNode) pn).getInlinePlanNodes().size(), 1);
        assertNotNull(((IndexScanPlanNode) pn).getInlinePlanNode(PlanNodeType.PROJECTION));


        // More single partition detection
        planNodes = compileToFragments("select C FROM (SELECT P1.C FROM P1, P2 " +
                "WHERE P1.A = P2.A AND P1.A = 3) T1 ");
        assertEquals(1, planNodes.size());

        planNodes = compileToFragments("select T1.C FROM (SELECT P1.C FROM P1, P2 " +
                "WHERE P1.A = P2.A AND P1.A = 3) T1, R1 where T1.C > R1.C ");
        assertEquals(1, planNodes.size());

        planNodes = compileToFragments("select T1.C FROM (SELECT P1.C FROM P1, P2 " +
                "WHERE P1.A = P2.A AND P1.A = 3) T1, (select C FROM R1) T2 where T1.C > T2.C ");
        assertEquals(1, planNodes.size());
    }

    public void testUnions() {
        AbstractPlanNode pn;
        pn = compile("select A, C FROM (SELECT A, C FROM R1 UNION SELECT A, C FROM R2 UNION SELECT A, C FROM R3) T1 order by A ");

        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof OrderByPlanNode);
        pn = pn.getChild(0);
        checkSeqScan(pn, "T1",  "A", "C");
        AbstractPlanNode upn = pn.getChild(0);
        assertTrue(upn instanceof UnionPlanNode);

        pn = upn.getChild(0);
        checkSeqScan(pn, "R1", "A", "C");
        pn = upn.getChild(1);
        checkSeqScan(pn, "R2", "A", "C");
        pn = upn.getChild(2);
        checkSeqScan(pn, "R3", "A", "C");

        String message = "This query is not plannable.  It has a subquery which needs cross-partition access.";
        failToCompile("select * FROM " +
                "(SELECT A, COUNT(*) FROM P1 GROUP BY A " +
                "UNION " +
                "SELECT A, COUNT(*) FROM R2 GROUP BY A) T1 , P2 where T1.A = P2.A ", message);
    }

    public void testParameters() {
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

    public void testMaterializedView() {
        List<AbstractPlanNode> planNodes;
        String sql;

        // partitioned matview self join on partition column
        sql = "SELECT user_heat.s, max(user_heat.hotspot_hm) "
                + "FROM user_heat, (SELECT s, max(heat) heat FROM user_heat  GROUP BY s) maxheat "
                + "WHERE user_heat.s = maxheat.s AND user_heat.heat = maxheat.heat "
                + "GROUP BY user_heat.s;";
        planNodes = compileToFragments(sql);
        assertEquals(2, planNodes.size());

        // rename the partition column and verify it works also
        sql = "SELECT user_heat.s, max(user_heat.hotspot_hm) "
                + "FROM user_heat, (SELECT s as sss, max(heat) heat FROM user_heat  GROUP BY s) maxheat "
                + "WHERE user_heat.s = maxheat.sss AND user_heat.heat = maxheat.heat "
                + "GROUP BY user_heat.s;";
        planNodes = compileToFragments(sql);
        assertEquals(2, planNodes.size());
    }

    /**
     * Expression subquery currently is not optimized to use any index. But this does not prevent the
     * parent query to use index for other purposes.
     */
    public void testExpressionSubqueryWithIndexScan() {
        AbstractPlanNode pn;
        String sql;

        // INDEX on A, for sort order only
        sql = "SELECT A FROM R4 where A in (select A from R4 where A > 3) order by A;";
        pn = compile(sql);

        pn = pn.getChild(0);
        assertTrue(pn instanceof IndexScanPlanNode);
        assertEquals(0, ((IndexScanPlanNode)pn).getSearchKeyExpressions().size());
        assertNotNull(((IndexScanPlanNode)pn).getPredicate());

        // INDEX on A, uniquely match A = 4,
        sql = "SELECT A FROM R4 where A = 4 and C in (select A from R4 where A > 3);";
        pn = compile(sql);

        pn = pn.getChild(0);
        assertTrue(pn instanceof IndexScanPlanNode);
        assertEquals(1, ((IndexScanPlanNode)pn).getSearchKeyExpressions().size());
        AbstractExpression comp = ((IndexScanPlanNode)pn).getSearchKeyExpressions().get(0);
        assertEquals(ExpressionType.VALUE_CONSTANT, comp.getExpressionType());
        assertEquals("4", ((ConstantValueExpression)comp).getValue());

        assertNotNull(((IndexScanPlanNode) pn).getPredicate());
    }

   /**
     * Test to see if scalar subqueries are either allowed where we
     * expect them to be or else cause compilation errors where we
     * don't expect them to be.
     *
     * @throws Exception
     */
    public void testScalarSubqueriesExpectedFailures() throws Exception {

        // Scalar subquery not allowed in limit.
        failToCompile("select A from r1 where C = 1 limit (select D from t where C = 2);",
                      "incompatible data type in operation: ; in LIMIT, OFFSET or FETCH");
        // Scalar subquery not allowed in offset.
        failToCompile("select A from r1 where C = 1 limit 1 offset (select D from r1 where C = 2);",
                      "SQL Syntax error in \"select A from r1 where C = 1 limit 1 offset (select D from r1 where C = 2);\" unexpected token: (");
        // Scalar subquery not allowed in order by
        failToCompile("select A from r1 as parent where C < 100 order by ( select D from r1 where r1.C = parent.C );",
                      "ORDER BY parsed with strange child node type: tablesubquery");

        // Scalar subquery with expression not allowed
        failToCompile("select A from r1 as parent where C < 100 order by ( select max(D) from r1 where r1.C = parent.C ) * 2;",
                "ORDER BY clause with subquery expression is not allowed.");

    }

    /**
     * This test fails to compile, and causes an NPE in the planner (I think).
     * The ticket number, obviously, is 8280.  It's commented out because
     * it fails.
     *
     * @throws Exception
     */

    public void testENG8280() throws Exception {
        // failToCompile("select A from r1 as parent where C < 100 order by ( select D from r1 where r1.C = parent.C ) * 2;","mumble");
    }

    /**
     * Asserts that the plan doesn't use index scans.
     * (Except to ensure determinism).
     * Only looks at the plan for the outermost query.
     * @param sqlText  SQL statement used to produce plan to check
     */
    private void assertPlanHasNoIndexScans(String sqlText) {
        AbstractPlanNode rootNode = compile(sqlText);
        Queue<AbstractPlanNode> nodes = new LinkedList<>();

        nodes.add(rootNode);
        while (! nodes.isEmpty()) {
            AbstractPlanNode node = nodes.remove();
            assertPlanNodeHasNoIndexScans(node);

            nodes.addAll(node.getInlinePlanNodes().values());
            int numChildren = node.getChildCount();
            for (int i = 0; i < numChildren; ++i) {
                nodes.add(node.getChild(i));
            }
        }
    }

    private void assertPlanNodeHasNoIndexScans(AbstractPlanNode node) {
        if (node instanceof IndexScanPlanNode) {
            IndexScanPlanNode indexScan = (IndexScanPlanNode)node;
            assertTrue("Expected plan to use no indexes, but it contains an index scan plan node "
                    + "used for something other than forcing a deterministic order",
                    indexScan.isForDeterminismOnly());
        }
        else {
            String className = node.getClass().getSimpleName();
            assertFalse("Expected plan to use no indexes, but it contains an instance of " + className,
                    className.toLowerCase().contains("index"));
        }
    }

    public void testNoIndexWithSubqueryExpressionIn() {

        // Table R4 has an index on column A.

        // A subquery on the RHS of IN.
        assertPlanHasNoIndexScans(
                "select * from r4 "
                + "where a in (select a from r1);");

        // A correlated subquery on the RHS of IN.
        assertPlanHasNoIndexScans(
                "select * from r4 "
                + "where a in (select a from r1 where r4.a = r1.a);");

        // A correlated subquery where inner table also has an index
        // Note: the inner query (which we are not checking) will have
        // an index scan this case, which is okay.
        assertPlanHasNoIndexScans(
                "select * from r4 "
                + "where a in (select a from r2 where r4.a = r2.a);");

        // Table R5 has an index on (a, c)

        // RowSubqueryExpression on the left
        assertPlanHasNoIndexScans(
                "select * from r5 "
                + "where (a, c) in (select a, c from r1);");

        // RowSubqueryExpression on the left, with correlation
        assertPlanHasNoIndexScans(
                "select * from r5 "
                + "where (a, c) in (select a, c from r1 where (r1.a, r1.c) = (r5.a, r5.c));");
    }

    public void testNoIndexWithSubqueryExpressionRelational() {
        String[] relationalOps = {"=", "!=", "<", "<=", ">", ">="};
        String[] quantifiers = {"", "any", "all"};

        String subqueryTemplates[] = {
                "select * from r4 where a %s %s (select a from r2)",
                "select * from r4 where a %s %s (select a from r2 where r2.a = r4.a)",

                "select * from r5 where (a, c) %s %s (select a, c from r1)",
                "select * from r5 where (a, c) %s %s (select a, c from r1 where (r1.a, r1.c) = (r5.a, r5.c))",

                // This would use an expression index, if not for the subquery.
                "select * from r5 where abs(a - c) %s %s (select abs(a - c) from r1)",
                "select * from r5 where abs(a - c) %s %s (select abs(a - c) from r1 where (r1.a, r1.c) = (r5.a, r5.c))"
        };

        for (String op : relationalOps) {
            for (String quantifier : quantifiers) {
                for (String template : subqueryTemplates) {
                    String query = String.format(template, op, quantifier);
                    assertPlanHasNoIndexScans(query);
                }
            }
        }
    }
}
