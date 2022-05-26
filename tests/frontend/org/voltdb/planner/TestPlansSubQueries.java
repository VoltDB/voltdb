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
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.hsqldb_voltpatches.HSQLInterface;
import org.voltdb.VoltType;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ComparisonExpression;
import org.voltdb.expressions.ConjunctionExpression;
import org.voltdb.expressions.ConstantValueExpression;
import org.voltdb.expressions.ParameterValueExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.parseinfo.StmtSubqueryScan;
import org.voltdb.planner.parseinfo.StmtTableScan;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AggregatePlanNode;
import org.voltdb.plannodes.HashAggregatePlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.MergeReceivePlanNode;
import org.voltdb.plannodes.NestLoopIndexPlanNode;
import org.voltdb.plannodes.NestLoopPlanNode;
import org.voltdb.plannodes.NodeSchema;
import org.voltdb.plannodes.OrderByPlanNode;
import org.voltdb.plannodes.PlanNodeTree;
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

    private void checkOutputSchema(AbstractPlanNode planNode, String... columns) {
        if (columns.length > 0) {
            checkOutputSchema(planNode, null, columns);
        }
    }

    private void checkOutputSchema(AbstractPlanNode planNode,
            String tableAlias, String[] columns) {
        NodeSchema schema = planNode.getOutputSchema();
        assertEquals(columns.length, schema.size());

        for (int i = 0; i < schema.size(); ++i) {
            SchemaColumn col = schema.getColumn(i);
            checkOutputColumn(tableAlias, columns[i], col);
        }
    }

    private void checkOutputSchema(NodeSchema schema, String... qualifiedColumns) {
        assertEquals(qualifiedColumns.length, schema.size());

        for (int i = 0; i < qualifiedColumns.length; ++i) {
            String[] qualifiedColumn = qualifiedColumns[i].split("\\.");
            SchemaColumn col = schema.getColumn(i);
            checkOutputColumn(qualifiedColumn[0], qualifiedColumn[1], col);
        }
    }

    private void checkOutputColumn(String tableAlias, String column, SchemaColumn col) {
        if (tableAlias != null) {
            assertEquals(tableAlias, col.getTableAlias());
        }
        // Try to check column. If not available, check its column alias instead.
        if (col.getColumnName() == null || col.getColumnName().equals("")) {
            assertNotNull(col.getColumnAlias());
            assertEquals(column, col.getColumnAlias());
        }
        else {
            assertEquals(column, col.getColumnName());
        }
    }

    private void checkSeqScan(AbstractPlanNode scanNode, String tableAlias, String... columns) {
        assertEquals(PlanNodeType.SEQSCAN, scanNode.getPlanNodeType());
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

    private void checkPredicateConjunction(AbstractPlanNode pn, int nTerms) {
        AbstractExpression expr = ((SeqScanPlanNode) pn).getPredicate();
        assertTrue(expr instanceof ConjunctionExpression);
        assertEquals(nTerms, countTerms(expr));
    }

    private static int countTerms(AbstractExpression expr) {
        int result = 0;
        AbstractExpression left = expr.getLeft();
        result += (left instanceof ConjunctionExpression) ? countTerms(left) : 1;
        AbstractExpression right = expr.getRight();
        result += (right instanceof ConjunctionExpression) ? countTerms(right) : 1;
        return result;
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
        // DDL use this pattern to define primary key
        // "CONSTRAINT P1_PK_TREE PRIMARY KEY"
        String primaryKeyIndexName = HSQLInterface.AUTO_GEN_NAMED_CONSTRAINT_IDX + tableName + "_PK_TREE";

        checkIndexScan(indexNode, tableName, primaryKeyIndexName, columns);
    }

    private void checkSimple(String sql, String topTableAlias, String[] outputColumns, String tableName, String[] origColumns, boolean checkPredicate) {
        AbstractPlanNode pn = compile(sql);
        pn = pn.getChild(0);

        checkSeqScan(pn, topTableAlias,  outputColumns);
        if (checkPredicate) {
            checkPredicateComparisonExpression(pn, topTableAlias);
        }
        pn = pn.getChild(0);
        checkSeqScan(pn, tableName, origColumns);
    }

    private void checkSimple(String sql, String topTableAlias, String[] outputColumns, String tableName, String[] origColumns) {
        checkSimple(sql, topTableAlias, outputColumns, tableName, origColumns, false);
    }

    public void testSimple() {
        String tbName = "T1";
        String sql, sqlNoSimplification, equivalentSql;

        // The subquery's LIMIT clause is there only to disable the subquery optimization
        // which replaces the subquery with a straight select from the table
        sql = "select A, C FROM (SELECT A, C FROM R1) T1";
        sqlNoSimplification = "select A, C FROM (SELECT A, C FROM R1 LIMIT 10) T1";
        equivalentSql = "SELECT A, C FROM R1 T1";
        checkSimple(sqlNoSimplification, tbName, new String[]{"A", "C"}, "R1", new String[]{"A", "C"});
        checkSubquerySimplification(sql, equivalentSql);

        sql = "select A, C FROM (SELECT A, C FROM R1) T1 WHERE A > 0";
        sqlNoSimplification = "select A, C FROM (SELECT A, C FROM R1  LIMIT 10) T1 WHERE A > 0";
        equivalentSql = "SELECT A, C FROM R1 T1 WHERE A > 0";
        checkSimple(sqlNoSimplification, tbName, new String[]{"A", "C"}, "R1", new String[]{"A", "C"}, true);
        checkSubquerySimplification(sql, equivalentSql);

        sql = "select A, C FROM (SELECT A, C FROM R1) T1 WHERE T1.A < 0";
        sqlNoSimplification = "select A, C FROM (SELECT A, C FROM R1  LIMIT 10) T1 WHERE T1.A < 0";
        equivalentSql = "SELECT A, C FROM R1 T1 WHERE T1.A < 0;";
        checkSimple(sqlNoSimplification, tbName, new String[]{"A", "C"}, "R1", new String[]{"A", "C"}, true);
        checkSubquerySimplification(sql, equivalentSql);

        sql = "select A1, C1 FROM (SELECT A A1, C C1 FROM R1) T1 WHERE T1.A1 < 0";
        sqlNoSimplification = "select A1, C1 FROM (SELECT A A1, C C1 FROM R1 LIMIT 10) T1 WHERE T1.A1 < 0";
        equivalentSql = "select A A1, C C1 FROM R1 T1 WHERE T1.A < 0";
        checkSimple(sqlNoSimplification, tbName, new String[]{"A1", "C1"}, "R1", new String[]{"A", "C"}, true);
        checkSubquerySimplification(sql, equivalentSql);

        // With projection.
        sql = "select C1 FROM (SELECT A A1, C C1 FROM R1) T1 WHERE T1.A1 < 0";
        sqlNoSimplification = "select C1 FROM (SELECT A A1, C C1 FROM R1  LIMIT 10) T1 WHERE T1.A1 < 0";
        equivalentSql = "select C C1 FROM R1 T1 WHERE T1.A < 0";
        checkSimple(sqlNoSimplification, tbName, new String[]{"C1"}, "R1", new String[]{"A", "C"}, true);
        checkSubquerySimplification(sql, equivalentSql);

        // LIMIT in sub selects
        // Complex columns in sub selects
        checkSimple("select COL1 FROM (SELECT A+3, C COL1 FROM R1  LIMIT 10) T1 WHERE T1.COL1 < 0",
                tbName, new String[]{"COL1"}, "R1", new String[]{"C1", "C"}, true);

        // select *
        sql = "select A, C FROM (SELECT * FROM R1) T1 WHERE T1.A < 0";
        sqlNoSimplification = "select A, C FROM (SELECT * FROM R1  LIMIT 10) T1 WHERE T1.A < 0";
        equivalentSql = "select A, C FROM R1 T1 WHERE T1.A < 0";
        checkSimple("select A, C FROM (SELECT * FROM R1  LIMIT 10) T1 WHERE T1.A < 0",
                tbName, new String[]{"A", "C"}, "R1", new String[]{"A", "C", "D"}, true);
        checkSubquerySimplification(sql, equivalentSql);

        sql = "select * FROM (SELECT A, D FROM R1) T1 WHERE T1.A < 0";
        sqlNoSimplification = "select * FROM (SELECT A, D FROM R1 LIMIT 10) T1 WHERE T1.A < 0";
        equivalentSql = "select A, D FROM R1 T1 WHERE T1.A < 0";
        checkSimple("select * FROM (SELECT A, D FROM R1  LIMIT 10) T1 WHERE T1.A < 0",
                tbName, new String[]{"A", "D"}, "R1", new String[]{"A", "D"}, true);
        checkSubquerySimplification(sql, equivalentSql);

        sql = "select A, C FROM (SELECT * FROM R1 where D > 3) T1 WHERE T1.A < 0";
        sqlNoSimplification = "select A, C FROM (SELECT * FROM R1 where D > 3  LIMIT 10) T1 WHERE T1.A < 0";
        equivalentSql = "select A, C FROM R1 T1 where T1.A < 0 and T1.D > 3";
        checkSimple(sqlNoSimplification, tbName, new String[]{"A", "C"}, "R1", new String[]{"A", "C", "D"}, true);
        checkSubquerySimplification(sql, equivalentSql);
    }

    public void testMultipleLevelsNested() {
        AbstractPlanNode pn;
        List<AbstractPlanNode> planNodes;

        // Three levels selects
        pn = compile("select A2 FROM " +
                "(SELECT A1 AS A2 FROM " +
                "(SELECT A AS A1 FROM R1 WHERE A < 3 LIMIT 10) T1 " +
                "WHERE T1.A1 > 0) T2 " +
                "WHERE T2.A2 = 3");
        pn = pn.getChild(0);
        checkSeqScan(pn, "T2",  "A2");
        checkPredicateComparisonExpression(pn, "T2");
        pn = pn.getChild(0);
        checkSeqScan(pn, "T1",  "A1");
        checkPredicateComparisonExpression(pn, "T1");
        pn = pn.getChild(0);
        checkSeqScan(pn, "R1",  "A");
        checkPredicateComparisonExpression(pn, "R1");

        pn = compile("SELECT A2 FROM " +
                "(SELECT A1 AS A2 FROM " +
                "(SELECT A + 1 AS A1 FROM R1 WHERE A < 3) T1 " +
                "WHERE T1.A1 > 0) T2 " +
                "WHERE T2.A2 = 3");
        pn = pn.getChild(0);
        checkSeqScan(pn, "T2", "A2");
        checkPredicateConjunction(pn, 3);

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

    private void checkFunctions(String sql, String tableName, String[] outputColumns, String origTable, String[] origColumns) {
        AbstractPlanNode pn;
        pn = compile(sql);
        pn = pn.getChild(0);
        checkSeqScan(pn, tableName,  outputColumns);
        pn = pn.getChild(0);
        checkSeqScan(pn, origTable, origColumns);
    }

    public void testFunctions() {
        String tbName = "T1";
        String sql, sqlNoSimplification, equivalentSql;

        // Function expression
        sql = "select ABS(C) FROM (SELECT A, C FROM R1) T1";
        sqlNoSimplification = "select ABS(C) FROM (SELECT A, C FROM R1 LIMIT 5) T1";
        equivalentSql =  "select ABS(C) FROM R1 T1";
        checkFunctions(sqlNoSimplification, tbName, new String[]{"C1"}, "R1", new String[]{"A", "C"});
        checkSubquerySimplification(sql, equivalentSql);

        // Use alias column from sub select instead.
        sql = "select A1, ABS(C) FROM (SELECT A A1, C FROM R1) T1";
        sqlNoSimplification = "select A1, ABS(C) FROM (SELECT A A1, C FROM R1 LIMIT 5) T1";
        equivalentSql = "select A A1, ABS(C) FROM R1 T1";
        checkFunctions(sqlNoSimplification, tbName, new String[]{"A1", "C2"}, "R1", new String[]{"A", "C"});
        checkSubquerySimplification(sql, equivalentSql);

        sql = "select A1 + 3, ABS(C) FROM (SELECT A A1, C FROM R1) T1";
        sqlNoSimplification = "select A1 + 3, ABS(C) FROM (SELECT A A1, C FROM R1 LIMIT 5) T1";
        equivalentSql = "select A + 3, ABS(C) FROM R1 T1";
        checkFunctions(sqlNoSimplification, tbName, new String[]{"C1", "C2"}, "R1", new String[]{"A", "C"});
        checkSubquerySimplification(sql, equivalentSql);

        sql = "select A1 + 3, ABS(C) FROM (SELECT A A1, C FROM R1) T1 WHERE ABS(A1) > 3";
        sqlNoSimplification = "select A1 + 3, ABS(C) FROM (SELECT A A1, C FROM R1 LIMIT 5) T1 WHERE ABS(A1) > 3";
        equivalentSql = "select A + 3, ABS(C) FROM R1 T1 WHERE ABS(A) > 3";
        checkFunctions(sqlNoSimplification, tbName, new String[]{"C1", "C2"}, "R1", new String[]{"A", "C"});
        checkSubquerySimplification(sql, equivalentSql);
    }

    private void checkReplicatedOne(String sql) {
        AbstractPlanNode pn;
        List<AbstractPlanNode> planNodes;
        AbstractPlanNode nlpn;
        planNodes = compileToFragments(sql);

        assertEquals(1, planNodes.size());
        pn = planNodes.get(0);
        assertTrue(pn instanceof SendPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        nlpn = pn.getChild(0);
        assertTrue(nlpn instanceof NestLoopPlanNode);
    }

    private void checkReplicatedTwo(String sql, int nljCount, int nlijCount) {
        AbstractPlanNode pn;
        List<AbstractPlanNode> planNodes;
        planNodes = compileToFragments(sql);

        assertEquals(2, planNodes.size());

        pn = planNodes.get(0).getChild(0);
        if (pn instanceof ProjectionPlanNode) {
            pn = pn.getChild(0);
        }
        assertTrue(pn instanceof ReceivePlanNode);

        pn = planNodes.get(1);
        assertTrue(pn instanceof SendPlanNode);
        checkJoinNode(pn, PlanNodeType.NESTLOOP, nljCount);
        checkJoinNode(pn, PlanNodeType.NESTLOOPINDEX, nlijCount);
    }

    public void testReplicated() {
        String sql, sqlNoSimplification, equivalentSql;

        sql = "select T1.A, P1.C FROM (SELECT A FROM R1) T1, P1 WHERE T1.A = P1.C AND P1.A = 3 ";
        sqlNoSimplification = "select T1.A, P1.C FROM (SELECT A FROM R1  LIMIT 5) T1, P1 WHERE T1.A = P1.C AND P1.A = 3 ";
        equivalentSql = "select T1.A, P1.C FROM R1 T1, P1 WHERE T1.A = P1.C AND P1.A = 3 ";
        checkReplicatedOne(sqlNoSimplification);
        checkSubquerySimplification(sql, equivalentSql);

        sql = "select T1.A FROM (SELECT A FROM R1) T1, P1 WHERE T1.A = P1.A AND P1.A = 3 ";
        sqlNoSimplification = "select T1.A FROM (SELECT A FROM R1 LIMIT 5) T1, P1 WHERE T1.A = P1.A AND P1.A = 3 ";
        equivalentSql = "select T1.A FROM R1 T1, P1 WHERE T1.A = P1.A AND P1.A = 3 ";
        checkReplicatedOne(sqlNoSimplification);
        checkSubquerySimplification(sql, equivalentSql);

        sql = "select T1.A FROM (SELECT A FROM R1) T1, P1 WHERE T1.A = P1.A AND T1.A = 3 ";
        sqlNoSimplification = "select T1.A FROM (SELECT A FROM R1 LIMIT 5) T1, P1 WHERE T1.A = P1.A AND T1.A = 3 ";
        equivalentSql = "select T1.A FROM R1 T1, P1 WHERE T1.A = P1.A AND T1.A = 3 ";
        checkReplicatedOne(sqlNoSimplification);
        checkSubquerySimplification(sql, equivalentSql);

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

        sql = "select T1.A, P1.C FROM (SELECT A FROM R1) T1, P1 WHERE T1.A = P1.C ";
        sqlNoSimplification = "select T1.A, P1.C FROM (SELECT A FROM R1 LIMIT 5) T1, P1 WHERE T1.A = P1.C ";
        equivalentSql = "select T1.A, P1.C FROM R1 T1, P1 WHERE T1.A = P1.C ";
        checkReplicatedTwo(sqlNoSimplification, 1, 0);
        checkSubquerySimplification(sql, equivalentSql);

        // Three table joins
        sql = "select T1.A, P1.A FROM (SELECT A FROM R1) T1, P1, P2 WHERE P2.A = P1.A and T1.A = P1.C ";
        sqlNoSimplification = "select T1.A, P1.A FROM (SELECT A FROM R1 LIMIT 10) T1, P1, P2 WHERE P2.A = P1.A and T1.A = P1.C ";
        equivalentSql = "select T1.A, P1.A FROM R1 T1, P1, P2 WHERE P2.A = P1.A and T1.A = P1.C ";
        checkReplicatedTwo(sqlNoSimplification, 1 ,1);
        checkSubquerySimplification(sql, equivalentSql);
    }

    public void testReplicatedGroupbyLIMIT() {
        AbstractPlanNode pn;
        AbstractPlanNode aggNode;

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
        aggNode = pn.getInlinePlanNode(PlanNodeType.HASHAGGREGATE);
        assertNotNull(aggNode);

        pn = pn.getChild(0);
        checkSeqScan(pn, "R1",  "A", "D" );
        checkPredicateComparisonExpression(pn, "R1");
        assertEquals(((SeqScanPlanNode) pn).getInlinePlanNodes().size(), 2);
        assertNotNull(((SeqScanPlanNode) pn).getInlinePlanNode(PlanNodeType.PROJECTION));
        assertNotNull(((SeqScanPlanNode) pn).getInlinePlanNode(PlanNodeType.LIMIT));

        // add order by node, without inline limit and projection node.
        pn = compile("select A, SUM(D) FROM (SELECT A, D FROM R1 WHERE A > 3 ORDER BY D Limit 3 ) T1 Group by A");
        pn = pn.getChild(0);
        aggNode = pn.getInlinePlanNode(PlanNodeType.HASHAGGREGATE);
        assertNotNull(aggNode);
        checkSeqScan(pn, "T1" );
        pn = pn.getChild(0);
        if (pn instanceof ProjectionPlanNode) {
            pn = pn.getChild(0);
        }
        // inline limit with order by
        assertTrue(pn instanceof OrderByPlanNode);
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.LIMIT));
        pn = pn.getChild(0);
        checkSeqScan(pn, "R1",  "A", "D" );
        checkPredicateComparisonExpression(pn, "R1");
        assertEquals(((SeqScanPlanNode) pn).getInlinePlanNodes().size(), 1);
        assertNotNull(((SeqScanPlanNode) pn).getInlinePlanNode(PlanNodeType.PROJECTION));

        pn = compile("select A, SUM(D) FROM (SELECT A, D FROM R1 WHERE A > 3 ORDER BY D Limit 3 ) T1 Group by A HAVING SUM(D) < 3");
        pn = pn.getChild(0);
        if (pn instanceof ProjectionPlanNode) {
            pn = pn.getChild(0);
        }
        aggNode = pn.getInlinePlanNode(PlanNodeType.HASHAGGREGATE);
        assertNotNull(aggNode);
        assertNotNull(((HashAggregatePlanNode)aggNode).getPostPredicate());
        checkSeqScan(pn, "T1" );
        if (pn instanceof ProjectionPlanNode) {
            pn = pn.getChild(0);
        }
        assertTrue(((SeqScanPlanNode)pn).isSubQuery());
        // SeqScan with an order by node.  The order by
        // comes from the subquery.
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
        aggNode = pn.getInlinePlanNode(PlanNodeType.HASHAGGREGATE);
        assertNotNull(aggNode);
        assertNotNull(((HashAggregatePlanNode)aggNode).getPostPredicate());

        checkSeqScan(pn, "T1");
        pn = pn.getChild(0);
        if (pn instanceof ProjectionPlanNode) {
            pn = pn.getChild(0);
        }
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
        aggNode = pn.getInlinePlanNode(PlanNodeType.HASHAGGREGATE);
        assertNotNull(aggNode);
        assertNotNull(((HashAggregatePlanNode)aggNode).getPostPredicate());

        checkSeqScan(pn, "T1");
        pn = pn.getChild(0);
        if (pn instanceof ProjectionPlanNode) {
            pn = pn.getChild(0);
        }
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
        if (pn instanceof ProjectionPlanNode) {
            pn = pn.getChild(0);
        }
        // inline limit with order by
        assertTrue(pn instanceof OrderByPlanNode);
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.LIMIT));
        pn = pn.getChild(0);
        aggNode = pn.getInlinePlanNode(PlanNodeType.HASHAGGREGATE);
        assertNotNull(aggNode);
        assertTrue(pn instanceof SeqScanPlanNode);
        checkSeqScan(pn, "R1");


        pn = compile("select SC, SUM(A) as SA FROM (SELECT A, SUM(C) as SC, MAX(D) as MD FROM R1 " +
                "WHERE A > 3 GROUP BY A ORDER BY A Limit 3) T1  " +
                "Group by SC");

        pn = pn.getChild(0);
        aggNode = pn.getInlinePlanNode(PlanNodeType.HASHAGGREGATE);
        assertNotNull(aggNode);
        assertTrue(pn instanceof SeqScanPlanNode);
        checkSeqScan(pn, "T1");
        pn = pn.getChild(0);
        if (pn instanceof ProjectionPlanNode) {
            pn = pn.getChild(0);
        }
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
        String sql, sqlNoSimplification, equivalentSql;

        //
        // Single partition detection : single table
        //
        sql = "select A FROM (SELECT A FROM P1 WHERE A = 3) T1 ";
        sqlNoSimplification = "select A FROM (SELECT A FROM P1 WHERE A = 3 LIMIT 1) T1 ";
        equivalentSql = "SELECT A FROM P1 T1 WHERE A = 3";
        planNodes = compileToFragments(sqlNoSimplification);
        assertEquals(1, planNodes.size());
        pn = planNodes.get(0);
        assertTrue(pn instanceof SendPlanNode);
        pn = pn.getChild(0);
        checkSeqScan(pn, "T1",  "A");
        pn = pn.getChild(0);
        checkPrimaryKeyIndexScan(pn, "P1", "A");
        assertNotNull(((IndexScanPlanNode) pn).getInlinePlanNode(PlanNodeType.PROJECTION));
        checkSubquerySimplification(sql, equivalentSql);

        sql = "select A, C FROM (SELECT A, C FROM P1 WHERE A = 3) T1 ";
        sqlNoSimplification = "select A, C FROM (SELECT A, C FROM P1 WHERE A = 3 LIMIT 1) T1 ";
        equivalentSql = "SELECT A, C FROM P1 T1 WHERE A = 3";
        planNodes = compileToFragments(sqlNoSimplification);
        assertEquals(1, planNodes.size());
        pn = planNodes.get(0);
        assertTrue(pn instanceof SendPlanNode);
        pn = pn.getChild(0);
        checkSeqScan(pn, "T1",  "A", "C");
        pn = pn.getChild(0);
        checkPrimaryKeyIndexScan(pn, "P1", "A", "C");
        assertNotNull(((IndexScanPlanNode) pn).getInlinePlanNode(PlanNodeType.PROJECTION));
        checkSubquerySimplification(sql, equivalentSql);

        // Single partition query without selecting partition column from sub-query
        planNodes = compileToFragments("select C FROM (SELECT A, C FROM P1 WHERE A = 3 LIMIT 1) T1 ");
        assertEquals(1, planNodes.size());
        planNodes = compileToFragments("select C FROM (SELECT C FROM P1 WHERE A = 3 LIMIT 1) T1 ");
        assertEquals(1, planNodes.size());

        //
        // AdHoc multiple partitioned sub-select queries.
        //
        sql = "select A1, C FROM (SELECT A A1, C FROM P1) T1  ";
        sqlNoSimplification = "select A1, C FROM (SELECT DISTINCT A A1, C FROM P1) T1 ";
        equivalentSql = "SELECT A A1, C FROM P1 T1";
        planNodes = compileToFragments(sqlNoSimplification);
        assertEquals(2, planNodes.size());
        pn = planNodes.get(0).getChild(0);
        if (pn instanceof ProjectionPlanNode) {
            pn = pn.getChild(0);
        }
        assertTrue(pn instanceof ReceivePlanNode);
        pn = planNodes.get(1).getChild(0);
        checkSeqScan(pn, "T1",  "A1", "C" );
        checkSubquerySimplification(sql, equivalentSql);

        sql = "select A1 FROM (SELECT A A1, C FROM P1 WHERE A > 3) T1 ";
        sqlNoSimplification = "select A1 FROM (SELECT A A1, C FROM P1 WHERE A > 3 LIMIT 10) T1 ";
        equivalentSql = "SELECT A A1 FROM P1 T1 WHERE A > 3";
        planNodes = compileToFragments(sqlNoSimplification);
        assertEquals(2, planNodes.size());
        pn = planNodes.get(0).getChild(0);
        checkSeqScan(pn, "T1", "A1");
        checkSubquerySimplification(sql, equivalentSql);


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

    private void checkFragmentCount(String sql, int fragmentCount) {
        List<AbstractPlanNode> planNodes = compileToFragments(sql);
        assertEquals(fragmentCount, planNodes.size());
    }

    public void testPartitionedCrossLevel() {
        AbstractPlanNode pn;
        List<AbstractPlanNode> planNodes;
        AbstractPlanNode nlpn;
        String sql, sqlNoSimplification, equivalentSql;

        sql = "SELECT T1.A, T1.C, P2.D FROM P2, (SELECT A, C FROM P1) T1 " +
                "where T1.A = P2.A ";
        sqlNoSimplification = "SELECT T1.A, T1.C, P2.D FROM P2, (SELECT DISTINCT A, C FROM P1 ) T1 " +
                "where T1.A = P2.A ";
        equivalentSql = "SELECT T1.A, T1.C, P2.D FROM P2, P1 T1 WHERE T1.A = P2.A";
        planNodes = compileToFragments(sqlNoSimplification);
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

        checkFragmentCount("SELECT P2.A, P2.C FROM P2, (SELECT A, C FROM P1 GROUP BY A) T1 " +
                "where T1.A = P2.A and P2.A = 1", 1);
        checkFragmentCount("SELECT P2.A, P2.C FROM P2, (SELECT A, C FROM P1) T1 " +
                "where T1.A = P2.A and P2.A = 1", 1);

        checkFragmentCount("SELECT P2.A, P2.C FROM P2, (SELECT A, C, A + 1 FROM P1) T1 " +
                "where T1.A = P2.A and T1.A = 1", 1);
        checkFragmentCount("SELECT P2.A, P2.C FROM P2, (SELECT A, C FROM P1) T1 " +
                "where T1.A = P2.A and T1.A = 1", 1);

        checkFragmentCount("SELECT P2.A, P2.C FROM P2, (SELECT A, C, A + 1 FROM P1 where P1.A = 3) T1 " +
                "where T1.A = P2.A ", 1);
        checkFragmentCount("SELECT P2.A, P2.C FROM P2, (SELECT A, C FROM P1 where P1.A = 3) T1 " +
                "where T1.A = P2.A ", 1);

        // Distributed join
        checkFragmentCount("select D1, D2 " +
                "FROM (SELECT A, D + 1 D1 FROM P1 ) T1, (SELECT A, D + 2 D2 FROM P2 ) T2 " +
                "WHERE T1.A = T2.A", 2);
        checkFragmentCount("select D1, D2 " +
                "FROM (SELECT A, D D1 FROM P1 ) T1, (SELECT A, D D2 FROM P2 ) T2 " +
                "WHERE T1.A = T2.A", 2);

        checkFragmentCount("select D1, P2.D " +
                "FROM (SELECT A, D + 1 D1 FROM P1 WHERE A=1) T1, P2 " +
                "WHERE T1.A = P2.A AND P2.A = 1", 1);
        checkFragmentCount("select D1, P2.D " +
                "FROM (SELECT A, D D1 FROM P1 WHERE A=1) T1, P2 " +
                "WHERE T1.A = P2.A AND P2.A = 1", 1);

        checkFragmentCount("select T1.A, T1.C, T1.SD FROM " +
                "(SELECT A, C, SUM(D) as SD FROM P1 WHERE A > 3 GROUP BY A, C) T1, P2 WHERE T1.A = P2.A", 2);


        // (1) Multiple level subqueries (recursive) partition detecting
        checkFragmentCount("select * from p2, " +
                "(select * from (SELECT A, D + 1 D1 FROM P1) T1) T2 where p2.A = T2.A", 2);
        checkFragmentCount("select * from p2, " +
                "(select * from (SELECT A, D D1 FROM P1) T1) T2 where p2.A = T2.A", 2);

        checkFragmentCount("select * from p2, " +
                "(select * from (SELECT A, D + 1 D1 FROM P1 WHERE A=2) T1) T2 " +
                "where p2.A = T2.A ", 1);
        checkFragmentCount("select * from p2, " +
                "(select * from (SELECT A, D D1 FROM P1 WHERE A=2) T1) T2 " +
                "where p2.A = T2.A ", 1);

        checkFragmentCount("select * from p2, " +
                "(select * from (SELECT P1.A, P1.D FROM P1, P3 where P1.A = P3.A) T1) T2 " +
                "where p2.A = T2.A", 2);

        checkFragmentCount("select * from p2, " +
                "(select * from (SELECT P1.A, P1.D FROM P1, P3 where P1.A = P3.A) T1) T2 " +
                "where p2.A = T2.A and P2.A = 1", 1);

        // (2) Multiple subqueries on the same level partition detecting
        planNodes = compileToFragments("select D1, D2 FROM " +
                "(SELECT A, D + 1 D1 FROM P1 WHERE A=2) T1, " +
                "(SELECT A, D + 1 D2 FROM P2 WHERE A=2) T2");
        assertEquals(1, planNodes.size());


        checkFragmentCount("select D1, D2 FROM " +
                "(SELECT A, D + 1 D1 FROM P1 WHERE A=2) T1, " +
                "(SELECT A, D + 1 D2 FROM P2) T2 where T2.A = 2", 1);
        checkFragmentCount("select D1, D2 FROM " +
                "(SELECT A, D D1 FROM P1 WHERE A=2) T1, " +
                "(SELECT A, D D2 FROM P2) T2 where T2.A = 2", 1);

        checkFragmentCount("select D1, D2 FROM " +
                "(SELECT A, D + 1 D1 FROM P1) T1, " +
                "(SELECT A, D + 1 D2 FROM P2 WHERE A=2) T2 where T1.A = 2", 1);
        checkFragmentCount("select D1, D2 FROM " +
                "(SELECT A, D D1 FROM P1) T1, " +
                "(SELECT A, D D2 FROM P2 WHERE A=2) T2 where T1.A = 2", 1);


        // partitioned column renaming tests
        checkFragmentCount("select D1, D2 FROM " +
                "(SELECT A A1, D + 1 D1 FROM P1) T1, " +
                "(SELECT A, D + 1 D2 FROM P2 WHERE A=2) T2 where T1.A1 = 2", 1);
        checkFragmentCount("select D1, D2 FROM " +
                "(SELECT A A1, D D1 FROM P1) T1, " +
                "(SELECT A, D D2 FROM P2 WHERE A=2) T2 where T1.A1 = 2", 1);

        checkFragmentCount("select D1, D2 FROM " +
                "(SELECT A, D + 1 D1 FROM P1 WHERE A=2) T1, " +
                "(SELECT A A2, D + 1 D2 FROM P2 ) T2 where T2.A2 = 2", 1);
        checkFragmentCount("select D1, D2 FROM " +
                "(SELECT A, D D1 FROM P1 WHERE A=2) T1, " +
                "(SELECT A A2, D D2 FROM P2 ) T2 where T2.A2 = 2", 1);

        checkFragmentCount("select A1, A2, D1, D2 " +
                "FROM (SELECT A A1, D + 1 D1 FROM P1 WHERE A=2) T1, " +
                "(SELECT A A2, D + 1 D2 FROM P2) T2 where T2.A2=2", 1);
        checkFragmentCount("select A1, A2, D1, D2 " +
                "FROM (SELECT A A1, D D1 FROM P1 WHERE A=2) T1, " +
                "(SELECT A A2, D D2 FROM P2) T2 where T2.A2=2", 1);

        checkFragmentCount("select A1, A2, D1, D2 " +
                "FROM (SELECT A A1, D + 1 D1 FROM P1 WHERE A=2) T1, " +
                "(SELECT A A2, D + 1 D2 FROM P2) T2 where T2.A2=2", 1);
        checkFragmentCount("select A1, A2, D1, D2 " +
                "FROM (SELECT A A1, D D1 FROM P1 WHERE A=2) T1, " +
                "(SELECT A A2, D D2 FROM P2) T2 where T2.A2=2", 1);


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
        if (pn instanceof ProjectionPlanNode) {
            pn = pn.getChild(0);
        }
        assertTrue(pn instanceof ReceivePlanNode);

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
        if (pn instanceof ProjectionPlanNode) {
            pn = pn.getChild(0);
        }
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
        if (pn instanceof ProjectionPlanNode) {
            pn = pn.getChild(0);
        }
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
        if (pn instanceof ProjectionPlanNode) {
            pn = pn.getChild(0);
        }
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
        if (pn instanceof ProjectionPlanNode) {
            pn = pn.getChild(0);
        }
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
        nlpn = pn;
        if (nlpn instanceof ProjectionPlanNode) {
            nlpn = nlpn.getChild(0);
        }
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
        nlpn = pn = pn.getChild(0);
        if (nlpn instanceof ProjectionPlanNode) {
            nlpn = nlpn.getChild(0);
        }
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
        nlpn = pn = pn.getChild(0);
        if (nlpn instanceof ProjectionPlanNode) {
            nlpn = nlpn.getChild(0);
        }
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
        nlpn = pn.getChild(0);
        if (nlpn instanceof ProjectionPlanNode) {
            nlpn = nlpn.getChild(0);
        }
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
        nlpn = pn = planNodes.get(0).getChild(0);
        if (nlpn instanceof ProjectionPlanNode) {
            nlpn = nlpn.getChild(0);
        }
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
        checkPushedDownJoins(3, 0,
                "SELECT * FROM (SELECT P1.A, R1.C FROM R1, P1,  " +
                "                (SELECT A, C FROM R2 LIMIT 5) T0 where R1.A = T0.A ) T1, " +
                "              P2 " +
                "where T1.A = P2.A");
        // Distinct apply on replicated table only
        checkPushedDownJoins(3, 0,
                "SELECT * FROM (SELECT P1.A, R1.C FROM R1, P1,  " +
                "                (SELECT Distinct A, C FROM R2 where A > 3) T0 where R1.A = T0.A ) T1, " +
                "              P2 " +
                "where T1.A = P2.A");
        // table count
        checkPushedDownJoins(3, 0,
                "SELECT * FROM (SELECT P1.A, R1.C FROM R1, P1,  " +
                "                (SELECT COUNT(*) AS A FROM R2 where C > 3) T0 where R1.A = T0.A ) T1, " +
                "              P2 " +
                "where T1.A = P2.A");
        // group by
        checkPushedDownJoins(3, 0,
                "SELECT * FROM (SELECT P1.A, R1.C FROM R1, P1,  " +
                "                (SELECT A, COUNT(*) C FROM R2 where C > 3 GROUP BY A) T0 where R1.A = T0.A ) T1, " +
                "              P2 " +
                "where T1.A = P2.A");
        //
        checkPushedDownJoins(3, 0,
                "SELECT * FROM (SELECT P1.A, R1.C FROM R1, P1,  " +
                "                (SELECT A, C FROM R2 where C > 3 LIMIT 10) T0 where R1.A = T0.A ) T1, " +
                "              P2 " +
                "where T1.A = P2.A");
        checkPushedDownJoins(2, 1,
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

    private void checkPushedDownJoins(int nestLoopCount, int nestLoopIndexCount, String joinQuery) {
        List<AbstractPlanNode> planNodes = compileToFragments(joinQuery);
        assertEquals(2, planNodes.size());
        //* enable to debug */ System.out.println(planNodes.get(0).toExplainPlanString());
        checkJoinNode(planNodes.get(0), PlanNodeType.NESTLOOP, 0);
        checkJoinNode(planNodes.get(0), PlanNodeType.NESTLOOPINDEX, 0);
        // Join on distributed node
        //* enable to debug */ System.out.println(planNodes.get(1).toExplainPlanString());
        checkJoinNode(planNodes.get(1), PlanNodeType.NESTLOOP, nestLoopCount);
        checkJoinNode(planNodes.get(1), PlanNodeType.NESTLOOPINDEX, nestLoopIndexCount);
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
                "object not found: A");
        failToCompile("select A+1, ABS(C) FROM (SELECT A A1, C FROM R1) T1",
                "object not found: A");

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

        // Ambiguous column aliases with and without the subquery optimization
        failToCompile("select * from (select A AC, C AC from R1) T where AC > 0",
                "object not found: AC");
        failToCompile("select * from (select A AC, C AC from R1  LIMIT 10) T where AC > 0",
                "object not found: AC");

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


        // Invalid LIMIT/OFFSET on parent subquery with partitioned nested subquery
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

    private void checkEdgeCases(String sql, String[] outputColumns,
            String table1, String column1, String table2, String column2,
            String subquery1, String subQueryColumn1, String subquery2, String subQueryColumn2) {
        AbstractPlanNode pn;

        pn = compile(sql);
        pn = pn.getChild(0);
        if (pn instanceof ProjectionPlanNode) {

            pn = pn.getChild(0);
        }
        // If we didn't see a projection plan node
        // before pn, then it's been optimized away.  In
        // this case the output schema should be identical
        // to the output schema of the NestLoopPlanNode we
        // expect here.  So checking the output schema should
        // succeed here.
        checkOutputSchema(pn.getOutputSchema(), outputColumns);
        assertTrue(pn instanceof NestLoopPlanNode);
        checkSeqScan(pn.getChild(0), table1, column1);
        if (subquery1 != null) {
            checkSeqScan(pn.getChild(0).getChild(0), subquery1, column1);
        }
        checkSeqScan(pn.getChild(1), table2, column2);
        if (subquery2 != null) {
            checkSeqScan(pn.getChild(1).getChild(0), subquery2, column2);
        }
    }

    public void testEdgeCases() {
        AbstractPlanNode pn;
        String[] outputSchema;
        String sql, sqlNoSimplification, equivalentSql;

        sql = "select T1.A, T2.A FROM (SELECT A FROM R1) T1, (SELECT A FROM R2)T2 ";
        sqlNoSimplification = "select T1.A, T2.A FROM (SELECT A FROM R1 LIMIT 1) T1, (SELECT A A FROM R2 LIMIT 1)T2 ";
        equivalentSql = "select T1.A, T2.A FROM R1 T1, R2 T2";
        outputSchema = new String[]{"T1.A", "T2.A"};
        checkEdgeCases(sqlNoSimplification,
                outputSchema, "T1", "A", "T2", "A", "R1", "A", "R2", "A");
        checkSubquerySimplification(sql, equivalentSql);

        // Quick tests of some past spectacular planner failures that sqlcoverage uncovered.

        sql = "SELECT 1, * FROM (select * from R1) T1, R2 T2 WHERE T2.A < 3737632230784348203";
        sqlNoSimplification = "SELECT 1, * FROM (select * from R1 LIMIT 5) T1, R2 T2 WHERE T2.A < 3737632230784348203";
        equivalentSql = "SELECT 1, * FROM R1 T1, R2 T2 WHERE T2.A < 3737632230784348203";
        pn = compile(sqlNoSimplification);
        assertTrue(pn.getChild(0) instanceof ProjectionPlanNode);
        checkSubquerySimplification(sql, equivalentSql);

        sql = "SELECT 2, * FROM (select * from R1) T1, R2 T2 WHERE CASE WHEN T2.A > 44 THEN T2.C END < 44 + 10";
        sqlNoSimplification = "SELECT 2, * FROM (select * from R1 LIMIT 5) T1, R2 T2 WHERE CASE WHEN T2.A > 44 THEN T2.C END < 44 + 10";
        equivalentSql = "SELECT 2, * FROM R1 T1, R2 T2 WHERE CASE WHEN T2.A > 44 THEN T2.C END < 44 + 10";
        pn = compile(sqlNoSimplification);
        assertTrue(pn.getChild(0) instanceof ProjectionPlanNode);
        checkSubquerySimplification(sql, equivalentSql);

        sql = "SELECT -8, T2.C FROM (select * from R1) T1, R1 T2 WHERE (T2.C + 5 ) > 44";
        sqlNoSimplification = "SELECT -8, T2.C FROM (select * from R1 LIMIT 5) T1, R1 T2 WHERE (T2.C + 5 ) > 44";
        equivalentSql = "SELECT -8, T2.C FROM R1 T1, R1 T2 WHERE (T2.C + 5 ) > 44";
        pn = compile(sqlNoSimplification);
        //* enable to debug */ System.out.println(pn.toExplainPlanString());
        assertTrue(pn.getChild(0) instanceof ProjectionPlanNode);
        checkSubquerySimplification(sql, equivalentSql);
    }

    public void testJoinsSimple() {
        AbstractPlanNode pn;
        AbstractPlanNode nlpn;
        String sql, sqlNoSimplification, equivalentSql;

        sql = "select A, C FROM (SELECT A FROM R1) T1, (SELECT C FROM R2) T2 WHERE T1.A = T2.C";
        sqlNoSimplification = "select A, C FROM (SELECT A FROM R1 LIMIT 10) T1, (SELECT C FROM R2 LIMIT 10) T2 WHERE T1.A = T2.C";
        equivalentSql = "select T1.A, T2.C FROM R1 T1, R2 T2 WHERE T1.A = T2.C";
        pn = compile(sqlNoSimplification);
        nlpn = pn = pn.getChild(0);
        if (nlpn instanceof ProjectionPlanNode) {
            nlpn = nlpn.getChild(0);
        }

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

        checkSubquerySimplification(sql, equivalentSql);


        // sub-selected table joins
        sql = "select A, C FROM (SELECT A FROM R1) T1, (SELECT C FROM R2) T2 WHERE A = C";
        sqlNoSimplification = "select A, C FROM (SELECT A FROM R1 LIMIT 10) T1, (SELECT C FROM R2 LIMIT 10) T2 WHERE A = C";
        equivalentSql = "select T1.A, T2.C FROM R1 T1, R2 T2 WHERE T1.A = T2.C";
        nlpn = compile(sqlNoSimplification);
        nlpn = nlpn.getChild(0);
        if (nlpn instanceof ProjectionPlanNode) {
            nlpn = nlpn.getChild(0);
        }
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
        checkSubquerySimplification(sql, equivalentSql);

    }

    public void testJoins() {
        AbstractPlanNode pn;
        List<AbstractPlanNode> planNodes;
        AbstractPlanNode nlpn;
        String sql, sqlNoSimplification, equivalentSql;


        // Left Outer join
        sql = "SELECT R1.A, R1.C FROM R1 LEFT JOIN (SELECT A, C FROM R2) T1 ON T1.C = R1.C ";
        sqlNoSimplification = "SELECT R1.A, R1.C FROM R1 LEFT JOIN (SELECT A, C FROM R2 LIMIT 10) T1 ON T1.C = R1.C ";
        equivalentSql = "SELECT R1.A, R1.C FROM R1 LEFT JOIN R2 T1 ON T1.C = R1.C ";
        planNodes = compileToFragments(sqlNoSimplification);
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
        checkSubquerySimplification(sql, equivalentSql);

        // Join with partitioned tables

        // Join on coordinator: LEFT OUTER JOIN, replicated table on left side
        sql = "SELECT R1.A, R1.C FROM R1 LEFT JOIN (SELECT A, C FROM P1) T1 ON T1.C = R1.C ";
        sqlNoSimplification = "SELECT R1.A, R1.C FROM R1 LEFT JOIN (SELECT DISTINCT A, C FROM P1) T1 ON T1.C = R1.C ";
        equivalentSql = "SELECT R1.A, R1.C FROM R1 LEFT JOIN P1 T1 ON T1.C = R1.C ";
        planNodes = compileToFragments(sqlNoSimplification);
        assertEquals(2, planNodes.size());
        pn = planNodes.get(0).getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        nlpn = pn.getChild(0);
        assertTrue(nlpn instanceof NestLoopPlanNode);
        assertEquals(JoinType.LEFT, ((NestLoopPlanNode) nlpn).getJoinType());
        pn = nlpn.getChild(0);
        checkSeqScan(pn, "R1", "A", "C");
        pn = nlpn.getChild(1);
        assertEquals(PlanNodeType.RECEIVE, pn.getPlanNodeType());

        pn = planNodes.get(1);
        assertTrue(pn instanceof SendPlanNode);
        pn = pn.getChild(0);
        checkSeqScan(pn, "T1", "C");
        checkSubquerySimplification(sql, equivalentSql);

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
        sql = "SELECT R1.A, R1.C FROM R1 INNER JOIN (SELECT A, C FROM P1) T1 ON T1.C = R1.C ";
        sqlNoSimplification = "SELECT R1.A, R1.C FROM R1 INNER JOIN (SELECT DISTINCT A, C FROM P1) T1 ON T1.C = R1.C ";
        equivalentSql = "SELECT R1.A, R1.C FROM R1 INNER JOIN P1 T1 ON T1.C = R1.C ";
        planNodes = compileToFragments(sqlNoSimplification);
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
        checkSubquerySimplification(sql, equivalentSql);


        // Two sub-queries. One is partitioned and the other one is replicated
        sql = "select A, AC FROM (SELECT A FROM R1) T1, (SELECT C AC FROM P1) T2 WHERE T1.A = T2.AC ";
        sqlNoSimplification = "select A, AC FROM (SELECT A FROM R1 LIMIT 10) T1, (SELECT DISTINCT A AC FROM P1) T2 WHERE T1.A = T2.AC ";
        equivalentSql = "select T1.A, T2.C AC FROM R1 T1, P1 T2 WHERE T1.A = T2.C ";
        planNodes = compileToFragments(sqlNoSimplification);
        assertEquals(2, planNodes.size());
        pn = planNodes.get(0).getChild(0);
        if (pn instanceof ProjectionPlanNode) {
            pn = pn.getChild(0);
        }
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
        checkSeqScan(pn, "T2", "AC");
        checkSubquerySimplification(sql, equivalentSql);

        // This is a single fragment plan because planner can detect "A = 3".
        // Join locally
        sql = "select A1, A2 FROM (SELECT A A1 FROM R1) T1, (SELECT A A2 FROM P1 where A = 3) T2 WHERE T1.A1 = T2.A2 ";
        sqlNoSimplification = "select A2, A1 FROM (SELECT DISTINCT A A1 FROM R1) T1, (SELECT DISTINCT A A2 FROM P1 where A = 3) T2 WHERE T1.A1 = T2.A2 ";
        equivalentSql = "select T1.A A1, T2.A A2 FROM R1 T1 join P1 T2 on T2.A = 3 and T1.A = T2.A";
        planNodes = compileToFragments(sqlNoSimplification);
        assertEquals(1, planNodes.size());
        pn = planNodes.get(0);
        assertTrue(pn instanceof SendPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        nlpn = pn.getChild(0);
        assertTrue(nlpn instanceof NestLoopPlanNode);
        pn = nlpn.getChild(0);
        checkSeqScan(pn, "T1", "A1");
        pn = nlpn.getChild(1);
        checkSeqScan(pn, "T2", "A2");
        pn = pn.getChild(0);
        checkPrimaryKeyIndexScan(pn, "P1", "A");

        assertEquals(2, ((IndexScanPlanNode) pn).getInlinePlanNodes().size());
        assertNotNull(((IndexScanPlanNode) pn).getInlinePlanNode(PlanNodeType.PROJECTION));
        assertNotNull(((IndexScanPlanNode) pn).getInlinePlanNode(PlanNodeType.AGGREGATE));
        checkSubquerySimplification(sql, equivalentSql);


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
        if (pn instanceof ProjectionPlanNode) {
            pn = pn.getChild(0);
        }
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
        AbstractPlanNode pn = compile("select A1 FROM (SELECT A A1 FROM R1 WHERE A > ? LIMIT 10) TEMP WHERE A1 < ?");
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

    private void checkSubqueryNoSimplification(String sql) {
        AbstractPlanNode pn = compile(sql);
        pn = pn.getChild(0);
        assertEquals(PlanNodeType.SEQSCAN, pn.getPlanNodeType());
        StmtTableScan tableScan = ((SeqScanPlanNode) pn).getTableScan();
        assertTrue(tableScan instanceof StmtSubqueryScan);
    }

    public void testSubqueryNoSimplification() {
        String sql;

        // Subquery is a UNION
        sql = "select * from (select A from R1 union select C from R2) T1";
        checkSubqueryNoSimplification(sql);

        // Subquery has AGGREGATION
        sql = "select * from (select count(*) C from R1) T1";
        checkSubqueryNoSimplification(sql);

        // Subquery has GROUP BY
        sql = "select * from (select A from R1 group by A) T1";
        checkSubqueryNoSimplification(sql);

        // Subquery has DISTINCT
        sql = "select * from (select DISTINCT(A) from R1) T1";
        checkSubqueryNoSimplification(sql);

        // Subquery has WINDOW functions
        // @TODO uncomment once the WINDOW functions are implemented
        //sql = "select A, SUM(A) OVER (ORDER BY C) as SUM_A from r1) T1;
        //checkSubqueryNoSimplification(sql);

        // Subquery has LIMIT
        sql = "select * from (select DISTINCT(A) from R1 limit 5) T1";
        checkSubqueryNoSimplification(sql);

        // Subquery has LIMIT parameter
        sql = "select * from (select DISTINCT(A) from R1 limit ?) T1";
        checkSubqueryNoSimplification(sql);

        // Subquery has OFFSET
        sql = "select * from (select DISTINCT(A) from R1 offset 5) T1";
        checkSubqueryNoSimplification(sql);

        // Subquery has OFFSET parameter
        sql = "select * from (select DISTINCT(A) from R1 offset ?) T1";
        checkSubqueryNoSimplification(sql);

        // Subquery has join
        sql = "select * from (select R1.A, R2.C from R1, R2 where R1.C = R2.C) T1";
        checkSubqueryNoSimplification(sql);

        // Subquery has join that itself has another subquery
        sql = "select * from (select R1.A, T1.C from R1, (select C from R2 LIMIT 4) T1 where R1.C = T1.C) T2";
        checkSubqueryNoSimplification(sql);

        // Subquery contains subquery
        sql = "select * from (select A from (select A from R1 limit 5) T1) T2";
        checkSubqueryNoSimplification(sql);
    }

    private void checkSubquerySimplification(String sql, String equivalentSql, List<String[]> ignoreList) {
        AbstractPlanNode pn = compile(sql);
        PlanNodeTree pnt = new PlanNodeTree(pn);
        String jsonSql = pnt.toJSONString();
        //* enable to debug */ System.out.println(jsonSql);
        AbstractPlanNode equivalentPne = compile(equivalentSql);
        PlanNodeTree equivalentPnt = new PlanNodeTree(equivalentPne);
        String equivalentJsonSql = equivalentPnt.toJSONString();
        //* enable to debug */ System.out.println(equivalentJsonSql);
        if (ignoreList != null) {
            for (String[] ignorePair : ignoreList) {
                jsonSql = jsonSql.replaceAll(ignorePair[0], ignorePair[1]);
            }
        }
        assertEquals(jsonSql, equivalentJsonSql);
    }

    private void checkSubquerySimplification(String sql, String equivalentSql) {
        checkSubquerySimplification(sql, equivalentSql, null);
    }

    public void testSubquerySimplification() {
        String sql;
        String equivalentSql;
        List<String[]> ignoreList = null;

        // Ambiguous column differentiator test
        sql = "select * from (select D, C as D from R1) T;";
        equivalentSql = "select D, C as D from R1 T";
        checkSubquerySimplification(sql, equivalentSql);

        // More ambiguous column differentiator test
        sql = "select * from (select D, C as D, A as C from R1) T where C = 1;";
        equivalentSql = "select D, C as D, A as C from R1 T where T.A = 1";
        checkSubquerySimplification(sql, equivalentSql);

        sql = "select C + 1 from (select D, C as D, A as C from R1) T where C = 1;";
        equivalentSql = "select A + 1 from R1 T where T.A = 1";
        checkSubquerySimplification(sql, equivalentSql);

        // Ambiguous column differentiator test
        sql = "select * from (select D, C as D from R1) T;";
        equivalentSql = "select D, C as D from R1 T";
        checkSubquerySimplification(sql, equivalentSql);

        // More ambiguous column differentiator test
        sql = "select * from (select D, C as D, A as C from R1) T where C = 1;";
        equivalentSql = "select D, C as D, A as C from R1 T where T.A = 1";
        checkSubquerySimplification(sql, equivalentSql);

        sql = "select C + 1 from (select D, C as D, A as C from R1) T where C = 1;";
        equivalentSql = "select A + 1 from R1 T where T.A = 1";
        checkSubquerySimplification(sql, equivalentSql);

        // Subquery SELECT *
        sql = "select * from (select * from R1) T1";
        equivalentSql = "select * from R1 T1";
        checkSubquerySimplification(sql, equivalentSql);

        // FROM Subquery with aliases
        sql = "select T1.AA AAA from (select R1A.A AA from R1 R1A) T1 where T1.AA > 0";
        equivalentSql = "select T1.A AAA from R1 T1 where T1.A > 0";
        checkSubquerySimplification(sql, equivalentSql);

        // FROM Subquery no aliases
        sql = "select A from (select A from R1) T1 where A > 0";
        equivalentSql = "select T1.A from R1 T1 where A > 0";
        checkSubquerySimplification(sql, equivalentSql);

        // Partitioned FROM Subquery no aliases
        sql = "select A from (select A from P1) T1 where A > 0";
        equivalentSql = "select T1.A from P1 T1 where A > 0";
        checkSubquerySimplification(sql, equivalentSql);

        // Multiple Nested FROM subqueries
        sql = "select * from (select * from (select * from R1 R1A) T1) T2";
        equivalentSql = "select * from R1 T2";
        checkSubquerySimplification(sql, equivalentSql);

        sql = "select * from (select * from (select * from R1 R1A) T1 LIMIT 10) T2";
        equivalentSql = "select * from (select * from R1 T1 LIMIT 10)T2";
        checkSubquerySimplification(sql, equivalentSql);

        // Multiple Nested FROM subqueries with ambiguous columns
        sql = "select a from (select * from (select d as a, c, a as d from R1) T1) T2;";
        equivalentSql = "select T2.D A from R1 T2";
        checkSubquerySimplification(sql, equivalentSql);

        sql = "select T2.AAA AAAA from (select T1.AA AAA from (select R1A.A AA from R1 R1A) T1) T2";
        equivalentSql = "select T2.A AAAA from R1 T2";
        checkSubquerySimplification(sql, equivalentSql);

        // Multiple Nested FROM subqueries with WHERE clauses
        sql = "select T2.AAA AAAA from " +
                "(select T1.AA AAA from " +
                "(select R1A.A AA, R1A.D DD from R1 R1A where R1A.C = 3) T1 where T1.DD < 5) " +
                "T2 where T2.AAA > 0";
        equivalentSql = "select T2.A AAAA from R1 T2 where T2.C = 3 and T2.D < 5 and T2.A > 0";
        checkSubquerySimplification(sql, equivalentSql);

        // FROM Subquery with WHERE clause
        sql = "select A from (select A from R1 where R1.C = 0) T1";
        equivalentSql = "select T1.A from R1 T1 where T1.C = 0";
        checkSubquerySimplification(sql, equivalentSql);

        // FROM Subquery with subquery expression
        sql = "select A from (select A from R1 where exists (select 1 from R2 where R2.A = 0)) T1";
        equivalentSql = "select T1.A from R1 T1 where exists (select 1 from R2 where R2.A = 0)";
        ignoreList = new ArrayList<>();
        ignoreList.add(new String[]{"\"SUBQUERY_ID\":2", "\"SUBQUERY_ID\":1"});
        ignoreList.add(new String[]{"\"STATEMENT_ID\":2", "\"STATEMENT_ID\":1"});
        checkSubquerySimplification(sql, equivalentSql, ignoreList);

        // ORDER BY expression column
        sql = "select C from (select A, C + 1 C from R1) T ORDER BY C";
        equivalentSql = "select C + 1 C from R1 T ORDER BY C";
        checkSubquerySimplification(sql, equivalentSql);

        // FROM Subquery with main query Aggregates
        sql = "select MAX(D) from (select A D, C A from R1) T";
        equivalentSql = "select MAX(A) from R1 T";
        checkSubquerySimplification(sql, equivalentSql);

        sql = "select MAX(A1), C from (select A + 1 A1, C from R1) T GROUP BY C";
        equivalentSql = "select MAX(A + 1), C from R1 T GROUP BY C";
        checkSubquerySimplification(sql, equivalentSql);

        // GROUP BY expression column
        sql = "select MAX(A), C from (select A, C + 1 C from R1) T GROUP BY C";
        equivalentSql = "select MAX(A), C + 1 C from R1 T GROUP BY (C + 1)";
        checkSubquerySimplification(sql, equivalentSql);

        sql = "select MAX(D), A from (select A D, C A from R1) T GROUP BY A HAVING MAX(D) > 5";
        equivalentSql = "select MAX(A), C A from R1 T GROUP BY C HAVING MAX(A) > 5";
        checkSubquerySimplification(sql, equivalentSql);

        sql = "select MAX(D), A from (select A + 1 D, C A from R1) T GROUP BY A HAVING MAX(D) > 5";
        equivalentSql = "select MAX(A + 1), C A from R1 T GROUP BY C HAVING MAX(A + 1) > 5";
        checkSubquerySimplification(sql, equivalentSql);

        sql = "select distinct * from (select A D, D from R1) T";
        equivalentSql = "select distinct A D, D from R1 T";
        checkSubquerySimplification(sql, equivalentSql);

        // Inner Join two FROM subqueries
        sql = "SELECT T1.TC, T2.TA FROM (SELECT C  TC FROM R1 WHERE R1.C > 0) T1 JOIN " +
                "(SELECT A  TA FROM R2 WHERE R2.A > 0) T2 ON T1.TC = T2.TA";
        equivalentSql = "SELECT T1.C TC, T2.A TA FROM R1 T1 JOIN " +
                "R2 T2 ON T1.C > 0 and T2.A > 0 and T1.C = T2.A";
        checkSubquerySimplification(sql, equivalentSql, ignoreList);

        // LEFT Join two FROM subqueries
        sql = "SELECT T1.TC, T2.TA FROM (SELECT C  TC FROM R1 WHERE R1.C > 0) T1 LEFT JOIN " +
                "(SELECT A  TA FROM R2 WHERE R2.A > 0) T2 ON T1.TC = T2.TA";
        equivalentSql = "SELECT T1.C TC, T2.A TA FROM R1 T1 LEFT JOIN " +
                "R2 T2 ON T1.C > 0 and T2.A > 0 and T1.C = T2.A";
        checkSubquerySimplification(sql, equivalentSql, ignoreList);

        // Partitioned inner join
        sql = "select T1.A, T1.C from (select A, C from P1 where A = 2) T1, P2 where T1.A = P2.A ";
        equivalentSql = "select T1.A, T1.C from P1 T1, P2 where T1.A = 2 and T1.A = P2.A ";
        checkSubquerySimplification(sql, equivalentSql, ignoreList);

        // Partitioned LEFT join
        sql = "select T1.A, T1.C from (select A, C from P1) T1 left join P2 on T1.A = P2.A ";
        equivalentSql = "select T1.A, T1.C from P1 T1 left join P2 on T1.A = P2.A ";
        checkSubquerySimplification(sql, equivalentSql, ignoreList);

        // Display column Expressions
        sql = "select A1 A11 from (select A + 1 A1 from R1 where R1.C = 0) T1";
        equivalentSql = "select T1.A + 1 A11 from R1 T1 where T1.C = 0";
        checkSubquerySimplification(sql, equivalentSql);

        sql = "select * from (select a + c + d as acd, a * c * d as acd from R1) T1;";
        equivalentSql = "select a + c + d as acd, a * c * d as acd from R1 T1";
        checkSubquerySimplification(sql, equivalentSql);

        sql = "select AC AC1 from (select A + C AC from R1 where R1.C = 0) T1";
        equivalentSql = "select T1.A + T1.C AC1 from R1 T1 where T1.C = 0";
        checkSubquerySimplification(sql, equivalentSql);

        sql = "select SCALAR AC1 from (select (select A from R2) SCALAR from R1 where R1.C = 0) T1";
        equivalentSql = "select  (select A from R2) AC1 from R1 T1 where T1.C = 0";
        ignoreList = new ArrayList<>();
        ignoreList.add(new String[]{"\"SUBQUERY_ID\":2", "\"SUBQUERY_ID\":1"});
        ignoreList.add(new String[]{"\"STATEMENT_ID\":2", "\"STATEMENT_ID\":1"});
        checkSubquerySimplification(sql, equivalentSql, ignoreList);
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
                "ORDER BY clauses with subquery expressions are not allowed.");

    }

    /**
     * This test fails to compile, and causes an NPE in the planner (I think).
     * The ticket number, obviously, is 8280.  It's commented out because
     * it fails.
     *
     * @throws Exception
     */

    public void testENG8280() throws Exception {
        failToCompile("select A from r1 as parent where C < 100 order by ( select D from r1 where r1.C = parent.C ) * 2;",
                      "ORDER BY clauses with subquery expressions are not allowed.");
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

    /*
     * ENG-10497 wants to make generated column names not conflict with
     * user column names.
     */
    public void testGeneratedNamesDontConflict() {
        String sql = "select C1 from ( select cast(a as varchar), c as c1 from r5 ) as SQ where SQ.C1 < 0;";
    AbstractPlanNode pn = compile(sql);
    assertNotNull(pn);
    VoltType vt = pn.getOutputSchema().getColumn(0).getValueType();
    assert(VoltType.INTEGER.equals(vt));
    }
}
