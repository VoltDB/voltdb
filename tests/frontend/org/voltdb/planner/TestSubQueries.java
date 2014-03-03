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
import org.voltdb.plannodes.HashAggregatePlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.LimitPlanNode;
import org.voltdb.plannodes.NestLoopPlanNode;
import org.voltdb.plannodes.NodeSchema;
import org.voltdb.plannodes.OrderByPlanNode;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.plannodes.SchemaColumn;
import org.voltdb.plannodes.SendPlanNode;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.types.JoinType;
import org.voltdb.types.PlanNodeType;

public class TestSubQueries extends PlannerTestCase {

/// I have not yet reviewed these test changes yet. --paul
    public void testUnsupportedSyntax() {
        failToCompile("DELETE FROM R1 WHERE A IN (SELECT A A1 FROM R1 WHERE A>1)", "Unsupported subquery syntax");
    }

    final String SYSTEM_SUBQUERY = "SYSTEM_SUBQUERY";
    //  final String tableAliases [] = {" ", " T1"};
    //  final String fromTables [] = {SYSTEM_SUBQUERY, "T1"};

    // Not going to test subtable without alias now.
    // Add "SYSTEM_SUBQUERY" if it's supported in future.

    final String tableAliases [] = {" T1"};
    final String fromTables [] =   {"T1"};
    final String columnRefs [] =   {" T1."};


    private void checkOutputSchema(AbstractPlanNode planNode, String... columns) {
        checkOutputSchema(null, planNode, columns);
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
            if (columns[i] != null) {
                if (col.getColumnName() == null || col.getColumnName().equals("")) {
                    assertNotNull(col.getColumnAlias());
                    assertEquals(columns[i], col.getColumnAlias());
                } else {
                    assertEquals(columns[i], col.getColumnName());
                }
            }
        }
    }

    private void checkSimpleSubSelects(AbstractPlanNode scanNode, String tableName, String... columns) {
        System.out.println(scanNode.toExplainPlanString());

        assertTrue(scanNode instanceof SeqScanPlanNode);
        SeqScanPlanNode snode = (SeqScanPlanNode) scanNode;
        if (tableName != null) {
            if (tableName.startsWith(SYSTEM_SUBQUERY)) {
                assertTrue(snode.getTargetTableName().contains(SYSTEM_SUBQUERY));
            } else {
                assertEquals(tableName, snode.getTargetTableName());
            }
        }

        checkOutputSchema(snode, columns);
    }

    private void checkPredicateComparisonExpression(AbstractPlanNode pn, String tableName) {
        AbstractExpression expr = ((SeqScanPlanNode) pn).getPredicate();
        assertTrue(expr instanceof ComparisonExpression);
        expr = expr.getLeft();
        assertTrue(expr instanceof TupleValueExpression);
        if (tableName.startsWith(SYSTEM_SUBQUERY)) {
            assertTrue(((TupleValueExpression) expr).getTableAlias().contains(tableName));
        } else {
            assertEquals(tableName, ((TupleValueExpression) expr).getTableAlias());
        }
    }

    private void checkIndexedSubSelects(AbstractPlanNode indexNode, String tableName, String indexName, String... columns) {
        System.out.println(indexNode.toExplainPlanString());

        assertTrue(indexNode instanceof IndexScanPlanNode);
        IndexScanPlanNode idxNode = (IndexScanPlanNode) indexNode;
        if (tableName != null) {
            if (tableName.startsWith(SYSTEM_SUBQUERY)) {
                assertTrue(idxNode.getTargetTableName().contains(SYSTEM_SUBQUERY));
            } else {
                assertEquals(tableName, idxNode.getTargetTableName());
            }
        }
        assertNotNull(indexName);
        String actualIndexName = idxNode.getTargetIndexName();
        assertTrue(actualIndexName.contains(indexName));

        checkOutputSchema(idxNode, columns);
    }

    public void testSubSelects_Simple() {
        AbstractPlanNode pn;

        for (int i = 0; i < tableAliases.length; i++) {
            String alias = tableAliases[i];
            String colRef = columnRefs[i];
            String tbName = fromTables[i];

            pn = compile("select A, C FROM (SELECT A, C FROM R1) " + alias);
            pn = pn.getChild(0);
            checkSimpleSubSelects(pn, tbName,  "A", "C");
            pn = pn.getChild(0);
            checkSimpleSubSelects(pn, "R1", "A", "C");


            pn = compile("select A, C FROM (SELECT A, C FROM R1) "+ alias +" WHERE A > 0");
            pn = pn.getChild(0);
            checkSimpleSubSelects(pn, tbName,  "A", "C");
            checkPredicateComparisonExpression(pn, tbName);
            pn = pn.getChild(0);
            checkSimpleSubSelects(pn, "R1", "A", "C");


            pn = compile(String.format("select A, C FROM (SELECT A, C FROM R1) %s WHERE %sA < 0", alias, colRef));
            pn = pn.getChild(0);
            checkSimpleSubSelects(pn, tbName,  "A", "C");
            checkPredicateComparisonExpression(pn, tbName);
            pn = pn.getChild(0);
            checkSimpleSubSelects(pn, "R1", "A", "C");


            pn = compile(String.format("select A1, C1 FROM (SELECT A A1, C C1 FROM R1) %s WHERE %sA1 < 0", alias, colRef));
            pn = pn.getChild(0);
            checkSimpleSubSelects(pn, tbName,  "A1", "C1");
            checkPredicateComparisonExpression(pn, tbName);
            pn = pn.getChild(0);
            checkSimpleSubSelects(pn, "R1", "A", "C");

            // With projection.
            pn = compile(String.format("select C1 FROM (SELECT A A1, C C1 FROM R1) %s WHERE %sA1 < 0", alias, colRef));
            pn = pn.getChild(0);
            checkSimpleSubSelects(pn, tbName,  "C1");
            assertEquals(((SeqScanPlanNode) pn).getInlinePlanNodes().size(), 1);
            checkPredicateComparisonExpression(pn, tbName);
            pn = pn.getChild(0);
            checkSimpleSubSelects(pn, "R1", "A", "C");

            // Complex columns in sub selects
            pn = compile(String.format("select C1 FROM (SELECT A+3 A1, C C1 FROM R1) %s WHERE %sA1 < 0", alias, colRef));
            pn = pn.getChild(0);
            checkSimpleSubSelects(pn, tbName,  "C1");
            assertEquals(((SeqScanPlanNode) pn).getInlinePlanNodes().size(), 1);
            checkPredicateComparisonExpression(pn, tbName);
            pn = pn.getChild(0);
            checkSimpleSubSelects(pn, "R1", "A1", "C");

            pn = compile(String.format("select C1 FROM (SELECT A+3, C C1 FROM R1) %s WHERE %sC1 < 0", alias, colRef));
            pn = pn.getChild(0);
            checkSimpleSubSelects(pn, tbName,  "C1");
            assertEquals(((SeqScanPlanNode) pn).getInlinePlanNodes().size(), 1);
            checkPredicateComparisonExpression(pn, tbName);
            pn = pn.getChild(0);
            checkSimpleSubSelects(pn, "R1",  "C1", "C");


            // select *
            pn = compile(String.format("select A, C FROM (SELECT * FROM R1) %s WHERE %sA < 0", alias, colRef));
            pn = pn.getChild(0);
            checkSimpleSubSelects(pn, tbName,  "A", "C");
            checkPredicateComparisonExpression(pn, tbName);
            pn = pn.getChild(0);
            checkSimpleSubSelects(pn, "R1", "A", "C", "D");


            pn = compile(String.format("select * FROM (SELECT A, D FROM R1) %s WHERE %sA < 0", alias, colRef));
            pn = pn.getChild(0);
            checkSimpleSubSelects(pn, tbName,  "A", "D");
            checkPredicateComparisonExpression(pn, tbName);
            pn = pn.getChild(0);
            checkSimpleSubSelects(pn, "R1", "A", "D");
        }
    }

    public void testSubSelects_Three_Levels() {
        AbstractPlanNode pn;

        // Three levels selects
        pn = compile("select A2 FROM " +
                "(SELECT A1 AS A2 FROM (SELECT A AS A1 FROM R1 WHERE A < 3) T1 WHERE T1.A1 > 0) T2  WHERE T2.A2 = 3");
        pn = pn.getChild(0);
        checkSimpleSubSelects(pn, "T2",  "A2");
        checkPredicateComparisonExpression(pn, "T2");
        pn = pn.getChild(0);
        checkSimpleSubSelects(pn, "T1",  "A1");
        checkPredicateComparisonExpression(pn, "T1");
        pn = pn.getChild(0);
        checkSimpleSubSelects(pn, "R1",  "A");
        checkPredicateComparisonExpression(pn, "R1");
    }

    public void testSubSelects_Function() {
        AbstractPlanNode pn;

        for (int i = 0; i < tableAliases.length; i++) {
            String alias = tableAliases[i];
            String tbName = fromTables[i];

            // Function expression
            pn = compile("select ABS(C) FROM (SELECT A, C FROM R1)" + alias);
            pn = pn.getChild(0);
            checkSimpleSubSelects(pn, tbName,  "C1" );
            pn = pn.getChild(0);
            checkSimpleSubSelects(pn, "R1",  "A", "C" );

            // Should this really be supported ?
            failToCompile("select A, ABS(C) FROM (SELECT A A1, C FROM R1)" + alias,
                    "user lacks privilege or object not found: A");
            failToCompile("select A+1, ABS(C) FROM (SELECT A A1, C FROM R1)" + alias,
                    "user lacks privilege or object not found: A");

            // Use alias column from sub select instead.
            pn = compile("select A1, ABS(C) FROM (SELECT A A1, C FROM R1)" + alias);
            pn = pn.getChild(0);
            checkSimpleSubSelects(pn, tbName,  "A1", "C2" ); // hsql auto generated column alias C2.
            pn = pn.getChild(0);
            checkSimpleSubSelects(pn, "R1",  "A", "C" );

            pn = compile("select A1 + 3, ABS(C) FROM (SELECT A A1, C FROM R1)" + alias);
            pn = pn.getChild(0);
            checkSimpleSubSelects(pn, tbName,  "C1", "C2" );
            assertEquals(((SeqScanPlanNode) pn).getInlinePlanNodes().size(), 1);
            pn = pn.getChild(0);
            checkSimpleSubSelects(pn, "R1",  "A", "C" );


            pn = compile("select A1 + 3, ABS(C) FROM (SELECT A A1, C FROM R1) " + alias + " WHERE ABS(A1) > 3");
            pn = pn.getChild(0);
            checkSimpleSubSelects(pn, tbName,  "C1", "C2" );
            assertEquals(((SeqScanPlanNode) pn).getInlinePlanNodes().size(), 1);
            pn = pn.getChild(0);
            checkSimpleSubSelects(pn, "R1",  "A", "C" );
        }
    }

    public void testSubSelects_Aggregation_Groupby() {
        AbstractPlanNode pn;

        pn = compile("select A, C FROM (SELECT * FROM R1 WHERE A > 3 Limit 3) T1 ");
        pn = pn.getChild(0);
        checkSimpleSubSelects(pn, "T1",  "A", "C" );
        pn = pn.getChild(0);
        checkSimpleSubSelects(pn, "R1",  "A", "C", "D");
        checkPredicateComparisonExpression(pn, "R1");
        assertEquals(((SeqScanPlanNode) pn).getInlinePlanNodes().size(), 2);
        assertNotNull(((SeqScanPlanNode) pn).getInlinePlanNode(PlanNodeType.PROJECTION));
        assertNotNull(((SeqScanPlanNode) pn).getInlinePlanNode(PlanNodeType.LIMIT));

        // inline limit and projection node.
        pn = compile("select A, SUM(D) FROM (SELECT A, D FROM R1 WHERE A > 3 Limit 3 ) T1 Group by A");
        pn = pn.getChild(0);
        assertTrue(pn instanceof HashAggregatePlanNode);
        pn = pn.getChild(0);
        checkSimpleSubSelects(pn, "T1",  "A", "D" );
        pn = pn.getChild(0);
        checkSimpleSubSelects(pn, "R1",  "A", "D" );
        checkPredicateComparisonExpression(pn, "R1");
        assertEquals(((SeqScanPlanNode) pn).getInlinePlanNodes().size(), 2);
        assertNotNull(((SeqScanPlanNode) pn).getInlinePlanNode(PlanNodeType.PROJECTION));
        assertNotNull(((SeqScanPlanNode) pn).getInlinePlanNode(PlanNodeType.LIMIT));

        // add order by node, wihtout inline limit and projection node.
        pn = compile("select A, SUM(D) FROM (SELECT A, D FROM R1 WHERE A > 3 ORDER BY D Limit 3 ) T1 Group by A");
        pn = pn.getChild(0);
        assertTrue(pn instanceof HashAggregatePlanNode);
        pn = pn.getChild(0);
        checkSimpleSubSelects(pn, "T1",  "A", "D" );
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof LimitPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof OrderByPlanNode);
        pn = pn.getChild(0);
        checkSimpleSubSelects(pn, "R1",  "A", "D" );
        checkPredicateComparisonExpression(pn, "R1");
        assertEquals(((SeqScanPlanNode) pn).getInlinePlanNodes().size(), 1);
        assertNotNull(((SeqScanPlanNode) pn).getInlinePlanNode(PlanNodeType.PROJECTION));

        pn = compile("select A, SUM(D) FROM (SELECT A, D FROM R1 WHERE A > 3 ORDER BY D Limit 3 ) T1 Group by A HAVING SUM(D) < 3");
        pn = pn.getChild(0);
        assertTrue(pn instanceof HashAggregatePlanNode);
        assertNotNull(((HashAggregatePlanNode)pn).getPostPredicate());
        pn = pn.getChild(0);
        checkSimpleSubSelects(pn, "T1",  "A", "D" );
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof LimitPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof OrderByPlanNode);
        pn = pn.getChild(0);
        checkSimpleSubSelects(pn, "R1",  "A", "D" );
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
        checkSimpleSubSelects(pn, "T1",  "A", "D" );
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof LimitPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof OrderByPlanNode);
        pn = pn.getChild(0);
        checkSimpleSubSelects(pn, "R1",  "A", "D" );
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
        checkSimpleSubSelects(pn, "T1",  "A", "D" );
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof LimitPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof OrderByPlanNode);
        pn = pn.getChild(0);
        checkSimpleSubSelects(pn, "R1",  "A", "D" );
        checkPredicateComparisonExpression(pn, "R1");
        assertEquals(((SeqScanPlanNode) pn).getInlinePlanNodes().size(), 1);
        assertNotNull(((SeqScanPlanNode) pn).getInlinePlanNode(PlanNodeType.PROJECTION));



        // Aggregation inside of the from clause
        pn = compile("select A FROM (SELECT A, SUM(C) FROM R1 WHERE A > 3 GROUP BY A ORDER BY A Limit 3) T1 ");
        pn = pn.getChild(0);
        assertTrue(pn instanceof SeqScanPlanNode);
        checkSimpleSubSelects(pn, "T1", "A");
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
        checkSimpleSubSelects(pn, "R1", "A", "C");


        pn = compile("select SC, SUM(A) as SA FROM (SELECT A, SUM(C) as SC, MAX(D) as MD FROM R1 " +
                "WHERE A > 3 GROUP BY A ORDER BY A Limit 3) T1  " +
                "Group by SC");

        pn = pn.getChild(0);
        assertTrue(pn instanceof HashAggregatePlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof SeqScanPlanNode);
        checkSimpleSubSelects(pn, "T1", "A", "SC");
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
        checkSimpleSubSelects(pn, "R1", "A", "C", "D");
    }

    public void testSubSelects_Distributed() {
        String tmpErrorMsg = "Subselect queries only are supported in single partition stored procedure.";
        // Partitioned sub-query
        failToCompile("select A, C FROM (SELECT A, C FROM P1) T1 ", tmpErrorMsg);
        failToCompile("select A FROM (SELECT A, C FROM P1 WHERE A > 3) T1 ", tmpErrorMsg);

        // AdHoc does not detect partitioning for sub-select query.
        failToCompile("select A, C FROM (SELECT A, C FROM P1 WHERE A = 3) T1 ", tmpErrorMsg);

        // force it to be single partitioned.
        AbstractPlanNode pn;
        pn = compileForSinglePartition("select A FROM (SELECT A, C FROM P1 WHERE A > 3) T1");
        System.out.println(pn.toExplainPlanString());
        pn = pn.getChild(0);
        checkSimpleSubSelects(pn, "T1",  "A" );
        pn = pn.getChild(0);
        checkIndexedSubSelects(pn, "P1", "SYS_IDX_P1_PK_TREE", "A", "C");

        // In future, turn the next block on when AdHoc multiple partitioned procedure are supported.
/*
        List<AbstractPlanNode> lpn;
        lpn = compileToFragments("select A, C FROM (SELECT A, C FROM P1) T1 ");
        assertTrue(lpn.size() == 2);
        pn = lpn.get(0).getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ReceivePlanNode);
        pn = lpn.get(1).getChild(0);
        checkSimpleSubSelects(pn, "T1",  "A", "C" );
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode); // This sounds incorrect, should it be inlined?
        pn = pn.getChild(0);
        checkSimpleSubSelects(pn, "P1",  "A", "C" );

        lpn = compileToFragments("select A, C FROM (SELECT A, C FROM P1 WHERE A = 3) T1 ");
        assertTrue(lpn.size() == 2);
        pn = lpn.get(0).getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ReceivePlanNode);
        pn = lpn.get(1).getChild(0);
        checkSimpleSubSelects(pn, "T1",  "A", "C" );
        pn = pn.getChild(0);
        checkIndexedSubSelects(pn, "P1", "SYS_IDX_P1_PK_TREE", "A", "C");

        lpn = compileToFragments("select A FROM (SELECT A, C FROM P1 WHERE A > 3) T1 ");
        assertTrue(lpn.size() == 2);
        pn = lpn.get(0).getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ReceivePlanNode);
        pn = lpn.get(1).getChild(0);
        checkSimpleSubSelects(pn, "T1",  "A" );
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        checkIndexedSubSelects(pn, "P1", "SYS_IDX_P1_PK_TREE", "A", "C");
*/
    }

    public void testTry() {
        //        List<AbstractPlanNode> planNodes;
        //        AbstractPlanNode nlpn;
        //        for (AbstractPlanNode p: planNodes) System.out.println(p.toExplainPlanString());

//        List<AbstractPlanNode> planNodes =
//                compileToFragments("select * from p2, (select * from (SELECT A, D D1 FROM P1 WHERE A=2) T1) T2 where p2.D= T2.D1 ");
//        assertTrue(planNodes.size() == 2);
//        for (AbstractPlanNode p: planNodes) System.out.println(p.toExplainPlanString());

        AbstractPlanNode pn;
//        pn = compileForSinglePartition("select * from p2, (select * from (SELECT A, D D1 FROM P1 WHERE A=2) T1) T2 where p2.D= T2.D1");
//        System.out.println(pn.toExplainPlanString());


        pn = compileForSinglePartition("select A, C FROM (SELECT A FROM R1) T1, (SELECT C FROM P1) T2 " +
                "WHERE T1.A = T2.C ");
        System.out.println(pn.toExplainPlanString());
    }

    public void testSubSelects_Unsupported_Cases() {
        String errorMessage = "Every derived table must have its own alias.";
        failToCompile("select C FROM (SELECT C FROM R1)  ", errorMessage);

        // Unsupported joins.
        String errorJoinMsg = "Join of multiple partitioned tables has insufficient join criteria.";
        failToCompile("select A, C FROM (SELECT A FROM P1) T1, (SELECT C FROM P2) T2 WHERE T1.A = T2.C ",
                errorJoinMsg);
        failToCompile("select D1, D2 FROM (SELECT A, D D1 FROM P1 ) T1, (SELECT A, D D2 FROM P2 ) T2 WHERE T1.A = T2.A",
                errorJoinMsg);
        failToCompile("select D1, P2.D FROM (SELECT A, D D1 FROM P1 WHERE A=1) T1, P2 WHERE T1.A = P2.A AND P2.A = 1",
                errorJoinMsg);

        // Join of a single partitioned sub-queries. The partitions are different
        failToCompile("select D1, D2 FROM (SELECT A, D D1 FROM P1 WHERE A=2) T1, (SELECT A, D D2 FROM P2 WHERE A=2) T2",
                errorJoinMsg);
        failToCompile("select D1, D2 FROM " +
                "(SELECT A, D D1 FROM P1) T1, (SELECT A, D D2 FROM P2) T2 WHERE T1.A = 1 AND T2.A = 2", errorJoinMsg);


        String tmpErrorMsg = "Subselect queries only are supported in single partition stored procedure.";

        // parent partition table join with subselect partitioned temp table
        failToCompile("select * from p2, (select * from (SELECT A, D D1 FROM P1) T1) T2 where p2.D= T2.D1",
                tmpErrorMsg);

        // FIXME(xin): This should be supported
        failToCompile("select * from p2, (select * from (SELECT A, D D1 FROM P1 WHERE A=2) T1) T2 where p2.D = T2.D1",
                tmpErrorMsg);
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
        checkSimpleSubSelects(pn.getChild(0), "T1", "A");
        checkSimpleSubSelects(pn.getChild(0).getChild(0), "R1", "A");
        checkSimpleSubSelects(pn.getChild(1), "T2", "A");
        checkSimpleSubSelects(pn.getChild(1).getChild(0), "R2", "A");


        pn = compile("select T2.A FROM (SELECT A FROM R1) T1, (SELECT A FROM R2)T2 ");
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        checkOutputSchema("T2", pn, "A");
        pn = pn.getChild(0);
        assertTrue(pn instanceof NestLoopPlanNode);
        checkOutputSchema(pn, "A", "A");
        checkSimpleSubSelects(pn.getChild(0), "T1", "A");
        checkSimpleSubSelects(pn.getChild(0).getChild(0), "R1", "A");
        checkSimpleSubSelects(pn.getChild(1), "T2", "A");
        checkSimpleSubSelects(pn.getChild(1).getChild(0), "R2", "A");


        // TODO(xin): hsql does not complain about the ambiguous column A, but use 'T1' as default.
        // FIX(xin): throw compiler exception for this query.
        pn = compile("select A FROM (SELECT A FROM R1) T1, (SELECT A FROM R2)T2 ");
//      System.out.println(pn.toExplainPlanString());
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        checkOutputSchema("T1", pn, "A");
        pn = pn.getChild(0);
        assertTrue(pn instanceof NestLoopPlanNode);
        checkOutputSchema(pn, "A", "A");
        checkSimpleSubSelects(pn.getChild(0), "T1", "A");
        checkSimpleSubSelects(pn.getChild(0).getChild(0), "R1", "A");
        checkSimpleSubSelects(pn.getChild(1), "T2", "A");
        checkSimpleSubSelects(pn.getChild(1).getChild(0), "R2", "A");
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
        checkSimpleSubSelects(pn, "T1",  "A");
        pn= pn.getChild(0);
        checkSimpleSubSelects(pn, "R1",  "A");

        pn = nlpn.getChild(1);
        checkSimpleSubSelects(pn, "T2",  "C");
        pn= pn.getChild(0);
        checkSimpleSubSelects(pn, "R2",  "C");


        // sub-selected table joins
        pn = compile("select A, C FROM (SELECT A FROM R1) T1, (SELECT C FROM R2) T2 WHERE A = C");
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);

        nlpn = pn.getChild(0);
        assertTrue(nlpn instanceof NestLoopPlanNode);
        assertEquals(2, nlpn.getChildCount());
        pn = nlpn.getChild(0);
        checkSimpleSubSelects(pn, "T1",  "A");
        pn= pn.getChild(0);
        checkSimpleSubSelects(pn, "R1",  "A");

        pn = nlpn.getChild(1);
        checkSimpleSubSelects(pn, "T2",  "C");
        pn= pn.getChild(0);
        checkSimpleSubSelects(pn, "R2",  "C");
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
        checkSimpleSubSelects(pn, "R1", "A", "C"); // so clever, not select "D"
        pn = nlpn.getChild(1);
        checkSimpleSubSelects(pn, "T1", "C"); // so clever, not select "A"
        pn = pn.getChild(0);
        checkSimpleSubSelects(pn, "R2", "A", "C");

/*
        planNodes = compileToFragments("SELECT A, C FROM R1 LEFT JOIN (SELECT A, C FROM P1) T1 ON T1.C = R1.C ");
        assertTrue(planNodes.size() == 2);
        pn = planNodes.get(0).getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ReceivePlanNode);
        nlpn = planNodes.get(1).getChild(0);
        assertTrue(nlpn instanceof NestLoopPlanNode);
        assertEquals(JoinType.LEFT, ((NestLoopPlanNode) nlpn).getJoinType());
        pn = nlpn.getChild(0);
        checkSimpleSubSelects(pn, "R1", "A", "C"); // so clever, not select "D"
        pn = nlpn.getChild(1);
        checkSimpleSubSelects(pn, "T1", "C"); // so clever, not select "A"
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode); // Why projection node here? Incorrect?
        pn = pn.getChild(0);
        checkSimpleSubSelects(pn, "P1", "A", "C");


        // Two sub-queries. One is partitioned and the other one is replicated
        planNodes = compileToFragments("select A, C FROM (SELECT A FROM R1) T1, (SELECT C FROM P1) T2 WHERE T1.A = T2.C ");
        assertTrue(planNodes.size() == 2);

        pn = planNodes.get(0);
        assertTrue(pn instanceof SendPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ReceivePlanNode);

        pn = planNodes.get(1);
        assertTrue(pn instanceof SendPlanNode);
        nlpn = pn.getChild(0);
        assertTrue(nlpn instanceof NestLoopPlanNode);
        pn = nlpn.getChild(0);
        checkSimpleSubSelects(pn, "T1", "A");
        pn = pn.getChild(0);
        checkSimpleSubSelects(pn, "R1", "A");
        pn = nlpn.getChild(1);
        checkSimpleSubSelects(pn, "T2", "C");
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode); // Why projection node here? Incorrect?
        pn = pn.getChild(0);
        checkSimpleSubSelects(pn, "P1", "C");


        // This should be a single fragment plan if planner can detect "A = 3".
        // FIXME(xin)
        planNodes = compileToFragments("select A, C FROM (SELECT A FROM R1) T1, (SELECT C FROM P1 where A = 3) T2 " +
                "WHERE T1.A = T2.C ");
        assertTrue(planNodes.size() == 2); // In future, this should be 1.
        for (AbstractPlanNode p: planNodes) System.out.println(p.toExplainPlanString());

        pn = planNodes.get(0);
        assertTrue(pn instanceof SendPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ReceivePlanNode);

        pn = planNodes.get(1);
        assertTrue(pn instanceof SendPlanNode);
        nlpn = pn.getChild(0);
        assertTrue(nlpn instanceof NestLoopPlanNode);
        pn = nlpn.getChild(0);
        checkSimpleSubSelects(pn, "T1", "A");
        pn = pn.getChild(0);
        checkSimpleSubSelects(pn, "R1", "A");
        pn = nlpn.getChild(1);
        checkSimpleSubSelects(pn, "T2", "C");
        pn = pn.getChild(0);
        checkIndexedSubSelects(pn, "P1", "SYS_IDX_P1_PK_TREE", "C");
*/


        //
        // compile for single partition
        //

        pn = compileForSinglePartition("select A, C FROM (SELECT A FROM R1) T1, (SELECT C FROM P1) T2 " +
                "WHERE T1.A = T2.C ");
        System.out.println(pn.toExplainPlanString());
        assertTrue(pn instanceof SendPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        nlpn = pn.getChild(0);
        assertTrue(nlpn instanceof NestLoopPlanNode);
        pn = nlpn.getChild(0);
        checkSimpleSubSelects(pn, "T1", "A");
        pn = pn.getChild(0);
        checkSimpleSubSelects(pn, "R1", "A");
        pn = nlpn.getChild(1);
        checkSimpleSubSelects(pn, "T2", "C");
        pn = pn.getChild(0);
        checkIndexedSubSelects(pn, "P1", "SYS_IDX_P1_PK_TREE", "C");


        pn = compileForSinglePartition("select A, C FROM (SELECT A FROM R1) T1, (SELECT C FROM P1 where A=3) T2 " +
                "WHERE T1.A = T2.C ");
        System.out.println(pn.toExplainPlanString());
        assertTrue(pn instanceof SendPlanNode);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        nlpn = pn.getChild(0);
        assertTrue(nlpn instanceof NestLoopPlanNode);
        pn = nlpn.getChild(0);
        checkSimpleSubSelects(pn, "T1", "A");
        pn = pn.getChild(0);
        checkSimpleSubSelects(pn, "R1", "A");
        pn = nlpn.getChild(1);
        checkSimpleSubSelects(pn, "T2", "C");
        pn = pn.getChild(0);
        checkIndexedSubSelects(pn, "P1", "SYS_IDX_P1_PK_TREE", "C");
    }


    public void testSubSelects_With_Unions() {
        AbstractPlanNode pn;
        //        List<AbstractPlanNode> planNodes;
        //        AbstractPlanNode nlpn;
        //        for (AbstractPlanNode p: planNodes) System.out.println(p.toExplainPlanString());

        pn = compile("select A, C FROM (SELECT A, C FROM R1 UNION SELECT A, C FROM R2 UNION SELECT A, C FROM R3) T1 order by A ");
        System.out.println(pn.toExplainPlanString());
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

    @Override
    protected void setUp() throws Exception {
        setupSchema(TestSubQueries.class.getResource("testplans-subqueries-ddl.sql"), "dd", false);
    }

}
