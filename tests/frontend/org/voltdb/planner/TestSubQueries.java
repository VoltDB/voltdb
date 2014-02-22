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

    final String SYSTEM_SUBQUERY = "SYSTEM_SUBQUERY";

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

        NodeSchema schema = snode.getOutputSchema();
        List<SchemaColumn> schemaColumn = schema.getColumns();
        assertEquals(columns.length, schemaColumn.size());

        for (int i = 0; i < schemaColumn.size(); ++i) {
            SchemaColumn col = schemaColumn.get(i);
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

    public void testSimpleSubSelects() {
        AbstractPlanNode pn;

        String tableAliases [] = {" ",             " T1" };
        String columnRefs [] =   {"",              " T1."};
        String fromTables [] =   {SYSTEM_SUBQUERY, "T1"  };
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

    public void testSubSelects_Simple_Joins() {
        AbstractPlanNode pn;

        // sub-selected table joins
        pn = compile("select A, C FROM (SELECT A FROM R1) T1, (SELECT C FROM R2) T2 WHERE A = C");
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);

        AbstractPlanNode nlpn = pn.getChild(0);
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


        // sub-selected table joins without alias
        pn = compile("select A, C FROM (SELECT A FROM R1), (SELECT C FROM R2) WHERE A = C");
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        nlpn = pn.getChild(0);
        assertTrue(nlpn instanceof NestLoopPlanNode);
        assertEquals(2, nlpn.getChildCount());
        pn = nlpn.getChild(0);
        checkSimpleSubSelects(pn, SYSTEM_SUBQUERY,  "A");
        pn= pn.getChild(0);
        checkSimpleSubSelects(pn, "R1",  "A");

        pn = nlpn.getChild(1);
        checkSimpleSubSelects(pn, SYSTEM_SUBQUERY,  "C");
        pn= pn.getChild(0);
        checkSimpleSubSelects(pn, "R2",  "C");
    }

    public void testSubSelects_Function() {
        AbstractPlanNode pn;

        String tableAliases [] = {" ", " T1"};
        String fromTables [] = {SYSTEM_SUBQUERY, "T1"};

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
        checkSimpleSubSelects(pn, "R1",  "A", "C", "D" );
        checkPredicateComparisonExpression(pn, "R1");
        assertEquals(((SeqScanPlanNode) pn).getInlinePlanNodes().size(), 0);


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
        checkSimpleSubSelects(pn, "R1",  "A", "C", "D" );
        checkPredicateComparisonExpression(pn, "R1");
        assertEquals(((SeqScanPlanNode) pn).getInlinePlanNodes().size(), 0);


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
        checkSimpleSubSelects(pn, "R1",  "A", "C", "D" );
        checkPredicateComparisonExpression(pn, "R1");
        assertEquals(((SeqScanPlanNode) pn).getInlinePlanNodes().size(), 0);



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
        checkSimpleSubSelects(pn, "R1",  "A", "C", "D" );
        checkPredicateComparisonExpression(pn, "R1");
        assertEquals(((SeqScanPlanNode) pn).getInlinePlanNodes().size(), 0);
    }

    public void testXin() {
        AbstractPlanNode pn;

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
        checkSimpleSubSelects(pn, "R1",  "A", "C", "D" );
        checkPredicateComparisonExpression(pn, "R1");
        assertEquals(((SeqScanPlanNode) pn).getInlinePlanNodes().size(), 0);
    }

    public void testUnsupportedSubqueries() {
        failToCompile("DELETE FROM R1 WHERE A IN (SELECT A A1 FROM R1 WHERE A>1)", "Unsupported subquery syntax");
    }


    public void testParameters() {
        AbstractPlanNode pn = compile("select A1 FROM (SELECT A A1 FROM R1 WHERE A>?) TEMP WHERE A1<?");
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

    public void testDistributedSubQuery() {
        {
            // Partitioned sub-query
            List<AbstractPlanNode> lpn = compileToFragments("select A, C FROM (SELECT A, C FROM P1) TEMP ");
            assertTrue(lpn.size() == 2);
            AbstractPlanNode n = lpn.get(1);
            n = n.getChild(0);
            assertTrue(n instanceof SeqScanPlanNode);
            assertEquals("TEMP", ((SeqScanPlanNode) n).getTargetTableAlias());
            n = n.getChild(0);
            assertTrue(n instanceof ProjectionPlanNode);
            assertTrue(n.getChild(0) instanceof SeqScanPlanNode);
        }

        {
            // Two sub-queries. One is partitioned and the other one is replicated
            List<AbstractPlanNode> lpn = compileToFragments("select A, C FROM (SELECT A FROM R1) TEMP1, (SELECT C FROM P2) TEMP2 WHERE TEMP1.A = TEMP2.C ");
            assertTrue(lpn.size() == 2);
            AbstractPlanNode n = lpn.get(0);
            assertTrue(n instanceof SendPlanNode);
            n = lpn.get(1);
            assertTrue(n instanceof SendPlanNode);
            n = n.getChild(0);
            assertTrue(n instanceof NestLoopPlanNode);
            AbstractPlanNode c = n.getChild(0);
            assertTrue(c instanceof SeqScanPlanNode);
            assertEquals("TEMP1", ((SeqScanPlanNode) c).getTargetTableAlias());
            c = n.getChild(1);
            assertTrue(c instanceof SeqScanPlanNode);
            assertEquals("TEMP2", ((SeqScanPlanNode) c).getTargetTableAlias());
        }

        {
            failToCompile("select A, C FROM (SELECT A FROM P1) TEMP1, (SELECT C FROM P2) TEMP2 WHERE TEMP1.A = TEMP2.C ",
                    "Join of multiple partitioned tables has insufficient join criteria.");
            failToCompile("select D1, D2 FROM (SELECT A, D D1 FROM P1 ) TEMP1, (SELECT A, D D2 FROM P2 ) TEMP2 WHERE TEMP1.A = TEMP2.A");
            failToCompile("select D1, P2.D FROM (SELECT A, D D1 FROM P1 WHERE A=1) TEMP1, P2 WHERE TEMP1.A = P2.A AND P2.A = 1");
            // Join of a single partitioned sub-queries. The partitions are different
            failToCompile("select D1, D2 FROM (SELECT A, D D1 FROM P1 WHERE A=2) TEMP1, (SELECT A, D D2 FROM P2 WHERE A=2) TEMP2",
                    "Join of multiple partitioned tables has insufficient join criteria.");
            failToCompile("select D1, D2 FROM (SELECT A, D D1 FROM P1) TEMP1, (SELECT A, D D2 FROM P2) TEMP2 WHERE TEMP1.A = 1 AND TEMP2.A = 2",
                    "Join of multiple partitioned tables has insufficient join criteria.");
        }
    }

    public void testOuterJoinSubQuery() {
        {
            List<AbstractPlanNode> lpn = compileToFragments("SELECT A, C FROM R1 LEFT JOIN (SELECT A, C FROM P1) TEMP ON TEMP.C = R1.C ");
            assertTrue(lpn.size() == 2);
            AbstractPlanNode n = lpn.get(0).getChild(0).getChild(0);
            assertTrue(n instanceof NestLoopPlanNode);
            assertEquals(JoinType.LEFT, ((NestLoopPlanNode) n).getJoinType());
            n = n.getChild(0);
            assertTrue(n instanceof SeqScanPlanNode);
            assertEquals("R1", ((SeqScanPlanNode) n).getTargetTableName());
            n = lpn.get(1).getChild(0);
            assertTrue(n instanceof SeqScanPlanNode);
            assertEquals("TEMP", ((SeqScanPlanNode) n).getTargetTableName());
            assertEquals(1, n.getChildCount());
            n = n.getChild(0);
            assertTrue(n instanceof ProjectionPlanNode);
            assertTrue(n.getChild(0) instanceof IndexScanPlanNode);
        }
    }


    @Override
    protected void setUp() throws Exception {
        setupSchema(TestSubQueries.class.getResource("testplans-subqueries-ddl.sql"), "dd", false);
    }

}
