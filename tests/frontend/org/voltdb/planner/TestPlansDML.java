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
import java.util.List;

import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.SelectSubqueryExpression;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.DeletePlanNode;
import org.voltdb.plannodes.InsertPlanNode;
import org.voltdb.plannodes.ReceivePlanNode;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.plannodes.UpdatePlanNode;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.PlanNodeType;

public class TestPlansDML extends PlannerTestCase {

    public void testBasicUpdateAndDelete() {
        // select * with ON clause should return all columns from all tables
        List<AbstractPlanNode> pns;
        AbstractPlanNode pn;
        AbstractPlanNode node;

        pns = compileToFragments("UPDATE R1 SET C = 1 WHERE C = 0");
        pn = pns.get(0);
        System.out.println(pn.toExplainPlanString());
        node = pn.getChild(0).getChild(0);
        assertTrue(node instanceof ReceivePlanNode);
        pn = pns.get(1);
        node = pn.getChild(0);
        assertTrue(node instanceof UpdatePlanNode);

        pns = compileToFragments("DELETE FROM R1 WHERE C = 0");
        pn = pns.get(0);
        System.out.println(pn.toExplainPlanString());
        node = pn.getChild(0).getChild(0);
        assertTrue(node instanceof ReceivePlanNode);
        pn = pns.get(1);
        node = pn.getChild(0);
        assertTrue(node instanceof DeletePlanNode);

        pns = compileToFragments("INSERT INTO R1 VALUES (1, 2, 3)");
        pn = pns.get(0);
        System.out.println(pn.toExplainPlanString());
        node = pn.getChild(0).getChild(0);
        assertTrue(node instanceof ReceivePlanNode);
        pn = pns.get(1);
        node = pn.getChild(0);
        assertTrue(node instanceof InsertPlanNode);

        pns = compileToFragments("UPDATE P1 SET C = 1 WHERE C = 0");
        pn = pns.get(0);
        System.out.println(pn.toExplainPlanString());
        node = pn.getChild(0).getChild(0);
        assertTrue(node instanceof ReceivePlanNode);
        pn = pns.get(1);
        node = pn.getChild(0);
        assertTrue(node instanceof UpdatePlanNode);

        pns = compileToFragments("DELETE FROM P1 WHERE C = 0");
        pn = pns.get(0);
        System.out.println(pn.toExplainPlanString());
        node = pn.getChild(0).getChild(0);
        assertTrue(node instanceof ReceivePlanNode);
        pn = pns.get(1);
        node = pn.getChild(0);
        assertTrue(node instanceof DeletePlanNode);

        pns = compileToFragments("UPDATE P1 SET C = 1 WHERE A = 0");
        pn = pns.get(0);
        System.out.println(pn.toExplainPlanString());
        //n = pn.getChild(0);
        assertTrue(pn instanceof UpdatePlanNode);

        pns = compileToFragments("DELETE FROM P1 WHERE A = 0");
        pn = pns.get(0);
        System.out.println(pn.toExplainPlanString());
        //n = pn.getChild(0);
        assertTrue(pn instanceof DeletePlanNode);

        pns = compileToFragments("INSERT INTO P1 VALUES (1, 2)");
        pn = pns.get(0);
        System.out.println(pn.toExplainPlanString());
        //n = pn.getChild(0).getChild(0);
        assertTrue(pn instanceof InsertPlanNode);

    }

    public void testTruncateTable() {
        List<AbstractPlanNode> pns;
        String tbs[] = {"R1", "P1"};
        for (String tb: tbs) {
            pns = compileToFragments("Truncate table " + tb);
            checkTruncateFlag(pns);

            pns = compileToFragments("DELETE FROM " + tb);
            checkTruncateFlag(pns);
        }
    }

    public void testInsertIntoSelectPlan() {
        System.out.println("\n\nRUNNING testInsertIntoSelectPlan\n\n");
        List<AbstractPlanNode> pns;

        // This should be inferred as single-partition
        pns = compileToFragments("INSERT INTO P1 SELECT * FROM P2 WHERE A = ?");

        // One fragment means a single-partition plan
        assertEquals(1, pns.size());

        // But this should be multi-partition
        pns = compileToFragments("INSERT INTO P1 SELECT * FROM P2");
        assertEquals(2, pns.size());

        // Single-partition
        pns = compileToFragments("INSERT INTO P1 " +
                "SELECT P2.A, P3.F " +
                "FROM P2 INNER JOIN P3 ON P2.A = P3.A " +
                "WHERE P3.A = ?");
        assertEquals(1, pns.size());

        // Multi-partition
        pns = compileToFragments("INSERT INTO P1 " +
                "SELECT P2.A, P3.F " +
                "FROM P2 INNER JOIN P3 ON P2.A = P3.A ");
        assertEquals(2, pns.size());


        pns = compileToFragments("INSERT INTO P1 " +
                "SELECT sq.sqa, 7 " +
                "FROM (SELECT P2.A AS sqa FROM P2) AS sq;");
        assertEquals(2, pns.size());

        pns = compileToFragments("INSERT INTO P1 " +
                "SELECT sq.sqa, 9 " +
                "FROM (SELECT P2.A AS sqa FROM P2 WHERE P2.A = 9) AS sq;");
        assertEquals(1, pns.size());

        pns = compileToFragments("INSERT INTO P1 " +
                "SELECT sq.sqa, 9 " +
                "FROM (SELECT P2.A AS sqa FROM P2) AS sq " +
                "WHERE sq.sqa = 10;");
        assertEquals(1, pns.size());

        pns = compileToFragments(
                "INSERT INTO P1 " +
                "select P2_subq.Asq, P3_subq.Fsq  " +
                "from (select 7, P2_subq_subq.Esqsq as Esq, P2_subq_subq.Asqsq as Asq from " +
                "   (select P2.E as Esqsq, P2.A as Asqsq from P2) as P2_subq_subq) as P2_subq " +
                "inner join " +
                "(select P3.A as Asq, P3.F as Fsq from P3) as P3_subq " +
                "on P3_subq.Asq = P2_subq.Asq;");
        assertEquals(2, pns.size());

        pns = compileToFragments(
                "INSERT INTO P1 " +
                "select P2_subq.Asq, P3_subq.Fsq  " +
                "from (select 7, P2_subq_subq.Esqsq as Esq, P2_subq_subq.Asqsq as Asq from " +
                "   (select P2.E as Esqsq, P2.A as Asqsq from P2 " +
                "     where P2.A = ?) as P2_subq_subq) as P2_subq " +
                "inner join " +
                "(select P3.A as Asq, P3.F as Fsq from P3) as P3_subq " +
                "on P3_subq.Asq = P2_subq.Asq;");
        assertEquals(1, pns.size());
    }

    public void testInsertSingleRowPlan() {
        System.out.println("\n\n\nRUNNING testInsertSingleRowPlan\n\n");
        List<AbstractPlanNode> pns;

        // These test cases are from ENG-5929.

        // This should be inferred as single-partition:
        pns = compileToFragments("INSERT INTO P1 (a, c) values(100, cast(? + 1 as integer))");
        // One fragment means a single-partition plan
        assertEquals(1, pns.size());

        // But this should be multi-partition:
        // Cannot evaluate expression except in EE.
        pns = compileToFragments("INSERT INTO P1 (a, c) values(cast(? + 1 as integer), 100)");
        assertEquals(2, pns.size());
    }

    public void testDeleteOrderByPlan() {
        System.out.println("\n\n\nRUNNING testDeleteOrderByPlan\n\n");
        List<AbstractPlanNode> pns;

        PlanNodeType[] deleteFromIndexScan = { PlanNodeType.SEND,
                PlanNodeType.DELETE,
                PlanNodeType.INDEXSCAN,
        };
        PlanNodeType[] deleteFromSortedIndexScan = { PlanNodeType.SEND,
                PlanNodeType.DELETE,
                PlanNodeType.ORDERBY,
                PlanNodeType.INDEXSCAN,
        };
        PlanNodeType[] deleteFromSortedSeqScan = { PlanNodeType.SEND,
                PlanNodeType.DELETE,
                PlanNodeType.ORDERBY,
                PlanNodeType.SEQSCAN,
        };

        // No ORDER BY node, since we can use index instead
        pns = compileToFragments("DELETE FROM R5 ORDER BY A LIMIT ?");
        assertEquals(2, pns.size());
        AbstractPlanNode collectorRoot = pns.get(1);
        assertLeftChain(collectorRoot, deleteFromIndexScan);

        // No ORDER BY node, since index scan is used to evaluate predicate
        pns = compileToFragments("DELETE FROM R5 WHERE A = 1 ORDER BY A LIMIT ?");
        assertEquals(2, pns.size());
        collectorRoot = pns.get(1);
        assertLeftChain(collectorRoot, deleteFromIndexScan);

        // Index used to evaluate predicate not suitable for ORDER BY
        pns = compileToFragments("DELETE FROM R5 WHERE A = 1 ORDER BY B, A, C, D LIMIT ?");
        assertEquals(2, pns.size());
        collectorRoot = pns.get(1);
        assertLeftChain(collectorRoot, deleteFromSortedIndexScan);

        // Index can't be used either for predicate evaluation or ORDER BY
        pns = compileToFragments("DELETE FROM R5 WHERE B = 1 ORDER BY B, A, C, D LIMIT ?");
        assertEquals(2, pns.size());
        collectorRoot = pns.get(1);
        assertLeftChain(collectorRoot, deleteFromSortedSeqScan);
    }

    /**
     * ENG-7384 Redundant predicate in DELETE/UPDATE statement plans.
     */
    public void testDMLPredicate() {
        List<AbstractPlanNode> pns;
        pns = compileToFragments("UPDATE P1 SET C = 1 WHERE A = 0");
        assertEquals(1, pns.size());
        checkPredicate(pns.get(0).getChild(0), ExpressionType.COMPARE_EQUAL);

        pns = compileToFragments("DELETE FROM P1 WHERE A > 0");
        assertTrue(pns.size() == 2);
        checkPredicate(pns.get(1).getChild(0).getChild(0), ExpressionType.COMPARE_GREATERTHAN);
    }

    public void testDMLwithExpressionSubqueries() {

        String dmlSQL;

        dmlSQL = "UPDATE R1 SET C = 1 WHERE C IN (SELECT A FROM R2 WHERE R2.A = R1.C);";
        checkDMLPlanNodeAndSubqueryExpression(dmlSQL, ExpressionType.OPERATOR_EXISTS);

        dmlSQL = "UPDATE R1 SET C = 1 WHERE EXISTS (SELECT A FROM R2 WHERE R1.C = R2.A);";
        checkDMLPlanNodeAndSubqueryExpression(dmlSQL, ExpressionType.OPERATOR_EXISTS);

        dmlSQL = "UPDATE R1 SET C = 1 WHERE C > ALL (SELECT A FROM R2);";
        checkDMLPlanNodeAndSubqueryExpression(dmlSQL, ExpressionType.COMPARE_GREATERTHAN);

        dmlSQL = "UPDATE P1 SET C = 1 WHERE A = 0 AND C > ALL (SELECT A FROM R2);";
        checkDMLPlanNodeAndSubqueryExpression(dmlSQL, ExpressionType.CONJUNCTION_AND);

        dmlSQL = "UPDATE P1 SET C = (SELECT C FROM R2 WHERE A = 0) ;";
        checkDMLPlanNodeAndSubqueryExpression(dmlSQL, null);

        dmlSQL = "UPDATE P1 set C = ? WHERE A = (SELECT MAX(R1.C) FROM R1 WHERE R1.A = P1.A);";
        checkDMLPlanNodeAndSubqueryExpression(dmlSQL, ExpressionType.COMPARE_EQUAL);

        dmlSQL = "UPDATE P1 set C = (SELECT C FROM R1 WHERE R1.C = P1.C LIMIT 1);";
        checkDMLPlanNodeAndSubqueryExpression(dmlSQL, null);

        dmlSQL = "UPSERT INTO R6 (A, C) SELECT A, C FROM R2 WHERE NOT EXISTS (SELECT 1 FROM R6 WHERE R6.A = R2.C) ORDER BY 1, 2;";
        checkDMLPlanNodeAndSubqueryExpression(dmlSQL, ExpressionType.OPERATOR_NOT);

        dmlSQL = "DELETE FROM R1 WHERE C IN (SELECT A FROM R2);";
        checkDMLPlanNodeAndSubqueryExpression(dmlSQL, ExpressionType.OPERATOR_EXISTS);

        dmlSQL = "DELETE FROM R1 WHERE EXISTS (SELECT A FROM R2 WHERE R1.C = R2.A);";
        checkDMLPlanNodeAndSubqueryExpression(dmlSQL, ExpressionType.OPERATOR_EXISTS);

        dmlSQL = "DELETE FROM R1 WHERE C > ALL (SELECT A FROM R2);";
        checkDMLPlanNodeAndSubqueryExpression(dmlSQL, ExpressionType.COMPARE_GREATERTHAN);

        dmlSQL = "DELETE FROM P1 WHERE C > ALL (SELECT A FROM R2);";
        checkDMLPlanNodeAndSubqueryExpression(dmlSQL, ExpressionType.COMPARE_GREATERTHAN);

        dmlSQL = "DELETE FROM P1 WHERE A = 0 AND C > ALL (SELECT A FROM R2);";
        checkDMLPlanNodeAndSubqueryExpression(dmlSQL, ExpressionType.CONJUNCTION_AND);

        dmlSQL = "INSERT INTO P1 SELECT * FROM P1 PA WHERE NOT EXISTS (SELECT A FROM R1 RB WHERE PA.A = RB.A);";
        checkDMLPlanNodeAndSubqueryExpression(dmlSQL, ExpressionType.OPERATOR_NOT);

        dmlSQL = "INSERT INTO R1 SELECT * FROM R1 RA WHERE NOT EXISTS (SELECT A FROM R1 RB WHERE RA.A = RB.A);";
        checkDMLPlanNodeAndSubqueryExpression(dmlSQL, ExpressionType.OPERATOR_NOT);

        dmlSQL = "INSERT INTO R1 SELECT * FROM R1 RA WHERE RA.A IN (SELECT A FROM R2 WHERE R2.A > 0);";
        checkDMLPlanNodeAndSubqueryExpression(dmlSQL, ExpressionType.OPERATOR_EXISTS);

        dmlSQL = "INSERT INTO R1 (A, C, D) SELECT (SELECT MAX(A) FROM R1), 32, 32 FROM R1;";
        checkDMLPlanNodeAndSubqueryExpression(dmlSQL, null);

        dmlSQL = "INSERT INTO R1 (A, C, D) VALUES ((SELECT MAX(A) FROM R1), 32, 32);";
        checkDMLPlanNodeAndSubqueryExpression(dmlSQL, null);

        // Distributed expression subquery
        failToCompile("DELETE FROM R1 WHERE C > ALL (SELECT A FROM P2 WHERE A = 1);", PlanAssembler.IN_EXISTS_SCALAR_ERROR_MESSAGE);

        // Distributed expression subquery
        failToCompile("UPDATE R1 SET C = (SELECT A FROM P2 WHERE A = 1);", PlanAssembler.IN_EXISTS_SCALAR_ERROR_MESSAGE);
        failToCompile("insert into P1 (A,C) " +
                "select A,C from R2 " +
                "where not exists (select A from P1 AP1 where R2.A = AP1.A);",
                "Subquery expressions are only supported for single partition procedures and AdHoc queries referencing only replicated tables.");

        // Distributed expression subquery with inferred partitioning
        failToCompile("DELETE FROM R1 WHERE C > ALL (SELECT A FROM P2);", PlanAssembler.IN_EXISTS_SCALAR_ERROR_MESSAGE);

        // Column count mismatch between the UPDATE columns and the columns returned by the scalar subquery
        failToCompile("UPDATE P1 set C = (SELECT (A,C) FROM R1 WHERE R1.C = P1.C LIMIT 1);", "row column count mismatch");
    }

    void checkDMLPlanNodeAndSubqueryExpression(String dmlSQL, ExpressionType filterType) {
        List<AbstractPlanNode> pns = compileToFragments(dmlSQL);
        AbstractPlanNode dmlNode;

        if (pns.size() == 2) {
            dmlNode = pns.get(1).getChild(0);
        } else {
            dmlNode = pns.get(0);
        }

        String dmlType = dmlSQL.substring(0, dmlSQL.indexOf(' ')).trim().toUpperCase();
        if ("UPSERT".equalsIgnoreCase(dmlType)) {
            // UPSERT is INSERT
            dmlType = "INSERT";
        }
        // This must somehow start with a node which is named by
        // dmlType.  It could be a sequential scan node with an
        // inline node named by dmlType, or it could be a dmlType
        // node just by itself.
        //
        // This seems somewhat fragile.  It seem like we should have
        // a description of the expected plan which we just match
        // here.  See the member function TestCase.validatePlan.  That
        // member function is not actually convenient here, but perhaps
        // it could be extended in some way.
        if ((dmlNode instanceof SeqScanPlanNode)
                && "INSERT".equals(dmlType)) {
            assertNotNull("Expected an insert node.",
                          dmlNode.getInlinePlanNode(PlanNodeType.INSERT));
        }
        else {
            assertEquals(dmlType, dmlNode.getPlanNodeType().toString());
        }

        PlanNodeType nodeType = dmlNode.getPlanNodeType();
        while (nodeType != PlanNodeType.SEQSCAN && nodeType != PlanNodeType.MATERIALIZE && nodeType != PlanNodeType.INDEXSCAN) {
            dmlNode = dmlNode.getChild(0);
            nodeType = dmlNode.getPlanNodeType();
        }
        assertNotNull(dmlNode);

        // Verify DML Predicate
        if (filterType != null) {
            AbstractExpression predicate = ((AbstractScanPlanNode) dmlNode).getPredicate();
            assertNotNull(predicate);
            assertEquals(filterType, predicate.getExpressionType());
            assertTrue(predicate.hasAnySubexpressionOfClass(SelectSubqueryExpression.class));
        }
    }

    private void checkPredicate(AbstractPlanNode pn, ExpressionType type) {
        assertTrue(pn instanceof SeqScanPlanNode);
        AbstractExpression e = ((SeqScanPlanNode) pn).getPredicate();
        assertEquals(type, e.getExpressionType());
    }

    private void checkTruncateFlag(List<AbstractPlanNode> pns) {
        assertTrue(pns.size() == 2);

        List<AbstractPlanNode> deletes = pns.get(1).findAllNodesOfType(PlanNodeType.DELETE);

        assertTrue(deletes.size() == 1);
        assertTrue(((DeletePlanNode) deletes.get(0) ).isTruncate());
    }


    @Override
    protected void setUp() throws Exception {
        setupSchema(TestJoinOrder.class.getResource("testplans-join-ddl.sql"), "testplansjoin", false);
    }

}
