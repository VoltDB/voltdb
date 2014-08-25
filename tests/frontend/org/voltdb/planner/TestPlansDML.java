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

import java.util.ArrayList;
import java.util.List;

import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.DeletePlanNode;
import org.voltdb.plannodes.InsertPlanNode;
import org.voltdb.plannodes.ReceivePlanNode;
import org.voltdb.plannodes.UpdatePlanNode;
import org.voltdb.types.PlanNodeType;

public class TestPlansDML extends PlannerTestCase {

    List<AbstractPlanNode> pns;
    public void testBasicUpdateAndDelete() {
        // select * with ON clause should return all columns from all tables
        AbstractPlanNode n;
        AbstractPlanNode pn;

        pns = compileToFragments("UPDATE R1 SET C = 1 WHERE C = 0");
        pn = pns.get(0);
        System.out.println(pn.toExplainPlanString());
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof ReceivePlanNode);
        pn = pns.get(1);
        n = pn.getChild(0);
        assertTrue(n instanceof UpdatePlanNode);

        pns = compileToFragments("DELETE FROM R1 WHERE C = 0");
        pn = pns.get(0);
        System.out.println(pn.toExplainPlanString());
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof ReceivePlanNode);
        pn = pns.get(1);
        n = pn.getChild(0);
        assertTrue(n instanceof DeletePlanNode);

        pns = compileToFragments("INSERT INTO R1 VALUES (1, 2, 3)");
        pn = pns.get(0);
        System.out.println(pn.toExplainPlanString());
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof ReceivePlanNode);
        pn = pns.get(1);
        n = pn.getChild(0);
        assertTrue(n instanceof InsertPlanNode);

        pns = compileToFragments("UPDATE P1 SET C = 1 WHERE C = 0");
        pn = pns.get(0);
        System.out.println(pn.toExplainPlanString());
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof ReceivePlanNode);
        pn = pns.get(1);
        n = pn.getChild(0);
        assertTrue(n instanceof UpdatePlanNode);

        pns = compileToFragments("DELETE FROM P1 WHERE C = 0");
        pn = pns.get(0);
        System.out.println(pn.toExplainPlanString());
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof ReceivePlanNode);
        pn = pns.get(1);
        n = pn.getChild(0);
        assertTrue(n instanceof DeletePlanNode);

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
        String tbs[] = {"R1", "P1"};
        for (String tb: tbs) {
            pns = compileToFragments("Truncate table " + tb);
            checkTruncateFlag();

            pns = compileToFragments("DELETE FROM " + tb);
            checkTruncateFlag();
        }
    }

    public void testInsertIntoSelectPlan() {
        System.out.println("\n\n\nRUNNING testInsertIntoSelectPlan\n\n");

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

    private void checkTruncateFlag() {
        assertTrue(pns.size() == 2);

        ArrayList<AbstractPlanNode> deletes = pns.get(1).findAllNodesOfType(PlanNodeType.DELETE);

        assertTrue(deletes.size() == 1);
        assertTrue(((DeletePlanNode) deletes.get(0) ).isTruncate());
    }


    @Override
    protected void setUp() throws Exception {
        setupSchema(TestJoinOrder.class.getResource("testplans-join-ddl.sql"), "testplansjoin", false);
    }

}
