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
