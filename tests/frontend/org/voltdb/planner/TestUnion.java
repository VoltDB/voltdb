/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

import junit.framework.TestCase;

import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Table;
import org.voltdb.plannodes.*;

public class TestUnion  extends TestCase {

    private PlannerTestAideDeCamp aide;

    private AbstractPlanNode compile(String sql, int paramCount,
                                     boolean singlePartition,
                                     String joinOrder)
    {
        List<AbstractPlanNode> pn = null;
        try {
            pn =  aide.compile(sql, paramCount, singlePartition, joinOrder);
        }
        catch (NullPointerException ex) {
            // aide may throw NPE if no plangraph was created
            ex.printStackTrace();
            fail();
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail();
        }
        assertTrue(pn != null);
        assertFalse(pn.isEmpty());
        assertTrue(pn.get(0) != null);
        return pn.get(0);
    }

    public void testUnion() {
        AbstractPlanNode pn = compile("select A from T1 UNION select B from T2 UNION select C from T3", 0, false, null);
        assertTrue(pn.getChild(0) instanceof UnionPlanNode);
        UnionPlanNode unionPN = (UnionPlanNode) pn.getChild(0);
        assertTrue(unionPN.getUnionType() == ParsedUnionStmt.UnionType.UNION);
        assertTrue(unionPN.getChildCount() == 3);
    }

    public void testUnionAll() {
        AbstractPlanNode pn = compile("select A from T1 UNION ALL select B from T2", 0, false, null);
        assertTrue(pn.getChild(0) instanceof UnionPlanNode);
        UnionPlanNode unionPN = (UnionPlanNode) pn.getChild(0);
        assertTrue(unionPN.getUnionType() == ParsedUnionStmt.UnionType.UNION_ALL);
        assertTrue(unionPN.getChildCount() == 2);
    }

    public void testExcept() {
        AbstractPlanNode pn = compile("select A from T1 EXCEPT select B from T2 EXCEPT select C from T3", 0, false, null);
        assertTrue(pn.getChild(0) instanceof UnionPlanNode);
        UnionPlanNode unionPN = (UnionPlanNode) pn.getChild(0);
        assertTrue(unionPN.getUnionType() == ParsedUnionStmt.UnionType.EXCEPT);
        assertTrue(unionPN.getChildCount() == 3);
    }

    public void testExceptAll() {
        AbstractPlanNode pn = compile("select A from T1 EXCEPT ALL select B from T2", 0, false, null);
        assertTrue(pn.getChild(0) instanceof UnionPlanNode);
        UnionPlanNode unionPN = (UnionPlanNode) pn.getChild(0);
        assertTrue(unionPN.getUnionType() == ParsedUnionStmt.UnionType.EXCEPT_ALL);
        assertTrue(unionPN.getChildCount() == 2);
    }

    public void testIntersect() {
        AbstractPlanNode pn = compile("select A from T1 INTERSECT select B from T2 INTERSECT select C from T3", 0, false, null);
        assertTrue(pn.getChild(0) instanceof UnionPlanNode);
        UnionPlanNode unionPN = (UnionPlanNode) pn.getChild(0);
        assertTrue(unionPN.getUnionType() == ParsedUnionStmt.UnionType.INTERSECT);
        assertTrue(unionPN.getChildCount() == 3);
    }

    public void testIntersectAll() {
        AbstractPlanNode pn = compile("select A from T1 INTERSECT ALL select B from T2", 0, false, null);
        assertTrue(pn.getChild(0) instanceof UnionPlanNode);
        UnionPlanNode unionPN = (UnionPlanNode) pn.getChild(0);
        assertTrue(unionPN.getUnionType() == ParsedUnionStmt.UnionType.INTERSECT_ALL);
        assertTrue(unionPN.getChildCount() == 2);
    }

    public void testDistributedUnion() {
        try {
            aide.compile("select A from T1 UNION select D from T4", 0, false, null);
            fail();
        }
        catch (Exception ex) {}
    }

    public void testUniqueTables() {
        try {
            aide.compile("select A from T1 UNION select A from T1", 0, false, null);
            fail();
        }
        catch (Exception ex) {}

        try {
            aide.compile("(select A from T1 UNION select B from T2) EXCEPT select A from T1", 0, false, null);
            fail();
        }
        catch (Exception ex) {}

        try {
            aide.compile("select A from T1 UNION (select B from T2 EXCEPT select A from T1)", 0, false, null);
            fail();
        }
        catch (Exception ex) {}


    }

    public void testMultipleSetOperations() {
        AbstractPlanNode pn = compile("select A from T1 UNION select B from T2 EXCEPT select C from T3", 0, false, null);
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

    public void testColumnMismatch() {
        try {
            aide.compile("select A, DESC from T1 UNION select B from T2", 0, false, null);
            fail();
        }
        catch (Exception ex) {}
        try {
            aide.compile("select B from T2 EXCEPT select A, DESC from T1 ", 0, false, null);
            fail();
        }
        catch (Exception ex) {}
        try {
            aide.compile("select B from T2 EXCEPT select F from T1 ", 0, false, null);
            fail();
        }
        catch (Exception ex) {}
    }

   public void testNonSupportedUnions() {
        try {
            aide.compile("select A from T1 NOUNION select B from T2", 0, false, null);
            fail();
        }
        catch (Exception ex) {}
        try {
            aide.compile("select A from T1 TERM select B from T2", 0, false, null);
            fail();
        }
        catch (Exception ex) {}
    }

    @Override
    protected void setUp() throws Exception {
        aide = new PlannerTestAideDeCamp(TestUnion.class.getResource("testunion-ddl.sql"),
                                         "testunion");
        // Set all tables to non-replicated.
        Cluster cluster = aide.getCatalog().getClusters().get("cluster");
        CatalogMap<Table> tmap = cluster.getDatabases().get("database").getTables();
        for (Table t : tmap) {
            String name = t.getTypeName();
            if ("T1".equalsIgnoreCase(name)) {
                t.setPartitioncolumn(t.getColumns().get("A"));
                t.setIsreplicated(false);
            } else if ("T4".equalsIgnoreCase(name)) {
                t.setPartitioncolumn(t.getColumns().get("D"));
                t.setIsreplicated(false);
            } else {
                t.setIsreplicated(true);
            }
        }
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        aide.tearDown();
    }
}
