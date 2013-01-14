/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.plannodes.UnionPlanNode;

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

        pn = compile("(select A from T1 UNION select B from T2) UNION select C from T3", 0, false, null);
        unionPN = (UnionPlanNode) pn.getChild(0);
        assertTrue(unionPN.getUnionType() == ParsedUnionStmt.UnionType.UNION);
        assertTrue(unionPN.getChildCount() == 3);

        pn = compile("select A from T1 UNION (select B from T2 UNION select C from T3)", 0, false, null);
        unionPN = (UnionPlanNode) pn.getChild(0);
        assertTrue(unionPN.getUnionType() == ParsedUnionStmt.UnionType.UNION);
        assertTrue(unionPN.getChildCount() == 3);
   }

    public void testPartitioningMixes() {
        // Sides are identically single-partitioned.
        AbstractPlanNode pn = compile("select DESC from T1 WHERE A = 1 UNION select TEXT from T5 WHERE E = 1", 0, false, null);
        assertTrue(pn.getChild(0) instanceof UnionPlanNode);
        UnionPlanNode unionPN = (UnionPlanNode) pn.getChild(0);
        assertTrue(unionPN.getUnionType() == ParsedUnionStmt.UnionType.UNION);
        assertTrue(unionPN.getChildCount() == 2);

        // In the future, new capabilities like "pushdown of set ops into the collector fragment" and
        // "designation of coordinator execution sites for multi-partition (multi-fragment) plans"
        // may allow more liberal mixes of selects on partitioned tables.
    }

    public void testUnionAll() {
        AbstractPlanNode pn = compile("select A from T1 UNION ALL select B from T2", 0, false, null);
        assertTrue(pn.getChild(0) instanceof UnionPlanNode);
        UnionPlanNode unionPN = (UnionPlanNode) pn.getChild(0);
        assertTrue(unionPN.getUnionType() == ParsedUnionStmt.UnionType.UNION_ALL);
        assertTrue(unionPN.getChildCount() == 2);
    }

    public void testExcept() {
        AbstractPlanNode pn = compile("select A from T1 EXCEPT select B from T2 EXCEPT select C from T3 EXCEPT select F from T6", 0, false, null);
        assertTrue(pn.getChild(0) instanceof UnionPlanNode);
        UnionPlanNode unionPN = (UnionPlanNode) pn.getChild(0);
        assertTrue(unionPN.getUnionType() == ParsedUnionStmt.UnionType.EXCEPT);
        assertTrue(unionPN.getChildCount() == 4);

        pn = compile("select A from T1 EXCEPT (select B from T2 EXCEPT select C from T3) EXCEPT select F from T6", 0, false, null);
        unionPN = (UnionPlanNode) pn.getChild(0);
        assertTrue(unionPN.getUnionType() == ParsedUnionStmt.UnionType.EXCEPT);
        assertTrue(unionPN.getChildCount() == 3);
        UnionPlanNode childPN = (UnionPlanNode) unionPN.getChild(1);
        assertTrue(childPN.getUnionType() == ParsedUnionStmt.UnionType.EXCEPT);
        assertTrue(childPN.getChildCount() == 2);
    }

    public void testExceptAll() {
        AbstractPlanNode pn = compile("select A from T1 EXCEPT ALL select B from T2", 0, false, null);
        assertTrue(pn.getChild(0) instanceof UnionPlanNode);
        UnionPlanNode unionPN = (UnionPlanNode) pn.getChild(0);
        assertTrue(unionPN.getUnionType() == ParsedUnionStmt.UnionType.EXCEPT_ALL);
        assertTrue(unionPN.getChildCount() == 2);

        pn = compile("select A from T1 EXCEPT ALL (select B from T2 EXCEPT ALL select C from T3) EXCEPT ALL select F from T6", 0, false, null);
        unionPN = (UnionPlanNode) pn.getChild(0);
        assertTrue(unionPN.getUnionType() == ParsedUnionStmt.UnionType.EXCEPT_ALL);
        assertTrue(unionPN.getChildCount() == 3);
        UnionPlanNode childPN = (UnionPlanNode) unionPN.getChild(1);
        assertTrue(childPN.getUnionType() == ParsedUnionStmt.UnionType.EXCEPT_ALL);
        assertTrue(childPN.getChildCount() == 2);
    }

    public void testIntersect() {
        AbstractPlanNode pn = compile("select A from T1 INTERSECT select B from T2 INTERSECT select C from T3", 0, false, null);
        assertTrue(pn.getChild(0) instanceof UnionPlanNode);
        UnionPlanNode unionPN = (UnionPlanNode) pn.getChild(0);
        assertTrue(unionPN.getUnionType() == ParsedUnionStmt.UnionType.INTERSECT);
        assertTrue(unionPN.getChildCount() == 3);

        pn = compile("(select A from T1 INTERSECT select B from T2) INTERSECT select C from T3", 0, false, null);
        assertTrue(pn.getChild(0) instanceof UnionPlanNode);
        unionPN = (UnionPlanNode) pn.getChild(0);
        assertTrue(unionPN.getUnionType() == ParsedUnionStmt.UnionType.INTERSECT);
        assertTrue(unionPN.getChildCount() == 3);

        pn = compile("select A from T1 INTERSECT (select B from T2 INTERSECT select C from T3)", 0, false, null);
        assertTrue(pn.getChild(0) instanceof UnionPlanNode);
        unionPN = (UnionPlanNode) pn.getChild(0);
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
            // If both sides are multi-partitioned, there is no facility for pushing down the
            // union processing below the send/receive, so each child of the union requires
            // its own send/receive so the plan ends up as an unsupported 3-fragment plan.
            aide.compile("select DESC from T1 UNION select TEXT from T5", 0, false, null);
            fail();
        }
        catch (Exception ex) {}

        try {
            // If ONE side is single-partitioned, it would theoretically be possible to satisfy
            // the query with a 2-fragment plan IFF the coordinator fragment could be forced to
            // execute on the designated single partition.
            // At this point, coordinator designation is only supported for single-fragment plans.
            // So, this case must also error out.
            aide.compile("select DESC from T1 WHERE A = 1 UNION select TEXT from T5", 0, false, null);
            fail();
        }
        catch (Exception ex) {}

        try {
            // If BOTH sides are single-partitioned, but for different partitions,
            // it would theoretically be possible to satisfy
            // the query with a 2-fragment plan IFF the coordinator fragment could be forced to
            // execute on one of the designated single partitions.
            // At this point, coordinator designation is only supported for single-fragment plans.
            aide.compile("select DESC from T1 WHERE A = 1 UNION select TEXT from T5 WHERE E = 2", 0, false, null);
            fail();
        }
        catch (Exception ex) {}

        try {
            // If both sides are multi-partitioned, there is no facility for pushing down the
            // union processing below the send/receive, so each child of the union requires
            // its own send/receive so the plan ends up as an unsupported 3-fragment plan.
            aide.compile("select DESC from T1 UNION select TEXT from T5", 0, false, null);
            fail();
        }
        catch (Exception ex) {}

        try {
            // If ONE side is single-partitioned, it would theoretically be possible to satisfy
            // the query with a 2-fragment plan IFF the coordinator fragment could be forced to
            // execute on the designated single partition.
            // At this point, coordinator designation is only supported for single-fragment plans.
            // So, this case must also error out.
            aide.compile("select DESC from T1 WHERE A = 1 UNION select TEXT from T5", 0, false, null);
            fail();
        }
        catch (Exception ex) {}

        try {
            // If BOTH sides are single-partitioned, but for different partitions,
            // it would theoretically be possible to satisfy
            // the query with a 2-fragment plan IFF the coordinator fragment could be forced to
            // execute on one of the designated single partitions.
            // At this point, coordinator designation is only supported for single-fragment plans.
            aide.compile("select DESC from T1 WHERE A = 1 UNION select TEXT from T5 WHERE E = 2", 0, false, null);
            fail();
        }
        catch (Exception ex) {}

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
        // Set partitioning for some tables.
        // TODO: Enable PARTITION statements in the ddl -- PlannerTestAideDeCamp ignores them
        // -- not sure why/how -- consider fixing or abandoning PlannerTestAideDeCamp.
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
            } else if ("T5".equalsIgnoreCase(name)) {
                t.setPartitioncolumn(t.getColumns().get("E"));
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
