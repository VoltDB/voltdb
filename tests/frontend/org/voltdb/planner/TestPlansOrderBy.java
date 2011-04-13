/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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
import org.voltdb.types.PlanNodeType;

public class TestPlansOrderBy extends TestCase {

    private PlannerTestAideDeCamp aide;

    private AbstractPlanNode compile(String sql, int paramCount) {
        List<AbstractPlanNode> pn = null;
        try {
            pn =  aide.compile(sql, paramCount);
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

    @Override
    protected void setUp() throws Exception {
        aide = new PlannerTestAideDeCamp(TestPlansGroupBy.class.getResource("testplans-orderby-ddl.sql"), "testplansorderby");

        // Set all tables to non-replicated.
        Cluster cluster = aide.getCatalog().getClusters().get("cluster");
        CatalogMap<Table> tmap = cluster.getDatabases().get("database").getTables();
        for (Table t : tmap) {
            t.setIsreplicated(true);
        }
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        aide.tearDown();
    }

    public void testOrderByOne() {
        AbstractPlanNode pn = null;
        pn = compile("SELECT * from T ORDER BY T_PKEY", 0);
        if (pn != null) {
            assertFalse(pn.findAllNodesOfType(PlanNodeType.INDEXSCAN).isEmpty());
            assertTrue(pn.findAllNodesOfType(PlanNodeType.SEQSCAN).isEmpty());
            assertTrue(pn.findAllNodesOfType(PlanNodeType.ORDERBY).isEmpty());
        }
    }

    public void testOrderByTwo() {
        AbstractPlanNode pn = null;
        pn = compile("SELECT * from T ORDER BY T_PKEY, T_D1", 0);
        if (pn != null) {
            assertFalse(pn.findAllNodesOfType(PlanNodeType.INDEXSCAN).isEmpty());
            assertTrue(pn.findAllNodesOfType(PlanNodeType.SEQSCAN).isEmpty());
            assertTrue(pn.findAllNodesOfType(PlanNodeType.ORDERBY).isEmpty());
        }
    }

    public void testOrderByTwoDesc() {
        AbstractPlanNode pn = null;
        pn = compile("SELECT * from T ORDER BY T_PKEY DESC, T_D1 DESC", 0);
        if (pn != null) {
            assertFalse(pn.findAllNodesOfType(PlanNodeType.INDEXSCAN).isEmpty());
            assertTrue(pn.findAllNodesOfType(PlanNodeType.SEQSCAN).isEmpty());
            assertTrue(pn.findAllNodesOfType(PlanNodeType.ORDERBY).isEmpty());
        }
    }

    public void testOrderByTwoAscDesc() {
        AbstractPlanNode pn = null;
        pn = compile("SELECT * from T ORDER BY T_PKEY, T_D1 DESC", 0);
        if (pn != null) {
            assertTrue(pn.findAllNodesOfType(PlanNodeType.INDEXSCAN).isEmpty());
            assertFalse(pn.findAllNodesOfType(PlanNodeType.SEQSCAN).isEmpty());
            assertFalse(pn.findAllNodesOfType(PlanNodeType.ORDERBY).isEmpty());
        }
    }

    public void testOrderByThree() {
        AbstractPlanNode pn = null;
        pn = compile("SELECT * from T ORDER BY T_PKEY, T_D1, T_D2", 0);
        if (pn != null) {
            assertTrue(pn.findAllNodesOfType(PlanNodeType.INDEXSCAN).isEmpty());
            assertFalse(pn.findAllNodesOfType(PlanNodeType.SEQSCAN).isEmpty());
            assertFalse(pn.findAllNodesOfType(PlanNodeType.ORDERBY).isEmpty());
        }
    }

    public void testNoOrderBy() {
        AbstractPlanNode pn = null;
        pn = compile("SELECT * FROM T ORDER BY T_D2", 0);
        if (pn != null) {
            assertTrue(pn.findAllNodesOfType(PlanNodeType.INDEXSCAN).isEmpty());
            assertFalse(pn.findAllNodesOfType(PlanNodeType.SEQSCAN).isEmpty());
            assertFalse(pn.findAllNodesOfType(PlanNodeType.ORDERBY).isEmpty());
        }
    }

    public void testOrderByCountStar() {
        AbstractPlanNode pn = null;
        pn = compile("SELECT T_PKEY, COUNT(*) AS FOO FROM T GROUP BY T_PKEY ORDER BY FOO", 0);
        if (pn != null) {
            assertFalse(pn.findAllNodesOfType(PlanNodeType.HASHAGGREGATE).isEmpty());
            assertFalse(pn.findAllNodesOfType(PlanNodeType.SEQSCAN).isEmpty());
            assertFalse(pn.findAllNodesOfType(PlanNodeType.ORDERBY).isEmpty());
        }
    }

    public void testEng450()
    {
        compile("select T.T_PKEY, " +
                     "sum(T.T_D1) " +
                     "from T " +
                     "group by T.T_PKEY " +
                     "order by T.T_PKEY;", 0);
    }
}
