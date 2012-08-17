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

//import java.util.List;

import junit.framework.TestCase;

//import org.voltdb.catalog.CatalogMap;
//import org.voltdb.catalog.Cluster;
//import org.voltdb.catalog.Table;
//import org.voltdb.plannodes.AbstractPlanNode;
//import org.voltdb.plannodes.IndexScanPlanNode;

public class TestDeterminism extends TestCase {

    private PlannerTestAideDeCamp aide;

    @Override
    protected void setUp() throws Exception {
        aide = new PlannerTestAideDeCamp(TestDeterminism.class.getResource("testplans-determinism-ddl.sql"),
                                         "testdeterminism");

/* It makes little sense to force tables to be non-replicated but specify no partitioning column.
 * It breaks join planning, anyway.
        // Set all tables to non-replicated.
        Cluster cluster = aide.getCatalog().getClusters().get("cluster");
        CatalogMap<Table> tmap = cluster.getDatabases().get("database").getTables();
        for (Table t : tmap) {
            t.setIsreplicated(false);
        }
 */
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        aide.tearDown();
    }

    /**
     * Checks determinism of statement against expected results, with or without factoring in order effects
     * @param sql
     * @param order expected statement determinism, including order effects
     * @param content expected statement determinism, ignoring order effects
     */
    private void assertPlanDeterminism(String sql, boolean order, boolean content)
    {
        CompiledPlan cp = null;
        try {
            cp = aide.compileAdHocPlan(sql);
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
        assertTrue(cp != null);
        assertTrue(order == cp.isOrderDeterministic());
        assertTrue(content == cp.isContentDeterministic());
        assertTrue(cp.isContentDeterministic() || ! cp.isOrderDeterministic());
        assertTrue(cp.isOrderDeterministic() || (null != cp.nondeterminismDetail()));
    }

    private void assertPlanDeterminism(String sql, boolean order, boolean content, boolean alsoTryWithLimit) {
        assertPlanDeterminism(sql, order, content);
        if (alsoTryWithLimit) {
            String limitedStatement = sql.replaceAll(";", " LIMIT 2;");
            assertPlanDeterminism(limitedStatement, order, order);
        }

    }

    // TODO: Replace w/ true when/if indexscan is forced, and forced to order results.
    static final boolean LATER_TO_BE_true = false;

    static final boolean ALSO_TRY_LIMIT = true;
    static final boolean UNORDERED = false;
    static final boolean ORDERED = true;
    static final boolean LATER_TO_BE_ORDERED = UNORDERED;
    static final boolean INCONSISTENT = false;
    static final boolean CONSISTENT = true;

    // TODO replace references to these with opposite constants as cases get support
    public void testDeterminismOfSelectStar() {
        assertPlanDeterminism("select * from ttree;", UNORDERED, CONSISTENT, ALSO_TRY_LIMIT);
        assertPlanDeterminism("select * from tunique;", UNORDERED, CONSISTENT, ALSO_TRY_LIMIT);
        assertPlanDeterminism("select * from tpk;", UNORDERED, CONSISTENT, ALSO_TRY_LIMIT);
    }

    public void testDeterminismOfJoin() {
        assertPlanDeterminism("select X.a, X.z, Y.z from ttree X, tunique Y where X.a = Y.a;", UNORDERED, CONSISTENT, ALSO_TRY_LIMIT);
        assertPlanDeterminism("select X.a, X.z, Y.z from ttree X, tunique Y where X.a = Y.a;", UNORDERED, CONSISTENT, ALSO_TRY_LIMIT);
        assertPlanDeterminism("select X.a, X.z, Y.z from ttree X, tunique Y where X.a = Y.a;", UNORDERED, CONSISTENT, ALSO_TRY_LIMIT);
    }

    public void testDeterminismOfSelectStarOrderOne() {
        assertPlanDeterminism("select * from ttree order by a;", UNORDERED, CONSISTENT, ALSO_TRY_LIMIT); // insufficient ordering
        assertPlanDeterminism("select * from tunique order by a;", ORDERED, CONSISTENT, ALSO_TRY_LIMIT);
        assertPlanDeterminism("select * from tpk order by a;", ORDERED, CONSISTENT, ALSO_TRY_LIMIT);
        assertPlanDeterminism("select * from ttree where a > 1 order by a;", UNORDERED, CONSISTENT, ALSO_TRY_LIMIT); // insufficient ordering
        assertPlanDeterminism("select * from tunique where a > 1 order by a;", ORDERED, CONSISTENT, ALSO_TRY_LIMIT);
        assertPlanDeterminism("select * from tpk where a > 1 order by a;", ORDERED, CONSISTENT, ALSO_TRY_LIMIT);
    }

    public void testDeterminismOfSelectOrderGroupKeys() {
        assertPlanDeterminism("select z, max(a)    from ttree group by z    order by z   ;", ORDERED, CONSISTENT, ALSO_TRY_LIMIT);
        // not yet supported by planner assertPlanDeterminism("select    max(a)    from ttree group by z    order by z   ;", ORDERED, CONSISTENT, ALSO_TRY_LIMIT);

        assertPlanDeterminism("select z, max(a), b from ttree group by z, b order by z, b;", ORDERED, CONSISTENT, ALSO_TRY_LIMIT);
        // not yet supported by planner assertPlanDeterminism("select z, max(a)    from ttree group by z, b order by z, b;", ORDERED, CONSISTENT, ALSO_TRY_LIMIT);
        // not yet supported by planner assertPlanDeterminism("select    max(a)    from ttree group by z, b order by z, b;", ORDERED, CONSISTENT, ALSO_TRY_LIMIT);

        assertPlanDeterminism("select z, max(a), b from ttree group by z, b order by z   ;", UNORDERED, CONSISTENT, ALSO_TRY_LIMIT);
        // not yet supported by planner assertPlanDeterminism("select z, max(a)    from ttree group by z, b order by z   ;", UNORDERED, CONSISTENT, ALSO_TRY_LIMIT);
        // not yet supported by planner assertPlanDeterminism("select    max(a)    from ttree group by z, b order by z   ;", UNORDERED, CONSISTENT, ALSO_TRY_LIMIT);

        // Odd edge cases of needlessly long ORDER BY clause
        // not yet supported by planner assertPlanDeterminism("select z, max(a), max(b) from ttree group by z order by z, max(b);", ORDERED, CONSISTENT, ALSO_TRY_LIMIT);
        assertPlanDeterminism("select z, max(a), max(b) from ttree group by z order by z, 3;", ORDERED, CONSISTENT, ALSO_TRY_LIMIT);
        // not yet supported by planner assertPlanDeterminism("select    max(a), max(b) from ttree group by z order by z, max(b);", ORDERED, CONSISTENT, ALSO_TRY_LIMIT);
        // not yet supported by planner assertPlanDeterminism("select z, max(a)         from ttree group by z order by z, max(b);", ORDERED, CONSISTENT, ALSO_TRY_LIMIT);
        // not yet supported by planner assertPlanDeterminism("select    max(a)         from ttree group by z order by z, max(b);", ORDERED, CONSISTENT, ALSO_TRY_LIMIT);

        // not yet supported by planner assertPlanDeterminism("select z, max(a), max(b) from ttree group by z order by max(b), z;", ORDERED, CONSISTENT, ALSO_TRY_LIMIT);
        // not yet supported by planner assertPlanDeterminism("select    max(a)         from ttree group by z order by max(b), z;", ORDERED, CONSISTENT, ALSO_TRY_LIMIT);
        // not yet supported by planner assertPlanDeterminism("select z, max(a), max(b) from ttree group by z order by max(b), z;", ORDERED, CONSISTENT, ALSO_TRY_LIMIT);
        // not yet supported by planner assertPlanDeterminism("select    max(a)         from ttree group by z order by max(b), z;", ORDERED, CONSISTENT, ALSO_TRY_LIMIT);
    }

    public void testDeterminismOfSelectOrderAll() {
        assertPlanDeterminism("select z from ttree order by z;", ORDERED, CONSISTENT, ALSO_TRY_LIMIT);
        assertPlanDeterminism("select a, b, z from ttree order by 1, 2, 3;", ORDERED, CONSISTENT, ALSO_TRY_LIMIT);
        assertPlanDeterminism("select a, b, z from ttree order by b, a, z;", ORDERED, CONSISTENT, ALSO_TRY_LIMIT);
        assertPlanDeterminism("select a, b, z from ttree order by 3, 2, 1;", ORDERED, CONSISTENT, ALSO_TRY_LIMIT);
    }

    public void latertestDeterminismOfJoinOrderAll() {
        assertPlanDeterminism("select X.a, X.z, Y.z from ttree X, tunique Y where X.a = Y.a order by 1, 2, 3;", ORDERED, CONSISTENT, ALSO_TRY_LIMIT);
        assertPlanDeterminism("select X.a, X.z, Y.z from ttree X, tunique Y where X.a = Y.a order by Y.z, X.a, X.z;", ORDERED, CONSISTENT, ALSO_TRY_LIMIT);
        assertPlanDeterminism("select X.a, X.z, Y.z from ttree X, tunique Y where X.a = Y.a order by 3, 2, 1;", ORDERED, CONSISTENT, ALSO_TRY_LIMIT);
    }

    public void testDeterminismOfSelectIndexKeysOnly() {
        assertPlanDeterminism("select a, b from ttree;", LATER_TO_BE_ORDERED, CONSISTENT, ALSO_TRY_LIMIT);
        assertPlanDeterminism("select a, b, c from ttree;", LATER_TO_BE_ORDERED, CONSISTENT, ALSO_TRY_LIMIT);
        // non-prefix keys don't help
        assertPlanDeterminism("select b, c from ttree;", UNORDERED, CONSISTENT, ALSO_TRY_LIMIT);
        assertPlanDeterminism("select a from tunique;", LATER_TO_BE_ORDERED, CONSISTENT, ALSO_TRY_LIMIT);
        assertPlanDeterminism("select a from tpk;", LATER_TO_BE_ORDERED, CONSISTENT, ALSO_TRY_LIMIT);
        // hashes don't help, here
        assertPlanDeterminism("select a, b from thash;", UNORDERED, CONSISTENT, ALSO_TRY_LIMIT);
        assertPlanDeterminism("select a, b, c from thash;", UNORDERED, CONSISTENT, ALSO_TRY_LIMIT);
    }

    public void testDeterminismOfSelectOneKeyValue() {
        assertPlanDeterminism("select a, z from ttree where a = 1;", UNORDERED, CONSISTENT, ALSO_TRY_LIMIT);
        assertPlanDeterminism("select a, z from ttree where a = 1 and b < 10;", UNORDERED, CONSISTENT, ALSO_TRY_LIMIT);
        assertPlanDeterminism("select a, z from tunique where a = 1;", ORDERED, CONSISTENT, ALSO_TRY_LIMIT);
        assertPlanDeterminism("select a, z from tunique where a = 1 and b < 10;", ORDERED, CONSISTENT, ALSO_TRY_LIMIT);
        assertPlanDeterminism("select a, z from tpk where a = 1;", ORDERED, CONSISTENT, ALSO_TRY_LIMIT);
        assertPlanDeterminism("select a, z from tpk where a = 1 and b < 10;", ORDERED, CONSISTENT, ALSO_TRY_LIMIT);
    }

    public void testDeterminismOfWrites() {
        // "LIMIT" not currently supported for DML.
        assertPlanDeterminism("insert into ttree values (1,2,3,4);", ORDERED, CONSISTENT);
        assertPlanDeterminism("insert into tunique values (1,2,3,4);", ORDERED, CONSISTENT);
        assertPlanDeterminism("insert into tpk values (1,2,3,4);", ORDERED, CONSISTENT);
        assertPlanDeterminism("delete from ttree;", ORDERED, CONSISTENT);
        assertPlanDeterminism("delete from tunique;", ORDERED, CONSISTENT);
        assertPlanDeterminism("delete from tpk;", ORDERED, CONSISTENT);
        assertPlanDeterminism("update ttree set z = 5 where a < 2;", ORDERED, CONSISTENT);
        assertPlanDeterminism("update tunique set z = 5 where a < 2;", ORDERED, CONSISTENT);
        assertPlanDeterminism("update tpk set z = 5 where a < 2;", ORDERED, CONSISTENT);
    }
}
