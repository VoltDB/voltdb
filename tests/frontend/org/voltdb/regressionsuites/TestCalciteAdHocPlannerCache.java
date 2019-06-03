/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

package org.voltdb.regressionsuites;

import java.io.IOException;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestCalciteAdHocPlannerCache extends RegressionSuite {

    private static final int Calcite_CACHE_MISS2 = 1;
    private static final int Calcite_CACHE_HIT1 = 2;
    private static final int Calcite_CACHE_HIT2 = 3;

    private static final int CACHE_PARAMS_EXCEPTION = -1;
    private static final int CACHE_SKIPPED = 0;

    private long m_calcite_cache1_level = 0;
    private long m_calcite_cache2_level = 0;
    private long m_calcite_cache1_hits = 0;
    private long m_calcite_cache2_hits = 0;
    private long m_calcite_cache_misses = 0;

    private void resetStatistics() {
        m_calcite_cache1_level = 0;
        m_calcite_cache2_level = 0;
        m_calcite_cache1_hits = 0;
        m_calcite_cache2_hits = 0;
        m_calcite_cache_misses = 0;
    }

    private void checkCacheStatistics(Client client, long cache1_level, long cache2_level,
            long cache1_hits, long cache2_hits, long cache_misses)
            throws IOException, ProcCallException {
        VoltTable vt;

        boolean checked = false;

        vt = client.callProcedure("@Statistics", "PLANNER", 0).getResults()[0];

        while(vt.advanceRow()) {
            // MPI's site id is -1 by design
            Integer siteID = (Integer) vt.get("SITE_ID", VoltType.INTEGER);
            assertNotNull(siteID);
            if (siteID != -1) {
                continue;
            }
            // The global cache is identified by a site and partition ID of minus one
            assertEquals(cache1_level, vt.getLong("CalciteL1Cache_LEVEL"));
            assertEquals(cache2_level, vt.getLong("CalciteL2Cache_LEVEL"));
            assertEquals(cache1_hits,  vt.getLong("CalciteL1Cache_HITS"));
            assertEquals(cache2_hits,  vt.getLong("CalciteL2Cache_HITS"));
            assertEquals(cache_misses, vt.getLong("CalciteCache_MISSES"));

            checked = true;
            break;
        }

        assertTrue(checked);
    }

    private void checkCalcitePlannerCache(Client client, int... cacheTypes) throws IOException, ProcCallException {
        for (int cacheType : cacheTypes) {
            if (cacheType == Calcite_CACHE_MISS2) {
                ++m_calcite_cache1_level;
                ++m_calcite_cache2_level;
                ++m_calcite_cache_misses;
            } else if (cacheType == Calcite_CACHE_HIT1) {
                ++m_calcite_cache1_hits;
            } else if (cacheType == Calcite_CACHE_HIT2) {
                ++m_calcite_cache1_level;
                ++m_calcite_cache2_hits;
            } else if (cacheType == CACHE_PARAMS_EXCEPTION) {
                ++m_calcite_cache_misses;
            } else if (cacheType == CACHE_SKIPPED) {
                // Has not gone through the planner cache code
            } else {
                fail("Wrong input cache type");
            }
        }

        // check statistics
        checkCacheStatistics(client, m_calcite_cache1_level, m_calcite_cache2_level, m_calcite_cache1_hits, m_calcite_cache2_hits, m_calcite_cache_misses);
    }

    public void testCalciteCacheHitMiss() throws IOException, ProcCallException {
        System.out.println("testAdHocCalcitePlannerCache...");
        resetStatistics();
        Client client = getClient();
        String sql = "SELECT * FROM R1 WHERE ID=1";

        client.callProcedure("R1.insert", 1, "foo1", 0, 1.1);
        client.callProcedure("R1.insert", 2, "foo2", 0, 2.2);
        client.callProcedure("R1.insert", 3, "foo3", 1, 3.3);

        // Test cache miss
        client.callProcedure("@AdHoc", sql);
        checkCalcitePlannerCache(client, Calcite_CACHE_MISS2);

        // Test hit L1 cache
        client.callProcedure("@AdHoc", sql);
        checkCalcitePlannerCache(client, Calcite_CACHE_HIT1);

        // Test hit L2 cache
        client.callProcedure("@AdHoc", sql.substring(0, 26) + "2");
        checkCalcitePlannerCache(client, Calcite_CACHE_HIT2);
    }
    //
    // Suite builder boilerplate
    //

    public TestCalciteAdHocPlannerCache(String name) {
        super(name);
    }
    static final Class<?>[] PROCEDURES = {
        org.voltdb_testprocs.regressionsuites.plansgroupbyprocs.CountT1A1.class,
        org.voltdb_testprocs.regressionsuites.plansgroupbyprocs.SumGroupSingleJoin.class };

    static public junit.framework.Test suite() {
        VoltServerConfig config;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(
                TestCalciteAdHocPlannerCache.class);
        VoltProjectBuilder project = new VoltProjectBuilder();
        final String literalSchema =
                "CREATE TABLE R1 ( " +
                " ID BIGINT DEFAULT 0 NOT NULL, "
                + "DESC VARCHAR(300), "
                + "NUM bigint,"
                + "RATIO FLOAT, "
                + "PRIMARY KEY (desc)); ";
        try {
            project.addLiteralSchema(literalSchema);
        } catch (IOException e) {
            assertFalse(true);
        }

        boolean success;

        config = new LocalCluster("plansgroupby-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        return builder;
    }
}
