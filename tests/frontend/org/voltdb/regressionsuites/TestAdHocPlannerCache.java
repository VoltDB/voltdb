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

package org.voltdb.regressionsuites;

import java.io.IOException;
import java.math.BigDecimal;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.sysprocs.AdHocNTBase;

public class TestAdHocPlannerCache extends RegressionSuite {
    // 1 means cache level 1, the literal sql string cache
    // 2 means cache level 2, the parameterized sql cache (auto parameters and user parameters)
    private static final int CACHE_MISS1 = 0;
    private static final int CACHE_MISS2 = 1;
    private static final int CACHE_MISS2_ADD1 = 2;
    private static final int CACHE_HIT1 = 3;
    private static final int CACHE_HIT2 = 4;
    private static final int CACHE_HIT2_ADD1 = 5;

    private static final int CACHE_PARAMS_EXCEPTION = 6;
    private static final int CACHE_SKIPPED = -1;

    private long m_cache1_level = 0;
    private long m_cache2_level = 0;
    private long m_cache1_hits = 0;
    private long m_cache2_hits = 0;
    private long m_cache_misses = 0;

    private static String pattern = "Incorrect number of parameters passed: expected %d, passed %d";

    private void resetStatistics() {
        m_cache1_level = 0;
        m_cache2_level = 0;
        m_cache1_hits = 0;
        m_cache2_hits = 0;
        m_cache_misses = 0;
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
            assertEquals(cache1_level, vt.getLong("CACHE1_LEVEL"));
            assertEquals(cache2_level, vt.getLong("CACHE2_LEVEL"));
            assertEquals(cache1_hits,  vt.getLong("CACHE1_HITS"));
            assertEquals(cache2_hits,  vt.getLong("CACHE2_HITS"));
            assertEquals(cache_misses, vt.getLong("CACHE_MISSES"));

            checked = true;
            break;
        }

        assertTrue(checked);
    }

    private void checkPlannerCache(Client client, int... cacheTypes) throws IOException, ProcCallException {
        for (int cacheType : cacheTypes) {
            if (cacheType == CACHE_MISS1) {
                ++m_cache1_level;
                ++m_cache_misses;
            } else if (cacheType == CACHE_MISS2) {
                ++m_cache2_level;
                ++m_cache_misses;
            } else if (cacheType == CACHE_MISS2_ADD1) {
                ++m_cache1_level;
                ++m_cache2_level;
                ++m_cache_misses;
            } else if (cacheType == CACHE_HIT1) {
                ++m_cache1_hits;
            } else if (cacheType == CACHE_HIT2) {
                ++m_cache2_hits;
            } else if (cacheType == CACHE_HIT2_ADD1) {
                ++m_cache1_level;
                ++m_cache2_hits;
            } else if (cacheType == CACHE_PARAMS_EXCEPTION) {
                ++m_cache_misses;
            } else if (cacheType == CACHE_SKIPPED) {
                // Has not gone through the planner cache code
            } else {
                fail("Wrong input cache type");
            }
        }

        // check statistics
        checkCacheStatistics(client, m_cache1_level, m_cache2_level, m_cache1_hits, m_cache2_hits, m_cache_misses);
    }

    // ENG-8424: fix for the L1 cache statistics on execution site, other than the MPI
    private void subtestENG8424(Client client) throws IOException, ProcCallException {
        System.out.println("subtestENG8424...");
        VoltTable vt;
        long l1Before = ExecutionEngine.EE_PLAN_CACHE_SIZE + 1;
        long l1After = ExecutionEngine.EE_PLAN_CACHE_SIZE + 1;

        vt = client.callProcedure("@Statistics", "PLANNER", 0).getResults()[0];
        assertTrue(vt.getRowCount() > 0);
        while(vt.advanceRow()) {
            // MPI's site id is -1 by design
            Integer siteID = (Integer) vt.get("SITE_ID", VoltType.INTEGER);
            assertNotNull(siteID);
            if (siteID == -1) {
                continue;
            }

            l1Before = vt.getLong("CACHE1_LEVEL");
            break;
        }
        assertTrue(l1Before <= ExecutionEngine.EE_PLAN_CACHE_SIZE);

        client.callProcedure("@AdHoc", "select * from R1 as ENG8424;");
        ++m_cache1_level;
        ++m_cache_misses;

        vt = client.callProcedure("@Statistics", "PLANNER", 0).getResults()[0];
        assertTrue(vt.getRowCount() > 0);
        while(vt.advanceRow()) {
            // MPI's site id is -1 by design
            Integer siteID = (Integer) vt.get("SITE_ID", VoltType.INTEGER);
            assertNotNull(siteID);
            if (siteID == -1) {
                continue;
            }

            l1After = vt.getLong("CACHE1_LEVEL");
            break;
        }
        assertTrue(l1After <= ExecutionEngine.EE_PLAN_CACHE_SIZE);

        // Assuming the max size of cache has not been reached
        assertEquals(l1Before + 1, l1After);
    }

    public void testAdHocPlannerCache() throws IOException, ProcCallException {
         System.out.println("testAdHocPlannerCache...");
         // useful when we have multiple unit tests in this suites
         resetStatistics();

         Client client = getClient();
         client.callProcedure("R1.insert", 1, "foo1", 0, 1.1);
         client.callProcedure("R1.insert", 2, "foo2", 0, 2.2);
         client.callProcedure("R1.insert", 3, "foo3", 1, 3.3);

         subtest1AdHocPlannerCache(client);

         subtest2AdHocParameters(client);

         subtest3AdHocParameterTypes(client);

         subtest4AdvancedParameterTypes(client);

         subtest5ExplainPlans(client);

         subtest6ExpressionIndex(client);

         subtestENG8424(client);
    }

    public void subtest1AdHocPlannerCache(Client client) throws IOException, ProcCallException {
        VoltTable vt;
        String sql;

        //
        // No constants AdHoc queries
        //
        sql = "SELECT ID FROM R1 sub1 order by ID;";
        validateTableOfScalarLongs(client, sql, new long[]{1, 2, 3});
        checkPlannerCache(client, CACHE_MISS2_ADD1);

        validateTableOfScalarLongs(client, sql, new long[]{1, 2, 3});
        checkPlannerCache(client, CACHE_HIT1);

        validateTableOfScalarLongs(client, sql, new long[]{1, 2, 3});
        checkPlannerCache(client, CACHE_HIT1);

        // rename table alias
        sql = "SELECT ID FROM R1 sub1_C order by ID;";
        validateTableOfScalarLongs(client, sql, new long[]{1, 2, 3});
        checkPlannerCache(client, CACHE_MISS2_ADD1);


        //
        // Contain constants AdHoc Queries
        //
        sql = "SELECT ID FROM R1 sub1 WHERE ID > 1 order by ID;";
        validateTableOfScalarLongs(client, sql, new long[]{2, 3});
        checkPlannerCache(client, CACHE_MISS2_ADD1);

        validateTableOfScalarLongs(client, sql, new long[]{2, 3});
        checkPlannerCache(client, CACHE_HIT1);

        sql = "SELECT ID FROM R1 sub1 WHERE ID > 2 order by ID;";
        validateTableOfScalarLongs(client, sql, new long[]{3});
        checkPlannerCache(client, CACHE_HIT2_ADD1);


        //
        // User question mark AdHoc queries
        //
        sql = "SELECT ID FROM R1 sub1 WHERE ID > ? ORDER BY ID;";
        vt = client.callProcedure("@AdHoc", sql, 1).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{2, 3});
        checkPlannerCache(client, CACHE_MISS2);

        vt = client.callProcedure("@AdHoc", sql, 2).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{3});
        checkPlannerCache(client, CACHE_HIT2);

        vt = client.callProcedure("@AdHoc", sql, 3).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{});
        checkPlannerCache(client, CACHE_HIT2);

        //
        // User question mark AdHoc queries and constants
        //
        sql = "SELECT ID FROM R1 sub1 WHERE num = 0 and ID > ? order by ID;";
        vt = client.callProcedure("@AdHoc", sql, 0).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{1, 2});
        checkPlannerCache(client, CACHE_MISS2);

        vt = client.callProcedure("@AdHoc", sql, 1).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{2});
        checkPlannerCache(client, CACHE_HIT2);

        vt = client.callProcedure("@AdHoc", sql, 3).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{});
        checkPlannerCache(client, CACHE_HIT2);

        // adjust the constant
        sql = "SELECT ID FROM R1 sub1 WHERE num = 1 and ID > ? order by ID;";
        vt = client.callProcedure("@AdHoc", sql, 0).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{3});
        checkPlannerCache(client, CACHE_MISS2);

        vt = client.callProcedure("@AdHoc", sql, 3).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{});
        checkPlannerCache(client, CACHE_HIT2);

        // replace constants with parameter
        sql = "SELECT ID FROM R1 sub1 WHERE num = ? and ID > ? order by ID;";
        vt = client.callProcedure("@AdHoc", sql, 0, 0).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{1, 2});
        checkPlannerCache(client, CACHE_MISS2);

        vt = client.callProcedure("@AdHoc", sql, 1, 2).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{3});
        checkPlannerCache(client, CACHE_HIT2);
    }

    public void subtest2AdHocParameters(Client client) throws IOException, ProcCallException {
        System.out.println("subtest2AdHocParameters...");
        String sql;
        String errorMsg = AdHocNTBase.AdHocErrorResponseMessage;

        //
        // Multiple AdHoc queries with question marks per procedure call
        //
        sql = "SELECT ID FROM R1 sub2 WHERE num = 0 and ID > ? order by ID;";
        try {
            client.callProcedure("@AdHoc", sql + sql, 0, 0);
            fail();
        } catch(Exception ex) {
            assertTrue(ex.getMessage().contains(errorMsg));
        }
        checkPlannerCache(client, CACHE_SKIPPED, CACHE_SKIPPED);

        // fewer parameters
        try {
            client.callProcedure("@AdHoc", sql + sql, 0);
            fail();
        } catch(Exception ex) {
            assertTrue(ex.getMessage().contains(errorMsg));
        }
        checkPlannerCache(client, CACHE_SKIPPED, CACHE_SKIPPED);

        try {
            client.callProcedure("@AdHoc", "select * from r1 sub2;" + sql, 0, 0);
            fail();
        } catch(Exception ex) {
            assertTrue(ex.getMessage().contains(errorMsg));
        }
        checkPlannerCache(client, CACHE_SKIPPED, CACHE_SKIPPED);


        // by pass the pre-planner check
        verifyAdHocFails(client, String.format(pattern, 1, 0), sql+sql);
        checkPlannerCache(client, CACHE_SKIPPED);

        // positive tests
        // multiple statements are not partition inferred, sent to every partitions.
        // not cacheable currently.
        sql = "SELECT ID FROM R1 sub2 order by ID;";
        VoltTable[] vts = client.callProcedure("@AdHoc", sql + sql).getResults();
        validateTableOfScalarLongs(vts[0], new long[]{1, 2, 3});
        validateTableOfScalarLongs(vts[1], new long[]{1, 2, 3});
        checkPlannerCache(client, CACHE_SKIPPED, CACHE_SKIPPED);

        //
        // Pass in incorrect number of parameters
        //
        verifyAdHocFails(client, String.format(pattern, 0, 1), sql, 1);
        checkPlannerCache(client, CACHE_PARAMS_EXCEPTION);

        sql = "SELECT ID FROM R1 sub2 WHERE num = 0 and ID > ? order by ID;";
        verifyAdHocFails(client, String.format(pattern, 1, 2), sql, 1, 500);
        checkPlannerCache(client, CACHE_PARAMS_EXCEPTION);

        verifyAdHocFails(client, String.format(pattern, 1, 0), sql);
        checkPlannerCache(client, CACHE_PARAMS_EXCEPTION);

        VoltTable vt;

        // rename table with "TB" to run it as a new query to the system
        sql = "SELECT ID FROM R1 sub2_TB WHERE ID > ? order by ID;";
        vt = client.callProcedure("@AdHoc", sql, 1).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{2, 3});
        checkPlannerCache(client, CACHE_MISS2);


        verifyAdHocFails(client, String.format(pattern, 1, 2), sql, 1, 500);
        checkPlannerCache(client, CACHE_PARAMS_EXCEPTION);

        verifyAdHocFails(client, String.format(pattern, 1, 0), sql);
        checkPlannerCache(client, CACHE_PARAMS_EXCEPTION);

        // no parameters passed in for multiple adhoc queries
        verifyAdHocFails(client, String.format(pattern, 1, 0), sql + sql);
        checkPlannerCache(client, CACHE_SKIPPED);
    }

    public void subtest3AdHocParameterTypes(Client client) throws IOException, ProcCallException {
        System.out.println("subtest3AdHocParameterTypes...");

        String sql;
        VoltTable vt;

        // decimal constants
        sql = "SELECT ID FROM R1 sub3 WHERE ID > 1.8 order by ID;";
        validateTableOfScalarLongs(client, sql, new long[]{2, 3});
        checkPlannerCache(client, CACHE_MISS2_ADD1);

        validateTableOfScalarLongs(client, sql, new long[]{2, 3});
        checkPlannerCache(client, CACHE_HIT1);

        sql = "SELECT ID FROM R1 sub3 WHERE ID > 1.9 order by ID;";
        validateTableOfScalarLongs(client, sql, new long[]{2, 3});
        checkPlannerCache(client, CACHE_HIT2_ADD1);

        // same query but use Integer constants
        // Then it's a completely new sql pattern to the cache,
        // as the cache is parameter type sensitive now.
        sql = "SELECT ID FROM R1 sub3 WHERE ID > 1 order by ID;";
        validateTableOfScalarLongs(client, sql, new long[]{2, 3});
        checkPlannerCache(client, CACHE_MISS2_ADD1);

        validateTableOfScalarLongs(client, sql, new long[]{2, 3});
        checkPlannerCache(client, CACHE_HIT1);

        sql = "SELECT ID FROM R1 sub3 WHERE ID > 2 order by ID;";
        validateTableOfScalarLongs(client, sql, new long[]{3});
        checkPlannerCache(client, CACHE_HIT2_ADD1);

        //
        // query with user parameters
        //
        sql = "SELECT ID FROM R1 sub3 WHERE ID > ? order by ID;";
        String errorMsg = "java.lang.Double is not a match or is out of range for the target parameter type: long";
        // TODO: the message should say: decimal not able to be converted to integer ?
        verifyAdHocFails(client, errorMsg, sql, 1.8);
        checkPlannerCache(client, CACHE_MISS2);

        vt = client.callProcedure("@AdHoc", sql, 2).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{3});
        checkPlannerCache(client, CACHE_HIT2);

        // user parameter with CAST operation
        sql = "SELECT ID FROM R1 sub3 WHERE ID > cast(? as float) order by ID;";
        vt = client.callProcedure("@AdHoc", sql, 1.8).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{2,3});
        checkPlannerCache(client, CACHE_MISS2);

        vt = client.callProcedure("@AdHoc", sql, 2).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{3});
        checkPlannerCache(client, CACHE_HIT2);

        vt = client.callProcedure("@AdHoc", sql, 1.5).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{2,3});
        checkPlannerCache(client, CACHE_HIT2);

        //
        // change the where clause to get the new query pattern
        //

        // try the normal integer value first
        sql = "SELECT ID FROM R1 sub3 WHERE NUM > 0 order by ID;";
        validateTableOfScalarLongs(client, sql, new long[]{3});
        checkPlannerCache(client, CACHE_MISS2_ADD1);

        // try the decimal value second, it has bad parameterization
        sql = "SELECT ID FROM R1 sub3 WHERE NUM > 0.8 order by ID;";
        validateTableOfScalarLongs(client, sql, new long[]{3});
        checkPlannerCache(client, CACHE_MISS2_ADD1);

        sql = "SELECT ID FROM R1 sub3 WHERE NUM > 0.9 order by ID;";
        validateTableOfScalarLongs(client, sql, new long[]{3});
        checkPlannerCache(client, CACHE_HIT2_ADD1);


        //
        // test the AVG function
        //
        sql = "SELECT AVG(ID) + 0.1 FROM R1 sub3;";
        validateTableColumnOfScalarDecimal(client, sql, new BigDecimal[]{new BigDecimal(2.1)});
        checkPlannerCache(client, CACHE_MISS2_ADD1);

        validateTableColumnOfScalarDecimal(client, sql, new BigDecimal[]{new BigDecimal(2.1)});
        checkPlannerCache(client, CACHE_HIT1);

        sql = "SELECT AVG(ID) + 0.2 FROM R1 sub3;";
        validateTableColumnOfScalarDecimal(client, sql, new BigDecimal[]{new BigDecimal(2.2)});
        checkPlannerCache(client, CACHE_HIT2_ADD1);

        // integer constants is a new SQL pattern to the planner cache
        sql = "SELECT AVG(ID) + 2 FROM R1 sub3;";
        validateTableOfScalarLongs(client, sql, new long[]{4});
        checkPlannerCache(client, CACHE_MISS2_ADD1);

        validateTableOfScalarLongs(client, sql, new long[]{4});
        checkPlannerCache(client, CACHE_HIT1);

        sql = "SELECT AVG(ID) + 3 FROM R1 sub3;";
        validateTableOfScalarLongs(client, sql, new long[]{5});
        checkPlannerCache(client, CACHE_HIT2_ADD1);

        // float constants is a new SQL pattern to the planner cache
        sql = "SELECT AVG(ID) + 1.0e-1 FROM R1 sub3;";
        validateTableColumnOfScalarFloat(client, sql, new double[]{2.1});
        checkPlannerCache(client, CACHE_MISS2_ADD1);

        validateTableColumnOfScalarFloat(client, sql, new double[]{2.1});
        checkPlannerCache(client, CACHE_HIT1);

        sql = "SELECT AVG(ID) + 2.0e-1 FROM R1 sub3;";
        validateTableColumnOfScalarFloat(client, sql, new double[]{2.2});
        checkPlannerCache(client, CACHE_HIT2_ADD1);

        //
        // change the table alias to get a completely new base line
        //
        sql = "SELECT AVG(ID) + 2 FROM R1 sub3_1;";
        validateTableOfScalarLongs(client, sql, new long[]{4});
        checkPlannerCache(client, CACHE_MISS2_ADD1);

        validateTableOfScalarLongs(client, sql, new long[]{4});
        checkPlannerCache(client, CACHE_HIT1);

        sql = "SELECT AVG(ID) + 3 FROM R1 sub3_1;";
        validateTableOfScalarLongs(client, sql, new long[]{5});
        checkPlannerCache(client, CACHE_HIT2_ADD1);

        // decimal constants is a new SQL pattern to the planner cache
        sql = "SELECT AVG(ID) + 0.1 FROM R1 sub3_1;";
        validateTableColumnOfScalarDecimal(client, sql, new BigDecimal[]{new BigDecimal(2.1)});
        checkPlannerCache(client, CACHE_MISS2_ADD1);


        // change the table name for new baseline
        //
        // float constants
        //
        sql = "SELECT ID FROM R1 sub3_2 WHERE ID > 0.18E1 order by ID;";
        validateTableOfScalarLongs(client, sql, new long[]{2, 3});
        checkPlannerCache(client, CACHE_MISS2_ADD1);

        validateTableOfScalarLongs(client, sql, new long[]{2, 3});
        checkPlannerCache(client, CACHE_HIT1);

        sql = "SELECT ID FROM R1 sub3_2 WHERE ID > 0.19E1 order by ID;";
        validateTableOfScalarLongs(client, sql, new long[]{2, 3});
        checkPlannerCache(client, CACHE_HIT2_ADD1);

        // same query but use Integer constants
        sql = "SELECT ID FROM R1 sub3_2 WHERE ID > 1 order by ID;";
        validateTableOfScalarLongs(client, sql, new long[]{2, 3});
        checkPlannerCache(client, CACHE_MISS2_ADD1);

        validateTableOfScalarLongs(client, sql, new long[]{2, 3});
        checkPlannerCache(client, CACHE_HIT1);

        sql = "SELECT ID FROM R1 sub3_2 WHERE ID > 2 order by ID;";
        validateTableOfScalarLongs(client, sql, new long[]{3});
        checkPlannerCache(client, CACHE_HIT2_ADD1);

        // same query but use Decimal constants
        sql = "SELECT ID FROM R1 sub3_2 WHERE ID > 1.8 order by ID;";
        validateTableOfScalarLongs(client, sql, new long[]{2, 3});
        checkPlannerCache(client, CACHE_MISS2_ADD1);

        validateTableOfScalarLongs(client, sql, new long[]{2, 3});
        checkPlannerCache(client, CACHE_HIT1);

        sql = "SELECT ID FROM R1 sub3_2 WHERE ID > 1.9 order by ID;";
        validateTableOfScalarLongs(client, sql, new long[]{2, 3});
        checkPlannerCache(client, CACHE_HIT2_ADD1);
    }

    public void subtest4AdvancedParameterTypes(Client client) throws IOException, ProcCallException {
        System.out.println("subtest4AdvancedParameterTypes...");

        String sql;
        VoltTable vt;

        // UNION
        // parameters in both
        sql = "SELECT ID FROM R1 sub4_B WHERE ID > ? UNION SELECT ID FROM R1 sub4_C WHERE ID > ?;";
        vt = client.callProcedure("@AdHoc", sql, 0, 0).getResults()[0];
        assertEquals(3, vt.getRowCount());
        checkPlannerCache(client, CACHE_MISS2);

        verifyAdHocFails(client, String.format(pattern, 2, 1), sql, 0);
        checkPlannerCache(client, CACHE_PARAMS_EXCEPTION);

        vt = client.callProcedure("@AdHoc", sql, 1, 2).getResults()[0];
        assertEquals(2, vt.getRowCount());
        checkPlannerCache(client, CACHE_HIT2);

        verifyAdHocFails(client, String.format(pattern, 2, 3), sql, 0, 1, 2);
        checkPlannerCache(client, CACHE_PARAMS_EXCEPTION);

        // parameters on right
        sql = "SELECT ID FROM R1 sub4_B WHERE NUM = 0 UNION SELECT ID FROM R1 sub4_C WHERE ID < ?;";
        verifyAdHocFails(client, String.format(pattern, 1, 0), sql);
        checkPlannerCache(client, CACHE_PARAMS_EXCEPTION);

        verifyAdHocFails(client, String.format(pattern, 1, 2), sql, 0, 0);
        checkPlannerCache(client, CACHE_PARAMS_EXCEPTION);

        vt = client.callProcedure("@AdHoc", sql, 2).getResults()[0];
        assertEquals(2, vt.getRowCount());
        checkPlannerCache(client, CACHE_MISS2);

        vt = client.callProcedure("@AdHoc", sql, 3).getResults()[0];
        assertEquals(2, vt.getRowCount());
        checkPlannerCache(client, CACHE_HIT2);

        // parameters on left
        sql = "SELECT ID FROM R1 sub4_B WHERE NUM > ? UNION SELECT ID FROM R1 sub4_C WHERE ID > 1;";
        vt = client.callProcedure("@AdHoc", sql, 0).getResults()[0];
        assertEquals(2, vt.getRowCount());
        checkPlannerCache(client, CACHE_MISS2);

        vt = client.callProcedure("@AdHoc", sql, -1).getResults()[0];
        assertEquals(3, vt.getRowCount());
        checkPlannerCache(client, CACHE_HIT2);

        verifyAdHocFails(client, String.format(pattern, 1, 2), sql, 0, 0);
        checkPlannerCache(client, CACHE_PARAMS_EXCEPTION);

        verifyAdHocFails(client, String.format(pattern, 1, 0), sql);
        checkPlannerCache(client, CACHE_PARAMS_EXCEPTION);

        //
        // Subquery should not make a difference here, but add some tests to make sure at least
        //

        // IN subquery
        sql = "SELECT ID FROM R1 sub4 WHERE ID > ? and ID IN (SELECT ID FROM R1 where id > ?) order by ID;";
        vt = client.callProcedure("@AdHoc", sql, 1, 2).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{3});
        checkPlannerCache(client, CACHE_MISS2);

        vt = client.callProcedure("@AdHoc", sql, 0, 1).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{2, 3});
        checkPlannerCache(client, CACHE_HIT2);

        verifyAdHocFails(client, String.format(pattern, 2, 1), sql, 0);
        checkPlannerCache(client, CACHE_PARAMS_EXCEPTION);

        // scalar subquery
        sql = "SELECT (select max(r1.id) from r1 where r1.id > sub4.ID and num >= ?) AS maxID FROM R1 sub4 "
                + " WHERE ID > ? order by id;";
        vt = client.callProcedure("@AdHoc", sql, 1, 1).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{3, Long.MIN_VALUE});
        checkPlannerCache(client, CACHE_MISS2);

        vt = client.callProcedure("@AdHoc", sql, 0, 0).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{3, 3, Long.MIN_VALUE});
        checkPlannerCache(client, CACHE_HIT2);

        verifyAdHocFails(client, String.format(pattern, 2, 1), sql, 0);
        checkPlannerCache(client, CACHE_PARAMS_EXCEPTION);

        verifyAdHocFails(client, String.format(pattern, 2, 0), sql);
        checkPlannerCache(client, CACHE_PARAMS_EXCEPTION);


        //
        // ENG-8238: AVG with decimal operation truncated to integer
        // FIXED in V5.4.
        //

        // start with decimal first
        sql = "select ID, (select AVG(ID) + 0.1 from R1) from R1 sub4;";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableColumnOfScalarDecimal(vt, 1,
                new BigDecimal[]{new BigDecimal(2.1), new BigDecimal(2.1), new BigDecimal(2.1)});
        checkPlannerCache(client, CACHE_MISS2_ADD1);

        sql = "select ID, (select AVG(ID) + 1 from R1) from R1 sub4;";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableColumnOfScalarLong(vt, 1, new long[]{3, 3, 3});
        checkPlannerCache(client, CACHE_MISS2_ADD1);

        sql = "select ID, (select AVG(ID) + 2 from R1) from R1 sub4;";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableColumnOfScalarLong(vt, 1, new long[]{4, 4, 4});
        checkPlannerCache(client, CACHE_HIT2_ADD1);

        sql = "select ID, (select AVG(ID) + 0.2 from R1) from R1 sub4;";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableColumnOfScalarDecimal(vt, 1,
                new BigDecimal[]{new BigDecimal(2.2), new BigDecimal(2.2), new BigDecimal(2.2)});
        checkPlannerCache(client, CACHE_HIT2_ADD1);

        sql = "select ID, (select AVG(ID) + 2.0E-1 from R1) from R1 sub4;";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableColumnOfScalarFloat(vt, 1, new double[]{2.2, 2.2, 2.2});
        checkPlannerCache(client, CACHE_MISS2_ADD1);

        sql = "select ID, (select AVG(ID) + 3.0E-1 from R1) from R1 sub4;";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableColumnOfScalarFloat(vt, 1, new double[]{2.3, 2.3, 2.3});
        checkPlannerCache(client, CACHE_HIT2_ADD1);

        // new SQL pattern with new alias

        // start with integer first
        sql = "select ID, (select AVG(ID) + 1 from R1) from R1 sub4_1;";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableColumnOfScalarLong(vt, 1, new long[]{3, 3, 3});
        checkPlannerCache(client, CACHE_MISS2_ADD1);

        sql = "select ID, (select AVG(ID) + 0.1 from R1) from R1 sub4_1;";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableColumnOfScalarDecimal(vt, 1,
                new BigDecimal[]{new BigDecimal(2.1), new BigDecimal(2.1), new BigDecimal(2.1)});
        checkPlannerCache(client, CACHE_MISS2_ADD1);

        sql = "select ID, (select AVG(ID) + 2 from R1) from R1 sub4_1;";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableColumnOfScalarLong(vt, 1, new long[]{4, 4, 4});
        checkPlannerCache(client, CACHE_HIT2_ADD1);

        sql = "select ID, (select AVG(ID) + 0.2 from R1) from R1 sub4_1;";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableColumnOfScalarDecimal(vt, 1,
                new BigDecimal[]{new BigDecimal(2.2), new BigDecimal(2.2), new BigDecimal(2.2)});
        checkPlannerCache(client, CACHE_HIT2_ADD1);


        // new SQL pattern with new alias

        // start with float first
        sql = "select ID, (select AVG(ID) + 1.0E-1 from R1) from R1 sub4_2;";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableColumnOfScalarFloat(vt, 1, new double[]{2.1, 2.1, 2.1});
        checkPlannerCache(client, CACHE_MISS2_ADD1);

        sql = "select ID, (select AVG(ID) + 1 from R1) from R1 sub4_2;";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableColumnOfScalarLong(vt, 1, new long[]{3, 3, 3});
        checkPlannerCache(client, CACHE_MISS2_ADD1);

        sql = "select ID, (select AVG(ID) + 2 from R1) from R1 sub4_2;";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableColumnOfScalarLong(vt, 1, new long[]{4, 4, 4});
        checkPlannerCache(client, CACHE_HIT2_ADD1);

        sql = "select ID, (select AVG(ID) + 3.0E-1 from R1) from R1 sub4_2;";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableColumnOfScalarFloat(vt, 1, new double[]{2.3, 2.3, 2.3});
        checkPlannerCache(client, CACHE_HIT2_ADD1);

        sql = "select ID, (select AVG(ID) + 0.2 from R1) from R1 sub4_2;";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableColumnOfScalarDecimal(vt, 1,
                new BigDecimal[]{new BigDecimal(2.2), new BigDecimal(2.2), new BigDecimal(2.2)});
        checkPlannerCache(client, CACHE_MISS2_ADD1);
    }

    public void subtest5ExplainPlans(Client client) throws IOException, ProcCallException {
        System.out.println("subtest5ExplainPlans...");

        String sql;
        VoltTable vt;

        //
        // AdHoc queries
        //
        sql = "SELECT ID FROM R1 sub5 WHERE ID > ? ORDER BY ID;";
        // incorrect parameter query, not cacheable
        vt = client.callProcedure("@Explain", sql).getResults()[0];
        checkPlannerCache(client, CACHE_PARAMS_EXCEPTION);

        vt = client.callProcedure("@AdHoc", sql, 0).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{1, 2, 3});
        checkPlannerCache(client, CACHE_MISS2);

        vt = client.callProcedure("@AdHoc", sql, 1).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{2, 3});
        checkPlannerCache(client, CACHE_HIT2);


        // rename table table to get a new pattern
        sql = "SELECT ID FROM R1 sub5_C WHERE ID > ? ORDER BY ID;";
        vt = client.callProcedure("@Explain", sql, -1).getResults()[0];
        checkPlannerCache(client, CACHE_MISS2);

        vt = client.callProcedure("@AdHoc", sql, 0).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{1, 2, 3});
        checkPlannerCache(client, CACHE_HIT2);

        // wrong number of parameters passed in
        verifyAdHocFails(client, String.format(pattern, 1, 0), sql);
        checkPlannerCache(client, CACHE_PARAMS_EXCEPTION);


        //
        // Procedure explain
        //
        // Procedure does not go through AdHoc queries code
        vt = client.callProcedure("@ExplainProc", "proc1").getResults()[0];
        checkPlannerCache(client, CACHE_SKIPPED);

        vt = client.callProcedure("proc1", 1).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{0, 1});
        checkPlannerCache(client, CACHE_SKIPPED);

        vt = client.callProcedure("@ExplainProc", "proc1").getResults()[0];
        checkPlannerCache(client, CACHE_SKIPPED);

        vt = client.callProcedure("@ExplainProc", "proc1").getResults()[0];
        checkPlannerCache(client, CACHE_SKIPPED);
    }

    public void subtest6ExpressionIndex(Client client) throws IOException, ProcCallException {
        System.out.println("subtest6ExpressionIndex...");

        String sql;
        VoltTable vt;

        //
        // With user question mark parameters
        //
        sql = "SELECT ID FROM R1 sub6 WHERE ABS(NUM-1) = 1 AND ID >= ? ORDER BY ID;";
        vt = client.callProcedure("@Explain", sql).getResults()[0];
        assertTrue(vt.toString().contains("ABSIDX"));
        checkPlannerCache(client, CACHE_PARAMS_EXCEPTION);

        vt = client.callProcedure("@AdHoc", sql, 1).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{1, 2});
        checkPlannerCache(client, CACHE_MISS2);

        vt = client.callProcedure("@AdHoc", sql, 2).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{2});
        checkPlannerCache(client, CACHE_HIT2);


        // change the index constants
        sql = "SELECT ID FROM R1 sub6 WHERE ABS(NUM-0) = 1 AND ID >= ? ORDER BY ID;";
        vt = client.callProcedure("@Explain", sql).getResults()[0];
        assertFalse(vt.toString().contains("ABSIDX"));
        checkPlannerCache(client, CACHE_PARAMS_EXCEPTION);

        vt = client.callProcedure("@AdHoc", sql, 1).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{3});
        checkPlannerCache(client, CACHE_MISS2);

        vt = client.callProcedure("@AdHoc", sql, 4).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{});
        checkPlannerCache(client, CACHE_HIT2);


        // use user parameter with the constant in the expression index
        // not able to use the index scan here
        // ENG-15255
        if (!USING_CALCITE) {
            sql = "SELECT ID FROM R1 sub6 WHERE ABS(NUM-?) = 1 AND ID >= ? ORDER BY ID;";
            vt = client.callProcedure("@Explain", sql).getResults()[0];
            assertFalse(vt.toString().contains("ABSIDX"));
            checkPlannerCache(client, CACHE_PARAMS_EXCEPTION);

            vt = client.callProcedure("@AdHoc", sql, 0, 1).getResults()[0];
            validateTableOfScalarLongs(vt, new long[]{3});
            checkPlannerCache(client, CACHE_MISS2);

            vt = client.callProcedure("@AdHoc", sql, 1, 1).getResults()[0];
            validateTableOfScalarLongs(vt, new long[]{1, 2});
            checkPlannerCache(client, CACHE_HIT2);
        }

        //
        // no user parameters
        //
        sql = "SELECT ID FROM R1 sub6 WHERE ABS(NUM-1) = 1 AND ID >= 1 ORDER BY ID;";
        vt = client.callProcedure("@Explain", sql).getResults()[0];
        assertTrue(vt.toString().contains("ABSIDX"));
        checkPlannerCache(client, CACHE_MISS2_ADD1);

        validateTableOfScalarLongs(client, sql, new long[]{1, 2});
        checkPlannerCache(client, CACHE_HIT1);
    }

    //
    // Suite builder boilerplate
    //

    public TestAdHocPlannerCache(String name) {
        super(name);
    }
    static final Class<?>[] PROCEDURES = {
        org.voltdb_testprocs.regressionsuites.plansgroupbyprocs.CountT1A1.class,
        org.voltdb_testprocs.regressionsuites.plansgroupbyprocs.SumGroupSingleJoin.class };

    static public junit.framework.Test suite() {
        VoltServerConfig config;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(
                TestAdHocPlannerCache.class);
        VoltProjectBuilder project = new VoltProjectBuilder();
        final String literalSchema =
                "CREATE TABLE R1 ( " +
                " ID BIGINT DEFAULT 0 NOT NULL, "
                + "DESC VARCHAR(300), "
                + "NUM bigint,"
                + "RATIO FLOAT, "
                + "PRIMARY KEY (desc)); "

             // ENG-8243 shows that expression index has a bug not being able to be created.
//                + "CREATE INDEX EXPRIDX ON R1 (POWER(num,2));"
                + "CREATE INDEX absIdx ON R1 (ABS(num-1));"

                + "create procedure proc1 AS select num as number from R1 where id > ? order by num;"
                + ""
                ;
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
