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

package org.voltdb.regressionsuites;

import java.io.IOException;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestAdHocPlannerCache extends RegressionSuite {
    private static final int CACHE_MISS1 = 0;
    private static final int CACHE_MISS2 = 1;
    private static final int CACHE_HIT1 = 2;
    private static final int CACHE_HIT2 = 3;
    private static final int QUERY_EXCEPTION = 4;

    private long m_cache1_level = 0;
    private long m_cache2_level = 0;
    private long m_cache1_hits = 0;
    private long m_cache2_hits = 0;
    private long m_cache_misses = 0;

    private void resetStatistics() {
        m_cache1_level = 0;
        m_cache2_level = 0;
        m_cache1_hits = 0;
        m_cache2_hits = 0;
        m_cache_misses = 0;
    }

    private void checkCacheStatistics(Client client, long cache1_level, long cache2_level,
            long cache1_hits, long cache2_hits, long cache_misses)
            throws NoConnectionsException, IOException, ProcCallException {
        VoltTable vt;
        vt = client.callProcedure("@Statistics", "PLANNER", 0).getResults()[0];

        int len = vt.getRowCount();
        assertTrue(len > 0);
        while(vt.advanceRow()) {
            if (vt.getLong("HOST_ID") != -1) {
                continue;
            }
            // The global cache is identified by a site and partition ID of minus one
            assertEquals(cache1_level, vt.getLong("CACHE1_LEVEL"));
            assertEquals(cache2_level, vt.getLong("CACHE2_LEVEL"));
            assertEquals(cache1_hits,  vt.getLong("CACHE1_HITS"));
            assertEquals(cache2_hits,  vt.getLong("CACHE2_HITS"));
            assertEquals(cache_misses, vt.getLong("CACHE_MISSES"));
        }
    }

    private void checkPlannerCache(Client client, int... cacheTypes) throws NoConnectionsException, IOException, ProcCallException {
        for (int cacheType : cacheTypes) {
            if (cacheType == CACHE_MISS1) {
                ++m_cache1_level;
                ++m_cache_misses;
            } else if (cacheType == CACHE_MISS2) {
                ++m_cache1_level;
                ++m_cache2_level;
                ++m_cache_misses;
            } else if (cacheType == CACHE_HIT1) {
                ++m_cache1_hits;
            } else if (cacheType == CACHE_HIT2) {
                ++m_cache2_hits;
            } else if (cacheType == QUERY_EXCEPTION) {
                // Has not gone through the planner cache code
            } else {
                fail("Wrong input cache type");
            }
        }

        // check statistics
        checkCacheStatistics(client, m_cache1_level, m_cache2_level, m_cache1_hits, m_cache2_hits, m_cache_misses);
    }


    public void testAdHocPlannerCache() throws IOException, ProcCallException {
        System.out.println("testAdHocPlannerCache...");
        // useful when we have multiple unit tests in this suites
        resetStatistics();

        Client client = getClient();
        VoltTable vt;
        String sql;

        client.callProcedure("R1.insert", 1, "foo1", 0, 1.1);
        client.callProcedure("R1.insert", 2, "foo2", 0, 2.2);
        client.callProcedure("R1.insert", 3, "foo3", 1, 3.3);

        //
        // No constants AdHoc queries
        //
        sql = "SELECT ID FROM R1 B order by ID;";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{1, 2, 3});
        checkPlannerCache(client, CACHE_MISS2);

        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{1, 2, 3});
        checkPlannerCache(client, CACHE_HIT1);

        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{1, 2, 3});
        checkPlannerCache(client, CACHE_HIT1);

        // rename table alias
        sql = "SELECT ID FROM R1 C order by ID;";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{1, 2, 3});
        checkPlannerCache(client, CACHE_MISS2);


        //
        // Contain constants AdHoc Queries
        //
        sql = "SELECT ID FROM R1 B WHERE B.ID > 1 order by ID;";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{2, 3});
        checkPlannerCache(client, CACHE_MISS2);

        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{2, 3});
        checkPlannerCache(client, CACHE_HIT1);

        sql = "SELECT ID FROM R1 B WHERE B.ID > 2 order by ID;";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{3});
        checkPlannerCache(client, CACHE_HIT2);


        //
        // User question mark AdHoc queries
        //
        sql = "SELECT ID FROM R1 B WHERE B.ID > ? ORDER BY ID;";
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
        sql = "SELECT ID FROM R1 B WHERE num = 0 and B.ID > ? order by ID;";
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
        sql = "SELECT ID FROM R1 B WHERE num = 1 and B.ID > ? order by ID;";
        vt = client.callProcedure("@AdHoc", sql, 0).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{3});
        checkPlannerCache(client, CACHE_MISS2);

        vt = client.callProcedure("@AdHoc", sql, 3).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{});
        checkPlannerCache(client, CACHE_HIT2);

        // replace constants with parameter
        sql = "SELECT ID FROM R1 B WHERE num = ? and B.ID > ? order by ID;";
        vt = client.callProcedure("@AdHoc", sql, 0, 0).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{1, 2});
        checkPlannerCache(client, CACHE_MISS2);

        vt = client.callProcedure("@AdHoc", sql, 1, 2).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{3});
        checkPlannerCache(client, CACHE_HIT2);
    }

    public void testAdHocParameters() throws IOException, ProcCallException {
        System.out.println("testAdHocParameters...");
        resetStatistics();

        Client client = getClient();
        String sql;
        String errorMsg = "~abcdefg~";

        client.callProcedure("R1.insert", 1, "foo1", 0, 1.1);
        client.callProcedure("R1.insert", 2, "foo2", 0, 2.2);
        client.callProcedure("R1.insert", 3, "foo3", 1, 3.3);

        //
        // Multiple AdHoc queries with question marks per procedure call
        //
        errorMsg = "Multiple AdHoc queries with question marks in a procedure call are not supported";
        sql = "SELECT ID FROM R1 B WHERE num = 0 and B.ID > ? order by ID;";
        try {
            client.callProcedure("@AdHoc", sql + sql, 0, 0);
            fail();
        } catch(Exception ex) {
            assertTrue(ex.getMessage().contains(errorMsg));
        }
        checkPlannerCache(client, QUERY_EXCEPTION);

        // fewer parameters
        try {
            client.callProcedure("@AdHoc", sql + sql, 0);
            fail();
        } catch(Exception ex) {
            assertTrue(ex.getMessage().contains(errorMsg));
        }
        checkPlannerCache(client, QUERY_EXCEPTION);

        try {
            client.callProcedure("@AdHoc", "select * from r1;" + sql, 0, 0);
            fail();
        } catch(Exception ex) {
            assertTrue(ex.getMessage().contains(errorMsg));
        }
        checkPlannerCache(client, QUERY_EXCEPTION);

        // by pass the pre-planner check
        try {
            client.callProcedure("@AdHoc", sql + sql);
            fail();
        } catch(Exception ex) {
            assertTrue(ex.getMessage().contains("Incorrect number of parameters passed: expected 1, passed 0"));
        }
        checkPlannerCache(client, CACHE_MISS1);

        // positive tests
        sql = "SELECT ID FROM R1 B order by ID;";
        VoltTable[] vts = client.callProcedure("@AdHoc", sql + sql).getResults();
        validateTableOfScalarLongs(vts[0], new long[]{1, 2, 3});
        validateTableOfScalarLongs(vts[1], new long[]{1, 2, 3});
        checkPlannerCache(client, CACHE_MISS2, CACHE_HIT1);

        //
        // Pass in incorrect number of parameters
        //
        try {
            client.callProcedure("@AdHoc", sql, 1);
            fail();
        } catch(Exception ex) {
            assertTrue(ex.getMessage().contains("Incorrect number of parameters passed: expected 0, passed 1"));
        }
        checkPlannerCache(client, CACHE_MISS1);

        sql = "SELECT ID FROM R1 B WHERE num = 0 and B.ID > ? order by ID;";
        try {
            client.callProcedure("@AdHoc", sql, 1, 500);
            fail();
        } catch(Exception ex) {
            assertTrue(ex.getMessage().contains("Incorrect number of parameters passed: expected 1, passed 2"));
        }
        checkPlannerCache(client, CACHE_MISS1);

        try {
            client.callProcedure("@AdHoc", sql);
            fail();
        } catch(Exception ex) {
            assertTrue(ex.getMessage().contains("Incorrect number of parameters passed: expected 1, passed 0"));
        }
        checkPlannerCache(client, CACHE_MISS1);

        VoltTable vt;

        // rename table with "TB" to run it as a new query to the system
        sql = "SELECT ID FROM R1 TB WHERE TB.ID > ? order by ID;";
        vt = client.callProcedure("@AdHoc", sql, 1).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{2, 3});
        checkPlannerCache(client, CACHE_MISS2);

        try {
            vt = client.callProcedure("@AdHoc", sql, 1, 500).getResults()[0];
        } catch(Exception ex) {
            assertTrue(ex.getMessage().contains("Incorrect number of parameters passed: expected 1, passed 2"));
        }
        checkPlannerCache(client, CACHE_MISS1);

        try {
            vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        } catch(Exception ex) {
            assertTrue(ex.getMessage().contains("Incorrect number of parameters passed: expected 1, passed 0"));
        }
        checkPlannerCache(client, CACHE_MISS1);

        // no parameters passed in for multiple adhoc queries
        try {
            vt = client.callProcedure("@AdHoc", sql + sql).getResults()[0];
        } catch(Exception ex) {
            assertTrue(ex.getMessage().contains("Incorrect number of parameters passed: expected 1, passed 0"));
        }
        checkPlannerCache(client, CACHE_MISS1);
    }

    public void testAdHocBadParameters() throws IOException, ProcCallException {
        System.out.println("testAdHocBadParameters...");
        resetStatistics();

        Client client = getClient();
        String sql;
        VoltTable vt;

        client.callProcedure("R1.insert", 1, "foo1", 0, 1.1);
        client.callProcedure("R1.insert", 2, "foo2", 0, 2.2);
        client.callProcedure("R1.insert", 3, "foo3", 1, 3.3);

        // Integer <-> Float
        sql = "SELECT ID FROM R1 B WHERE B.ID > 1.8 order by ID;";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{2, 3});
        checkPlannerCache(client, CACHE_MISS1);

        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{2, 3});
        checkPlannerCache(client, CACHE_HIT1);

        sql = "SELECT ID FROM R1 B WHERE B.ID > 1 order by ID;";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{2, 3});
        checkPlannerCache(client, CACHE_MISS2);

        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{2, 3});
        checkPlannerCache(client, CACHE_HIT1);

        sql = "SELECT ID FROM R1 B WHERE B.ID > 2 order by ID;";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{3});
        checkPlannerCache(client, CACHE_HIT2);

        sql = "SELECT ID FROM R1 B WHERE B.ID > ? order by ID;";
        try {
            vt = client.callProcedure("@AdHoc", sql, 1.8).getResults()[0];
            fail();
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains("tryToMakeCompatible: The provided value: (1.8) of type: "
                    + "java.lang.Double is not a match or is out of range for the target parameter type: long"));
        }

        // change the where clause to get the new query pattern

        // try the normal integer value first
        sql = "SELECT ID FROM R1 B WHERE B.NUM > 0 order by ID;";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{3});
        checkPlannerCache(client, CACHE_MISS2);

        // try the float value second, it has bad parameterization
        sql = "SELECT ID FROM R1 B WHERE B.NUM > 0.8 order by ID;";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{3});
        checkPlannerCache(client, CACHE_MISS1);

        sql = "SELECT ID FROM R1 B WHERE B.NUM > 0.9 order by ID;";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{3});
        checkPlannerCache(client, CACHE_MISS1);
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
        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(
                TestAdHocPlannerCache.class);
        VoltProjectBuilder project = new VoltProjectBuilder();
        final String literalSchema =
                "CREATE TABLE R1 ( " +
                " ID BIGINT DEFAULT 0 NOT NULL, "
                + "DESC VARCHAR(300), "
                + "NUM bigint,"
                + "RATIO FLOAT, "
                + "PRIMARY KEY (desc)); " +

                ""
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
