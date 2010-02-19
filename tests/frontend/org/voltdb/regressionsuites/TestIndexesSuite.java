/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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
import org.voltdb.VoltTableRow;
import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.indexes.*;

/**
 * Actual regression tests for SQL that I found that was broken and
 * have fixed.  Didn't like any of the other potential homes that already
 * existed for this for one reason or another.
 */

public class TestIndexesSuite extends RegressionSuite {

    /** Procedures used by this suite */
    static final Class<?>[] PROCEDURES = { Insert.class,
        CheckMultiMultiIntGTEFailure.class};

    // Index stuff to test:
    // scans against tree
    // - < <= = > >=, range with > and <
    // - single column
    // - multi-column
    // - multi-map

    public void testOrderedUniqueOneColumnIntIndex()
    throws IOException, ProcCallException
    {
        String[] tables = {"P1", "R1", "P2", "R2"};
        Client client = getClient();
        for (String table : tables)
        {
            client.callProcedure("Insert", table, 1, "a", 100, 1, 14.5);
            client.callProcedure("Insert", table, 2, "b", 100, 2, 15.5);
            client.callProcedure("Insert", table, 3, "c", 200, 3, 16.5);
            client.callProcedure("Insert", table, 6, "f", 200, 6, 17.5);
            client.callProcedure("Insert", table, 7, "g", 300, 7, 18.5);
            client.callProcedure("Insert", table, 8, "h", 300, 8, 19.5);
            String query = String.format("select * from %s where %s.ID > 1",
                                         table, table);
            VoltTable[] results = client.callProcedure("@AdHoc", query);
            assertEquals(5, results[0].getRowCount());
            // make sure that we work if the value we want isn't present
            query = String.format("select * from %s where %s.ID > 4",
                                  table, table);
            results = client.callProcedure("@AdHoc", query);
            assertEquals(3, results[0].getRowCount());
            query = String.format("select * from %s where %s.ID > 8",
                                  table, table);
            results = client.callProcedure("@AdHoc", query);
            assertEquals(0, results[0].getRowCount());
            query = String.format("select * from %s where %s.ID >= 1",
                                  table, table);
            results = client.callProcedure("@AdHoc", query);
            assertEquals(6, results[0].getRowCount());
            query = String.format("select * from %s where %s.ID >= 4",
                                  table, table);
            results = client.callProcedure("@AdHoc", query);
            assertEquals(3, results[0].getRowCount());
            query = String.format("select * from %s where %s.ID >= 9",
                                  table, table);
            results = client.callProcedure("@AdHoc", query);
            assertEquals(0, results[0].getRowCount());
            query = String.format("select * from %s where %s.ID > 1 and %s.ID < 6",
                                  table, table, table);
            results = client.callProcedure("@AdHoc", query);
            assertEquals(2, results[0].getRowCount());
            query = String.format("select * from %s where %s.ID > 1 and %s.ID <= 6",
                                  table, table, table);
            results = client.callProcedure("@AdHoc", query);
            assertEquals(3, results[0].getRowCount());
            query = String.format("select * from %s where %s.ID > 1 and %s.ID <= 5",
                                  table, table, table);
            results = client.callProcedure("@AdHoc", query);
            assertEquals(2, results[0].getRowCount());
            query = String.format("select * from %s where %s.ID >= 1 and %s.ID < 7",
                                  table, table, table);
            results = client.callProcedure("@AdHoc", query);
            assertEquals(4, results[0].getRowCount());
            // Check that >= work in conjunction with <
            // run over the end of the index to catch the keyIterate bug
            // in the first >= index fix
            query = String.format("select * from %s where %s.ID >= 1 and %s.ID < 10",
                                  table, table, table);
            results = client.callProcedure("@AdHoc", query);
            assertEquals(6, results[0].getRowCount());
            // XXX THIS CASE CURRENTLY FAILS
            // SEE TICKET 194
//            query = String.format("select * from %s where %s.ID >= 2.9",
//                                  table, table);
//            results = client.callProcedure("@AdHoc", query);
//            assertEquals(4, results[0].getRowCount());
        }
    }

    /**
     * Multimap single column
     * @throws IOException
     * @throws ProcCallException
     */
    public void testOrderedMultiOneColumnIntIndex()
    throws IOException, ProcCallException
    {
        String[] tables = {"P1", "R1", "P2", "R2"};
        Client client = getClient();
        for (String table : tables)
        {
            client.callProcedure("Insert", table, 1, "a", 100, 1, 14.5);
            client.callProcedure("Insert", table, 2, "b", 100, 2, 15.5);
            client.callProcedure("Insert", table, 3, "c", 200, 3, 16.5);
            client.callProcedure("Insert", table, 6, "f", 200, 6, 17.5);
            client.callProcedure("Insert", table, 7, "g", 300, 7, 18.5);
            client.callProcedure("Insert", table, 8, "h", 300, 8, 19.5);
            String query = String.format("select * from %s where %s.NUM > 100",
                                         table, table);
            VoltTable[] results = client.callProcedure("@AdHoc", query);
            assertEquals(4, results[0].getRowCount());
            query = String.format("select * from %s where %s.NUM > 150",
                                  table, table);
            results = client.callProcedure("@AdHoc", query);
            assertEquals(4, results[0].getRowCount());
            query = String.format("select * from %s where %s.NUM > 300",
                                  table, table);
            results = client.callProcedure("@AdHoc", query);
            assertEquals(0, results[0].getRowCount());
            query = String.format("select * from %s where %s.NUM >= 100",
                                  table, table);
            results = client.callProcedure("@AdHoc", query);
            assertEquals(6, results[0].getRowCount());
            query = String.format("select * from %s where %s.NUM >= 150",
                                  table, table);
            results = client.callProcedure("@AdHoc", query);
            assertEquals(4, results[0].getRowCount());
            query = String.format("select * from %s where %s.NUM >= 301",
                                  table, table);
            results = client.callProcedure("@AdHoc", query);
            assertEquals(0, results[0].getRowCount());
            query = String.format("select * from %s where %s.NUM > 100 and %s.NUM < 300",
                                  table, table, table);
            results = client.callProcedure("@AdHoc", query);
            assertEquals(2, results[0].getRowCount());
            // Check that >= work in conjunction with <
            // run over the end of the index to catch the keyIterate bug
            // in the first >= index fix
            query = String.format("select * from %s where %s.NUM >= 100 and %s.NUM < 400",
                                  table, table, table);
            results = client.callProcedure("@AdHoc", query);
            assertEquals(6, results[0].getRowCount());
            query = String.format("select * from %s where %s.NUM = 100",
                                  table, table);
            results = client.callProcedure("@AdHoc", query);
            assertEquals(2, results[0].getRowCount());
            query = String.format("select * from %s where %s.NUM > 100 and %s.NUM <= 300",
                                  table, table, table);
            results = client.callProcedure("@AdHoc", query);
            assertEquals(4, results[0].getRowCount());
            query = String.format("select * from %s where %s.NUM > 100 and %s.NUM <= 250",
                                  table, table, table);
            results = client.callProcedure("@AdHoc", query);
            assertEquals(2, results[0].getRowCount());
        }
    }

    public void testTicket195()
    throws IOException, ProcCallException
    {
        String[] tables = {"P1", "R1", "P2", "R2"};
        Client client = getClient();
        for (String table : tables)
        {
            client.callProcedure("Insert", table, 1, "a", 100, 1, 14.5);
            client.callProcedure("Insert", table, 2, "b", 100, 2, 15.5);
            client.callProcedure("Insert", table, 3, "c", 200, 3, 16.5);
            client.callProcedure("Insert", table, 6, "f", 200, 6, 17.5);
            client.callProcedure("Insert", table, 7, "g", 300, 7, 18.5);
            client.callProcedure("Insert", table, 8, "h", 300, 8, 19.5);
            String query = String.format("select * from %s where %s.NUM >= 100 and %s.NUM <= 400",
                                  table, table, table);
            VoltTable[] results = client.callProcedure("@AdHoc", query);
            assertEquals(6, results[0].getRowCount());
        }
    }

    /**
     * Multimap multi column
     * @throws IOException
     * @throws ProcCallException
     */
    public void testOrderedMultiMultiColumnIntIndex()
    throws IOException, ProcCallException
    {
        String[] tables = {"P3", "R3"};
        Client client = getClient();
        for (String table : tables)
        {
            client.callProcedure("Insert", table, 1, "a", 100, 1, 14.5);
            client.callProcedure("Insert", table, 2, "b", 100, 2, 15.5);
            client.callProcedure("Insert", table, 3, "c", 200, 3, 16.5);
            client.callProcedure("Insert", table, 6, "f", 200, 6, 17.5);
            client.callProcedure("Insert", table, 7, "g", 300, 7, 18.5);
            client.callProcedure("Insert", table, 8, "h", 300, 8, 19.5);
            String query = String.format("select * from %s where %s.NUM > 100 AND %s.NUM2 > 1",
                                         table, table, table);
            VoltTable[] results = client.callProcedure("@AdHoc", query);
            assertEquals(4, results[0].getRowCount());
        }
    }

    public void testOrderedMultiMultiIntGTEFailure()
    throws IOException, ProcCallException
    {
        final Client client = getClient();
        final VoltTable results[] = client.callProcedure("CheckMultiMultiIntGTEFailure");
        if (results == null || results.length == 0) {
            fail();
        }
        assertEquals( 2, results[0].getRowCount());
        final VoltTableRow row0 = results[0].fetchRow(0);
        assertEquals( 0, row0.getLong(0));
        assertEquals( 0, row0.getLong(1));

        final VoltTableRow row1 = results[0].fetchRow(1);
        assertEquals( 0, row1.getLong(0));
        assertEquals( 1, row1.getLong(1));
    }

    //
    // JUnit / RegressionSuite boilerplate
    //
    public TestIndexesSuite(String name) {
        super(name);
    }

    static public junit.framework.Test suite() {

        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestIndexesSuite.class);

        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addSchema(Insert.class.getResource("indexes-ddl.sql"));
        project.addPartitionInfo("P1", "ID");
        project.addPartitionInfo("P2", "ID");
        project.addPartitionInfo("P3", "ID");
        project.addProcedures(PROCEDURES);

    /*
        // CONFIG #1: Local Site/Partitions running on IPC backend
        config = new LocalSingleProcessServer("sqltypes-onesite.jar", 1, BackendTarget.NATIVE_EE_IPC);
        config.compile(project);
        builder.addServerConfig(config);
        // CONFIG #2: HSQL
        config = new LocalSingleProcessServer("testindexes-hsql.jar", 1, BackendTarget.HSQLDB_BACKEND);
        config.compile(project);
        builder.addServerConfig(config);
     */

        // JNI
        config = new LocalSingleProcessServer("testindexes-onesite.jar", 1, BackendTarget.NATIVE_EE_JNI);
        config.compile(project);
        builder.addServerConfig(config);


        // CLUSTER?
        config = new LocalCluster("testindexes-cluster.jar", 2, 2,
                                  1, BackendTarget.NATIVE_EE_JNI);
        config.compile(project);
        builder.addServerConfig(config);

        return builder;
    }

}
