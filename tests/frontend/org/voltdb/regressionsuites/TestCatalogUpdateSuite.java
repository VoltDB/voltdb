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

package org.voltdb.regressionsuites;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Test;

import org.apache.commons.lang3.RandomStringUtils;
import org.voltdb.BackendTarget;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.benchmark.tpcc.procedures.InsertNewOrder;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltProjectBuilder.GroupInfo;
import org.voltdb.compiler.VoltProjectBuilder.ProcedureInfo;
import org.voltdb.compiler.VoltProjectBuilder.UserInfo;
import org.voltdb.utils.MiscUtils;

/**
 * Tests a mix of multi-partition and single partition procedures on a
 * mix of replicated and partititioned tables on a mix of single-site and
 * multi-site VoltDB instances.
 *
 */
public class TestCatalogUpdateSuite extends RegressionSuite {

    // procedures used by these tests
    static Class<?>[] BASEPROCS = { org.voltdb.benchmark.tpcc.procedures.InsertNewOrder.class,
                                    org.voltdb.benchmark.tpcc.procedures.SelectAll.class,
                                    org.voltdb.benchmark.tpcc.procedures.delivery.class };

    static Class<?>[] BASEPROCS_OPROCS =  { org.voltdb.benchmark.tpcc.procedures.InsertNewOrder.class,
                                            org.voltdb.benchmark.tpcc.procedures.SelectAll.class,
                                            org.voltdb.benchmark.tpcc.procedures.delivery.class,
                                            org.voltdb_testprocs.regressionsuites.orderbyprocs.InsertO1.class};


    static Class<?>[] EXPANDEDPROCS = { org.voltdb.benchmark.tpcc.procedures.InsertNewOrder.class,
                                        org.voltdb.benchmark.tpcc.procedures.SelectAll.class,
                                        org.voltdb.benchmark.tpcc.procedures.delivery.class,
                                        org.voltdb.benchmark.tpcc.procedures.InsertOrderLineBatched.class };

    static Class<?>[] CONFLICTPROCS = { org.voltdb.catalog.InsertNewOrder.class,
                                        org.voltdb.benchmark.tpcc.procedures.SelectAll.class,
                                        org.voltdb.benchmark.tpcc.procedures.delivery.class };

    static Class<?>[] SOMANYPROCS =   { org.voltdb.benchmark.tpcc.procedures.InsertNewOrder.class,
                                        org.voltdb.benchmark.tpcc.procedures.SelectAll.class,
                                        org.voltdb.benchmark.tpcc.procedures.neworder.class,
                                        org.voltdb.benchmark.tpcc.procedures.ostatByCustomerId.class,
                                        org.voltdb.benchmark.tpcc.procedures.ostatByCustomerName.class,
                                        org.voltdb.benchmark.tpcc.procedures.paymentByCustomerId.class,
                                        org.voltdb.benchmark.tpcc.procedures.paymentByCustomerName.class,
                                        org.voltdb.benchmark.tpcc.procedures.slev.class,
                                        org.voltdb.benchmark.tpcc.procedures.delivery.class };

    // testUpdateHonkingBigCatalog constants and statistics. 100/100/40 makes a ~2MB jar.
    private static final int HUGE_TABLES = 100;
    private static final int HUGE_COLUMNS = 100;
    private static final int HUGE_NAME_SIZE = 40;
    private static double hugeCompileElapsed = 0.0;
    private static double hugeTestElapsed = 0.0;
    private static String hugeCatalogXML;
    private static String hugeCatalogJar;

    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestCatalogUpdateSuite(String name) {
        super(name);
    }

    AtomicInteger m_outstandingCalls = new AtomicInteger(0);

    boolean callbackSuccess;

    class CatTestCallback implements ProcedureCallback {

        final byte m_expectedStatus;

        CatTestCallback(byte expectedStatus) {
            m_expectedStatus = expectedStatus;
            m_outstandingCalls.incrementAndGet();
        }

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            m_outstandingCalls.decrementAndGet();
            if (m_expectedStatus != clientResponse.getStatus()) {
                if (clientResponse.getStatusString() != null)
                    System.err.println(clientResponse.getStatusString());
                if (clientResponse.getException() != null)
                    clientResponse.getException().printStackTrace();
                callbackSuccess = false;
            }
        }
    }

    public void loadSomeData(Client client, int start, int count) throws IOException, ProcCallException {
        for (int i = start; i < (start + count); i++) {
            CatTestCallback callback = new CatTestCallback(ClientResponse.SUCCESS);
            client.callProcedure(callback, InsertNewOrder.class.getSimpleName(), i, i, (short)i);
        }
    }

    public void negativeTests(Client client) throws UnsupportedEncodingException {
        // this fails because the catalog URL isn't a real thing but needs to point at
        // a file that actually exists.  Point to the compiled java class for this suite
        URL url = TestCatalogUpdateSuite.class.getResource("TestCatalogUpdateSuite.class");
        String newCatalogURL = URLDecoder.decode(url.getPath(), "UTF-8");
        String deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-addtables.xml");
        try {
            client.updateApplicationCatalog(new File(newCatalogURL), new File(deploymentURL));
            fail();
        }
        catch (Exception e) {
            assertTrue(e.getMessage().startsWith("Database catalog not found"));
        }
    }

    public long indexEntryCountFromStats(Client client, String name) throws Exception {
        ClientResponse callProcedure = client.callProcedure("@Statistics", "INDEX", 0);
        assertTrue(callProcedure.getResults().length == 1);
        assertTrue(callProcedure.getStatus() == ClientResponse.SUCCESS);
        VoltTable result = callProcedure.getResults()[0];
        long tupleCount = 0;
        while (result.advanceRow()) {
            if (result.getString("TABLE_NAME").equals("NEW_ORDER") && result.getString("INDEX_NAME").equals("NEWINDEX")) {
                tupleCount += result.getLong("ENTRY_COUNT");
            }
        }
        return tupleCount;
    }

    public void testAddDropIndex() throws Exception
    {
        ClientResponse callProcedure;
        String explanation;

        Client client = getClient();
        loadSomeData(client, 0, 10);
        client.drain();
        assertTrue(callbackSuccess);

        // check that no index was used by checking the plan itself
        callProcedure = client.callProcedure("@Explain", "select * from NEW_ORDER where NO_O_ID = 5;");
        explanation = callProcedure.getResults()[0].fetchRow(0).getString(0);
        assertFalse(explanation.contains("INDEX SCAN"));

        // add index to NEW_ORDER
        String newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-addindex.jar");
        String deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-addindex.xml");
        VoltTable[] results = client.updateApplicationCatalog(new File(newCatalogURL), new File(deploymentURL)).getResults();
        assertTrue(results.length == 1);

        // check the index for non-zero size
        callProcedure = client.callProcedure("@Statistics", "INDEX", 0);
        assertTrue(callProcedure.getResults().length == 1);
        assertTrue(callProcedure.getStatus() == ClientResponse.SUCCESS);
        VoltTable result = callProcedure.getResults()[0];
        long tupleCount = 0;
        while (result.advanceRow()) {
            if (result.getString("TABLE_NAME").equals("NEW_ORDER") && result.getString("INDEX_NAME").equals("NEWINDEX")) {
                tupleCount += result.getLong("ENTRY_COUNT");
            }
        }
        assertTrue(tupleCount > 0);

        // verify that the new table(s) support an insert
        callProcedure = client.callProcedure("@AdHoc", "insert into NEW_ORDER values (-1, -1, -1);");
        assertTrue(callProcedure.getResults().length == 1);
        assertTrue(callProcedure.getStatus() == ClientResponse.SUCCESS);

        // do a call that uses the index
        callProcedure = client.callProcedure("@AdHoc", "select * from NEW_ORDER where NO_O_ID = 5;");
        result = callProcedure.getResults()[0];
        result.advanceRow();
        assertEquals(5, result.getLong("NO_O_ID"));

        // check that an index was used by checking the plan itself
        callProcedure = client.callProcedure("@Explain", "select * from NEW_ORDER where NO_O_ID = 5;");
        explanation = callProcedure.getResults()[0].fetchRow(0).getString(0);
        assertTrue(explanation.contains("INDEX SCAN"));

        // tables can still be accessed
        loadSomeData(client, 20, 10);
        client.drain();
        assertTrue(callbackSuccess);

        // check the index for even biggerer size from stats
        callProcedure = client.callProcedure("@Statistics", "INDEX", 0);
        assertTrue(callProcedure.getResults().length == 1);
        assertTrue(callProcedure.getStatus() == ClientResponse.SUCCESS);
        result = callProcedure.getResults()[0];
        long newTupleCount = 0;
        while (result.advanceRow()) {
            if (result.getString("TABLE_NAME").equals("NEW_ORDER") && result.getString("INDEX_NAME").equals("NEWINDEX")) {
                newTupleCount += result.getLong("ENTRY_COUNT");
            }
        }
        assertTrue(newTupleCount > tupleCount);

        // check another index for the same number of tuples
        callProcedure = client.callProcedure("@Statistics", "INDEX", 0);
        assertTrue(callProcedure.getResults().length == 1);
        assertTrue(callProcedure.getStatus() == ClientResponse.SUCCESS);
        result = callProcedure.getResults()[0];
        long newTupleCount = 0;
        while (result.advanceRow()) {
            if (result.getString("TABLE_NAME").equals("NEW_ORDER") && result.getString("INDEX_NAME").equals("NEWINDEX")) {
                newTupleCount += result.getLong("ENTRY_COUNT");
            }
        }
        assertTrue(newTupleCount > tupleCount);

        // revert to the original schema
        newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-base.jar");
        deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-base.xml");
        results = client.updateApplicationCatalog(new File(newCatalogURL), new File(deploymentURL)).getResults();
        assertTrue(results.length == 1);

        // do a call that uses the index
        callProcedure = client.callProcedure("@AdHoc", "select * from NEW_ORDER where NO_O_ID = 5;");
        result = callProcedure.getResults()[0];
        result.advanceRow();
        assertEquals(5, result.getLong("NO_O_ID"));

        // check that no index was used by checking the plan itself
        callProcedure = client.callProcedure("@Explain", "select * from NEW_ORDER where NO_O_ID = 5;");
        explanation = callProcedure.getResults()[0].fetchRow(0).getString(0);
        assertFalse(explanation.contains("INDEX SCAN"));

        // and loading still succeeds
        loadSomeData(client, 30, 10);
        client.drain();
        assertTrue(callbackSuccess);
    }

    public void testAddDropTable() throws IOException, ProcCallException, InterruptedException
    {
        Client client = getClient();
        loadSomeData(client, 0, 10);
        client.drain();
        assertTrue(callbackSuccess);

        // verify that an insert w/o a table fails.
        try {
            client.callProcedure("@AdHoc", "insert into O1 values (1, 1, 'foo', 'foobar');");
            fail();
        }
        catch (ProcCallException e) {
        }

        // Also can't call this not-yet-existing stored procedure
        try {
            client.callProcedure("InsertO1", new Integer(100), new Integer(200), "foo", "bar");
            fail();
        }
        catch (ProcCallException e) {
        }

        // add tables O1, O2, O3
        String newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-addtables.jar");
        String deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-addtables.xml");
        VoltTable[] results = client.updateApplicationCatalog(new File(newCatalogURL), new File(deploymentURL)).getResults();
        assertTrue(results.length == 1);

        // verify that the new table(s) support an insert
        ClientResponse callProcedure = client.callProcedure("@AdHoc", "insert into O1 values (1, 1, 'foo', 'foobar');");
        assertTrue(callProcedure.getResults().length == 1);
        assertTrue(callProcedure.getStatus() == ClientResponse.SUCCESS);

        callProcedure = client.callProcedure("@AdHoc", "insert into O2 values (1, 1, 'foo', 'foobar');");
        assertTrue(callProcedure.getResults().length == 1);
        assertTrue(callProcedure.getStatus() == ClientResponse.SUCCESS);

        callProcedure = client.callProcedure("@AdHoc", "select * from O1");
        VoltTable result = callProcedure.getResults()[0];
        result.advanceRow();
        assertTrue(result.get(2, VoltType.STRING).equals("foo"));

        // old tables can still be accessed
        loadSomeData(client, 20, 10);
        client.drain();
        assertTrue(callbackSuccess);

        // and this new procedure is happy like clams
        callProcedure = client.callProcedure("InsertO1", new Integer(100), new Integer(200), "foo", "bar");
        assertTrue(callProcedure.getResults().length == 1);
        assertTrue(callProcedure.getStatus() == ClientResponse.SUCCESS);

        // revert to the original schema
        newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-base.jar");
        deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-base.xml");
        results = client.updateApplicationCatalog(new File(newCatalogURL), new File(deploymentURL)).getResults();
        assertTrue(results.length == 1);

        // requests to the dropped table should fail
        try {
            client.callProcedure("@AdHoc", "insert into O1 values (1, 1, 'foo', 'foobar');");
            fail();
        }
        catch (ProcCallException e) {
        }

        try {
            client.callProcedure("InsertO1", new Integer(100), new Integer(200), "foo", "bar");
            fail();
        }
        catch (ProcCallException e) {
        }

        // and other requests still succeed
        loadSomeData(client, 30, 10);
        client.drain();
        assertTrue(callbackSuccess);
    }

    public void testAddTableWithMatView() throws IOException, ProcCallException, InterruptedException {
        Client client = getClient();
        loadSomeData(client, 0, 10);
        client.drain();
        assertTrue(callbackSuccess);

        // add new tables and materialized view
        String newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-addtableswithmatview.jar");
        String deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-addtableswithmatview.xml");
        VoltTable[] results = client.updateApplicationCatalog(new File(newCatalogURL), new File(deploymentURL)).getResults();
        assertTrue(results.length == 1);

        // verify that the new table(s) support an insert
        for (int i=0; i < 10; ++i) {
            ClientResponse callProcedure = client.callProcedure("@AdHoc", "insert into O1 values (" + i + ", " + i % 2 + ", 'foo', 'foobar');");
            assertTrue(callProcedure.getResults().length == 1);
            assertTrue(callProcedure.getStatus() == ClientResponse.SUCCESS);
        }

        // read it - expect 10 rows
        ClientResponse callProcedure = client.callProcedure("@AdHoc", "select * from O1");
        VoltTable result = callProcedure.getResults()[0];
        assertTrue(result.getRowCount() == 10);

        // read the mat view. expect two rows (grouped on x % 2)
        callProcedure = client.callProcedure("@AdHoc", "select C1,NUM from MATVIEW_O1 order by C1");
        result = callProcedure.getResults()[0];

        System.out.println("MATVIEW:"); System.out.println(result);
        assertTrue(result.getRowCount() == 2);
    }

    public void testAddDropTableRepeat() throws Exception {
        Client client = getClient();
        loadSomeData(client, 0, 10);
        client.drain();
        assertTrue(callbackSuccess);

        /*
         * Reduced from 100 to 30 so that it doesn't take quite as long
         * We run tests often enough that this will get plenty of fuzzing.
         */
        for (int i=0; i < 30; i++)
        {
            // add tables O1, O2, O3
            String newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-addtables.jar");
            String deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-addtables.xml");
            VoltTable[] results = client.updateApplicationCatalog(new File(newCatalogURL), new File(deploymentURL)).getResults();
            assertTrue(results.length == 1);

            // Thread.sleep(2000);

            // revert to the original schema
            newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-base.jar");
            deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-base.xml");
            results = client.updateApplicationCatalog(new File(newCatalogURL), new File(deploymentURL)).getResults();
            assertTrue(results.length == 1);
       }
    }

    public void testUpdateHonkingBigCatalog() throws IOException, ProcCallException, InterruptedException {
        System.out.println("\n\n-----\n testUpdateHonkingBigCatalog\n");
        System.out.printf("jar: %s (%.2f MB)\n", hugeCatalogJar, new File(hugeCatalogJar).length() / 1048576.0);
        System.out.printf("compile: %.2f seconds (%.2f/second)\n", hugeCompileElapsed, HUGE_TABLES / hugeCompileElapsed);
        long t = System.currentTimeMillis();
        Client client = getClient();
        loadSomeData(client, 0, 10);
        client.drain();
        assertTrue(callbackSuccess);

        String newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-huge.jar");
        String deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-huge.xml");
        try {
            VoltTable[] results = client.updateApplicationCatalog(new File(newCatalogURL), new File(deploymentURL)).getResults();
            assertTrue(results.length == 1);
        }
        catch (ProcCallException e) {
            fail(String.format("@UpdateApplicationCatalog: ProcCallException: %s", e.getLocalizedMessage()));
        }
        hugeTestElapsed = (System.currentTimeMillis() - t) / 1000.0;
        System.out.printf("test: %.2f seconds (%.2f/second)\n", hugeTestElapsed, HUGE_TABLES / hugeTestElapsed);
        System.out.println("-----\n\n");
    }

    private void deleteDirectory(File dir) {
        if (!dir.exists() || !dir.isDirectory()) {
            return;
        }

        for (File f : dir.listFiles()) {
            assertTrue(f.delete());
        }
        assertTrue(dir.delete());
    }

    private static String generateRandomDDL(String name, int ntables, int ncols, int width)
            throws IOException {
        // Generate huge DDL file. Make it relatively uncompressible with randomness.
        File temp = File.createTempFile(name, ".sql");
        temp.deleteOnExit();
        FileWriter out = new FileWriter(temp);
        char[] charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_".toCharArray();
        Random random = new Random(99);
        for (int itable = 0; itable < ntables; itable++) {
            out.write(String.format("\nCREATE TABLE HUGE_TABLE_%d (\n", itable));
            out.write("C_FIRST INTEGER,\n");
            for (int icolumn = 0; icolumn < ncols; icolumn++) {
                String columnID = RandomStringUtils.random(width,
                                                           0,
                                                           charset.length,
                                                           false,
                                                           false,
                                                           charset,
                                                           random);
                out.write(String.format("C_%s INTEGER,\n", columnID));
            }
            out.write("PRIMARY KEY (C_FIRST));\n");
        }
        out.close();
        return URLEncoder.encode(temp.getAbsolutePath(), "UTF-8");
    }

    /**
     * Build a list of the tests that will be run when TestTPCCSuite gets run by JUnit.
     * Use helper classes that are part of the RegressionSuite framework.
     * This particular class runs all tests on the the local JNI backend with both
     * one and two partition configurations, as well as on the hsql backend.
     *
     * @return The TestSuite containing all the tests to be run.
     * @throws Exception
     */
    static public Test suite() throws Exception {
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestCatalogUpdateSuite.class);

        final int sitesPerHost = 2;
        final int hosts = 2;
        final int k = 1;

        /////////////////////////////////////////////////////////////
        // CONFIG #1: 1 Local Site/Partitions running on JNI backend
        /////////////////////////////////////////////////////////////

        // get a server config for the native backend with one sites/partitions
        VoltServerConfig config = new LocalCluster("catalogupdate-cluster-base.jar", sitesPerHost, hosts, k, BackendTarget.NATIVE_EE_JNI);
        ((LocalCluster) config).setHasLocalServer(true);

        // build up a project builder for the workload
        TPCCProjectBuilder project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        project.addProcedures(BASEPROCS);
        // build the jarfile
        boolean basecompile = config.compile(project);
        assertTrue(basecompile);
        MiscUtils.copyFile(project.getPathToDeployment(), Configuration.getPathToCatalogForTest("catalogupdate-cluster-base.xml"));

        // add this config to the set of tests to run
        builder.addServerConfig(config);

        /////////////////////////////////////////////////////////////
        // DELTA CATALOGS FOR TESTING
        /////////////////////////////////////////////////////////////

        // As catalogupdate-cluster-base but with security enabled. This requires users and groups..
        GroupInfo groups[] = new GroupInfo[] {new GroupInfo("group1", false, false, false)};
        UserInfo users[] = new UserInfo[] {new UserInfo("user1", "userpass1", new String[] {"group1"})};
        ProcedureInfo procInfo = new ProcedureInfo(new String[] {"group1"}, InsertNewOrder.class);

        config = new LocalCluster("catalogupdate-cluster-base-secure.jar", sitesPerHost, hosts, k, BackendTarget.NATIVE_EE_JNI);
        project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        project.addUsers(users);
        project.addGroups(groups);
        project.addProcedures(procInfo);
        project.setSecurityEnabled(true);
        boolean compile = config.compile(project);
        assertTrue(compile);
        MiscUtils.copyFile(project.getPathToDeployment(), Configuration.getPathToCatalogForTest("catalogupdate-cluster-base-secure.xml"));

        //config = new LocalSingleProcessServer("catalogupdate-local-addtables.jar", 2, BackendTarget.NATIVE_EE_JNI);
        config = new LocalCluster("catalogupdate-cluster-addtables.jar", sitesPerHost, hosts, k, BackendTarget.NATIVE_EE_JNI);
        project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addSchema(TestCatalogUpdateSuite.class.getResource("testorderby-ddl.sql").getPath());
        project.addDefaultPartitioning();
        project.addPartitionInfo("O1", "PKEY");
        project.addProcedures(BASEPROCS_OPROCS);
        compile = config.compile(project);
        assertTrue(compile);
        MiscUtils.copyFile(project.getPathToDeployment(), Configuration.getPathToCatalogForTest("catalogupdate-cluster-addtables.xml"));

        // as above but also with a materialized view added to O1
        try {
            config = new LocalCluster("catalogupdate-cluster-addtableswithmatview.jar", sitesPerHost, hosts, k, BackendTarget.NATIVE_EE_JNI);
            project = new TPCCProjectBuilder();
            project.addDefaultSchema();
            project.addSchema(TestCatalogUpdateSuite.class.getResource("testorderby-ddl.sql").getPath());
            project.addLiteralSchema("CREATE VIEW MATVIEW_O1(C1,NUM) AS SELECT A_INT, COUNT(*) FROM O1 GROUP BY A_INT;");
            project.addDefaultPartitioning();
            project.addPartitionInfo("O1", "PKEY");
            project.addProcedures(BASEPROCS_OPROCS);
            compile = config.compile(project);
            assertTrue(compile);
            MiscUtils.copyFile(project.getPathToDeployment(), Configuration.getPathToCatalogForTest("catalogupdate-cluster-addtableswithmatview.xml"));
        } catch (IOException e) {
            fail();
        }

        config = new LocalCluster("catalogupdate-cluster-addindex.jar", sitesPerHost, hosts, k, BackendTarget.NATIVE_EE_JNI);
        project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addLiteralSchema("CREATE INDEX NEWINDEX ON NEW_ORDER (NO_O_ID);");
        project.addDefaultPartitioning();
        project.addProcedures(BASEPROCS);
        compile = config.compile(project);
        assertTrue(compile);
        MiscUtils.copyFile(project.getPathToDeployment(), Configuration.getPathToCatalogForTest("catalogupdate-cluster-addindex.xml"));

        //config = new LocalSingleProcessServer("catalogupdate-local-expanded.jar", 2, BackendTarget.NATIVE_EE_JNI);
        config = new LocalCluster("catalogupdate-cluster-expanded.jar", sitesPerHost, hosts, k, BackendTarget.NATIVE_EE_JNI);
        project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        project.addProcedures(EXPANDEDPROCS);
        compile = config.compile(project);
        assertTrue(compile);
        MiscUtils.copyFile(project.getPathToDeployment(), Configuration.getPathToCatalogForTest("catalogupdate-cluster-expanded.xml"));

        //config = new LocalSingleProcessServer("catalogupdate-local-conflict.jar", 2, BackendTarget.NATIVE_EE_JNI);
        config = new LocalCluster("catalogupdate-cluster-conflict.jar", sitesPerHost, hosts, k, BackendTarget.NATIVE_EE_JNI);
        project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        project.addProcedures(CONFLICTPROCS);
        compile = config.compile(project);
        assertTrue(compile);
        MiscUtils.copyFile(project.getPathToDeployment(), Configuration.getPathToCatalogForTest("catalogupdate-cluster-conflict.xml"));

        //config = new LocalSingleProcessServer("catalogupdate-local-many.jar", 2, BackendTarget.NATIVE_EE_JNI);
        config = new LocalCluster("catalogupdate-cluster-many.jar", sitesPerHost, hosts, k, BackendTarget.NATIVE_EE_JNI);
        project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        project.addProcedures(SOMANYPROCS);
        compile = config.compile(project);
        assertTrue(compile);
        MiscUtils.copyFile(project.getPathToDeployment(), Configuration.getPathToCatalogForTest("catalogupdate-cluster-many.xml"));


        // A catalog change that enables snapshots
        config = new LocalCluster("catalogupdate-cluster-enable_snapshot.jar", sitesPerHost, hosts, k, BackendTarget.NATIVE_EE_JNI);
        project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        project.addProcedures(BASEPROCS);
        project.setSnapshotSettings( "1s", 3, "/tmp/snapshotdir1", "foo1");
        // build the jarfile
        compile = config.compile(project);
        assertTrue(compile);
        MiscUtils.copyFile(project.getPathToDeployment(), Configuration.getPathToCatalogForTest("catalogupdate-cluster-enable_snapshot.xml"));

        //Another catalog change to modify the schedule
        config = new LocalCluster("catalogupdate-cluster-change_snapshot.jar", sitesPerHost, hosts, k, BackendTarget.NATIVE_EE_JNI);
        project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        project.addProcedures(BASEPROCS);
        project.setSnapshotSettings( "1s", 3, "/tmp/snapshotdir2", "foo2");
        // build the jarfile
        compile = config.compile(project);
        assertTrue(compile);
        MiscUtils.copyFile(project.getPathToDeployment(), Configuration.getPathToCatalogForTest("catalogupdate-cluster-change_snapshot.xml"));

        //Another catalog change to modify the schedule
        config = new LocalCluster("catalogupdate-cluster-change_snapshot_dir_not_exist.jar", sitesPerHost, hosts, k, BackendTarget.NATIVE_EE_JNI);
        project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        project.addProcedures(BASEPROCS);
        project.setSnapshotSettings( "1s", 3, "/tmp/snapshotdirasda2", "foo2");
        // build the jarfile
        compile = config.compile(project);
        assertTrue(compile);
        MiscUtils.copyFile(project.getPathToDeployment(), Configuration.getPathToCatalogForTest("catalogupdate-cluster-change_snapshot_dir_not_exist.xml"));

        //A huge catalog update to test size limits
        config = new LocalCluster("catalogupdate-cluster-huge.jar", sitesPerHost, hosts, k, BackendTarget.NATIVE_EE_JNI);
        project = new TPCCProjectBuilder();
        long t = System.currentTimeMillis();
        String hugeSchemaURL = generateRandomDDL("catalogupdate-cluster-huge",
                                                  HUGE_TABLES, HUGE_COLUMNS, HUGE_NAME_SIZE);
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        project.addSchema(hugeSchemaURL);
        project.addProcedures(BASEPROCS);
        compile = config.compile(project);
        assertTrue(compile);
        hugeCompileElapsed = (System.currentTimeMillis() - t) / 1000.0;
        hugeCatalogXML = Configuration.getPathToCatalogForTest("catalogupdate-cluster-huge.xml");
        hugeCatalogJar = Configuration.getPathToCatalogForTest("catalogupdate-cluster-huge.jar");
        MiscUtils.copyFile(project.getPathToDeployment(), hugeCatalogXML);

        return builder;
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        assertTrue(callbackSuccess);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        callbackSuccess = true;
    }
}
