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
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.MiscUtils;

/**
 * Test some more catalog changes that are a bit tricky.
 * Limited for now.
 *
 */
public class TestLiveSchemaChanges extends RegressionSuite {

    static final int SITES_PER_HOST = 2;
    static final int HOSTS = 2;
    static final int K = 1;

    static String m_globalDeploymentURL = null;

    static String makeTable(String name, boolean pkey, boolean replicated, boolean oolString, boolean uniqueConstraint) {
        name = name.toUpperCase().trim();

        // CREATE TABLE
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(name).append(" (");
        sb.append("ID INTEGER NOT NULL, TINY TINYINT, SMALL SMALLINT, BIG BIGINT");
        if (uniqueConstraint) sb.append(" UNIQUE");
        sb.append(", ");
        if (oolString) sb.append("STRVAL VARCHAR(250)");
        else sb.append("STRVAL VARCHAR(60)");
        if (pkey) sb.append(", PRIMARY KEY (ID)");
        sb.append(");\n");

        // PARTITION TABLE
        if (!replicated) {
            sb.append("PARTITION TABLE ").append(name).append(" ON COLUMN ID;\n");
        }

        return sb.toString();
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

    public void loadSomeData(Client client, String tableName, int start, int count) throws IOException, ProcCallException {
        tableName = tableName.toUpperCase().trim();

        for (int i = start; i < (start + count); i++) {
            CatTestCallback callback = new CatTestCallback(ClientResponse.SUCCESS);
            client.callProcedure(callback,
                                 tableName + ".insert",
                                 i,
                                 (byte)(i % 128),
                                 (short)(i % 32768),
                                 i,
                                 String.valueOf(i));
        }
    }

    /**
     * Pretty weak sauce test for now that verifies adding a table with a pkey in the middle of
     * two other tables with pkey. This fails on 2.8.2.
     */
    public void testAddDropTable() throws IOException, ProcCallException, InterruptedException
    {
        Client client = getClient();
        loadSomeData(client, "P1", 0, 10);
        client.drain();
        assertTrue(callbackSuccess);

        // add a new table in the middle
        String newCatalogURL = Configuration.getPathToCatalogForTest("liveschema-newtableinmiddle.jar");
        VoltTable[] results = client.updateApplicationCatalog(new File(newCatalogURL), new File(m_globalDeploymentURL)).getResults();
        assertTrue(results.length == 1);

        loadSomeData(client, "P2", 10, 20);
        client.drain();
        assertTrue(callbackSuccess);

        loadSomeData(client, "P1", 10, 2000);
        client.drain();
        assertTrue(callbackSuccess);

        // revert to the original schema
        newCatalogURL = Configuration.getPathToCatalogForTest("liveschema-base.jar");
        results = client.updateApplicationCatalog(new File(newCatalogURL), new File(m_globalDeploymentURL)).getResults();
        assertTrue(results.length == 1);

        loadSomeData(client, "P1", 2010, 100);
        client.drain();
        assertTrue(callbackSuccess);
    }

    /**
     * Pretty weak sauce test for now that verifies adding a table with a pkey in the middle of
     * two other tables with pkey. This fails on 2.8.2.
     */
    public void testAddDropConstraint() throws IOException, ProcCallException, InterruptedException
    {
        Client client = getClient();
        loadSomeData(client, "P1", 0, 10);
        client.drain();
        assertTrue(callbackSuccess);

        // drop a unique constraint
        String newCatalogURL = Configuration.getPathToCatalogForTest("liveschema-dropconstraint.jar");
        VoltTable[] results = client.updateApplicationCatalog(new File(newCatalogURL), new File(m_globalDeploymentURL)).getResults();
        assertTrue(results.length == 1);

        loadSomeData(client, "P1", 10, 2000);
        client.drain();
        assertTrue(callbackSuccess);

        // add a constraint back in, supported by a unique index
        newCatalogURL = Configuration.getPathToCatalogForTest("liveschema-addconstraint.jar");
        results = client.updateApplicationCatalog(new File(newCatalogURL), new File(m_globalDeploymentURL)).getResults();
        assertTrue(results.length == 1);

        loadSomeData(client, "P1", 2010, 100);
        client.drain();
        assertTrue(callbackSuccess);

        // assume we can't revert to the base catalog, as it makes a unique constraint out of whole cloth
        newCatalogURL = Configuration.getPathToCatalogForTest("liveschema-base.jar");
        try {
            client.updateApplicationCatalog(new File(newCatalogURL), new File(m_globalDeploymentURL));
            fail();
        }
        catch (Exception e) {
            // assume this fails
        }
    }

    /**
     * Pretty weak sauce test for now that verifies adding a table with a pkey in the middle of
     * two other tables with pkey. This fails on 2.8.2.
     * @throws Exception
     */
    public void testRenameIndex() throws Exception
    {
        Client client = getClient();
        loadSomeData(client, "P1", 0, 10);
        client.drain();
        assertTrue(callbackSuccess);

        long tupleCount = TestCatalogUpdateSuite.indexEntryCountFromStats(client, "P1", "RLTY");
        assertTrue(tupleCount > 0);

        // rename the one user-created index in the table
        String newCatalogURL = Configuration.getPathToCatalogForTest("liveschema-renamedindex.jar");
        VoltTable[] results = client.updateApplicationCatalog(new File(newCatalogURL), new File(m_globalDeploymentURL)).getResults();
        assertTrue(results.length == 1);

        long newTupleCount = TestCatalogUpdateSuite.indexEntryCountFromStats(client, "P1", "RLTYX");
        assertTrue(tupleCount > 0);
        assertEquals(newTupleCount, tupleCount);

        loadSomeData(client, "P1", 10, 2000);
        client.drain();
        assertTrue(callbackSuccess);

        // rename the index back for fun
        newCatalogURL = Configuration.getPathToCatalogForTest("liveschema-base.jar");
        results = client.updateApplicationCatalog(new File(newCatalogURL), new File(m_globalDeploymentURL)).getResults();
        assertTrue(results.length == 1);

        loadSomeData(client, "P1", 2010, 100);
        client.drain();
        assertTrue(callbackSuccess);
    }

    /**
     * Variant of testRenameIndex to test structural identity checks for expression indexes.
     * @throws Exception
     */
    public void testRenameExpressionIndex() throws Exception
    {
        Client client = getClient();
        loadSomeData(client, "P1", 0, 10);
        client.drain();
        assertTrue(callbackSuccess);

        long tupleCount = TestCatalogUpdateSuite.indexEntryCountFromStats(client, "P1", "EXPRESS");
        assertTrue(tupleCount > 0);

        // rename the one user-created index in the table
        String newCatalogURL = Configuration.getPathToCatalogForTest("liveschema-renamedexpressionindex.jar");
        VoltTable[] results = client.updateApplicationCatalog(new File(newCatalogURL), new File(m_globalDeploymentURL)).getResults();
        assertTrue(results.length == 1);

        long newTupleCount = TestCatalogUpdateSuite.indexEntryCountFromStats(client, "P1", "EXPRESS_RENAMED");
        assertTrue(tupleCount > 0);
        assertEquals(newTupleCount, tupleCount);

        loadSomeData(client, "P1", 10, 2000);
        client.drain();
        assertTrue(callbackSuccess);

        // rename the index back for fun
        newCatalogURL = Configuration.getPathToCatalogForTest("liveschema-base.jar");
        results = client.updateApplicationCatalog(new File(newCatalogURL), new File(m_globalDeploymentURL)).getResults();
        assertTrue(results.length == 1);

        loadSomeData(client, "P1", 2010, 100);
        client.drain();
        assertTrue(callbackSuccess);
    }

    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestLiveSchemaChanges(String name) {
        super(name);
    }

    /**
     * Build a list of the tests that will be run when TestLiveSchemaChanges gets run by JUnit.
     * Use helper classes that are part of the RegressionSuite framework.
     *
     * @return The TestSuite containing all the tests to be run.
     * @throws Exception
     */
    static public Test suite() throws Exception {
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestLiveSchemaChanges.class);

        /////////////////////////////////////////////////////////////
        // CONFIG #1: 1 Local Site/Partitions running on JNI backend
        /////////////////////////////////////////////////////////////

        // get a server config for the native backend with one sites/partitions
        VoltServerConfig config = new LocalCluster("liveschema-base.jar", SITES_PER_HOST, HOSTS, K, BackendTarget.NATIVE_EE_JNI);
        ((LocalCluster) config).setHasLocalServer(true);

        // build up a project builder for the workload
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addLiteralSchema(makeTable("P1", true, false, false, true));
        project.addLiteralSchema(makeTable("R1", true, true, false, true));
        project.addLiteralSchema("CREATE UNIQUE INDEX RLTY ON P1 (BIG);");
        project.addLiteralSchema("CREATE UNIQUE INDEX EXPRESS ON P1 (ABS(SMALL));");
        // build the jarfile
        boolean basecompile = config.compile(project);
        assertTrue(basecompile);
        m_globalDeploymentURL = Configuration.getPathToCatalogForTest("liveschema.xml");
        MiscUtils.copyFile(project.getPathToDeployment(), m_globalDeploymentURL);

        // add this config to the set of tests to run
        builder.addServerConfig(config);

        /////////////////////////////////////////////////////////////
        // DELTA CATALOGS FOR TESTING
        /////////////////////////////////////////////////////////////

        config = new LocalCluster("liveschema-newtableinmiddle.jar", SITES_PER_HOST, HOSTS, K, BackendTarget.NATIVE_EE_JNI);
        project = new VoltProjectBuilder();
        project.addLiteralSchema(makeTable("P1", true, false, false, true));
        project.addLiteralSchema(makeTable("P2", true, false, false, false));
        project.addLiteralSchema(makeTable("R1", true, true, false, true));
        project.addLiteralSchema("CREATE UNIQUE INDEX EXPRESS ON P1 (ABS(SMALL));");
        boolean compile = config.compile(project);
        assertTrue(compile);

        config = new LocalCluster("liveschema-dropconstraint.jar", SITES_PER_HOST, HOSTS, K, BackendTarget.NATIVE_EE_JNI);
        project = new VoltProjectBuilder();
        project.addLiteralSchema(makeTable("P1", true, false, false, false));
        project.addLiteralSchema(makeTable("R1", true, true, false, false));
        project.addLiteralSchema("CREATE UNIQUE INDEX RLTY ON P1 (BIG);");
        project.addLiteralSchema("CREATE UNIQUE INDEX EXPRESS ON P1 (ABS(SMALL));");
        compile = config.compile(project);
        assertTrue(compile);

        config = new LocalCluster("liveschema-addconstraint.jar", SITES_PER_HOST, HOSTS, K, BackendTarget.NATIVE_EE_JNI);
        project = new VoltProjectBuilder();
        project.addLiteralSchema(makeTable("P1", true, false, false, true));
        project.addLiteralSchema(makeTable("R1", true, true, false, false));
        project.addLiteralSchema("CREATE UNIQUE INDEX RLTY ON P1 (BIG);");
        project.addLiteralSchema("CREATE UNIQUE INDEX EXPRESS ON P1 (ABS(SMALL));");
        compile = config.compile(project);
        assertTrue(compile);

        config = new LocalCluster("liveschema-renamedindex.jar", SITES_PER_HOST, HOSTS, K, BackendTarget.NATIVE_EE_JNI);
        project = new VoltProjectBuilder();
        project.addLiteralSchema(makeTable("P1", true, false, false, true));
        project.addLiteralSchema(makeTable("R1", true, true, false, true));
        project.addLiteralSchema("CREATE UNIQUE INDEX RLTYX ON P1 (BIG);");
        project.addLiteralSchema("CREATE UNIQUE INDEX EXPRESS ON P1 (ABS(SMALL));");
        compile = config.compile(project);
        assertTrue(compile);

        config = new LocalCluster("liveschema-renamedexpressionindex.jar", SITES_PER_HOST, HOSTS, K, BackendTarget.NATIVE_EE_JNI);
        project = new VoltProjectBuilder();
        project.addLiteralSchema(makeTable("P1", true, false, false, true));
        project.addLiteralSchema(makeTable("R1", true, true, false, true));
        project.addLiteralSchema("CREATE UNIQUE INDEX RLTY ON P1 (BIG);");
        project.addLiteralSchema("CREATE UNIQUE INDEX EXPRESS_RENAMED ON P1 (ABS(SMALL));");
        compile = config.compile(project);
        assertTrue(compile);

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
