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
public class TestTrickyCatalogChanges extends RegressionSuite {

    static final int SITES_PER_HOST = 1;
    static final int HOSTS = 1;
    static final int K = 0;

    static String m_globalDeploymentURL = null;

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

    /**
     * Pretty weak sauce test for now that verifies adding a table with a pkey in the middle of
     * two other tables with pkey. This fails on 2.8.2.
     */
    public void testAddDropTable() throws IOException, ProcCallException, InterruptedException
    {
        Client client = getClient();
        ClientResponse cr;
        VoltTable result;

        cr = client.callProcedure("@Explain", "select col2 from p1 where col2 >1");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        System.out.println(result.toJSONString());

        // add a new table in the middle
        String newCatalogURL = Configuration.getPathToCatalogForTest("cheng-newindex.jar");
        VoltTable[] results = client.updateApplicationCatalog(new File(newCatalogURL), new File(m_globalDeploymentURL)).getResults();
        assertTrue(results.length == 1);

        try {
            cr = client.callProcedure("@Explain", "select col2 from p1 where col2 >1");
        }
        catch (ProcCallException e) {
            cr = e.getClientResponse();
            System.out.println(cr.getStatusString());
        }
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        System.out.println(result.toJSONString());

        cr = client.callProcedure("@Statistics", "INDEX", 0);
        assertTrue(cr.getStatus() == ClientResponse.SUCCESS);
        assertTrue(cr.getResults().length == 1);
        result = cr.getResults()[0];
        System.out.println(result.toString());
    }

    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestTrickyCatalogChanges(String name) {
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
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestTrickyCatalogChanges.class);

        String schema1 =
                "CREATE TABLE P1 (\n" +
                "col1 INTEGER DEFAULT '0' NOT NULL,\n" +
                "col2 INTEGER DEFAULT '0' NOT NULL,\n" +
                ");\n" +
                "CREATE INDEX P1_IDX_col21_n on P1 (col2, col1);";
        String schema2 =
                "CREATE TABLE P1 (\n" +
                "col1 INTEGER DEFAULT '0' NOT NULL,\n" +
                "col2 INTEGER DEFAULT '0' NOT NULL,\n" +
                ");\n" +
                "CREATE INDEX P1_IDX_col21_n on P1 (col2, col1);";

        /////////////////////////////////////////////////////////////
        // CONFIG #1: 1 Local Site/Partitions running on JNI backend
        /////////////////////////////////////////////////////////////

        // get a server config for the native backend with one sites/partitions
        VoltServerConfig config = new LocalCluster("cheng-base.jar", SITES_PER_HOST, HOSTS, K, BackendTarget.NATIVE_EE_JNI);
        ((LocalCluster) config).setHasLocalServer(true);

        // build up a project builder for the workload
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addLiteralSchema(schema1);
        //project.addStmtProcedure("foo", "select col2 from p1 where col2 >1");
        // build the jarfile
        boolean basecompile = config.compile(project);
        assertTrue(basecompile);
        m_globalDeploymentURL = Configuration.getPathToCatalogForTest("cheng.xml");
        MiscUtils.copyFile(project.getPathToDeployment(), m_globalDeploymentURL);

        // add this config to the set of tests to run
        builder.addServerConfig(config);

        /////////////////////////////////////////////////////////////
        // DELTA CATALOGS FOR TESTING
        /////////////////////////////////////////////////////////////

        config = new LocalCluster("cheng-newindex.jar", SITES_PER_HOST, HOSTS, K, BackendTarget.NATIVE_EE_JNI);
        project = new VoltProjectBuilder();
        project.addLiteralSchema(schema2);
        //project.addStmtProcedure("foo", "select col2 from p1 where col2 >1");
        boolean compile = config.compile(project);
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
