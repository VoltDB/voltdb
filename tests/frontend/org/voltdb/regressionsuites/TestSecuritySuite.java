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

package org.voltdb.regressionsuites;

import java.io.IOException;
import java.util.ArrayList;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder.GroupInfo;
import org.voltdb.compiler.VoltProjectBuilder.ProcedureInfo;
import org.voltdb.compiler.VoltProjectBuilder.UserInfo;
import org.voltdb.export.ExportTestClient;
import org.voltdb.exportclient.ExportClientException;
import org.voltdb_testprocs.regressionsuites.securityprocs.DoNothing1;
import org.voltdb_testprocs.regressionsuites.securityprocs.DoNothing2;
import org.voltdb_testprocs.regressionsuites.securityprocs.DoNothing3;

public class TestSecuritySuite extends RegressionSuite {

    // procedures used by these tests
    static final Class<?>[] PROCEDURES = {
        DoNothing1.class,
        DoNothing2.class,
        DoNothing3.class
    };

    public TestSecuritySuite(String name) {
        super(name);
    }

    public void testAuthentication() throws IOException {
        //Test failed auth
        this.m_username = "user1";
        this.m_password = "wrongpassword";
        boolean exceptionThrown = false;
        try {
            getClient();
        } catch (IOException e) {
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);

        //Test success
        this.m_password = "password";
        getClient();

        //Test failure again with wrong username
        this.m_username = "wronguser";
        exceptionThrown = false;
        try {
            getClient();
        } catch (IOException e) {
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);
    }

    public void testSysprocAndAdhocPermissions() throws Exception {
        Client client;
        boolean exceptionThrown;
        VoltTable modCount;
        VoltTable[] results;

        // user1 can't run anything
        m_username = "user1";
        client = getClient();
        exceptionThrown = false;
        try {
            modCount = client.callProcedure("@AdHoc", "INSERT INTO NEW_ORDER VALUES (2, 2, 2);").getResults()[0];
        } catch (ProcCallException e) {
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);
        exceptionThrown = false;
        try {
            results = client.callProcedure("@Statistics", "INITIATOR", 0).getResults();
        } catch (ProcCallException e) {
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);

        // user2 can run sysprocs due to his group
        m_username = "user2";
        client = getClient();
        modCount = client.callProcedure("@AdHoc", "INSERT INTO NEW_ORDER VALUES (4, 4, 4);").getResults()[0];
        assertTrue(modCount.getRowCount() == 1);
        assertTrue(modCount.asScalarLong() == 1);
        results = client.callProcedure("@Statistics", "INITIATOR", 0).getResults();
        // one aggregate table returned
        assertTrue(results.length == 1);

        // user3 can only run adhoc due to his group
        m_username = "user3";
        client = getClient();
        modCount = client.callProcedure("@AdHoc", "INSERT INTO NEW_ORDER VALUES (5, 5, 5);").getResults()[0];
        assertTrue(modCount.getRowCount() == 1);
        assertTrue(modCount.asScalarLong() == 1);
        exceptionThrown = false;
        try {
            results = client.callProcedure("@Statistics", "INITIATOR", 0).getResults();
        } catch (ProcCallException e) {
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);

        // user4 can do anything due to his group
        m_username = "user4";
        client = getClient();
        modCount = client.callProcedure("@AdHoc", "INSERT INTO NEW_ORDER VALUES (6, 6, 6);").getResults()[0];
        assertTrue(modCount.getRowCount() == 1);
        assertTrue(modCount.asScalarLong() == 1);
        results = client.callProcedure("@Statistics", "INITIATOR", 0).getResults();
        // one aggregate table returned
        assertTrue(results.length == 1);
    }

    public void testProcedurePermissions() throws Exception {
        Client client;
        boolean exceptionThrown;

        // user1 should be able to invoke 2 and 3
        m_username = "user1";
        client = getClient();
        exceptionThrown = false;
        try {
            client.callProcedure("DoNothing1", 1);
        } catch (ProcCallException e) {
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);
        client.callProcedure("DoNothing2", 1);
        client.callProcedure("DoNothing3", 1);

        // user2 should be able to invoke 3
        m_username = "user2";
        client = getClient();
        exceptionThrown = false;
        try {
            client.callProcedure("DoNothing1", 1);
        } catch (ProcCallException e) {
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);
        exceptionThrown = false;
        try {
            client.callProcedure("DoNothing2", 1);
        } catch (ProcCallException e) {
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);
        client.callProcedure("DoNothing3", 1);

        // user3 shouldn't be able to invoke any
        m_username = "user3";
        client = getClient();
        exceptionThrown = false;
        try {
            client.callProcedure("DoNothing1", 1);
        } catch (ProcCallException e) {
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);
        exceptionThrown = false;
        try {
            client.callProcedure("DoNothing2", 1);
        } catch (ProcCallException e) {
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);
        exceptionThrown = false;
        try {
            client.callProcedure("DoNothing3", 1);
        } catch (ProcCallException e) {
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);

        // user3 shouldn't gleam much info from a made up proc
        try {
            client.callProcedure("RyanLikesTheYankees", 1);
        } catch (ProcCallException e) {
            assertFalse(e.getMessage().contains("lost before a response was received"));
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);
    }

    public void testAllowedExportConnectorPermissions() throws ExportClientException {
        // user1 can connect (in groups list)
        ExportTestClient eclient = new ExportTestClient(1);
        eclient.addCredentials("user1", "password");
        eclient.connect();
        eclient.disconnect();

        // Expected to throw an exception on failure
        assertTrue(true);
    }

    public void testRejectedExportConnectorPermissions() {
        boolean caught = false;
        ExportTestClient eclient = new ExportTestClient(1);
        try {
            // bad group
            eclient.addCredentials("user2", "password");
            eclient.connect();
        }
        catch (ExportClientException e) {
            caught = true;
        }
        assertTrue(caught);

        eclient.disconnect();
    }

    /**
     * Build a list of the tests that will be run when TestSecuritySuite gets run by JUnit.
     * Use helper classes that are part of the RegressionSuite framework.
     * This particular class runs all tests on the the local JNI backend with both
     * one and two partition configurations, as well as on the hsql backend.
     *
     * @return The TestSuite containing all the tests to be run.
     */
    static public Test suite() {
        VoltServerConfig config = null;

        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestSecuritySuite.class);

        // build up a project builder for the workload
        TPCCProjectBuilder project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        ArrayList<ProcedureInfo> procedures = new ArrayList<ProcedureInfo>();
        procedures.add(new ProcedureInfo(new String[0], PROCEDURES[0]));
        procedures.add(new ProcedureInfo(new String[] {"group1"}, PROCEDURES[1]));
        procedures.add(new ProcedureInfo(new String[] {"group1", "group2"}, PROCEDURES[2]));
        project.addProcedures(procedures);

        UserInfo users[] = new UserInfo[] {
                new UserInfo("user1", "password", new String[] {"group1"}),
                new UserInfo("user2", "password", new String[] {"group2"}),
                new UserInfo("user3", "password", new String[] {"group3"}),
                new UserInfo("user4", "password", new String[] {"group4"})
        };
        project.addUsers(users);

        GroupInfo groups[] = new GroupInfo[] {
                new GroupInfo("group1", false, false),
                new GroupInfo("group2", false, true),
                new GroupInfo("group3", true, false),
                new GroupInfo("group4", true, true)
        };
        project.addGroups(groups);
        project.setSecurityEnabled(true);

        ArrayList<String> elgroups = new ArrayList<String>();
        elgroups.add("group1");

        project.addExport("org.voltdb.export.processors.RawProcessor", true /*enabled*/, elgroups);

        /////////////////////////////////////////////////////////////
        // CONFIG #1: 1 Local Site/Partitions running on JNI backend
        /////////////////////////////////////////////////////////////

        // get a server config for the native backend with one sites/partitions
        config = new LocalSingleProcessServer("security-onesite.jar", 1, BackendTarget.NATIVE_EE_JNI);

        // build the jarfile
        config.compile(project);

        // add this config to the set of tests to run
        builder.addServerConfig(config);

        // Cluster
        config = new LocalCluster("security-cluster.jar", 2, 2,
                                  1, BackendTarget.NATIVE_EE_JNI);
        config.compile(project);
        builder.addServerConfig(config);

        return builder;
    }

    public static void main(String args[]) {
        org.junit.runner.JUnitCore.runClasses(TestRollbackSuite.class);
    }
}
