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

import junit.framework.Test;

import java.io.IOException;
import java.util.ArrayList;
import org.voltdb.compiler.VoltProjectBuilder.*;

import org.voltdb.BackendTarget;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.regressionsuites.securityprocs.*;
import org.voltdb.client.Client;
import org.voltdb.client.*;
import org.voltdb.VoltTable;

public class TestSecuritySuite extends RegressionSuite {

    // procedures used by these tests
    static final Class<?>[] PROCEDURES = {
        DoNothing1.class,
        DoNothing2.class,
        DoNothing3.class,
        DoNothing4.class
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
        //User1 should be able to do both
        m_username = "user1";
        Client client = getClient();
        VoltTable modCount = client.callProcedure("@AdHoc", "INSERT INTO NEW_ORDER VALUES (1, 1, 1);")[0];
        assertTrue(modCount.getRowCount() == 1);
        assertTrue(modCount.asScalarLong() == 1);
        VoltTable results[] = client.callProcedure("@Statistics", "INITIATOR");
        // one aggregate table returned
        assertTrue(results.length == 1);

        //User2 can only do adhoc
        m_username = "user2";
        client = getClient();
        modCount = client.callProcedure("@AdHoc", "INSERT INTO NEW_ORDER VALUES (2, 2, 2);")[0];
        assertTrue(modCount.getRowCount() == 1);
        assertTrue(modCount.asScalarLong() == 1);
        boolean exceptionThrown = false;
        try {
            results = client.callProcedure("@Statistics", "INITIATOR");
        } catch (ProcCallException e) {
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);

        //User3 can "only" run sysprocs which includes adhoc
        m_username = "user3";
        client = getClient();
        modCount = client.callProcedure("@AdHoc", "INSERT INTO NEW_ORDER VALUES (3, 3, 3);")[0];
        assertTrue(modCount.getRowCount() == 1);
        assertTrue(modCount.asScalarLong() == 1);
        results = client.callProcedure("@Statistics", "INITIATOR");
        // one aggregate table returned
        assertTrue(results.length == 1);

        //User 4 can't run anything
        m_username = "user4";
        client = getClient();
        exceptionThrown = false;
        try {
            modCount = client.callProcedure("@AdHoc", "INSERT INTO NEW_ORDER VALUES (2, 2, 2);")[0];
        } catch (ProcCallException e) {
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);
        exceptionThrown = false;
        try {
            results = client.callProcedure("@Statistics", "INITIATOR");
        } catch (ProcCallException e) {
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);

        //User 5 can run sysprocs due to his group
        m_username = "user5";
        client = getClient();
        modCount = client.callProcedure("@AdHoc", "INSERT INTO NEW_ORDER VALUES (4, 4, 4);")[0];
        assertTrue(modCount.getRowCount() == 1);
        assertTrue(modCount.asScalarLong() == 1);
        results = client.callProcedure("@Statistics", "INITIATOR");
        // one aggregate table returned
        assertTrue(results.length == 1);

        //User 6 can only run adhoc due to his group
        m_username = "user6";
        client = getClient();
        modCount = client.callProcedure("@AdHoc", "INSERT INTO NEW_ORDER VALUES (5, 5, 5);")[0];
        assertTrue(modCount.getRowCount() == 1);
        assertTrue(modCount.asScalarLong() == 1);
        exceptionThrown = false;
        try {
            results = client.callProcedure("@Statistics", "INITIATOR");
        } catch (ProcCallException e) {
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);

        //User 7 can do anything due to his group
        m_username = "user7";
        client = getClient();
        modCount = client.callProcedure("@AdHoc", "INSERT INTO NEW_ORDER VALUES (6, 6, 6);")[0];
        assertTrue(modCount.getRowCount() == 1);
        assertTrue(modCount.asScalarLong() == 1);
        results = client.callProcedure("@Statistics", "INITIATOR");
        // one aggregate table returned
        assertTrue(results.length == 1);
    }

    public void testProcedurePermissions() throws Exception {
        //User1 should be able to invoke all procedures except the last one
        m_username = "user1";
        Client client = getClient();
        client.callProcedure("DoNothing1", 1);
        client.callProcedure("DoNothing2", 1);
        client.callProcedure("DoNothing3", 1);
        boolean exceptionThrown = false;
        try {
            client.callProcedure("DoNothing4", 1);
        } catch (ProcCallException e) {
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);

        //User2 should be able to invoke 1 and 3
        m_username = "user2";
        client = getClient();
        client.callProcedure("DoNothing1", 1);
        exceptionThrown = false;
        try {
            client.callProcedure("DoNothing2", 1);
        } catch (ProcCallException e) {
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);
        client.callProcedure("DoNothing3", 1);
        exceptionThrown = false;
        try {
            client.callProcedure("DoNothing4", 1);
        } catch (ProcCallException e) {
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);

        //User 5 should be able to invoke #1
        m_username = "user5";
        client = getClient();
        client.callProcedure("DoNothing1", 1);
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
        exceptionThrown = false;
        try {
            client.callProcedure("DoNothing4", 1);
        } catch (ProcCallException e) {
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);

        //User 6 should able to invoke 1 and 3
        m_username = "user6";
        client = getClient();
        client.callProcedure("DoNothing1", 1);
        exceptionThrown = false;
        try {
            client.callProcedure("DoNothing2", 1);
        } catch (ProcCallException e) {
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);
        client.callProcedure("DoNothing3", 1);
        exceptionThrown = false;
        try {
            client.callProcedure("DoNothing4", 1);
        } catch (ProcCallException e) {
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);

        //User 7 should be able to invoke 3
        m_username = "user7";
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
        exceptionThrown = false;
        try {
            client.callProcedure("DoNothing4", 1);
        } catch (ProcCallException e) {
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);
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
        procedures.add(new ProcedureInfo(new String[] { "user1", "user5", "user6" }, new String[] { "group1" }, PROCEDURES[0]));
        procedures.add(new ProcedureInfo(new String[] { "user1" }, new String[0], PROCEDURES[1]));
        procedures.add(new ProcedureInfo(new String[0], new String[] { "group1", "group4", "group5" }, PROCEDURES[2]));
        procedures.add(new ProcedureInfo(new String[0], new String[0], PROCEDURES[3]));
        project.addProcedures(procedures);

        UserInfo users[] = new UserInfo[] {
                new UserInfo("user1", true, true, "password", new String[] {"group1"}),
                new UserInfo("user2", true, false, "password", new String[]{"group1"}),
                new UserInfo("user3", false, true, "password", new String[]{"group2"}),
                new UserInfo("user4", false, false, "password", new String[]{"group2"}),
                new UserInfo("user5", false, false, "password", new String[] { "group3" }),
                new UserInfo("user6", false, false, "password", new String[] { "group4" }),
                new UserInfo("user7", false, false, "password", new String[] { "group5" })
        };
        project.addUsers(users);

        GroupInfo groups[] = new GroupInfo[] {
                new GroupInfo("group1", false, false),
                new GroupInfo("group2", false, false),
                new GroupInfo("group3", false, true),
                new GroupInfo("group4", true, false),
                new GroupInfo("group5", true, true)
        };
        project.addGroups(groups);
        project.setSecurityEnabled(true);

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
