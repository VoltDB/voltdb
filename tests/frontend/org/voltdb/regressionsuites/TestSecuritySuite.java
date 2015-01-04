/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.VoltProjectBuilder.RoleInfo;
import org.voltdb.compiler.VoltProjectBuilder.ProcedureInfo;
import org.voltdb.compiler.VoltProjectBuilder.UserInfo;
import org.voltdb.utils.MiscUtils;
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
            results = client.callProcedure("@Quiesce").getResults();
        } catch (ProcCallException e) {
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);

        // user2 can run adhoc due to his group
        m_username = "user2";
        client = getClient();
        modCount = client.callProcedure("@AdHoc", "INSERT INTO NEW_ORDER VALUES (4, 4, 4);").getResults()[0];
        assertTrue(modCount.getRowCount() == 1);
        assertTrue(modCount.asScalarLong() == 1);

        // user3 can only run adhoc due to his group
        m_username = "user3";
        client = getClient();
        modCount = client.callProcedure("@AdHoc", "INSERT INTO NEW_ORDER VALUES (5, 5, 5);").getResults()[0];
        assertTrue(modCount.getRowCount() == 1);
        assertTrue(modCount.asScalarLong() == 1);
        exceptionThrown = false;
        try {
            results = client.callProcedure("@Quiesce").getResults();
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
        results = client.callProcedure("@Quiesce").getResults();
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

        //"userWithAllProc" should be able to call any user procs but not RW sysprocs
        m_username = "userWithAllProc";
        client = getClient();
        client.callProcedure("DoNothing1", 1);
        client.callProcedure("DoNothing2", 1);
        client.callProcedure("DoNothing3", 1);

        //We should not be able to call RW sysproc
        exceptionThrown = false;
        try {
            client.callProcedure("@Quiesce").getResults();
        } catch (ProcCallException e) {
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);

        // users shouldn't gleam much info from a made up proc
        exceptionThrown = false;
        try {
            client.callProcedure("RyanLikesTheYankees", 1);
        } catch (ProcCallException e) {
            assertFalse(e.getMessage().contains("lost before a response was received"));
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);

    }

    // Tests permissions applied to auto-generated default CRUD procedures.
    public void testDefaultProcPermissions() throws Exception {
        Client client;
        boolean exceptionThrown;

        // userWithDefaultProcPerm is allowed to invoke default CRUD procs
        m_username = "userWithDefaultProcPerm";
        client = getClient();
        exceptionThrown = false;
        try {
            client.callProcedure("NEW_ORDER.insert", 100, 100, 100);
        } catch (ProcCallException e) {
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertFalse(exceptionThrown);

        // userWithoutDefaultProcPerm is not allowed to invoke default CRUD procs
        m_username = "userWithoutDefaultProcPerm";
        client = getClient();
        exceptionThrown = false;
        try {
            client.callProcedure("NEW_ORDER.insert", 101, 101, 101);
        } catch (ProcCallException e) {
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);
    }

    // Tests permissions applied to auto-generated read/write CRUD procedures.
    public void testDefaultProcReadPermissions() throws Exception {
        Client client;
        boolean exceptionThrown;

        // userWithDefaultProcReadPerm is not allowed to invoke write CRUD procs
        m_username = "userWithDefaultProcReadPerm";
        client = getClient();
        exceptionThrown = false;
        try {
            client.callProcedure("NEW_ORDER.insert", 100, 100, 100);
        } catch (ProcCallException e) {
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);

        // userWithoutDefaultProcReadPerm is  allowed to invoke read CRUD procs
        m_username = "userWithDefaultProcReadPerm";
        client = getClient();
        exceptionThrown = false;
        try {
            client.callProcedure("NEW_ORDER.select", 0, 0, 0);
        } catch (ProcCallException e) {
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertFalse(exceptionThrown);
    }

    public void testReadOnlyProcs() throws Exception {
        m_username = "user1";
        m_password = "password";
        Client client = getClient();

        byte result = client.callProcedure("@SystemCatalog", "TABLES").getStatus();
        assertEquals(ClientResponse.SUCCESS, result);

        result = client.callProcedure("@Statistics", "INITIATOR", 0).getStatus();
        assertEquals(ClientResponse.SUCCESS, result);

        result = client.callProcedure("@Subscribe", "TOPOLOGY").getStatus();
        assertEquals(ClientResponse.SUCCESS, result);

        result = client.callProcedure("@GetPartitionKeys", "INTEGER").getStatus();
        assertEquals(ClientResponse.SUCCESS, result);

        result = client.callProcedure("@SystemInformation").getStatus();
        assertEquals(ClientResponse.SUCCESS, result);
    }

    public void testAdmin() throws Exception {
        m_username = "user4";
        m_password = "password";
        Client client = getClient();

        // adhoc
        VoltTable modCount = client.callProcedure("@AdHoc", "INSERT INTO NEW_ORDER VALUES (4, 4, 4);").getResults()[0];
        assertTrue(modCount.getRowCount() == 1);
        assertTrue(modCount.asScalarLong() == 1);

        // sysproc
        assertEquals(ClientResponse.SUCCESS, client.callProcedure("@Quiesce").getStatus());

        // default proc
        assertEquals(ClientResponse.SUCCESS, client.callProcedure("NEW_ORDER.insert", 100, 100, 100).getStatus());

        // user proc
        assertEquals(ClientResponse.SUCCESS, client.callProcedure("DoNothing3", 1).getStatus());
    }

    public void testDefaultUser() throws Exception
    {
        m_username = "userWithDefaultUserPerm";
        m_password = "password";
        Client client = getClient();

        // adhoc
        VoltTable modCount = client.callProcedure("@AdHoc", "INSERT INTO NEW_ORDER VALUES (4, 4, 4);").getResults()[0];
        assertTrue(modCount.getRowCount() == 1);
        assertTrue(modCount.asScalarLong() == 1);
        // read-only adhoc
        modCount = client.callProcedure("@AdHoc", "SELECT COUNT(*) FROM NEW_ORDER;").getResults()[0];
        assertTrue(modCount.getRowCount() == 1);
        assertTrue(modCount.asScalarLong() == 1);

        // user proc
        assertEquals(ClientResponse.SUCCESS, client.callProcedure("DoNothing3", 1).getStatus());

        // sysproc
        try {
            client.callProcedure("@Quiesce").getStatus();
            fail("Should not allow RW sysproc");
        } catch (ProcCallException e) {}
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
                new UserInfo("user1", "password", new String[] {"grouP1"}),
                new UserInfo("user2", "password", new String[] {"grouP2"}),
                new UserInfo("user3", "password", new String[] {"grouP3"}),
                new UserInfo("user4", "password", new String[] {"AdMINISTRATOR"}),
                new UserInfo("userWithDefaultUserPerm", "password", new String[] {"User"}),
                new UserInfo("userWithAllProc", "password", new String[] {"GroupWithAllProcPerm"}),
                new UserInfo("userWithDefaultProcPerm", "password", new String[] {"groupWithDefaultProcPerm"}),
                new UserInfo("userWithoutDefaultProcPerm", "password", new String[] {"groupWiThoutDefaultProcPerm"}),
                new UserInfo("userWithDefaultProcReadPerm", "password", new String[] {"groupWiThDefaultProcReadPerm"})
        };
        project.addUsers(users);

        RoleInfo groups[] = new RoleInfo[] {
                new RoleInfo("Group1", false, false, false, false, false, false),
                new RoleInfo("Group2", true, false, false, false, false, false),
                new RoleInfo("Group3", true, false, false, false, false, false),
                new RoleInfo("GroupWithDefaultUserPerm", true, false, false, false, false, true),
                new RoleInfo("GroupWithAllProcPerm", false, false, false, false, false, true),
                new RoleInfo("GroupWithDefaultProcPerm", false, false, false, true, false, false),
                new RoleInfo("GroupWithoutDefaultProcPerm", false, false, false, false, false, false),
                new RoleInfo("GroupWithDefaultProcReadPerm", false, false, false, false, true, false)
        };
        project.addRoles(groups);
        // suite defines its own ADMINISTRATOR user
        project.setSecurityEnabled(true, false);

        // export disabled in community
        if (MiscUtils.isPro()) {
            project.addExport(true /*enabled*/);
        }

        /////////////////////////////////////////////////////////////
        // CONFIG #1: 1 Local Site/Partitions running on JNI backend
        /////////////////////////////////////////////////////////////

        // get a server config for the native backend with one sites/partitions
        config = new LocalCluster("security-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);

        // build the jarfile
        if (!config.compile(project)) fail();

        // add this config to the set of tests to run
        builder.addServerConfig(config);

        // Not testing a cluster and assuming security shouldn't be affected by this

        return builder;
    }

    public static void main(String args[]) {
        org.junit.runner.JUnitCore.runClasses(TestRollbackSuite.class);
    }
}
