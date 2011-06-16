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
import java.util.HashMap;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.compiler.VoltProjectBuilder.GroupInfo;
import org.voltdb.compiler.VoltProjectBuilder.UserInfo;

public class TestSystemInformationSuite extends RegressionSuite {

    public TestSystemInformationSuite(String name) {
        super(name);
    }

    public void testInvalidSelector() throws IOException
    {
        Client client = getClient();
        try
        {
            client.callProcedure("@SystemInformation", "NONSENSE");
        }
        catch (ProcCallException pce)
        {
            assertTrue(pce.getMessage().contains("Invalid @SystemInformation selector NONSENSE"));
            return;
        }
        fail("Invalid selector should have resulted in a ProcCallException but didn't");
    }

    public void testOverviewSelector() throws IOException, ProcCallException
    {
        Client client = getClient();
        VoltTable[] results = client.callProcedure("@SystemInformation").getResults();
        System.out.println(results[0]);
        VoltTable[] results2 = client.callProcedure("@SystemInformation", "OVERVIEW").getResults();
        assertTrue(results[0].hasSameContents(results2[0]));
    }

    public void testDeploymentSelector() throws IOException, ProcCallException
    {
        Client client = getClient();
        VoltTable[] results = client.callProcedure("@SystemInformation", "DEPLOYMENT").getResults();
        System.out.println(results[0]);
        HashMap<String, String> sysinfo = new HashMap<String, String>();
        while (results[0].advanceRow())
        {
            sysinfo.put(results[0].getString(0), results[0].getString(1));
        }
        System.out.println(sysinfo.toString());
        // We'll be lame for now and just check some easy stuff
        // This is all horribly hardcoded for now.  Would be great to
        // add accessors to VoltServerConfig and VoltProjectBuilder to get them
        // at some point, maybe
        assertEquals(m_expectedVals.get("adminport"), sysinfo.get("adminport"));
        assertEquals(m_expectedVals.get("adminstartup"), sysinfo.get("adminstartup"));
        assertEquals(m_expectedVals.get("heartbeattimeout"), sysinfo.get("heartbeattimeout"));
        assertEquals(m_expectedVals.get("partitiondetection"), sysinfo.get("partitiondetection"));
        assertEquals(m_expectedVals.get("partitiondetectionsnapshotprefix"),
                     sysinfo.get("partitiondetectionsnapshotprefix"));
        assertEquals(m_expectedVals.get("export"), sysinfo.get("export"));
        assertEquals(m_expectedVals.get("snapshotenabled"), sysinfo.get("snapshotenabled"));
        assertEquals(m_expectedVals.get("snapshotfrequency"), sysinfo.get("snapshotfrequency"));
        assertEquals(m_expectedVals.get("snapshotretain"), sysinfo.get("snapshotretain"));
        assertEquals(m_expectedVals.get("snapshotprefix"), sysinfo.get("snapshotprefix"));
        assertEquals(m_expectedVals.get("httpenabled"), sysinfo.get("httpenabled"));
        assertEquals(m_expectedVals.get("jsonenabled"), sysinfo.get("jsonenabled"));
    }

    static HashMap<String, String> m_expectedVals = new HashMap<String, String>();

    //
    // Build a list of the tests to be run. Use the regression suite
    // helpers to allow multiple backends.
    // JUnit magic that uses the regression suite helper classes.
    //
    static public Test suite() throws IOException
    {
        VoltServerConfig config = null;

        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestSystemInformationSuite.class);

        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addLiteralSchema("CREATE TABLE T(A1 INTEGER NOT NULL, A2 INTEGER, PRIMARY KEY(A1));");
        project.addPartitionInfo("T", "A1");
        project.addStmtProcedure("InsertA", "INSERT INTO T VALUES(?,?);", "T.A1: 0");

        // Add groups and users
        GroupInfo[] groups = new GroupInfo[2];
        groups[0] = new GroupInfo("admins", true, true);
        groups[1] = new GroupInfo("users", false, false);
        project.addGroups(groups);
        UserInfo[] users = new UserInfo[3];
        users[0] = new UserInfo("joeadmin", "adminpass",
                                new String[] { new String("admins"),
                                               new String("users") });
        users[1] = new UserInfo("joeuser", "userpass",
                                new String[] { new String("users") });
        users[2] = new UserInfo("why:would;you\\dothis", "dummy",
                                new String[] { new String("users") });
        project.addUsers(users);

        // Add snapshots
        m_expectedVals.put("snapshotenabled", "true");
        m_expectedVals.put("snapshotfrequency", "10s");
        m_expectedVals.put("snapshotretain", "5");
        m_expectedVals.put("snapshotpath", "/tmp");
        m_expectedVals.put("snapshotprefix", "dude");
        project.setSnapshotSettings(m_expectedVals.get("snapshotfrequency"),
                                    Integer.valueOf(m_expectedVals.get("snapshotretain")),
                                    m_expectedVals.get("snapshotpath"),
                                    m_expectedVals.get("snapshotprefix"));


        // Add partition detection
        m_expectedVals.put("partitiondetection", "true");
        m_expectedVals.put("partitiondetectionsnapshotprefix", "/tmp");
        project.setPartitionDetectionSettings(m_expectedVals.get("snapshotpath"),
                                              m_expectedVals.get("partitiondetectionsnapshotprefix"));

        // Add export
        m_expectedVals.put("export", "true");
        project.addExport("org.voltdb.export.processors.RawProcessor", true, null);

        // Add command logging
        // XXX currently broken because we can't restart the server
        // when the command log segment files exist without doing the
        // work to implement create database on startup rather than recover
        m_expectedVals.put("commandlogenabled", "false");
        //m_expectedVals.put("commandlogenabled", "true");
        //m_expectedVals.put("commandlogmode", "sync");
        //m_expectedVals.put("commandlogfreqtime", "500");
        //m_expectedVals.put("commandlogfreqtxns", "50000");
        //m_expectedVals.put("commandlogpath", "/tmp");
        //m_expectedVals.put("commandlogsnapshotpath", "/tmp");
        //project.configureLogging(m_expectedVals.get("commandlogsnapshotpath"),
        //                         m_expectedVals.get("commandlogpath"),
        //                         m_expectedVals.get("commandlogmode").equals("sync"),
        //                         m_expectedVals.get("commandlogenabled").equals("true"),
        //                         Integer.valueOf(m_expectedVals.get("commandlogfreqtime")),
        //                         Integer.valueOf(m_expectedVals.get("commandlogfreqtxns")));

        // Add other defaults
        m_expectedVals.put("adminport", "21211");
        m_expectedVals.put("adminstartup", "false");
        m_expectedVals.put("heartbeattimeout", "10");
        m_expectedVals.put("httpenabled", "false");
        m_expectedVals.put("jsonenabled", "false");

        config = new LocalSingleProcessServer("getclusterinfo-twosites.jar", 2,
                                              BackendTarget.NATIVE_EE_JNI);
        config.compile(project);
        builder.addServerConfig(config);

        LocalCluster lcconfig = new LocalCluster("getclusterinfo-cluster.jar", 2, 2, 1,
                                               BackendTarget.NATIVE_EE_JNI);
        lcconfig.compile(project);
        // need no local server so we set the VoltFileRoot property properly
        lcconfig.setHasLocalServer(false);
        builder.addServerConfig(lcconfig);

        return builder;
    }
}


