/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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


import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.voltdb.BackendTarget;
import org.voltdb.ServerThread;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltDB.SimulatedExitException;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.common.Constants;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.compiler.deploymentfile.DeploymentType;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.MiscUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;


/**
 * Test LocalCluster startup with one in process and other out of process.
 *
 */
public class TestInitStartLocalClusterInProcess extends JUnit4LocalClusterTest {

    static final int SITES_PER_HOST = 8;
    static final int HOSTS = 3;
    static final int K = MiscUtils.isPro() ? 1 : 0;
    VoltProjectBuilder builder;
    LocalCluster cluster;
    String listener;
    Client client;
    String voltDbRootPath;
    String voltDBRootParent;

    @Before
    public void setUp() throws Exception {
        String simpleSchema =
                "create table blah (" +
                "ival bigint default 0 not null, " +
                "PRIMARY KEY(ival));";

        builder = new VoltProjectBuilder();
        builder.addLiteralSchema(simpleSchema);

        cluster = new LocalCluster("collect.jar",
                SITES_PER_HOST, HOSTS, K, BackendTarget.NATIVE_EE_JNI);
        boolean success = cluster.compile(builder);
        assert (success);
        File voltDbRoot;
        cluster.startUp(true);
        //Get server specific root after startup.
        if (cluster.isNewCli()) {
            voltDbRoot = new File(cluster.getServerSpecificRoot("1"));
        } else {
            String voltDbFilePrefix = cluster.getSubRoots().get(0).getPath();
            voltDbRoot = new File(voltDbFilePrefix, builder.getPathToVoltRoot().getPath());
        }
        voltDbRootPath = voltDbRoot.getCanonicalPath();
        voltDBRootParent = voltDbRoot.getParent();
        listener = cluster.getListenerAddresses().get(0);
        client = ClientFactory.createClient();
        client.createConnection(listener);
    }

    @After
    public void tearDown() throws Exception {
        client.close();
        cluster.shutDown();
    }

    @Test
    public void testClusterUp() throws Exception
    {
        boolean found = false;
        int timeout = -1;
        VoltTable result = client.callProcedure("@SystemInformation", "DEPLOYMENT").getResults()[0];
        while (result.advanceRow()) {
            if (result.getString("PROPERTY").equalsIgnoreCase("heartbeattimeout")) {
                found = true;
                timeout = Integer.valueOf(result.getString("VALUE"));
            }
        }
        assertTrue(found);
        assertEquals(org.voltcore.common.Constants.DEFAULT_HEARTBEAT_TIMEOUT_SECONDS, timeout);
    }

    @Test
    public void testGetDeployment() throws Exception {
        File deployment = File.createTempFile("get_deployment", ".xm");

        Configuration c1 = new VoltDB.Configuration(new String[]{"get", "deployment",
            "getvoltdbroot", voltDBRootParent,
            "file", deployment.getAbsolutePath() + "l", "forceget"});
        ServerThread server = new ServerThread(c1);

        try {
            server.cli();
        } catch (SimulatedExitException ex) {
            //Good
        }

        DeploymentType dt = CatalogUtil.parseDeployment(deployment.getAbsolutePath() + "l");
        assertNotNull(dt);
        assertEquals(dt.getPaths().getVoltdbroot().getPath(), voltDbRootPath);
    }

    @Test
    public void testGetSchema() throws Exception {
        File schema = File.createTempFile("schema", ".sql");
        if (schema.exists()) schema.delete();

        Configuration c1 = new VoltDB.Configuration(new String[]{"get", "schema",
            "getvoltdbroot", voltDBRootParent,
            "file", schema.getAbsolutePath()});
        ServerThread server = new ServerThread(c1);

        try {
            server.cli();
        } catch (SimulatedExitException ex) {
            //Good
        }

        byte[] encoded = Files.readAllBytes(Paths.get(schema.getAbsolutePath()));
        assertNotNull(encoded);
        assertTrue(encoded.length > 0);
        String ddl = new String(encoded, Constants.UTF8ENCODING);
        assertTrue(ddl.toLowerCase().contains("create table blah ("));
        assertTrue(ddl.toLowerCase().contains("ival bigint default '0' not null"));
        assertTrue(ddl.toLowerCase().contains("primary key (ival)"));
    }

}
