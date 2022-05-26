/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Set;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.utils.MiscUtils;

import org.voltdb.ServerThread;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltProcedure;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltCompiler;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.compiler.deploymentfile.DeploymentType;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.InMemoryJarfile;


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
    String voltDBRootParentPath;

    @Before
    public void setUp() throws Exception {
        String simpleSchema =
                "create table blah (" +
                "ival bigint default 0 not null, " +
                "PRIMARY KEY(ival));";

        builder = new VoltProjectBuilder();
        builder.setUseDDLSchema(true);
        builder.addLiteralSchema(simpleSchema);

        cluster = new LocalCluster("collect.jar",
                SITES_PER_HOST, HOSTS, K, BackendTarget.NATIVE_EE_JNI);
        boolean success = cluster.compile(builder);
        assert (success);
        File voltDbRoot;
        cluster.startUp(true);
        //Get server specific root after startup.
        voltDbRoot = new File(cluster.getServerSpecificRoot("1"));
        voltDbRootPath = voltDbRoot.getCanonicalPath();
        voltDBRootParentPath = voltDbRoot.getParentFile().getCanonicalPath();
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

        testGetDeployment();
        testGetSchema();
        testGetClasses();
    }

    // Test get deployment
    public void testGetDeployment() throws Exception {


        File deployment = File.createTempFile("get_deployment", ".xm");
        Configuration config = new VoltDB.Configuration(new String[]{"get", "deployment",
            "getvoltdbroot", voltDBRootParentPath,
            "file", deployment.getAbsolutePath() + "l", "forceget"});
        ServerThread server = new ServerThread(config);

        try {
            server.cli();
        } catch (Throwable ex) {
            //Good
        }

        DeploymentType dt = CatalogUtil.parseDeployment(deployment.getAbsolutePath() + "l");
        assertNotNull(dt);
        assertEquals(dt.getPaths().getVoltdbroot().getPath(), voltDbRootPath);
    }

    // Test get schema
    public void testGetSchema() throws Exception {

        File schema = File.createTempFile("schema", ".sql");
        Configuration config = new VoltDB.Configuration(new String[]{"get", "schema",
            "getvoltdbroot", voltDBRootParentPath,
            "file", schema.getAbsolutePath(), "forceget"});
        ServerThread server = new ServerThread(config);

        try {
            server.cli();
        } catch (Throwable ex) {
            //Good
        }

        byte[] encoded = Files.readAllBytes(Paths.get(schema.getAbsolutePath()));
        assertNotNull(encoded);
        assertTrue(encoded.length > 0);
        String ddl = new String(encoded, StandardCharsets.UTF_8);
        assertTrue(ddl.toLowerCase().contains("create table blah ("));
        assertTrue(ddl.toLowerCase().contains("ival bigint default '0' not null"));
        assertTrue(ddl.toLowerCase().contains("primary key (ival)"));
    }

    class RangeCount extends VoltProcedure {
        SQLStmt sql = new SQLStmt("select count(*) from blah where ival > ? and ival < ?;");
        public VoltTable[] run(long value1, long value2) {
            voltQueueSQL(sql, value1, value2);
            return voltExecuteSQL(true);
        }
    }

    void loadAndAddProcs() throws IOException, NoConnectionsException {
        ClientResponse resp = null;
        long numberOfClasses = 0;
        try {
            resp = client.callProcedure("@SystemCatalog", "CLASSES");
        } catch (ProcCallException excp) {
            assert false : "@SystemCatalogClasses failed";
        }
        numberOfClasses = resp.getResults()[0].getRowCount();

        InMemoryJarfile jarfile = new InMemoryJarfile();
        VoltCompiler comp = new VoltCompiler(false);
        try {
            comp.addClassToJar(jarfile, org.voltdb_testprocs.updateclasses.testImportProc.class);
            comp.addClassToJar(jarfile, org.voltdb_testprocs.updateclasses.testCreateProcFromClassProc.class);
            comp.addClassToJar(jarfile, org.voltdb_testprocs.updateclasses.InnerClassesTestProc.class);
            comp.addClassToJar(jarfile, RangeCount.class);
        } catch (Exception e) {
            assert false : "Failed add class to jar: " + e.getMessage();
        }

        try {
            client.callProcedure("@UpdateClasses", jarfile.getFullJarBytes(), null);
        } catch (ProcCallException excp) {
            assert false : "Failed updating the class";
        }

        try {
            resp = client.callProcedure("@SystemCatalog", "CLASSES");
        } catch (ProcCallException excp) {
            assert false : "@SystemCatalogClasses failed";
        }
        assertTrue( (numberOfClasses + jarfile.getLoader().getClassNames().size()) == resp.getResults()[0].getRowCount());
    }

    InMemoryJarfile getProcJarFromCatalog() throws IOException {
        File jar = File.createTempFile("procedure", ".jar");
        Configuration config = new VoltDB.Configuration(new String[]{"get", "classes",
            "getvoltdbroot", voltDBRootParentPath,
            "file", jar.getAbsolutePath(), "forceget"});
        ServerThread server = new ServerThread(config);
        try {
            server.cli();
        } catch (Throwable ex) {
            //Good
        }

        byte[] bytesRead = Files.readAllBytes(Paths.get(jar.getAbsolutePath()));
        assertNotNull(bytesRead);
        assertTrue(bytesRead.length > 0);
        return new InMemoryJarfile(bytesRead);
    }

    static boolean anyCatalogDefaultArtifactsExists(InMemoryJarfile jarFile) {
        Set<String> files = jarFile.keySet();
        // if empty, none
        if (files.size() == 0) return false;
        for (String artifacts : CatalogUtil.CATALOG_DEFAULT_ARTIFACTS) {
            if (files.contains(artifacts)) return true;
        }
        return false;
    }

    public void testGetClasses() throws IOException {
        InMemoryJarfile jarFile = getProcJarFromCatalog();
        assertTrue(!anyCatalogDefaultArtifactsExists(jarFile));
        org.voltdb.client.ClientResponse resp = null;
        // No java stored proc at this time, will give jar with no classes
        try {
            resp = client.callProcedure("@SystemCatalog", "CLASSES");
        } catch (ProcCallException excp) {
            assert false : "@SystemCatalogClasses failed";
        }
        assertTrue(jarFile.getLoader().getClassNames().size() == resp.getResults()[0].getRowCount());

        // load java stored proc classes and verify the retrieved classes count
        loadAndAddProcs();
        jarFile = getProcJarFromCatalog();
        assertTrue(!anyCatalogDefaultArtifactsExists(jarFile));
        try {
            resp = client.callProcedure("@SystemCatalog", "CLASSES");
        } catch (ProcCallException excp) {
            assert false : "@SystemCatalogClasses failed";
        }
        assertTrue(jarFile.getLoader().getClassNames().size() == resp.getResults()[0].getRowCount());
    }

}
