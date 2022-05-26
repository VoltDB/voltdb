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

package org.voltdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.catalog.Catalog;
import org.voltdb.compiler.VoltProjectBuilder.RoleInfo;
import org.voltdb.compiler.VoltProjectBuilder.UserInfo;
import org.voltdb.utils.BuildDirectoryUtils;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.MiscUtils;

final public class TestVoltDB {

    @Rule
    public TestName testName = new TestName();

    @BeforeClass
    public static void setupClass() throws Exception {
        System.setProperty("VOLT_JUSTATEST", "YESYESYES");
    }

    @Before
    public void setup() {
        System.out.printf("=-=-=-= Start %s =-=-=-=\n", testName.getMethodName());
        VoltDB.ignoreCrash = true;
    }

    @After
    public void teardown() {
        System.out.printf("=-=-=-= End %s =-=-=-=\n", testName.getMethodName());
    }

    @Rule
    public final TemporaryFolder tmp = new TemporaryFolder();

    @Test
    public void testConfigurationConstructor() {
        VoltDB.Configuration blankConfig = new VoltDB.Configuration();
        assertFalse(blankConfig.m_noLoadLibVOLTDB);
        assertEquals(BackendTarget.NATIVE_EE_JNI, blankConfig.m_backend);
        assertNull(blankConfig.m_pathToCatalog);
        assertNull(blankConfig.m_pathToDeployment);
        assertEquals(VoltDB.DEFAULT_PORT, blankConfig.m_port);

        // Following tests use 'initialize' to exercise the constructor even
        // though the selected arguments may not be valid for init. This avoids
        // the check for an initialized root that 'probe' would make.

        String args101[] = { "initialize" };
        VoltDB.Configuration cfg101 = new VoltDB.Configuration(args101);
        assertEquals(StartAction.INITIALIZE, cfg101.m_startAction);

        String args102[] = { "initialize", "noloadlib" };
        VoltDB.Configuration cfg102 = new VoltDB.Configuration(args102);
        assertTrue(cfg102.m_noLoadLibVOLTDB);

        String args103[] = { "initialize", "hsqldb" };
        VoltDB.Configuration cfg103 = new VoltDB.Configuration(args103);
        assertEquals(BackendTarget.HSQLDB_BACKEND, cfg103.m_backend);

        String args104[] = { "initialize", "jni" };
        VoltDB.Configuration cfg104 = new VoltDB.Configuration(args104);
        assertEquals(BackendTarget.NATIVE_EE_JNI, cfg104.m_backend);

        String args105[] = { "initialize", "ipc" };
        VoltDB.Configuration cfg105 = new VoltDB.Configuration(args105);
        assertEquals(BackendTarget.NATIVE_EE_IPC, cfg105.m_backend);

        // what happens if arguments conflict?
        String args106[] = { "initialize", "ipc", "hsqldb" };
        VoltDB.Configuration cfg106 = new VoltDB.Configuration(args106);
        assertEquals(BackendTarget.HSQLDB_BACKEND, cfg106.m_backend);

        String args107[] = { "initialize", "port", "1234" };
        VoltDB.Configuration cfg107 = new VoltDB.Configuration(args107);
        assertEquals(1234, cfg107.m_port);

        String args108[] = { "initialize", "port", "5678" };
        VoltDB.Configuration cfg108 = new VoltDB.Configuration(args108);
        assertEquals(5678, cfg108.m_port);

        String args109[] = { "initialize", "safemode" };
        VoltDB.Configuration cfg109 = new VoltDB.Configuration(args109);
        assertEquals(StartAction.INITIALIZE, cfg109.m_startAction);
        assertTrue(cfg109.m_safeMode);

        // test host:port formats
        String args110[] = {"initialize", "port", "localhost:5678"};
        VoltDB.Configuration cfg110 = new VoltDB.Configuration(args110);
        assertEquals(5678, cfg110.m_port);
        assertEquals("localhost", cfg110.m_clientInterface);

        String args111[] = {"initialize", "adminport", "localhost:5678"};
        VoltDB.Configuration cfg111 = new VoltDB.Configuration(args111);
        assertEquals(5678, cfg111.m_adminPort);
        assertEquals("localhost", cfg111.m_adminInterface);

        String args112[] = {"initialize", "httpport", "localhost:7777"};
        VoltDB.Configuration cfg112 = new VoltDB.Configuration(args112);
        assertEquals(7777, cfg112.m_httpPort);
        assertEquals("localhost", cfg112.m_httpPortInterface);

        String args113[] = {"initialize", "internalport", "localhost:7777"};
        VoltDB.Configuration cfg113 = new VoltDB.Configuration(args113);
        assertEquals(7777, cfg113.m_internalPort);
        assertEquals("localhost", cfg113.m_internalInterface);

        //with override
        String args114[] = {"initialize", "internalinterface", "xxxxxx", "internalport", "localhost:7777"};
        VoltDB.Configuration cfg114 = new VoltDB.Configuration(args114);
        assertEquals(7777, cfg114.m_internalPort);
        assertEquals("localhost", cfg114.m_internalInterface);

        // XXX don't test what happens if port is invalid, because the code
        // doesn't handle that
    }

    private boolean causesExit(String[] args) {
        boolean threw = false;
        try {
            new VoltDB.Configuration(args);
        } catch (VoltDB.SimulatedExitException ex) {
            threw = true;
        }
        return threw;
    }

    @Test
    public void testConfigurationConstructorBadArgs() {
        String[] args201 = { "initialize", "deployment" };
        assertTrue(causesExit(args201));

        String[] args202 = { "initialize", "deployment xtestxstringx" };
        assertTrue(causesExit(args202));

        String[] args203 = {"probe", "voltdbroot", "qwerty"};
        assertTrue(causesExit(args203));
    }

    @Test
    public void testConfigurationValidate() throws Exception {
        VoltDB.Configuration config;

        // There is very little remaining validation to
        // check, once the old verbs are removed.  We are
        // currently unable to test 'probe' options here
        // due to the need for a complete init first.

        /* TODO - these ought to be validated but are not yet validated
        String[] args301 = {"initialize", "paused"};
        config = new VoltDB.Configuration(args301);
        assertFalse(config.validate());

        String[] args302 = {"initialize", "safemode"};
        config = new VoltDB.Configuration(args302);
        assertFalse(config.validate());

        String[] args303 = {"initialize", "hostcount", "2"};
        config = new VoltDB.Configuration(args303);
        assertFalse(config.validate());
        */

        String[] args304 = {"initialize", "deployment", "qwerty"};
        config = new VoltDB.Configuration(args304);
        assertTrue(config.validate());
    }

    AtomicReference<Throwable> serverException = new AtomicReference<>(null);

    final Thread.UncaughtExceptionHandler handleUncaught = new Thread.UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
            serverException.compareAndSet(null, e);
        }
    };

    @Test
    public void testHostCountValidations() throws Exception {
        final File path = tmp.newFolder();

        String [] init = {"initialize", "voltdbroot", path.getPath()};
        VoltDB.Configuration config = new VoltDB.Configuration(init);
        assertTrue(config.validate()); // false in both pro and community]

        ServerThread server = new ServerThread(config);
        server.setUncaughtExceptionHandler(handleUncaught);
        server.start();
        server.join();

        // invalid host count
        String [] args400 = {"probe", "voltdbroot", path.getPath(), "hostcount", "2", "mesh", "uno,", "due", ",", "tre", ",quattro" };
        config = new VoltDB.Configuration(args400);
        assertFalse(config.validate()); // false in both pro and community

        String [] args401 = {"probe", "voltdbroot", path.getPath(), "hostcount", "-3" , "mesh", "uno,", "due", ",", "tre", ",quattro"};
        config = new VoltDB.Configuration(args401);
        assertFalse(config.validate()); // false in both pro and community

        String [] args402 = {"probe", "voltdbroot", path.getPath(), "hostcount", "4" , "mesh", "uno,", "due", ",", "tre", ",quattro"};
        config = new VoltDB.Configuration(args402);
        assertTrue(config.validate()); // true in both pro and community

        String [] args403 = {"probe", "voltdbroot", path.getPath(), "hostcount", "6" , "mesh", "uno,", "due", ",", "tre", ",quattro"};
        config = new VoltDB.Configuration(args403);
        assertTrue(config.validate()); // true in both pro and community

        String [] args404 = {"probe", "voltdbroot", path.getPath(), "mesh", "uno,", "due", ",", "tre", ",quattro"};
        config = new VoltDB.Configuration(args404);
        assertTrue(config.validate()); // true in both pro and community
        assertEquals(4, config.m_hostCount);
    }

    /**
     * ENG-7088: Validate that deployment file users that want to belong to roles which
     * don't yet exist don't render the deployment file invalid.
     */
    @Test
    public void testCompileDeploymentAddUserToNonExistentGroup() throws IOException {
        TPCCProjectBuilder project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        project.addDefaultProcedures();

        project.setSecurityEnabled(true, true);
        RoleInfo groups[] = new RoleInfo[] {
                new RoleInfo("foo", false, false, false, false, false, false),
                new RoleInfo("blah", false, false, false, false, false, false)
        };
        project.addRoles(groups);
        UserInfo users[] = new UserInfo[] {
                new UserInfo("john", "hugg", new String[] {"foo"}),
                new UserInfo("ryan", "betts", new String[] {"foo", "bar"}),
                new UserInfo("ariel", "weisberg", new String[] {"bar"})
        };
        project.addUsers(users);

        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();
        String jarName = "compile-deployment.jar";
        String catalogJar = testDir + File.separator + jarName;
        assertTrue("Project failed to compile", project.compile(catalogJar));

        byte[] bytes = MiscUtils.fileToBytes(new File(catalogJar));
        String serializedCatalog = CatalogUtil.getSerializedCatalogStringFromJar(CatalogUtil.loadAndUpgradeCatalogFromJar(bytes, false).getFirst());
        assertNotNull("Error loading catalog from jar", serializedCatalog);

        Catalog catalog = new Catalog();
        catalog.execute(serializedCatalog);

        // this should succeed even though group "bar" does not exist
        String err = CatalogUtil.compileDeployment(catalog, project.getPathToDeployment(), true);
        assertNull("Deployment file should have been able to validate: ", err);
    }
}
