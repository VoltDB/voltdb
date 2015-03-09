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

package org.voltdb;

import java.io.File;
import java.io.IOException;

import junit.framework.TestCase;

import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.catalog.Catalog;
import org.voltdb.compiler.VoltProjectBuilder.RoleInfo;
import org.voltdb.compiler.VoltProjectBuilder.UserInfo;
import org.voltdb.utils.BuildDirectoryUtils;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.MiscUtils;

public class TestVoltDB extends TestCase {

    public void testConfigurationConstructor() {
        VoltDB.Configuration blankConfig = new VoltDB.Configuration();
        assertFalse(blankConfig.m_noLoadLibVOLTDB);
        assertEquals(BackendTarget.NATIVE_EE_JNI, blankConfig.m_backend);
        assertEquals(null, blankConfig.m_pathToCatalog);
        assertEquals(null, blankConfig.m_pathToDeployment);
        assertEquals(VoltDB.DEFAULT_PORT, blankConfig.m_port);

        String args1[] = { "create", "noloadlib" };
        assertTrue(new VoltDB.Configuration(args1).m_noLoadLibVOLTDB);

        String args2[] = { "create", "hsqldb" };
        VoltDB.Configuration cfg2 = new VoltDB.Configuration(args2);
        assertEquals(BackendTarget.HSQLDB_BACKEND, cfg2.m_backend);
        String args3[] = { "create", "jni" };
        VoltDB.Configuration cfg3 = new VoltDB.Configuration(args3);
        assertEquals(BackendTarget.NATIVE_EE_JNI, cfg3.m_backend);
        String args4[] = { "create", "ipc" };
        VoltDB.Configuration cfg4 = new VoltDB.Configuration(args4);
        assertEquals(BackendTarget.NATIVE_EE_IPC, cfg4.m_backend);
        // what happens if arguments conflict?
        String args5[] = { "create", "ipc", "hsqldb" };
        VoltDB.Configuration cfg5 = new VoltDB.Configuration(args5);
        assertEquals(BackendTarget.HSQLDB_BACKEND, cfg5.m_backend);

        String args9[] = { "create", "catalog xtestxstringx" };
        VoltDB.Configuration cfg9 = new VoltDB.Configuration(args9);
        assertEquals("xtestxstringx", cfg9.m_pathToCatalog);
        String args10[] = { "create", "catalog", "ytestystringy" };
        VoltDB.Configuration cfg10 = new VoltDB.Configuration(args10);
        assertEquals("ytestystringy", cfg10.m_pathToCatalog);

        String args12[] = { "create", "port", "1234" };
        VoltDB.Configuration cfg12 = new VoltDB.Configuration(args12);
        assertEquals(1234, cfg12.m_port);
        String args13[] = { "create", "port", "5678" };
        VoltDB.Configuration cfg13 = new VoltDB.Configuration(args13);
        assertEquals(5678, cfg13.m_port);

        String args14[] = { "create" };
        VoltDB.Configuration cfg14 = new VoltDB.Configuration(args14);
        assertEquals(StartAction.CREATE, cfg14.m_startAction);
        String args15[] = { "recover" };
        VoltDB.Configuration cfg15 = new VoltDB.Configuration(args15);
        assertEquals(StartAction.RECOVER, cfg15.m_startAction);
        String args16[] = { "recover", "safemode" };
        VoltDB.Configuration cfg16 = new VoltDB.Configuration(args16);
        assertEquals(StartAction.SAFE_RECOVER, cfg16.m_startAction);

        String args17[] = { "replica" };
        VoltDB.Configuration cfg17 = new VoltDB.Configuration(args17);
        assertEquals(ReplicationRole.REPLICA, cfg17.m_replicationRole);

        // test host:port formats
        String args18[] = {"create", "port", "localhost:5678"};
        VoltDB.Configuration cfg18 = new VoltDB.Configuration(args18);
        assertEquals(5678, cfg18.m_port);
        assertEquals("localhost", cfg18.m_clientInterface);

        String args19[] = {"create", "adminport", "localhost:5678"};
        VoltDB.Configuration cfg19 = new VoltDB.Configuration(args19);
        assertEquals(5678, cfg19.m_adminPort);
        assertEquals("localhost", cfg19.m_adminInterface);

        String args20[] = {"create", "httpport", "localhost:7777"};
        VoltDB.Configuration cfg20 = new VoltDB.Configuration(args20);
        assertEquals(7777, cfg20.m_httpPort);
        assertEquals("localhost", cfg20.m_httpPortInterface);

        String args21[] = {"create", "internalport", "localhost:7777"};
        VoltDB.Configuration cfg21 = new VoltDB.Configuration(args21);
        assertEquals(7777, cfg21.m_internalPort);
        assertEquals("localhost", cfg21.m_internalPortInterface);

        //with override
        String args22[] = {"create", "internalinterface", "xxxxxx", "internalport", "localhost:7777"};
        VoltDB.Configuration cfg22 = new VoltDB.Configuration(args22);
        assertEquals(7777, cfg22.m_internalPort);
        assertEquals("localhost", cfg22.m_internalPortInterface);

        // XXX don't test what happens if port is invalid, because the code
        // doesn't handle that
    }

    public void testConfigurationValidate() {
        VoltDB.Configuration config;

        // missing leader provided deployment - not okay.
        String[] argsya = {"create", "catalog", "qwerty", "deployment", "qwerty"};
        config = new VoltDB.Configuration(argsya);
        assertFalse(config.validate());

        // missing deployment (it's okay now that a default deployment is supported)
        String[] args3 = {"create", "host", "hola", "catalog", "teststring2"};
        config = new VoltDB.Configuration(args3);
        assertTrue(config.validate());

        // default deployment with default leader -- okay.
        config = new VoltDB.Configuration(new String[]{"create", "catalog", "catalog.jar"});
        assertTrue(config.validate());

        // empty leader -- tests could pass in empty leader to indicate bind to all interfaces on mac
        String[] argsyo = {"create", "host", "", "catalog", "sdfs", "deployment", "sdfsd"};
        config = new VoltDB.Configuration(argsyo);
        assertTrue(config.validate());

        // empty deployment
        String[] args6 = {"create", "host", "hola", "catalog", "teststring6", "deployment", ""};
        config = new VoltDB.Configuration(args6);
        assertFalse(config.validate());

        // replica with non-create
        String[] args7 = {"create", "host", "hola", "deployment", "teststring4", "replica", "recover"};
        config = new VoltDB.Configuration(args7);
        assertFalse(config.validate());

        // replica with explicit create
        String[] args8 = {"host", "hola", "deployment", "teststring4", "catalog", "catalog.jar", "replica", "create"};
        config = new VoltDB.Configuration(args8);
        assertTrue(config.validate());

        // replica with default action of create
        String[] args9 = {"host", "hola", "deployment", "teststring4", "catalog", "catalog.jar", "replica"};
        config = new VoltDB.Configuration(args9);
        assertTrue(config.validate());
        assertEquals(StartAction.CREATE, config.m_startAction);

        // valid config
        String[] args10 = {"create", "leader", "localhost", "deployment", "te", "catalog", "catalog.jar"};
        config = new VoltDB.Configuration(args10);
        assertTrue(config.validate());

        // valid config
        String[] args100 = {"create", "host", "hola", "deployment", "teststring4", "catalog", "catalog.jar"};
        config = new VoltDB.Configuration(args100);
        assertTrue(config.validate());

        // valid rejoin config
        String[] args200 = {"rejoin", "host", "localhost"};
        config = new VoltDB.Configuration(args200);
        assertEquals(config.validate(), MiscUtils.isPro());

        // invalid rejoin config, missing rejoin host
        String[] args250 = {"rejoin"};
        config = new VoltDB.Configuration(args250);
        assertFalse(config.validate()); // false in both pro and community

        // rejoinhost should still work
        String[] args201 = {"rejoinhost", "localhost"};
        config = new VoltDB.Configuration(args201);
        assertEquals(config.validate(), MiscUtils.isPro());
    }

    /**
     * ENG-7088: Validate that deployment file users that want to belong to roles which
     * don't yet exist don't render the deployment file invalid.
     */
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
        String serializedCatalog = CatalogUtil.getSerializedCatalogStringFromJar(CatalogUtil.loadAndUpgradeCatalogFromJar(bytes).getFirst());
        assertNotNull("Error loading catalog from jar", serializedCatalog);

        Catalog catalog = new Catalog();
        catalog.execute(serializedCatalog);

        // this should succeed even though group "bar" does not exist
        assertTrue("Deployment file should have been able to validate",
                CatalogUtil.compileDeployment(catalog, project.getPathToDeployment(), true) == null);
    }
}
