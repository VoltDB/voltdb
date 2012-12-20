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

package org.voltdb;

import java.io.File;
import java.io.IOException;

import junit.framework.TestCase;

import org.voltdb.VoltDB.START_ACTION;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.catalog.Catalog;
import org.voltdb.compiler.VoltProjectBuilder.GroupInfo;
import org.voltdb.compiler.VoltProjectBuilder.UserInfo;
import org.voltdb.utils.BuildDirectoryUtils;
import org.voltdb.utils.CatalogUtil;

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

        String args12[] = { "create", "port 1234" };
        VoltDB.Configuration cfg12 = new VoltDB.Configuration(args12);
        assertEquals(1234, cfg12.m_port);
        String args13[] = { "create", "port", "5678" };
        VoltDB.Configuration cfg13 = new VoltDB.Configuration(args13);
        assertEquals(5678, cfg13.m_port);

        String args14[] = { "create" };
        VoltDB.Configuration cfg14 = new VoltDB.Configuration(args14);
        assertEquals(START_ACTION.CREATE, cfg14.m_startAction);
        String args15[] = { "recover" };
        VoltDB.Configuration cfg15 = new VoltDB.Configuration(args15);
        assertEquals(START_ACTION.RECOVER, cfg15.m_startAction);

        String args17[] = { "replica" };
        VoltDB.Configuration cfg17 = new VoltDB.Configuration(args17);
        assertEquals(ReplicationRole.REPLICA, cfg17.m_replicationRole);

        // XXX don't test what happens if port is invalid, because the code
        // doesn't handle that
    }

    public void testConfigurationValidate() {
        VoltDB.Configuration config;

        // missing leader, catalog and missing deployment:
        String[] args1 = {"create"};
        config = new VoltDB.Configuration(args1);
        assertFalse(config.validate());

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
        assertEquals(START_ACTION.CREATE, config.m_startAction);

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
        assertTrue(config.validate());

        // invalid rejoin config, missing rejoin host
        String[] args250 = {"rejoin"};
        config = new VoltDB.Configuration(args250);
        assertFalse(config.validate());

        // rejoinhost should still work
        String[] args201 = {"rejoinhost", "localhost"};
        config = new VoltDB.Configuration(args201);
        assertTrue(config.validate());
    }

    /**
     * ENG-639: Improve deployment.xml parser error reporting
     *
     * This test tries to assign a user in the deployment file to a group that does not exist and asserts that
     * deployment file compilation fails.
     * @throws IOException
     */
    public void testCompileDeploymentAddUserToNonExistentGroup() throws IOException {
        TPCCProjectBuilder project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        project.addDefaultProcedures();

        project.setSecurityEnabled(true);
        GroupInfo groups[] = new GroupInfo[] {
                new GroupInfo("foo", false, false, false),
                new GroupInfo("blah", false, false, false)
        };
        project.addGroups(groups);
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

        byte[] bytes = CatalogUtil.toBytes(new File(catalogJar));
        String serializedCatalog = CatalogUtil.loadCatalogFromJar(bytes, null);
        assertNotNull("Error loading catalog from jar", serializedCatalog);

        Catalog catalog = new Catalog();
        catalog.execute(serializedCatalog);

        // this should fail because group "bar" does not exist
        assertTrue("Deployment file shouldn't have been able to validate",
                CatalogUtil.compileDeploymentAndGetCRC(catalog,project.getPathToDeployment(), true) < 0);
    }

    /**
     * ENG-720: NullPointerException when trying to start server with no users
     *
     * This test makes sure deployment validation passes when there are no users.
     * @throws IOException
     */
    public void testCompileDeploymentNoUsers() throws IOException {
        TPCCProjectBuilder project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        project.addDefaultProcedures();

        project.setSecurityEnabled(true);
        GroupInfo groups[] = new GroupInfo[] {
                new GroupInfo("foo", false, false, false),
                new GroupInfo("blah", false, false, false)
        };
        project.addGroups(groups);
        UserInfo users[] = new UserInfo[] {};
        project.addUsers(users);

        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();
        String jarName = "compile-deployment.jar";
        String catalogJar = testDir + File.separator + jarName;
        assertTrue("Project failed to compile", project.compile(catalogJar));

        byte[] bytes = CatalogUtil.toBytes(new File(catalogJar));
        String serializedCatalog = CatalogUtil.loadCatalogFromJar(bytes, null);
        assertNotNull("Error loading catalog from jar", serializedCatalog);

        Catalog catalog = new Catalog();
        catalog.execute(serializedCatalog);

        assertTrue("Deployment file should have been able to validate",
                CatalogUtil.compileDeploymentAndGetCRC(catalog,project.getPathToDeployment(), true) >= 0);
    }

}
