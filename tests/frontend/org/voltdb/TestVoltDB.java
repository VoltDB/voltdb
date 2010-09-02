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

package org.voltdb;

import java.io.File;

import org.voltdb.VoltDB;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.catalog.Catalog;
import org.voltdb.compiler.VoltProjectBuilder.GroupInfo;
import org.voltdb.compiler.VoltProjectBuilder.UserInfo;
import org.voltdb.utils.BuildDirectoryUtils;
import org.voltdb.utils.CatalogUtil;

import junit.framework.TestCase;

public class TestVoltDB extends TestCase {

    public void testConfigurationConstructor() {
        VoltDB.Configuration blankConfig = new VoltDB.Configuration();
        assertFalse(blankConfig.m_noLoadLibVOLTDB);
        assertEquals(BackendTarget.NATIVE_EE_JNI, blankConfig.m_backend);
        assertEquals(ProcedureProfiler.Level.DISABLED, blankConfig.m_profilingLevel);
        assertEquals(null, blankConfig.m_pathToCatalog);
        assertEquals(null, blankConfig.m_pathToDeployment);
        assertEquals(VoltDB.DEFAULT_PORT, blankConfig.m_port);

        String args1[] = { "noloadlib" };
        assertTrue(new VoltDB.Configuration(args1).m_noLoadLibVOLTDB);

        String args2[] = { "hsqldb" };
        VoltDB.Configuration cfg2 = new VoltDB.Configuration(args2);
        assertEquals(BackendTarget.HSQLDB_BACKEND, cfg2.m_backend);
        String args3[] = { "jni" };
        VoltDB.Configuration cfg3 = new VoltDB.Configuration(args3);
        assertEquals(BackendTarget.NATIVE_EE_JNI, cfg3.m_backend);
        String args4[] = { "ipc" };
        VoltDB.Configuration cfg4 = new VoltDB.Configuration(args4);
        assertEquals(BackendTarget.NATIVE_EE_IPC, cfg4.m_backend);
        // what happens if arguments conflict?
        String args5[] = { "ipc", "hsqldb" };
        VoltDB.Configuration cfg5 = new VoltDB.Configuration(args5);
        assertEquals(BackendTarget.HSQLDB_BACKEND, cfg5.m_backend);

        String args6[] = { "polite" };
        VoltDB.Configuration cfg6 = new VoltDB.Configuration(args6);
        assertEquals(ProcedureProfiler.Level.POLITE, cfg6.m_profilingLevel);
        String args7[] = { "intrusive" };
        VoltDB.Configuration cfg7 = new VoltDB.Configuration(args7);
        assertEquals(ProcedureProfiler.Level.INTRUSIVE, cfg7.m_profilingLevel);
        String args8[] = { "intrusive", "polite" };
        VoltDB.Configuration cfg8 = new VoltDB.Configuration(args8);
        assertEquals(ProcedureProfiler.Level.POLITE, cfg8.m_profilingLevel);

        String args9[] = { "catalog xtestxstringx" };
        VoltDB.Configuration cfg9 = new VoltDB.Configuration(args9);
        assertEquals("xtestxstringx", cfg9.m_pathToCatalog);
        String args10[] = { "catalog", "ytestystringy" };
        VoltDB.Configuration cfg10 = new VoltDB.Configuration(args10);
        assertEquals("ytestystringy", cfg10.m_pathToCatalog);

        String args12[] = { "port 1234" };
        VoltDB.Configuration cfg12 = new VoltDB.Configuration(args12);
        assertEquals(1234, cfg12.m_port);
        String args13[] = { "port", "5678" };
        VoltDB.Configuration cfg13 = new VoltDB.Configuration(args13);
        assertEquals(5678, cfg13.m_port);

        // XXX don't test what happens if port is invalid, because the code
        // doesn't handle that
    }

    public void testConfigurationValidate() {
        VoltDB.Configuration config;

        // missing catalog and missing deployment
        String[] args1 = {};
        config = new VoltDB.Configuration(args1);
        assertFalse(config.validate());

        // missing catalog
        String[] args2 = {"deployment", "teststring1"};
        config = new VoltDB.Configuration(args2);
        assertFalse(config.validate());

        // missing deployment
        String[] args3 = {"catalog", "teststring2"};
        config = new VoltDB.Configuration(args3);
        assertFalse(config.validate());

        // empty catalog and empty deployment
        String[] args4 = {"catalog", "", "deployment", ""};
        config = new VoltDB.Configuration(args4);
        assertFalse(config.validate());

        // empty catalog
        String[] args5 = {"catalog", "", "deployment", "teststring5"};
        config = new VoltDB.Configuration(args5);
        assertFalse(config.validate());

        // empty deployment
        String[] args6 = {"catalog", "teststring6", "deployment", ""};
        config = new VoltDB.Configuration(args6);
        assertFalse(config.validate());

        // valid config
        String[] args7 = {"catalog", "teststring3", "deployment", "teststring4"};
        config = new VoltDB.Configuration(args7);
        assertTrue(config.validate());
    }

    /**
     * ENG-639: Improve deployment.xml parser error reporting
     *
     * This test tries to assign a user in the deployment file to a group that does not exist and asserts that
     * deployment file compilation fails.
     */
    public void testCompileDeploymentAddUserToNonExistentGroup() {
        TPCCProjectBuilder project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        project.addDefaultProcedures();

        project.setSecurityEnabled(true);
        GroupInfo groups[] = new GroupInfo[] {
                new GroupInfo("foo", false, false),
                new GroupInfo("blah", false, false)
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

        String serializedCatalog = CatalogUtil.loadCatalogFromJar(catalogJar, null);
        assertNotNull("Error loading catalog from jar", serializedCatalog);

        Catalog catalog = new Catalog();
        catalog.execute(serializedCatalog);

        // this should fail because group "bar" does not exist
        assertFalse("Deployment file shouldn't have been able to validate",
                CatalogUtil.compileDeployment(catalog,project.getPathToDeployment()));
    }

    /**
     * ENG-720: NullPointerException when trying to start server with no users
     *
     * This tests makes sure deployment validation passes when there are no users.
     */
    public void testCompileDeploymentNoUsers() {
        TPCCProjectBuilder project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        project.addDefaultProcedures();

        project.setSecurityEnabled(true);
        GroupInfo groups[] = new GroupInfo[] {
                new GroupInfo("foo", false, false),
                new GroupInfo("blah", false, false)
        };
        project.addGroups(groups);
        UserInfo users[] = new UserInfo[] {};
        project.addUsers(users);

        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();
        String jarName = "compile-deployment.jar";
        String catalogJar = testDir + File.separator + jarName;
        assertTrue("Project failed to compile", project.compile(catalogJar));

        String serializedCatalog = CatalogUtil.loadCatalogFromJar(catalogJar, null);
        assertNotNull("Error loading catalog from jar", serializedCatalog);

        Catalog catalog = new Catalog();
        catalog.execute(serializedCatalog);

        assertTrue("Deployment file should have been able to validate",
                CatalogUtil.compileDeployment(catalog,project.getPathToDeployment()));
    }

}