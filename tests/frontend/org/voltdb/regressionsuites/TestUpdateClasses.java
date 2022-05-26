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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

import org.junit.Test;
import org.voltdb.AdhocDDLTestBase;
import org.voltdb.BackendTarget;
import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableTestHelpers;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltCompiler;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.compiler.VoltProjectBuilder.RoleInfo;
import org.voltdb.compiler.VoltProjectBuilder.UserInfo;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.InMemoryJarfile;
import org.voltdb.utils.MiscUtils;
import org.voltdb_testprocs.updateclasses.jars.TestProcedure;

/**
 * Tests a mix of multi-partition and single partition procedures on a
 * mix of replicated and partititioned tables on a mix of single-site and
 * multi-site VoltDB instances.
 *
 */
public class TestUpdateClasses extends AdhocDDLTestBase {

    static Class<?>[] PROC_CLASSES = { org.voltdb_testprocs.updateclasses.testImportProc.class,
        org.voltdb_testprocs.updateclasses.testCreateProcFromClassProc.class };

    static Class<?>[] EXTRA_CLASSES = { org.voltdb_testprocs.updateclasses.NoMeaningClass.class };

    static Class<?>[] COLLIDING_CLASSES = { org.voltdb_testprocs.fullddlfeatures.testImportProc.class,
        org.voltdb_testprocs.fullddlfeatures.testCreateProcFromClassProc.class };

    @Test
    public void testBasic() throws Exception {
        System.out.println("\n\n-----\n testBasic \n-----\n\n");

        String pathToCatalog = Configuration.getPathToCatalogForTest("updateclasses.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("updateclasses.xml");
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("-- Don't care");
        builder.setUseDDLSchema(true);
        boolean success = builder.compile(pathToCatalog, 2, 1, 0);
        assertTrue("Schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        // This is maybe cheating a little bit?
        InMemoryJarfile jarfile = new InMemoryJarfile();
        for (Class<?> clazz : PROC_CLASSES) {
            VoltCompiler comp = new VoltCompiler(false);
            comp.addClassToJar(jarfile, clazz);
        }
        for (Class<?> clazz : EXTRA_CLASSES) {
            VoltCompiler comp = new VoltCompiler(false);
            comp.addClassToJar(jarfile, clazz);
        }
        // Add a deployment file just to have something other than classes in the jar
        jarfile.put("deployment.xml", new File(pathToDeployment));

        try {
            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToCatalog = pathToCatalog;
            config.m_pathToDeployment = pathToDeployment;
            startSystem(config);

            // --- ENG-17189: DDLs being processed twice --- starts
            assertTrue(new VoltCompiler(false)
                    .compileDDLString("CREATE TABLE V0(id BIGINT);",
                            Configuration.getPathToCatalogForTest("ENG17189.jar")));
            assertEquals(ClientResponse.SUCCESS,
                    m_client.callProcedure("@AdHoc", "CREATE TABLE V0(id1 BIGINT)").getStatus());
            assertEquals(ClientResponse.SUCCESS,
                    m_client.callProcedure("@AdHoc", "DROP TABLE V0").getStatus());
            // --- ENG-17189: DDLs being processed twice --- ends

            ClientResponse resp;
            resp = m_client.callProcedure("@SystemCatalog", "CLASSES");
            System.out.println(resp.getResults()[0]);
            // New cluster, you're like summer vacation...
            assertEquals(0, resp.getResults()[0].getRowCount());
            assertFalse(VoltTableTestHelpers.moveToMatchingRow(resp.getResults()[0], "CLASS_NAME",
                        PROC_CLASSES[0].getCanonicalName()));
            boolean threw = false;
            try {
                m_client.callProcedure(PROC_CLASSES[0].getSimpleName());
            }
            catch (ProcCallException pce) {
                assertTrue(pce.getMessage().contains("was not found"));
                threw = true;
            }
            assertTrue(threw);

            // First, some tests of incorrect parameters
            // only 1 param
            threw = false;
            try {
                m_client.callProcedure("@UpdateClasses", jarfile.getFullJarBytes());
            }
            catch (ProcCallException pce) {
                assertTrue(pce.getMessage().contains("UpdateClasses system procedure requires exactly two parameters"));
                threw = true;
            }
            assertTrue(threw);

            // wrong jarfile param type
            threw = false;
            try {
                m_client.callProcedure("@UpdateClasses", 10L, null);
            }
            catch (ProcCallException pce) {
                assertTrue(pce.getMessage().contains("UpdateClasses system procedure takes the jarfile bytes as a byte array"));
                threw = true;
            }
            assertTrue(threw);

            // wrong delete string param type
            threw = false;
            try {
                resp = m_client.callProcedure("@UpdateClasses", jarfile.getFullJarBytes(), 10L);
            }
            catch (ProcCallException pce) {
                assertTrue(pce.getMessage().contains("UpdateClasses system procedure takes the list of classes"));
                threw = true;
            }
            assertTrue(threw);

            resp = m_client.callProcedure("@UpdateClasses", jarfile.getFullJarBytes(), null);
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());

            // Are we still like summer vacation?
            resp = m_client.callProcedure("@SystemCatalog", "CLASSES");
            VoltTable results = resp.getResults()[0];
            System.out.println(results);
            assertEquals(3, results.getRowCount());
            assertTrue(VoltTableTestHelpers.moveToMatchingRow(results, "CLASS_NAME",
                        PROC_CLASSES[0].getCanonicalName()));
            assertEquals(1L, results.getLong("VOLT_PROCEDURE"));
            assertEquals(0L, results.getLong("ACTIVE_PROC"));
            // Can we turn it into a procedure?
            resp = m_client.callProcedure("@AdHoc", "create procedure from class " +
                    PROC_CLASSES[0].getCanonicalName() + ";");
            System.out.println(((ClientResponseImpl)resp).toJSONString());
            resp = m_client.callProcedure(PROC_CLASSES[0].getSimpleName());
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());
            results = resp.getResults()[0];
            assertEquals(10L, results.asScalarLong());
        } finally {
            teardownSystem();
        }
    }

    @Test
    public void testRoleControl() throws Exception {
        System.out.println("\n\n-----\n testRoleControl \n-----\n\n");

        String pathToCatalog = Configuration.getPathToCatalogForTest("updateclasses.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("updateclasses.xml");
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("-- Don't care");
        builder.setUseDDLSchema(true);
        RoleInfo groups[] = new RoleInfo[] {
            new RoleInfo("adhoc", true, false, false, false, false, false)
        };
        UserInfo users[] = new UserInfo[] {
            new UserInfo("adhocuser", "adhocuser", new String[] {"adhoc"}),
            new UserInfo("sysuser", "sysuser", new String[] {"ADMINISTRATOR"})
        };
        builder.addRoles(groups);
        builder.addUsers(users);
        // Test defines its own ADMIN user
        builder.setSecurityEnabled(true, false);
        boolean success = builder.compile(pathToCatalog, 2, 1, 0);
        assertTrue("Schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        // This is maybe cheating a little bit?
        InMemoryJarfile jarfile = new InMemoryJarfile();
        for (Class<?> clazz : PROC_CLASSES) {
            VoltCompiler comp = new VoltCompiler(false);
            comp.addClassToJar(jarfile, clazz);
        }
        for (Class<?> clazz : EXTRA_CLASSES) {
            VoltCompiler comp = new VoltCompiler(false);
            comp.addClassToJar(jarfile, clazz);
        }

        Client auth_client = null;
        try {
            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToCatalog = pathToCatalog;
            config.m_pathToDeployment = pathToDeployment;
            // Default client auth is going to fail, catch and keep chugging
            try {
                startSystem(config);
            }
            catch (IOException ioe) {
                assertTrue(ioe.getMessage().contains("Authentication rejected"));
            }
            m_client.close();
            // reconnect m_client with auth that will connect but no sysproc powers
            ClientConfig bad_config = new ClientConfig("adhocuser", "adhocuser");
            m_client = ClientFactory.createClient(bad_config);
            m_client.createConnection("localhost");

            // Need a client with the right auth
            ClientConfig auth_config = new ClientConfig("sysuser", "sysuser");
            auth_client = ClientFactory.createClient(auth_config);
            auth_client.createConnection("localhost");

            ClientResponse resp;
            resp = auth_client.callProcedure("@SystemCatalog", "CLASSES");
            System.out.println(resp.getResults()[0]);
            // New cluster, you're like summer vacation...
            assertEquals(0, resp.getResults()[0].getRowCount());
            assertFalse(VoltTableTestHelpers.moveToMatchingRow(resp.getResults()[0], "CLASS_NAME",
                        PROC_CLASSES[0].getCanonicalName()));
            boolean threw = false;
            try {
                resp = auth_client.callProcedure(PROC_CLASSES[0].getSimpleName());
            }
            catch (ProcCallException pce) {
                assertTrue(pce.getMessage().contains("was not found"));
                threw = true;
            }
            assertTrue(threw);

            threw = false;
            try {
                resp = m_client.callProcedure("@UpdateClasses", jarfile.getFullJarBytes(), null);
            }
            catch (ProcCallException pce) {
                assertTrue(pce.getMessage().contains("does not have admin permission"));
                threw = true;
            }
            assertTrue(threw);

            resp = auth_client.callProcedure("@UpdateClasses", jarfile.getFullJarBytes(), null);
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());

            // Are we still like summer vacation?
            resp = auth_client.callProcedure("@SystemCatalog", "CLASSES");
            VoltTable results = resp.getResults()[0];
            System.out.println(results);
            assertEquals(3, results.getRowCount());
            assertTrue(VoltTableTestHelpers.moveToMatchingRow(results, "CLASS_NAME",
                        PROC_CLASSES[0].getCanonicalName()));
            assertEquals(1L, results.getLong("VOLT_PROCEDURE"));
            assertEquals(0L, results.getLong("ACTIVE_PROC"));
        }
        finally {
            if (auth_client != null) {
                auth_client.close();
            }
            teardownSystem();
        }
    }

    @Test
    public void testCollidingClasses() throws Exception {
        System.out.println("\n\n-----\n testCollidingProc \n-----\n\n");

        String pathToCatalog = Configuration.getPathToCatalogForTest("updateclasses.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("updateclasses.xml");
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("-- Don't care");
        builder.setUseDDLSchema(true);
        boolean success = builder.compile(pathToCatalog, 2, 1, 0);
        assertTrue("Schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        // This is maybe cheating a little bit?
        InMemoryJarfile jarfile = new InMemoryJarfile();
        for (Class<?> clazz : PROC_CLASSES) {
            VoltCompiler comp = new VoltCompiler(false);
            comp.addClassToJar(jarfile, clazz);
        }
        for (Class<?> clazz : EXTRA_CLASSES) {
            VoltCompiler comp = new VoltCompiler(false);
            comp.addClassToJar(jarfile, clazz);
        }

        try {
            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToCatalog = pathToCatalog;
            config.m_pathToDeployment = pathToDeployment;
            startSystem(config);

            ClientResponse resp;
            resp = m_client.callProcedure("@SystemCatalog", "CLASSES");
            System.out.println(resp.getResults()[0]);
            // New cluster, you're like summer vacation...
            assertEquals(0, resp.getResults()[0].getRowCount());
            assertFalse(VoltTableTestHelpers.moveToMatchingRow(resp.getResults()[0], "CLASS_NAME",
                        PROC_CLASSES[0].getCanonicalName()));
            boolean threw = false;
            try {
                resp = m_client.callProcedure(PROC_CLASSES[0].getSimpleName());
            }
            catch (ProcCallException pce) {
                assertTrue(pce.getMessage().contains("was not found"));
                threw = true;
            }
            assertTrue(threw);

            resp = m_client.callProcedure("@UpdateClasses", jarfile.getFullJarBytes(), null);
            System.out.println(((ClientResponseImpl)resp).toJSONString());

            // Are we still like summer vacation?
            resp = m_client.callProcedure("@SystemCatalog", "CLASSES");
            VoltTable results = resp.getResults()[0];
            System.out.println(results);
            assertEquals(3, results.getRowCount());
            assertTrue(VoltTableTestHelpers.moveToMatchingRow(results, "CLASS_NAME",
                        PROC_CLASSES[0].getCanonicalName()));
            assertEquals(1L, results.getLong("VOLT_PROCEDURE"));
            assertEquals(0L, results.getLong("ACTIVE_PROC"));
            // Can we turn it into a procedure?
            resp = m_client.callProcedure("@AdHoc", "create procedure from class " +
                    PROC_CLASSES[0].getCanonicalName() + ";");
            System.out.println(((ClientResponseImpl)resp).toJSONString());
            resp = m_client.callProcedure(PROC_CLASSES[0].getSimpleName());
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());
            results = resp.getResults()[0];
            assertEquals(10L, results.asScalarLong());

            // now, let's collide identically simpleName'd classes
            InMemoryJarfile boom = new InMemoryJarfile();
            for (Class<?> clazz : COLLIDING_CLASSES) {
                VoltCompiler comp = new VoltCompiler(false);
                comp.addClassToJar(boom, clazz);
            }
            resp = m_client.callProcedure("@UpdateClasses", boom.getFullJarBytes(), null);
            System.out.println(((ClientResponseImpl)resp).toJSONString());

            // should be okay to have classnames with same simplename
            resp = m_client.callProcedure("@SystemCatalog", "CLASSES");
            results = resp.getResults()[0];
            System.out.println(results);
            assertEquals(5, results.getRowCount());
            assertTrue(VoltTableTestHelpers.moveToMatchingRow(results, "CLASS_NAME",
                        COLLIDING_CLASSES[0].getCanonicalName()));
            assertEquals(1L, results.getLong("VOLT_PROCEDURE"));
            assertEquals(0L, results.getLong("ACTIVE_PROC"));
        }
        finally {
            teardownSystem();
        }
    }

    @Test
    public void testNonJarInput() throws Exception {
        System.out.println("\n\n-----\n testNonJarInput \n-----\n\n");

        String pathToCatalog = Configuration.getPathToCatalogForTest("updateclasses.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("updateclasses.xml");
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("-- Don't care");
        builder.setUseDDLSchema(true);
        boolean success = builder.compile(pathToCatalog, 2, 1, 0);
        assertTrue("Schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        try {
            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToCatalog = pathToCatalog;
            config.m_pathToDeployment = pathToDeployment;
            startSystem(config);

            ClientResponse resp;
            try {
                resp = m_client.callProcedure("@UpdateClasses",
                        Encoder.hexEncode("This is not a pipe"),
                        null);
                System.out.println(((ClientResponseImpl)resp).toJSONString());
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
            }
            // Check that we haven't made any obvious changes to the state based on garbage
            resp = m_client.callProcedure("@SystemCatalog", "CLASSES");
            System.out.println(resp.getResults()[0]);
            assertEquals(0, resp.getResults()[0].getRowCount());
        }
        finally {
            teardownSystem();
        }
    }

    @Test
    public void testInnerClasses() throws Exception {
        System.out.println("\n\n-----\n testInnerClasses \n-----\n\n");

        String pathToCatalog = Configuration.getPathToCatalogForTest("updateclasses.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("updateclasses.xml");
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("-- Don't care");
        builder.setUseDDLSchema(true);
        boolean success = builder.compile(pathToCatalog, 2, 1, 0);
        assertTrue("Schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        try {
            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToCatalog = pathToCatalog;
            config.m_pathToDeployment = pathToDeployment;
            startSystem(config);

            // Something sane ought to work
            ClientResponse resp;
            InMemoryJarfile boom = new InMemoryJarfile();
            VoltCompiler comp = new VoltCompiler(false);
            comp.addClassToJar(boom, org.voltdb_testprocs.updateclasses.InnerClassesTestProc.class);
            try {
                resp = m_client.callProcedure("@UpdateClasses", boom.getFullJarBytes(), null);
                System.out.println(((ClientResponseImpl)resp).toJSONString());
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Loading proc with inner classes should succeed");
            }

            // Error in non-visible inner class static initializer?
            boolean threw = false;
            boom = new InMemoryJarfile();
            comp = new VoltCompiler(false);
            comp.addClassToJar(boom, org.voltdb_testprocs.updateclasses.BadInnerClassesTestProc.class);
            try {
                resp = m_client.callProcedure("@UpdateClasses", boom.getFullJarBytes(), null);
                System.out.println(((ClientResponseImpl)resp).toJSONString());
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                threw = true;
            }
            assertTrue("Bad inner class should have failed", threw);
        }
        finally {
            teardownSystem();
        }
    }

    @Test
    public void testBadInitializerClasses() throws Exception {
        System.out.println("\n\n-----\n testBadInitializerClasses \n-----\n\n");

        String pathToCatalog = Configuration.getPathToCatalogForTest("updateclasses.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("updateclasses.xml");
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("-- Don't care");
        builder.setUseDDLSchema(true);
        boolean success = builder.compile(pathToCatalog, 2, 1, 0);
        assertTrue("Schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        try {
            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToCatalog = pathToCatalog;
            config.m_pathToDeployment = pathToDeployment;
            startSystem(config);

            ClientResponse resp;
            InMemoryJarfile boom = new InMemoryJarfile();
            VoltCompiler comp = new VoltCompiler(false);
            comp.addClassToJar(boom, org.voltdb_testprocs.updateclasses.testBadInitializerProc.class);
            boolean threw = false;
            try {
                resp = m_client.callProcedure("@UpdateClasses", boom.getFullJarBytes(), null);
                System.out.println(((ClientResponseImpl)resp).toJSONString());
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                threw = true;
            }
            assertTrue("Bad class jar should have thrown", threw);

            threw = false;
            boom = new InMemoryJarfile();
            comp = new VoltCompiler(false);
            comp.addClassToJar(boom, org.voltdb_testprocs.updateclasses.BadClassLoadClass.class);
            try {
                resp = m_client.callProcedure("@UpdateClasses", boom.getFullJarBytes(), null);
                System.out.println(((ClientResponseImpl)resp).toJSONString());
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                threw = true;
            }
            assertTrue("Bad class jar should have thrown", threw);
        }
        finally {
            teardownSystem();
        }
    }

    // Delete tests:
    // single file match
    // * match
    // ** match
    // comma-separated matches
    // combine new jarfile with deleted stuff
    // deleting inner classes
    @Test
    public void testDeleteClasses() throws Exception {
        System.out.println("\n\n-----\n testCollidingProc \n-----\n\n");

        String pathToCatalog = Configuration.getPathToCatalogForTest("updateclasses.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("updateclasses.xml");
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("-- Don't care");
        builder.setUseDDLSchema(true);
        boolean success = builder.compile(pathToCatalog, 2, 1, 0);
        assertTrue("Schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        // This is maybe cheating a little bit?
        InMemoryJarfile jarfile = new InMemoryJarfile();
        for (Class<?> clazz : PROC_CLASSES) {
            VoltCompiler comp = new VoltCompiler(false);
            comp.addClassToJar(jarfile, clazz);
        }
        for (Class<?> clazz : EXTRA_CLASSES) {
            VoltCompiler comp = new VoltCompiler(false);
            comp.addClassToJar(jarfile, clazz);
        }
        for (Class<?> clazz : COLLIDING_CLASSES) {
            VoltCompiler comp = new VoltCompiler(false);
            comp.addClassToJar(jarfile, clazz);
        }

        try {
            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToCatalog = pathToCatalog;
            config.m_pathToDeployment = pathToDeployment;
            startSystem(config);

            ClientResponse resp;
            // Make sure we're clean
            resp = m_client.callProcedure("@SystemCatalog", "CLASSES");
            assertEquals(0, resp.getResults()[0].getRowCount());

            // Add the jarfile we built
            resp = m_client.callProcedure("@UpdateClasses", jarfile.getFullJarBytes(), null);
            resp = m_client.callProcedure("@SystemCatalog", "CLASSES");
            assertEquals(PROC_CLASSES.length + EXTRA_CLASSES.length + COLLIDING_CLASSES.length,
                    resp.getResults()[0].getRowCount());

            // remove one class
            assertTrue(findClassInSystemCatalog(PROC_CLASSES[0].getCanonicalName()));
            resp = m_client.callProcedure("@UpdateClasses", null, PROC_CLASSES[0].getCanonicalName());
            assertFalse(findClassInSystemCatalog(PROC_CLASSES[0].getCanonicalName()));

            // remove everything under fullddlfeatures
            assertTrue(findClassInSystemCatalog(COLLIDING_CLASSES[0].getCanonicalName()));
            assertTrue(findClassInSystemCatalog(COLLIDING_CLASSES[1].getCanonicalName()));
            resp = m_client.callProcedure("@UpdateClasses", null,
                    "org.voltdb_testprocs.fullddlfeatures.*");
            assertFalse(findClassInSystemCatalog(COLLIDING_CLASSES[0].getCanonicalName()));
            assertFalse(findClassInSystemCatalog(COLLIDING_CLASSES[1].getCanonicalName()));

            // Remove everything left
            assertTrue(findClassInSystemCatalog(PROC_CLASSES[1].getCanonicalName()));
            assertTrue(findClassInSystemCatalog(EXTRA_CLASSES[0].getCanonicalName()));
            resp = m_client.callProcedure("@UpdateClasses", null, "org.voltdb**");
            assertFalse(findClassInSystemCatalog(PROC_CLASSES[1].getCanonicalName()));
            assertFalse(findClassInSystemCatalog(EXTRA_CLASSES[0].getCanonicalName()));
            resp = m_client.callProcedure("@SystemCatalog", "CLASSES");
            assertEquals(0, resp.getResults()[0].getRowCount());

            // put everything back
            // Add the jarfile we built
            resp = m_client.callProcedure("@UpdateClasses", jarfile.getFullJarBytes(), null);
            resp = m_client.callProcedure("@SystemCatalog", "CLASSES");
            assertEquals(PROC_CLASSES.length + EXTRA_CLASSES.length + COLLIDING_CLASSES.length,
                    resp.getResults()[0].getRowCount());

            // delete the common simple names from both packages simultaneously
            resp = m_client.callProcedure("@UpdateClasses", null,
                    "**testImportProc   , **testCreateProcFromClassProc");
            resp = m_client.callProcedure("@SystemCatalog", "CLASSES");
            // should be the only thing left
            assertEquals(1, resp.getResults()[0].getRowCount());
            assertTrue(findClassInSystemCatalog(EXTRA_CLASSES[0].getCanonicalName()));

            // make a jar without the extra
            InMemoryJarfile jarfile2 = new InMemoryJarfile();
            for (Class<?> clazz : PROC_CLASSES) {
                VoltCompiler comp = new VoltCompiler(false);
                comp.addClassToJar(jarfile2, clazz);
            }
            for (Class<?> clazz : COLLIDING_CLASSES) {
                VoltCompiler comp = new VoltCompiler(false);
                comp.addClassToJar(jarfile2, clazz);
            }

            // finally, delete what's left and put the new jar in simultaneously
            resp = m_client.callProcedure("@UpdateClasses", jarfile2.getFullJarBytes(),
                    "**updateclasses.*");
            resp = m_client.callProcedure("@SystemCatalog", "CLASSES");
            // extra class should be gone, others installed
            assertEquals(PROC_CLASSES.length + COLLIDING_CLASSES.length,
                    resp.getResults()[0].getRowCount());
            assertFalse(findClassInSystemCatalog(EXTRA_CLASSES[0].getCanonicalName()));
            assertTrue(findClassInSystemCatalog(PROC_CLASSES[0].getCanonicalName()));
            assertTrue(findClassInSystemCatalog(PROC_CLASSES[1].getCanonicalName()));
            assertTrue(findClassInSystemCatalog(COLLIDING_CLASSES[0].getCanonicalName()));
            assertTrue(findClassInSystemCatalog(COLLIDING_CLASSES[1].getCanonicalName()));

            // now add a class with inner classes
            InMemoryJarfile inner = new InMemoryJarfile();
            VoltCompiler comp = new VoltCompiler(false);
            comp.addClassToJar(inner, org.voltdb_testprocs.updateclasses.InnerClassesTestProc.class);
            resp = m_client.callProcedure("@UpdateClasses", inner.getFullJarBytes(), null);
            // old stuff should have survived
            assertTrue(findClassInSystemCatalog(PROC_CLASSES[0].getCanonicalName()));
            assertTrue(findClassInSystemCatalog(PROC_CLASSES[1].getCanonicalName()));
            assertTrue(findClassInSystemCatalog(COLLIDING_CLASSES[0].getCanonicalName()));
            assertTrue(findClassInSystemCatalog(COLLIDING_CLASSES[1].getCanonicalName()));
            // Did we get the new class and inner classes too?
            assertTrue(findClassInSystemCatalog("org.voltdb_testprocs.updateclasses.InnerClassesTestProc"));
            assertTrue(findClassInSystemCatalog("org.voltdb_testprocs.updateclasses.InnerClassesTestProc$InnerNotPublic"));
            assertTrue(findClassInSystemCatalog("org.voltdb_testprocs.updateclasses.InnerClassesTestProc$InnerWithConstructorArgs"));
            assertTrue(findClassInSystemCatalog("org.voltdb_testprocs.updateclasses.InnerClassesTestProc$InnerWithEasyConstructor"));
            assertTrue(findClassInSystemCatalog("org.voltdb_testprocs.updateclasses.InnerClassesTestProc$InnerWithNoConstructor"));

            // now just delete the parent class
            resp = m_client.callProcedure("@UpdateClasses", null,
                    "org.voltdb_testprocs.updateclasses.InnerClassesTestProc");
            // old stuff should have survived
            resp = m_client.callProcedure("@SystemCatalog", "CLASSES");
            // Non-inner stuff should have survived
            assertEquals(PROC_CLASSES.length + COLLIDING_CLASSES.length,
                    resp.getResults()[0].getRowCount());
            assertTrue(findClassInSystemCatalog(PROC_CLASSES[0].getCanonicalName()));
            assertTrue(findClassInSystemCatalog(PROC_CLASSES[1].getCanonicalName()));
            assertTrue(findClassInSystemCatalog(COLLIDING_CLASSES[0].getCanonicalName()));
            assertTrue(findClassInSystemCatalog(COLLIDING_CLASSES[1].getCanonicalName()));
            // Inner classes and parent gone
            assertFalse(findClassInSystemCatalog("org.voltdb_testprocs.updateclasses.InnerClassesTestProc"));
            assertFalse(findClassInSystemCatalog("org.voltdb_testprocs.updateclasses.InnerClassesTestProc$InnerNotPublic"));
            assertFalse(findClassInSystemCatalog("org.voltdb_testprocs.updateclasses.InnerClassesTestProc$InnerWithConstructorArgs"));
            assertFalse(findClassInSystemCatalog("org.voltdb_testprocs.updateclasses.InnerClassesTestProc$InnerWithEasyConstructor"));
            assertFalse(findClassInSystemCatalog("org.voltdb_testprocs.updateclasses.InnerClassesTestProc$InnerWithNoConstructor"));

            // Empty string has no effect
            resp = m_client.callProcedure("@UpdateClasses", null, "");
            // old stuff should have survived
            resp = m_client.callProcedure("@SystemCatalog", "CLASSES");
            // Non-inner stuff should have survived
            assertEquals(PROC_CLASSES.length + COLLIDING_CLASSES.length,
                    resp.getResults()[0].getRowCount());
            assertTrue(findClassInSystemCatalog(PROC_CLASSES[0].getCanonicalName()));
            assertTrue(findClassInSystemCatalog(PROC_CLASSES[1].getCanonicalName()));
            assertTrue(findClassInSystemCatalog(COLLIDING_CLASSES[0].getCanonicalName()));
            assertTrue(findClassInSystemCatalog(COLLIDING_CLASSES[1].getCanonicalName()));

            // pattern that matches nothing has no effect
            resp = m_client.callProcedure("@UpdateClasses", null, "com.voltdb.*");
            // old stuff should have survived
            resp = m_client.callProcedure("@SystemCatalog", "CLASSES");
            // Non-inner stuff should have survived
            assertEquals(PROC_CLASSES.length + COLLIDING_CLASSES.length,
                    resp.getResults()[0].getRowCount());
            assertTrue(findClassInSystemCatalog(PROC_CLASSES[0].getCanonicalName()));
            assertTrue(findClassInSystemCatalog(PROC_CLASSES[1].getCanonicalName()));
            assertTrue(findClassInSystemCatalog(COLLIDING_CLASSES[0].getCanonicalName()));
            assertTrue(findClassInSystemCatalog(COLLIDING_CLASSES[1].getCanonicalName()));
        }
        finally {
            teardownSystem();
        }
    }

    @Test
    public void testStatsAfterUpdateClasses() throws Exception {
        System.out.println("\n\n-----\n testStatsAfterUpdateClasses \n-----\n\n");

        String pathToCatalog = Configuration.getPathToCatalogForTest("updateclasses.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("updateclasses.xml");
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("create table tb1 (a int);");
        builder.setUseDDLSchema(true);
        boolean success = builder.compile(pathToCatalog, 1, 1, 0);
        assertTrue("Schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        // This is maybe cheating a little bit?
        InMemoryJarfile jarfile = new InMemoryJarfile();
        for (Class<?> clazz : PROC_CLASSES) {
            VoltCompiler comp = new VoltCompiler(false);
            comp.addClassToJar(jarfile, clazz);
        }
        for (Class<?> clazz : EXTRA_CLASSES) {
            VoltCompiler comp = new VoltCompiler(false);
            comp.addClassToJar(jarfile, clazz);
        }
        // Add a deployment file just to have something other than classes in the jar
        jarfile.put("deployment.xml", new File(pathToDeployment));

        try {
            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToCatalog = pathToCatalog;
            config.m_pathToDeployment = pathToDeployment;
            startSystem(config);

            ClientResponse resp;
            VoltTable vt;
            resp = m_client.callProcedure("@SystemCatalog", "CLASSES");
            // New cluster, you're like summer vacation...
            assertEquals(0, resp.getResults()[0].getRowCount());
            assertFalse(VoltTableTestHelpers.moveToMatchingRow(resp.getResults()[0], "CLASS_NAME",
                        PROC_CLASSES[0].getCanonicalName()));

            resp = m_client.callProcedure("@UpdateClasses", jarfile.getFullJarBytes(), null);

            // check stats after UAC
            vt = m_client.callProcedure("@Statistics", "PROCEDURE", 0).getResults()[0];
            // All procedure stats are cleared after catalog change
            assertEquals(0, vt.getRowCount());

            // create procedure 0
            resp = m_client.callProcedure("@AdHoc", "create procedure from class " +
                    PROC_CLASSES[0].getCanonicalName() + ";");
            // check stats after UAC
            vt = m_client.callProcedure("@Statistics", "PROCEDURE", 0).getResults()[0];
            // All procedure stats are cleared after catalog change
            assertEquals(vt.getRowCount(), 0);

            // invoke a new user procedure
            vt = m_client.callProcedure(PROC_CLASSES[0].getSimpleName()).getResults()[0];
            assertEquals(10L, vt.asScalarLong());
            vt = m_client.callProcedure(PROC_CLASSES[0].getSimpleName()).getResults()[0];
            assertEquals(10L, vt.asScalarLong());
            vt = m_client.callProcedure(PROC_CLASSES[0].getSimpleName()).getResults()[0];
            assertEquals(10L, vt.asScalarLong());

            // check stats
            vt = m_client.callProcedure("@Statistics", "PROCEDURE", 0).getResults()[0];
            // All procedure stats are cleared after catalog change
            assertEquals(1, vt.getRowCount());
            assertTrue(vt.toString().contains("org.voltdb_testprocs.updateclasses.testImportProc"));

            // create procedure 1
            resp = m_client.callProcedure("@AdHoc", "create procedure from class " +
                    PROC_CLASSES[1].getCanonicalName() + ";");
            // check stats
            vt = m_client.callProcedure("@Statistics", "PROCEDURE", 0).getResults()[0];
            assertEquals(0, vt.getRowCount());

            resp = m_client.callProcedure(PROC_CLASSES[1].getSimpleName(), 1l, "", "");
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());

            vt = m_client.callProcedure("@Statistics", "PROCEDURE", 0).getResults()[0];
            assertEquals(1, vt.getRowCount());

            vt = m_client.callProcedure(PROC_CLASSES[0].getSimpleName()).getResults()[0];
            assertEquals(10L, vt.asScalarLong());

            vt = m_client.callProcedure("@Statistics", "PROCEDURE", 0).getResults()[0];
            assertEquals(2, vt.getRowCount());

        }
        finally {
            teardownSystem();
        }
    }

    @Test
    public void testUpdateClassesAdvanced() throws Exception {
        System.out.println("\n\n-----\n testCreateProceduresBeforeUpdateClasses \n-----\n\n");

        String pathToCatalog = Configuration.getPathToCatalogForTest("updateclasses.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("updateclasses.xml");
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(
                "create table t1 (a int, b int); \n" +
                "create procedure proc1 as select a from t1 where b = ?;");
        builder.setUseDDLSchema(true);
        boolean success = builder.compile(pathToCatalog, 2, 1, 0);
        assertTrue("Schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        try {
            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToCatalog = pathToCatalog;
            config.m_pathToDeployment = pathToDeployment;
            startSystem(config);

            ClientResponse resp;
            resp = m_client.callProcedure("T1.insert", 1, 10);
            resp = m_client.callProcedure("T1.insert", 2, 20);
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());

            InMemoryJarfile boom = new InMemoryJarfile();
            VoltCompiler comp = new VoltCompiler(false);
            comp.addClassToJar(boom, org.voltdb_testprocs.updateclasses.NoMeaningClass.class);
            comp.addClassToJar(boom, org.voltdb_testprocs.updateclasses.testImportProc.class);
            try {
                resp = m_client.callProcedure("@UpdateClasses", boom.getFullJarBytes(), null);
                assertEquals(ClientResponse.SUCCESS, resp.getStatus());
            }
            catch (ProcCallException pce) {
                fail("@UpdateClasses should not fail with message: " + pce.getMessage());
            }

            resp = m_client.callProcedure("proc1", 3);
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());

            // create procedure
            resp = m_client.callProcedure("@AdHoc", "CREATE PROCEDURE FROM CLASS org.voltdb_testprocs.updateclasses.testImportProc;");
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());

            resp = m_client.callProcedure("testImportProc");
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());

            resp = m_client.callProcedure("@AdHoc", "select a from t1 where b = 10;");
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());

            // add a new class
            boom = new InMemoryJarfile();
            comp = new VoltCompiler(false);
            comp.addClassToJar(boom, org.voltdb_testprocs.updateclasses.TestProcWithSQLStmt.class);

            resp = m_client.callProcedure("@UpdateClasses", boom.getFullJarBytes(), null);
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());

            resp = m_client.callProcedure("@AdHoc", "select a from t1 where b = 10;");
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());

            // redundant operation
            resp = m_client.callProcedure("@UpdateClasses", boom.getFullJarBytes(), null);
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());

        }
        finally {
            teardownSystem();
        }
    }

    // See ENG-12536: Test UpdateClasses with changed SQLStmts
    @Test
    public void testUpdateClassesWithSQLStmtChanges() throws Exception {
        System.out.println("\n\n-----\n testUpdateClassesWithSQLStmtChanges \n-----\n\n");

        String pathToCatalog = Configuration.getPathToCatalogForTest("updateclasses.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("updateclasses.xml");
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(
                "create table tt (PID varchar(20 BYTES) NOT NULL, CITY varchar(6 BYTES), " +
                "CONSTRAINT IDX_TT_PKEY PRIMARY KEY (PID)); \n" +
                "PARTITION TABLE TT ON COLUMN PID;\n");

        builder.setUseDDLSchema(true);
        boolean success = builder.compile(pathToCatalog, 2, 1, 0);
        assertTrue("Schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        try {
            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToCatalog = pathToCatalog;
            config.m_pathToDeployment = pathToDeployment;
            startSystem(config);

            ClientResponse resp;

            // Testing system can load jar file from class path, but not the internal class files
            try {
                resp = m_client.callProcedure("TestProcedure", "12345", "boston");
                fail("TestProcedure is not loaded");
            } catch (ProcCallException e) {
                assertTrue(e.getMessage().contains("Procedure TestProcedure was not found"));
            }

            try {
                Class.forName("voter.TestProcedure");
                fail("Should not load the class file from the jar file on disk automatically");
            } catch (ClassNotFoundException e) {
                assertTrue(e.getMessage().contains("voter.TestProcedure"));
            }

            InMemoryJarfile boom = new InMemoryJarfile(TestProcedure.class.getResource("addSQLStmt.jar"));
            resp = m_client.callProcedure("@UpdateClasses", boom.getFullJarBytes(), null);
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());

            resp = m_client.callProcedure("@AdHoc", "create procedure partition ON TABLE tt COLUMN pid from class voter.TestProcedure;");
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());

            resp = m_client.callProcedure("TestProcedure", "12345", "boston");
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());

            // UpdateClass with the new changed StmtSQL jar
            boom = new InMemoryJarfile(TestProcedure.class.getResource("addSQLStmtNew.jar"));
            resp = m_client.callProcedure("@UpdateClasses", boom.getFullJarBytes(), null);
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());

            // run with a new query without problems
            resp = m_client.callProcedure("TestProcedure", "12345", "boston");
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());

            // Invalid SQLStmt should fail during UpdateClasses
            boom = new InMemoryJarfile(TestProcedure.class.getResource("addSQLStmtInvalid.jar"));
            try {
                resp = m_client.callProcedure("@UpdateClasses", boom.getFullJarBytes(), null);
                fail("Invalid SQLStmt should fail during UpdateClasses");
            } catch (ProcCallException e) {
                assertTrue(e.getMessage().contains("Failed to plan for statement"));
                assertTrue(e.getMessage().contains("object not found: TT_INVALID_QUERY"));
            }
        }
        finally {
            teardownSystem();
        }
    }

    @Test
    public void testUpdateClassesInvalidSQLStmt() throws Exception {
        System.out.println("\n\n-----\n testUpdateClassesInvalidSQLStmt \n-----\n\n");

        String pathToCatalog = Configuration.getPathToCatalogForTest("updateclasses.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("updateclasses.xml");
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(
                "create table t1 (a int, b int); \n" +
                "create procedure proc1 as select a from t1 where b = ?;");
        builder.setUseDDLSchema(true);
        boolean success = builder.compile(pathToCatalog, 2, 1, 0);
        assertTrue("Schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        try {
            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToCatalog = pathToCatalog;
            config.m_pathToDeployment = pathToDeployment;
            startSystem(config);

            ClientResponse resp;
            resp = m_client.callProcedure("T1.insert", 1, 10);
            resp = m_client.callProcedure("T1.insert", 2, 20);
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());

            // add a new class
            InMemoryJarfile boom = new InMemoryJarfile();
            VoltCompiler comp = new VoltCompiler(false);
            comp.addClassToJar(boom, org.voltdb_testprocs.updateclasses.TestProcWithInvalidSQLStmt.class);

            resp = m_client.callProcedure("@UpdateClasses", boom.getFullJarBytes(), null);
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());

            // create procedure
            resp = m_client.callProcedure("@AdHoc", "CREATE PROCEDURE FROM CLASS "
                    + "org.voltdb_testprocs.updateclasses.TestProcWithInvalidSQLStmt;");
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());

            try {
                resp = m_client.callProcedure("TestProcWithInvalidSQLStmt", 1);
                fail("Dynamic non-final SQLSTMT is invalid and should be caught with better error message");
            } catch (ProcCallException ex) {
                assertTrue(ex.getMessage().contains("SQLStmt is not declared as final or initialized at compile time"));
            }

            // redundant operation
            resp = m_client.callProcedure("@UpdateClasses", boom.getFullJarBytes(), null);
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());

        }
        finally {
            teardownSystem();
        }
    }

    /*
     * Do multiple big catalog updates that bring the total size of catalog in memory go beyond 50 MB.
     */
    @Test
    public void testUpdateBigJarFile() throws Exception {
        System.out.println("\n\n-----\n testUpdateBigJarFile \n-----\n\n");

        String pathToCatalog = Configuration.getPathToCatalogForTest("updateclasses.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("updateclasses.xml");
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(
                "create table tt (PID varchar(20 BYTES) NOT NULL, CITY varchar(6 BYTES), " +
                "CONSTRAINT IDX_TT_PKEY PRIMARY KEY (PID)); \n" +
                "PARTITION TABLE TT ON COLUMN PID;\n");

        builder.setUseDDLSchema(true);
        // Command logging and memcheck are incompatible so do not enable memcheck
        if (MiscUtils.isPro() && !LocalCluster.isMemcheckDefined()) {
            builder.configureLogging(true, true, 2, 2, 64);
        }
        LocalCluster lc = new LocalCluster("updateclasses.jar", 2, 1, 0, BackendTarget.NATIVE_EE_JNI);
        lc.setHasLocalServer(false);
        boolean success = lc.compile(builder);
        assertTrue("Schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);


        try {
            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToCatalog = pathToCatalog;
            config.m_pathToDeployment = pathToDeployment;
            lc.startUp();
            m_client = ClientFactory.createClient();
            m_client.createConnection("", lc.port(0));

            ClientResponse resp;

            // Testing system can load jar file from class path, but not the internal class files
            try {
                resp = m_client.callProcedure("TestProcedure", "12345", "boston");
                fail("TestProcedure is not loaded");
            } catch (ProcCallException e) {
                assertTrue(e.getMessage().contains("Procedure TestProcedure was not found"));
            }

            try {
                Class.forName("voter.TestProcedure");
                fail("Should not load the class file from the jar file on disk automatically");
            } catch (ClassNotFoundException e) {
                assertTrue(e.getMessage().contains("voter.TestProcedure"));
            }

            // Update classes with a 33M jar file
            InMemoryJarfile boom = new InMemoryJarfile(TestProcedure.class.getResource("addSQLStmtBig.jar"));
            resp = m_client.callProcedure("@UpdateClasses", boom.getFullJarBytes(), null);
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());

            resp = m_client.callProcedure("@AdHoc", "create procedure partition ON TABLE tt COLUMN pid from class voter.TestProcedure;");
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());

            resp = m_client.callProcedure("TestProcedure", "12345", "boston");
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());

            // UpdateClass with the new changed StmtSQL jar with the size of 23M, bring the total
            // catalog in memory to 56M.
            boom = new InMemoryJarfile(TestProcedure.class.getResource("addSQLStmtNewBig.jar"));
            resp = m_client.callProcedure("@UpdateClasses", boom.getFullJarBytes(), null);
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());

            if (MiscUtils.isPro() && !LocalCluster.isMemcheckDefined()) {
                // Shutdown then recover the cluster
                lc.shutDown();
                m_client.close();
                lc.startUp(false);

                m_client = ClientFactory.createClient();
                m_client.createConnection("", lc.port(0));
            }

            // run with a new query without problems
            resp = m_client.callProcedure("TestProcedure", "12345", "boston");
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());

            // Invalid SQLStmt should fail during UpdateClasses
            boom = new InMemoryJarfile(TestProcedure.class.getResource("addSQLStmtInvalid.jar"));
            try {
                resp = m_client.callProcedure("@UpdateClasses", boom.getFullJarBytes(), null);
                fail("Invalid SQLStmt should fail during UpdateClasses");
            } catch (ProcCallException e) {
                assertTrue(e.getMessage().contains("Failed to plan for statement"));
                assertTrue(e.getMessage().contains("object not found: TT_INVALID_QUERY"));
            }
        }
        finally {
            lc.shutDown();
            stopClient();
        }
    }
}
