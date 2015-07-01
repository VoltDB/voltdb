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

import java.io.File;
import java.io.IOException;

import org.voltdb.AdhocDDLTestBase;
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
            VoltCompiler comp = new VoltCompiler();
            comp.addClassToJar(jarfile, clazz);
        }
        for (Class<?> clazz : EXTRA_CLASSES) {
            VoltCompiler comp = new VoltCompiler();
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

            // First, some tests of incorrect parameters
            // only 1 param
            threw = false;
            try {
                resp = m_client.callProcedure("@UpdateClasses", jarfile.getFullJarBytes());
            }
            catch (ProcCallException pce) {
                assertTrue(pce.getMessage().contains("UpdateClasses system procedure requires exactly two parameters"));
                threw = true;
            }
            assertTrue(threw);

            // wrong jarfile param type
            threw = false;
            try {
                resp = m_client.callProcedure("@UpdateClasses", 10L, null);
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
        }
        finally {
            teardownSystem();
        }
    }

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
            VoltCompiler comp = new VoltCompiler();
            comp.addClassToJar(jarfile, clazz);
        }
        for (Class<?> clazz : EXTRA_CLASSES) {
            VoltCompiler comp = new VoltCompiler();
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
                assertTrue(pce.getMessage().contains("does not have sysproc permission"));
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
            VoltCompiler comp = new VoltCompiler();
            comp.addClassToJar(jarfile, clazz);
        }
        for (Class<?> clazz : EXTRA_CLASSES) {
            VoltCompiler comp = new VoltCompiler();
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
                VoltCompiler comp = new VoltCompiler();
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
            VoltCompiler comp = new VoltCompiler();
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
            comp = new VoltCompiler();
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
            VoltCompiler comp = new VoltCompiler();
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
            comp = new VoltCompiler();
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
            VoltCompiler comp = new VoltCompiler();
            comp.addClassToJar(jarfile, clazz);
        }
        for (Class<?> clazz : EXTRA_CLASSES) {
            VoltCompiler comp = new VoltCompiler();
            comp.addClassToJar(jarfile, clazz);
        }
        for (Class<?> clazz : COLLIDING_CLASSES) {
            VoltCompiler comp = new VoltCompiler();
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
                VoltCompiler comp = new VoltCompiler();
                comp.addClassToJar(jarfile2, clazz);
            }
            for (Class<?> clazz : COLLIDING_CLASSES) {
                VoltCompiler comp = new VoltCompiler();
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
            VoltCompiler comp = new VoltCompiler();
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
}
