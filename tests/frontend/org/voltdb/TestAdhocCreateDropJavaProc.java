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

import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltCompiler;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.utils.InMemoryJarfile;
import org.voltdb.utils.MiscUtils;

public class TestAdhocCreateDropJavaProc extends AdhocDDLTestBase {

    static Class<?>[] PROC_CLASSES = { org.voltdb_testprocs.updateclasses.testImportProc.class,
        org.voltdb_testprocs.updateclasses.testCreateProcFromClassProc.class,
        org.voltdb_testprocs.updateclasses.InnerClassesTestProc.class };

    static Class<?>[] EXTRA_CLASSES = { org.voltdb_testprocs.updateclasses.NoMeaningClass.class };

    public void testBasic() throws Exception
    {
        System.out.println("\n\n-----\n testBasic \n-----\n\n");

        String pathToCatalog = Configuration.getPathToCatalogForTest("updateclasses.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("updateclasses.xml");
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("-- Don't care");
        builder.setUseDDLSchema(true);
        boolean success = builder.compile(pathToCatalog, 2, 1, 0);
        assertTrue("Schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        try {
            LocalCluster cluster = new LocalCluster("updateclasses.jar", 2, 1, 0, BackendTarget.NATIVE_EE_JNI);
            cluster.compile(builder);
            cluster.setHasLocalServer(false);
            cluster.startUp();
            m_client = ClientFactory.createClient();
            m_client.createConnection(cluster.getListenerAddress(0));

            ClientResponse resp;
            // Can't create a procedure without a class, Marge
            resp = m_client.callProcedure("@SystemCatalog", "CLASSES");
            System.out.println("CLASSES: " + resp.getResults()[0]);
            boolean threw = false;
            try {
                resp = m_client.callProcedure("@AdHoc",
                        "create procedure from class org.voltdb_testprocs.updateclasses.testImportProc");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                threw = true;
            }
            assertTrue("Shouldn't be able to create a procedure backed by no class", threw);
            assertFalse(findProcedureInSystemCatalog("testImportProc"));

            InMemoryJarfile jarfile = new InMemoryJarfile();
            VoltCompiler comp = new VoltCompiler();
            comp.addClassToJar(jarfile, org.voltdb_testprocs.updateclasses.testImportProc.class);

            resp = m_client.callProcedure("@UpdateClasses", jarfile.getFullJarBytes(), null);
            // Missing the dependency, try again.  It will succeed but we won't be able to
            // call the procedure.  Maybe this gets better in the future
            try {
                resp = m_client.callProcedure("@AdHoc",
                        "create procedure from class org.voltdb_testprocs.updateclasses.testImportProc");
            }
            catch (ProcCallException pce) {
                fail("We allow procedures to be created with unsatisfied dependencies");
            }
            assertTrue(findProcedureInSystemCatalog("testImportProc"));
            // Make sure we don't crash when we call it though
            threw = false;
            try {
                resp = m_client.callProcedure("testImportProc");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                assertTrue(pce.getMessage().contains("ClassNotFoundException"));
                threw = true;
            }
            assertTrue("Should return an error and not crash calling procedure w/ bad dependencies",
                    threw);

            // Okay, add the missing dependency
            jarfile = new InMemoryJarfile();
            comp = new VoltCompiler();
            comp.addClassToJar(jarfile, org.voltdb_testprocs.updateclasses.NoMeaningClass.class);
            resp = m_client.callProcedure("@UpdateClasses", jarfile.getFullJarBytes(), null);
            // now we should be able to call it
            try {
                resp = m_client.callProcedure("testImportProc");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to call fully consistent procedure");
            }
            assertEquals(10L, resp.getResults()[0].asScalarLong());

            // Now try to remove the procedure class
            threw = false;
            try {
                resp = m_client.callProcedure("@UpdateClasses", null,
                        "org.voltdb_testprocs.updateclasses.*");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                assertTrue(pce.getMessage().contains("modifying classes from catalog"));
                threw = true;
            }
            assertTrue("Shouldn't be able to rip a class out from under an active proc",
                    threw);
            // Make sure we didn't purge anything (even the extra dependency)
            resp = m_client.callProcedure("@SystemCatalog", "CLASSES");
            assertEquals(2, resp.getResults()[0].getRowCount());

            // Okay, drop the procedure first
            try {
                resp = m_client.callProcedure("@AdHoc", "drop procedure testImportProc");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to drop a stored procedure");
            }
            assertFalse(findProcedureInSystemCatalog("testImportProc"));

            // Now try to remove the procedure class again
            try {
                resp = m_client.callProcedure("@UpdateClasses", null,
                        "org.voltdb_testprocs.updateclasses.*");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to remove the classes for an inactive procedure");
            }
            resp = m_client.callProcedure("@SystemCatalog", "CLASSES");
            assertEquals(0, resp.getResults()[0].getRowCount()); // no classes in catalog
        }
        finally {
            teardownSystem();
        }
    }

    // This test should trigger the same failure seen in ENG-6611
    public void testCreateUsingExistingImport() throws Exception
    {
        System.out.println("\n\n-----\n testCreateUsingExistingImport \n-----\n\n");

        String pathToCatalog = Configuration.getPathToCatalogForTest("updateclasses.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("updateclasses.xml");
        VoltProjectBuilder builder = new VoltProjectBuilder();
        // Start off with the dependency imported
        builder.addLiteralSchema("import class org.voltdb_testprocs.updateclasses.NoMeaningClass;");
        builder.setUseDDLSchema(true);
        boolean success = builder.compile(pathToCatalog, 2, 1, 0);
        assertTrue("Schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        try {
            LocalCluster cluster = new LocalCluster("updateclasses.jar", 2, 1, 0, BackendTarget.NATIVE_EE_JNI);
            cluster.compile(builder);
            cluster.setHasLocalServer(false);
            cluster.startUp();
            m_client = ClientFactory.createClient();
            m_client.createConnection(cluster.getListenerAddress(0));

            ClientResponse resp;
            resp = m_client.callProcedure("@SystemCatalog", "CLASSES");
            System.out.println(resp.getResults()[0]);

            // Now load the procedure requiring the already-resident dependency
            InMemoryJarfile jarfile = new InMemoryJarfile();
            VoltCompiler comp = new VoltCompiler();
            comp.addClassToJar(jarfile, org.voltdb_testprocs.updateclasses.testImportProc.class);

            try {
                resp = m_client.callProcedure("@UpdateClasses", jarfile.getFullJarBytes(), null);
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Triggered ENG-6611!");
            }
            resp = m_client.callProcedure("@SystemCatalog", "CLASSES");
            assertEquals(2, resp.getResults()[0].getRowCount());
            // create the proc and make sure it runs
            try {
                resp = m_client.callProcedure("@AdHoc",
                        "create procedure from class org.voltdb_testprocs.updateclasses.testImportProc");
            }
            catch (ProcCallException pce) {
                fail("Should be able to create testImportProc procedure");
            }
            assertTrue(findProcedureInSystemCatalog("testImportProc"));
            try {
                resp = m_client.callProcedure("testImportProc");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to call fully consistent procedure");
            }
            assertEquals(10L, resp.getResults()[0].asScalarLong());
        }
        finally {
            teardownSystem();
        }
    }
}
