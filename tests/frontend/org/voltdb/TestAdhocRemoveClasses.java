/* This file is part of VoltDB.
 * Copyright (C) 2022 VoltDB Inc.
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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltCompiler;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.InMemoryJarfile;
import org.voltdb.utils.MiscUtils;

public class TestAdhocRemoveClasses extends AdhocDDLTestBase {

    @Before
    public void setUp() throws Exception {
        String pathToCatalog = Configuration.getPathToCatalogForTest("adhocddl.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("adhocddl.xml");
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("-- empty schema");
        builder.setUseDDLSchema(true);
        boolean success = builder.compile(pathToCatalog, 2, 1, 0);
        assertTrue("Schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;

        startSystem(config);
    }

    public void loadClasses(Class<?>[] classes) throws Exception {
        if (classes.length == 0) {
            return;
        }

        InMemoryJarfile jarfile = new InMemoryJarfile();
        VoltCompiler comp = new VoltCompiler(false);
        for (Class<?> c : classes) {
            comp.addClassToJar(jarfile, c);
        }
        try {
             ClientResponse resp = m_client.callProcedure("@UpdateClasses", jarfile.getFullJarBytes(), null);
        } catch (ProcCallException pce) {
            fail("Should be able to load classes. Exception: " + pce.getMessage());
        }
    }

    @Test
    public void testRemoveProcedureClass() throws Exception {

        // load procedure class
        Class<?>[] classList = {org.voltdb_testprocs.fullddlfeatures.testCreateProcFromClassProc.class};
        loadClasses(classList);

        ClientResponse resp;

        // create procedure
        try {
            resp = m_client.callProcedure("@AdHoc","create procedure from class org.voltdb_testprocs.fullddlfeatures.testCreateProcFromClassProc");
        } catch (ProcCallException pce) {
            fail("Should be able to create a procedure from a procedure class. Exception: " + pce.getMessage());
        }
        assertTrue("Something is wrong, procedure not found",findProcedureInSystemCatalog("testCreateProcFromClassProc"));

        // try to remove the procedure class, should fail
        try {
            resp = m_client.callProcedure("@UpdateClasses", null,
                                          "org.voltdb_testprocs.fullddlfeatures.*");
            fail("Shouldn't be able to rip a class out from under an active proc");
        }
        catch (ProcCallException pce) {
            assertTrue(pce.getMessage(),
                       pce.getMessage().contains("Invalid modification of classes: Class "
                                                 + "org.voltdb_testprocs.fullddlfeatures.testCreateProcFromClassProc"
                                                 + " cannot be removed, it is being used by procedure"));
        }

        // drop procedure
        try {
            resp = m_client.callProcedure("@AdHoc","drop procedure testCreateProcFromClassProc");
        } catch (ProcCallException pce) {
            fail("Should be able to drop procedure. Exception: " + pce.getMessage());
        }
        assertFalse("Something is wrong, procedure should be gone",findProcedureInSystemCatalog("testCreateProcFromClassProc"));

        // try to remove the procedure class, should succeed
        try {
            resp = m_client.callProcedure("@UpdateClasses", null,
                                          "org.voltdb_testprocs.fullddlfeatures.*");
        }
        catch (ProcCallException pce) {
            fail("Couldn't remove class: " + pce.getMessage());
        }

    }

    @Test
    public void testRemoveFunctionClass() throws Exception {

        // load a function class
        Class<?>[] classList = {org.voltdb_testfuncs.Ucount.class};
        loadClasses(classList);

        ClientResponse resp;

        // create a function
        try {
            resp = m_client.callProcedure("@AdHoc","CREATE AGGREGATE FUNCTION ucount FROM CLASS org.voltdb_testfuncs.Ucount;");
        } catch (ProcCallException pce) {
            fail("Should be able to create an aggregate function from a class. Exception: " + pce.getMessage());
        }
        assertTrue("Something is wrong, function not found",findFunctionInSystemCatalog("ucount"));

        // try to remove class - should fail
        try {
            resp = m_client.callProcedure("@UpdateClasses", null,
                                          "org.voltdb_testfuncs.*");
            fail("Shouldn't be able to rip a class out from under an active function");
        }
        catch (ProcCallException pce) {
            assertTrue(pce.getMessage(),
                       pce.getMessage().contains("Invalid modification of classes: Class "
                                                 + "org.voltdb_testfuncs.Ucount"
                                                 + " cannot be removed, it is used by function"));
        }

        // drop function
        try {
            resp = m_client.callProcedure("@AdHoc","drop function ucount");
        } catch (ProcCallException pce) {
            fail("Should be able to drop function. Exception: " + pce.getMessage());
        }
        assertFalse("Something is wrong, function should be gone",findFunctionInSystemCatalog("ucount"));

        // try to remove class - should succeed
        try {
            resp = m_client.callProcedure("@UpdateClasses", null,
                                          "org.voltdb_testfuncs.*");
        }
        catch (ProcCallException pce) {
            fail("Should be able to remove class: " + pce.getMessage());
        }
    }

    @Test
    public void testRemoveTaskClass() throws Exception {

        // load task classes
        Class<?>[] classList = {
            org.voltdb.tasktest.CustomTestActionGenerator.class,
            org.voltdb.tasktest.CustomTestActionScheduler.class,
            org.voltdb.task.TestTasksEnd2End.class
        };
        loadClasses(classList);

        // create a task with a custom action scheduler
        String actionScheduler = org.voltdb.task.TestTasksEnd2End.CustomScheduler.class.getName();
        String taskName = "test_task";
        ClientResponse resp;
        try {
            resp = m_client.callProcedure("@AdHoc","CREATE TASK " + taskName + " FROM CLASS " + actionScheduler + " WITH (5, NULL);");
        } catch (ProcCallException pce) {
            fail("Should be able to create task");
        }
        assertTrue("Something is wrong, task not found",findTaskInSystemCatalog(taskName));

        // try to remove class - should fail
        try {
            resp = m_client.callProcedure("@UpdateClasses", null,
                                          "org.voltdb.task.TestTasksEnd2End*");
            fail("Shouldn't be able to rip a class out from under an active task");
        }
        catch (ProcCallException pce) {
            assertTrue(pce.getMessage(),
                       pce.getMessage().contains("Invalid modification of classes: Class org.voltdb.task.TestTasksEnd2End cannot be removed, it is the action scheduler for task " + taskName));

        }

        // drop task
        try {
            resp = m_client.callProcedure("@AdHoc","drop task " + taskName);
        } catch (ProcCallException pce) {
            fail("Should be able to drop task. Exception: " + pce.getMessage());
        }

        // try to remove class - should succeed
        try {
            resp = m_client.callProcedure("@UpdateClasses", null,
                                          "org.voltdb.task.TestTasksEnd2End*");
        }
        catch (ProcCallException pce) {
            fail("Couldn't remove class: " + pce.getMessage());
        }

        // create task with custom action generator
        String actionGenerator = "org.voltdb.tasktest.CustomTestActionGenerator";
        taskName = "test_task2";
        try {
            resp = m_client.callProcedure("@AdHoc","CREATE TASK " + taskName + " ON SCHEDULE DELAY 5 SECONDS PROCEDURE FROM CLASS " + actionGenerator + ";");
        } catch (ProcCallException pce) {
            fail("Should be able to create task: " + pce.getMessage());
        }
        assertTrue("Something is wrong, task not found",findTaskInSystemCatalog(taskName));

        // try to remove class - should fail
        try {
            resp = m_client.callProcedure("@UpdateClasses", null, actionGenerator);
            fail("Shouldn't be able to rip a class out from under an active task");
        }
        catch (ProcCallException pce) {
            assertTrue(pce.getMessage(),
                       pce.getMessage().contains("Invalid modification of classes: Class " + actionGenerator + " cannot be removed, it is the action generator for task " + taskName));

        }

        // drop task
        try {
            resp = m_client.callProcedure("@AdHoc","drop task " + taskName);
        } catch (ProcCallException pce) {
            fail("Should be able to drop task. Exception: " + pce.getMessage());
        }

        // try to remove class - should succeed
        try {
            resp = m_client.callProcedure("@UpdateClasses", null, actionGenerator);
        }
        catch (ProcCallException pce) {
            fail("Couldn't remove class: " + pce.getMessage());
        }
    }

    @After
    public void teardown() throws Exception {
        teardownSystem();
    }
}
