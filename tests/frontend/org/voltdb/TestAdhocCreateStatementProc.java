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
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.MiscUtils;

public class TestAdhocCreateStatementProc extends AdhocDDLTestBase {

    public void testBasicCreateStatementProc() throws Exception
    {
        String pathToCatalog = Configuration.getPathToCatalogForTest("adhocddl.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("adhocddl.xml");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(
                "create table FOO (" +
                "ID integer not null," +
                "VAL bigint, " +
                "constraint PK_TREE primary key (ID)" +
                ");\n" +
                "create table FOO_R (" +
                "ID integer not null," +
                "VAL bigint, " +
                "constraint PK_TREE_R primary key (ID)" +
                ");\n"
                );
        builder.addPartitionInfo("FOO", "ID");
        builder.setUseDDLSchema(true);
        boolean success = builder.compile(pathToCatalog, 2, 1, 0);
        assertTrue("Schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;

        try {
            startSystem(config);
            // Procedure shouldn't exist
            boolean threw = false;
            assertFalse(findProcedureInSystemCatalog("FOOCOUNT"));
            try {
                m_client.callProcedure("FOOCOUNT", 1000L);
            }
            catch (ProcCallException pce) {
                assertTrue(pce.getMessage().contains("Procedure FOOCOUNT was not found"));
                threw = true;
            }
            assertTrue("FOOCOUNT procedure shouldn't exist", threw);
            try {
                m_client.callProcedure("@AdHoc",
                        "create procedure FOOCOUNT as select * from FOO where ID=?;");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to create statement procedure");
            }
            assertTrue(findProcedureInSystemCatalog("FOOCOUNT"));
            assertFalse(verifySinglePartitionProcedure("FOOCOUNT"));
            // Make sure we can call it
            try {
                m_client.callProcedure("FOOCOUNT", 1000L);
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to call procedure FOOCOUNT");
            }
            // partition that sucker
            try {
                m_client.callProcedure("@AdHoc",
                        "partition procedure FOOCOUNT on table FOO column ID parameter 0;");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to partition the procedure FOOCOUNT");
            }
            // Make sure we can call it
            assertTrue(verifySinglePartitionProcedure("FOOCOUNT"));
            try {
                m_client.callProcedure("FOOCOUNT", 1000L);
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to call procedure FOOCOUNT");
            }

            // now drop it
            try {
                m_client.callProcedure("@AdHoc", "drop procedure FOOCOUNT");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to drop procedure FOOCOUNT");
            }
            assertFalse(findProcedureInSystemCatalog("FOOCOUNT"));

            // Can't drop it twice
            threw = false;
            try {
                m_client.callProcedure("@AdHoc", "drop procedure FOOCOUNT");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                threw = true;
            }
            assertTrue("Can't vanilla drop procedure FOOCOUNT twice", threw);

            // unless we use if exists
            try {
                m_client.callProcedure("@AdHoc", "drop procedure FOOCOUNT if exists");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to drop procedure FOOCOUNT twice with if exists");
            }

            // Create it again so we can destroy it with drop with if exists, just to be sure
            try {
                m_client.callProcedure("@AdHoc",
                        "create procedure FOOCOUNT as select * from FOO where ID=?;");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to create statement procedure");
            }
            assertTrue(findProcedureInSystemCatalog("FOOCOUNT"));

            // now drop it
            try {
                m_client.callProcedure("@AdHoc", "drop procedure FOOCOUNT if exists");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to drop procedure FOOCOUNT");
            }
            assertFalse(findProcedureInSystemCatalog("FOOCOUNT"));
        }
        finally {
            teardownSystem();
        }
    }
}
