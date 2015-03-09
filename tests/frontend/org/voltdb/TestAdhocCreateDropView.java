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

public class TestAdhocCreateDropView extends AdhocDDLTestBase {

    public void testBasicCreateView() throws Exception
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

            // create a basic view
            assertFalse(findTableInSystemCatalogResults("FOOVIEW"));
            try {
                m_client.callProcedure("@AdHoc",
                    "create view FOOVIEW (VAL, TOTAL) as " +
                    "select VAL, COUNT(*) from FOO group by VAL;");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to create a view");
            }
            assertTrue(findTableInSystemCatalogResults("FOOVIEW"));
            // can't do it again
            boolean threw = false;
            try {
                m_client.callProcedure("@AdHoc",
                    "create view FOOVIEW (VAL, TOTAL) as " +
                    "select VAL, COUNT(*) from FOO group by VAL;");
            }
            catch (ProcCallException pce) {
                threw = true;
            }
            assertTrue("Shouldn't be able to create the same view twice", threw);
            assertTrue(findTableInSystemCatalogResults("FOOVIEW"));
            // drop it like it's hot
            try {
                m_client.callProcedure("@AdHoc",
                    "drop view FOOVIEW;");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to drop a view");
            }
            assertFalse(findTableInSystemCatalogResults("FOOVIEW"));
            // Not a second time
            threw = false;
            try {
                m_client.callProcedure("@AdHoc",
                    "drop view FOOVIEW;");
            }
            catch (ProcCallException pce) {
                threw = true;
            }
            assertTrue("Shouldn't be able to drop a view twice", threw);
            assertFalse(findTableInSystemCatalogResults("FOOVIEW"));
            // unless if exists is there
            try {
                m_client.callProcedure("@AdHoc",
                    "drop view FOOVIEW if exists;");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to drop a bad view with if exists");
            }
            assertFalse(findTableInSystemCatalogResults("FOOVIEW"));
        }
        finally {
            teardownSystem();
        }
    }
}
