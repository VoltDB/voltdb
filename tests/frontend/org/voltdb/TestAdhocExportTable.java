/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.MiscUtils;

public class TestAdhocExportTable extends AdhocDDLTestBase {

    // Add a test for partitioning a table

    public void testBasicExportTable() throws Exception
    {
        String pathToCatalog = Configuration.getPathToCatalogForTest("adhocddl.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("adhocddl.xml");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("--dont care");
        builder.addExport(true, null, null);
        builder.setUseAdhocSchema(true);
        boolean success = builder.compile(pathToCatalog, 2, 1, 0);
        assertTrue("Schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;

        try {
            startSystem(config);

            assertFalse(findTableInSystemCatalogResults("FOO"));
            try {
                m_client.callProcedure("@AdHoc",
                        "create table FOO (ID int default 0, VAL varchar(64 bytes)); export table foo;");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("create table should have succeeded");
            }
            assertTrue(findTableInSystemCatalogResults("FOO"));

            boolean threw = false;
            try {
                m_client.callProcedure("@AdHoc", "drop table FOO;");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to export a table");
            }
        }
        finally {
            teardownSystem();
        }
    }

    public void testExportPartitionedTable() throws Exception {

        String pathToCatalog = Configuration.getPathToCatalogForTest("adhocddl.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("adhocddl.xml");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("--dont care");
        builder.addExport(true, null, null);
        builder.setUseAdhocSchema(true);
        boolean success = builder.compile(pathToCatalog, 2, 1, 0);
        assertTrue("Schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;

        try {
            startSystem(config);

            // Check basic create of partitioned table that should work.
            ClientResponse resp = m_client.callProcedure("@SystemCatalog", "TABLES");
            assertFalse(findTableInSystemCatalogResults("FOO"));
            System.out.println(resp.getResults()[0]);
            try {
                m_client.callProcedure("@AdHoc",
                        "create table FOO (\n" +
                        "ID int default 0 not null,\n" +
                        "VAL varchar(32 bytes)\n" +
                        ");\n" +
                        "partition table FOO on column ID;\n"
                        );
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("create table should have succeeded");
            }
            resp = m_client.callProcedure("@SystemCatalog", "TABLES");
            System.out.println(resp.getResults()[0]);
            assertTrue(findTableInSystemCatalogResults("FOO"));
            assertTrue(isColumnPartitionColumn("FOO", "ID"));

            boolean threw = false;
            try {
                m_client.callProcedure("@AdHoc", "export table FOO;");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to export a table");
            }
        }
        finally {
            teardownSystem();
        }
    }
}
