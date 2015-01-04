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
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.MiscUtils;

public class TestAdhocCreateTable extends AdhocDDLTestBase {

    // Add a test for partitioning a table

    public void testBasicCreateTable() throws Exception
    {
        String pathToCatalog = Configuration.getPathToCatalogForTest("adhocddl.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("adhocddl.xml");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("--dont care");
        builder.setUseDDLSchema(true);
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
                        "create table FOO (ID int default 0, VAL varchar(64 bytes));");
            }
            catch (ProcCallException pce) {
                fail("create table should have succeeded");
            }
            assertTrue(findTableInSystemCatalogResults("FOO"));
            // make sure we can't create the same table twice
            boolean threw = false;
            try {
                m_client.callProcedure("@AdHoc",
                        "create table FOO (ID int default 0, VAL varchar(64 bytes));");
            }
            catch (ProcCallException pce) {
                threw = true;
            }
            assertTrue("Shouldn't have been able to create table FOO twice.", threw);
        }
        finally {
            teardownSystem();
        }
    }

    // Test creating a table when we feed a statement containing newlines.
    // I honestly didn't expect this to work yet --izzy
    public void testMultiLineCreateTable() throws Exception {

        String pathToCatalog = Configuration.getPathToCatalogForTest("adhocddl.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("adhocddl.xml");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("--dont care");
        builder.setUseDDLSchema(true);
        boolean success = builder.compile(pathToCatalog, 2, 1, 0);
        assertTrue("Schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;

        try {
            startSystem(config);

            // Check basic drop of partitioned table that should work.
            ClientResponse resp = m_client.callProcedure("@SystemCatalog", "TABLES");
            assertFalse(findTableInSystemCatalogResults("FOO"));
            System.out.println(resp.getResults()[0]);
            try {
                m_client.callProcedure("@AdHoc",
                        "create table FOO (\n" +
                        "ID int default 0 not null,\n" +
                        "VAL varchar(32 bytes)\n" +
                        ");");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("create table should have succeeded");
            }
            resp = m_client.callProcedure("@SystemCatalog", "TABLES");
            System.out.println(resp.getResults()[0]);
            assertTrue(findTableInSystemCatalogResults("FOO"));
        }
        finally {
            teardownSystem();
        }
    }

    public void testCreatePartitionedTable() throws Exception {

        String pathToCatalog = Configuration.getPathToCatalogForTest("adhocddl.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("adhocddl.xml");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("--dont care");
        builder.setUseDDLSchema(true);
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
                        "VAL varchar(32 bytes),\n" +
                        "VAL2 bigint not null assumeunique\n" +
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

            // This, however, not being batched, won't work until the empty table
            // check goes in.
            resp = m_client.callProcedure("@SystemCatalog", "TABLES");
            assertFalse(findTableInSystemCatalogResults("BAR"));
            boolean threw = false;
            try {
                m_client.callProcedure("@AdHoc",
                        "create table BAR (\n" +
                        "ID int default 0 not null,\n" +
                        "VAL varchar(32 bytes),\n" +
                        // check that we can create a table with assumeunique
                        // (starts replicated) and partition it in a separate @AdHoc call
                        "VAL2 bigint not null assumeunique,\n" +
                        "constraint blerg assumeunique(VAL)\n" +
                        ");\n");
                m_client.callProcedure("@AdHoc",
                        "partition table BAR on column ID;\n"
                        );
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                threw = true;
            }
            assertFalse("Failed to partition an already created table.", threw);
            resp = m_client.callProcedure("@SystemCatalog", "TABLES");
            assertTrue(findTableInSystemCatalogResults("BAR"));
            assertTrue(isColumnPartitionColumn("BAR", "ID"));
        }
        finally {
            teardownSystem();
        }
    }
}
