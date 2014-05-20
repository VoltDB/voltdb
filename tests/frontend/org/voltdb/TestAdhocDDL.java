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


import junit.framework.TestCase;

import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.MiscUtils;

public class TestAdhocDDL extends TestCase {

            //adHocQuery = "CREATE TABLE ICAST2 (C1 INT, C2 FLOAT);";
            //adHocQuery = "CREATE INDEX IDX_PROJ_PNAME ON PROJ(PNAME);";
            //adHocQuery = "DROP TABLE PROJ;";
            //adHocQuery = "PARTITION TABLE PROJ ON COLUMN PNUM;";
            //adHocQuery = "CREATE PROCEDURE AS SELECT 1 FROM PROJ;";
            //adHocQuery = "CREATE PROCEDURE FROM CLASS bar.Foo;";

    private boolean findTableInSystemCatalogResults(VoltTable tables, String table)
    {
        boolean found = false;
        tables.resetRowPosition();
        while (tables.advanceRow()) {
            String thisTable = tables.getString("TABLE_NAME");
            if (thisTable.equalsIgnoreCase(table)) {
                found = true;
                break;
            }
        }
        return found;
    }

    private boolean findIndexInSystemCatalogResults(VoltTable indexinfo, String index)
    {
        boolean found = false;
        indexinfo.resetRowPosition();
        while (indexinfo.advanceRow()) {
            String thisindex = indexinfo.getString("INDEX_NAME");
            if (thisindex.equalsIgnoreCase(index)) {
                found = true;
                break;
            }
        }
        return found;
    }

    public void testDropTableBasic() throws Exception {

        String pathToCatalog = Configuration.getPathToCatalogForTest("adhocddl.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("adhocddl.xml");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(
            "create table BLAH (" +
            "ID int default 0 not null, " +
            "VAL varchar(32) default null," +
            "PRIMARY KEY(ID));\n" +
            "create table DROPME (" +
            "ID int default 0 not null, " +
            "VAL varchar(32) default null," +
            "PRIMARY KEY(ID))\n;" +
            "create table DROPME_R (" +
            "ID int default 0 not null, " +
            "VAL varchar(32) default null," +
            "PRIMARY KEY(ID));");
        builder.addPartitionInfo("BLAH", "ID");
        builder.addPartitionInfo("DROPME", "ID");
        boolean success = builder.compile(pathToCatalog, 2, 1, 0);
        assertTrue("Schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;
        ServerThread localServer = new ServerThread(config);

        Client client = null;

        try {
            localServer.start();
            localServer.waitForInitialization();

            client = ClientFactory.createClient();
            client.createConnection("localhost");

            // Check basic drop of partitioned table that should work.
            ClientResponse resp = client.callProcedure("@SystemCatalog", "TABLES");
            System.out.println(resp.getResults()[0]);
            assertTrue(findTableInSystemCatalogResults(resp.getResults()[0], "DROPME"));
            try {
                client.callProcedure("@AdHoc", "drop table DROPME;");
            }
            catch (ProcCallException pce) {
                fail("drop table should have succeeded");
            }
            resp = client.callProcedure("@SystemCatalog", "TABLES");
            System.out.println(resp.getResults()[0]);
            assertFalse(findTableInSystemCatalogResults(resp.getResults()[0], "DROPME"));
            // Check basic drop of replicated table that should work.
            resp = client.callProcedure("@SystemCatalog", "TABLES");
            System.out.println(resp.getResults()[0]);
            assertTrue(findTableInSystemCatalogResults(resp.getResults()[0], "DROPME_R"));
            try {
                client.callProcedure("@AdHoc", "drop table DROPME_R;");
            }
            catch (ProcCallException pce) {
                fail("drop table should have succeeded");
            }
            resp = client.callProcedure("@SystemCatalog", "TABLES");
            System.out.println(resp.getResults()[0]);
            assertFalse(findTableInSystemCatalogResults(resp.getResults()[0], "DROPME_R"));

            // Verify dropping a table that doesn't exist fails
            boolean threw = false;
            try {
                client.callProcedure("@AdHoc", "drop table DROPME;");
            }
            catch (ProcCallException pce) {
                assertTrue(pce.getMessage().contains("object not found: DROPME"));
                threw = true;
            }
            assertTrue("Dropping bad table should have failed", threw);

            // Verify dropping a table that doesn't exist is fine with IF EXISTS
            threw = false;
            try {
                client.callProcedure("@AdHoc", "drop table DROPME IF EXISTS;");
            }
            catch (ProcCallException pce) {
                threw = true;
            }
            assertFalse("Dropping bad table with IF EXISTS should not have failed", threw);
        }
        finally {
            if (client != null) client.close();
            client = null;

            if (localServer != null) {
                localServer.shutdown();
                localServer.join();
            }
            localServer = null;

            // no clue how helpful this is
            System.gc();
        }
    }

    public void testDropTableWithIndexesAndProcedures() throws Exception {

        String pathToCatalog = Configuration.getPathToCatalogForTest("adhocddl.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("adhocddl.xml");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(
            "create table BLAH (" +
            "ID int default 0 not null, " +
            "VAL varchar(32) default null," +
            "PRIMARY KEY(ID));\n" +
            "create table DROPME (" +
            "ID int default 0 not null, " +
            "VAL varchar(32) default null," +
            "PRIMARY KEY(ID));\n" +
            "create assumeunique index pkey_idx on DROPME(VAL);\n");
        builder.addPartitionInfo("BLAH", "ID");
        builder.addPartitionInfo("DROPME", "ID");
        builder.addStmtProcedure("BLERG", "select * from BLAH where ID = ?");
        boolean success = builder.compile(pathToCatalog, 2, 1, 0);
        assertTrue("Schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;
        ServerThread localServer = new ServerThread(config);

        Client client = null;

        try {
            localServer.start();
            localServer.waitForInitialization();

            client = ClientFactory.createClient();
            client.createConnection("localhost");

            // Check basic drop of table with an index on it
            ClientResponse resp = client.callProcedure("@SystemCatalog", "TABLES");
            System.out.println(resp.getResults()[0]);
            assertTrue(findTableInSystemCatalogResults(resp.getResults()[0], "DROPME"));
            resp = client.callProcedure("@SystemCatalog", "INDEXINFO");
            System.out.println(resp.getResults()[0]);
            assertTrue(findIndexInSystemCatalogResults(resp.getResults()[0], "PKEY_IDX"));
            try {
                client.callProcedure("@AdHoc", "drop table DROPME;");
            }
            catch (ProcCallException pce) {
                fail("drop table should have succeeded");
            }
            resp = client.callProcedure("@SystemCatalog", "TABLES");
            System.out.println(resp.getResults()[0]);
            assertFalse(findTableInSystemCatalogResults(resp.getResults()[0], "DROPME"));
            resp = client.callProcedure("@SystemCatalog", "INDEXINFO");
            System.out.println(resp.getResults()[0]);
            assertFalse(findIndexInSystemCatalogResults(resp.getResults()[0], "PKEY_IDX"));

            // Verify that we can't drop a table that a procedure depends on
            resp = client.callProcedure("@SystemCatalog", "TABLES");
            System.out.println(resp.getResults()[0]);
            assertTrue(findTableInSystemCatalogResults(resp.getResults()[0], "BLAH"));
            boolean threw = false;
            try {
                client.callProcedure("@AdHoc", "drop table BLAH;");
            }
            catch (ProcCallException pce) {
                // The error message is really confusing for this case, not sure
                // how to make it better though
                pce.printStackTrace();
                threw = true;
            }
            assertTrue("Shouldn't be able to drop a table used in a procedure", threw);
            resp = client.callProcedure("@SystemCatalog", "TABLES");
            System.out.println(resp.getResults()[0]);
            assertTrue(findTableInSystemCatalogResults(resp.getResults()[0], "BLAH"));
        }
        finally {
            if (client != null) client.close();
            client = null;

            if (localServer != null) {
                localServer.shutdown();
                localServer.join();
            }
            localServer = null;

            // no clue how helpful this is
            System.gc();
        }
    }

    public void testMultiLineCreateTable() throws Exception {

        String pathToCatalog = Configuration.getPathToCatalogForTest("adhocddl.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("adhocddl.xml");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(
            "create table BLAH (" +
            "ID int default 0 not null, " +
            "VAL varchar(32) default null," +
            "PRIMARY KEY(ID));\n" +
            "create table DROPME (" +
            "ID int default 0 not null, " +
            "VAL varchar(32) default null," +
            "PRIMARY KEY(ID))\n;" +
            "create table DROPME_R (" +
            "ID int default 0 not null, " +
            "VAL varchar(32) default null," +
            "PRIMARY KEY(ID));");
        builder.addPartitionInfo("BLAH", "ID");
        builder.addPartitionInfo("DROPME", "ID");
        boolean success = builder.compile(pathToCatalog, 2, 1, 0);
        assertTrue("Schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;
        ServerThread localServer = new ServerThread(config);

        Client client = null;

        try {
            localServer.start();
            localServer.waitForInitialization();

            client = ClientFactory.createClient();
            client.createConnection("localhost");

            // Check basic drop of partitioned table that should work.
            ClientResponse resp = client.callProcedure("@SystemCatalog", "TABLES");
            System.out.println(resp.getResults()[0]);
            try {
                client.callProcedure("@AdHoc",
                        "create table FOO (\n" +
                        "ID int default 0 not null,\n" +
                        "VAL varchar(32 bytes)\n" +
                        ");");
            }
            catch (ProcCallException pce) {
                fail("create table should have succeeded");
            }
            resp = client.callProcedure("@SystemCatalog", "TABLES");
            System.out.println(resp.getResults()[0]);
            assertTrue(findTableInSystemCatalogResults(resp.getResults()[0], "FOO"));
        }
        finally {
            if (client != null) client.close();
            client = null;

            if (localServer != null) {
                localServer.shutdown();
                localServer.join();
            }
            localServer = null;

            // no clue how helpful this is
            System.gc();
        }
    }

}
