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

public class TestAdhocDropTable extends AdhocDDLTestBase {

            //adHocQuery = "CREATE TABLE ICAST2 (C1 INT, C2 FLOAT);";
            //adHocQuery = "CREATE INDEX IDX_PROJ_PNAME ON PROJ(PNAME);";
            //adHocQuery = "DROP TABLE PROJ;";
            //adHocQuery = "PARTITION TABLE PROJ ON COLUMN PNUM;";
            //adHocQuery = "CREATE PROCEDURE AS SELECT 1 FROM PROJ;";
            //adHocQuery = "CREATE PROCEDURE FROM CLASS bar.Foo;";

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
            System.out.println(resp.getResults()[0]);
            assertTrue(findTableInSystemCatalogResults("DROPME"));
            try {
                m_client.callProcedure("@AdHoc", "drop table DROPME;");
            }
            catch (ProcCallException pce) {
                fail("drop table should have succeeded");
            }
            resp = m_client.callProcedure("@SystemCatalog", "TABLES");
            System.out.println(resp.getResults()[0]);
            assertFalse(findTableInSystemCatalogResults("DROPME"));
            // Check basic drop of replicated table that should work.
            resp = m_client.callProcedure("@SystemCatalog", "TABLES");
            System.out.println(resp.getResults()[0]);
            assertTrue(findTableInSystemCatalogResults("DROPME_R"));
            try {
                m_client.callProcedure("@AdHoc", "drop table DROPME_R;");
            }
            catch (ProcCallException pce) {
                fail("drop table should have succeeded");
            }
            resp = m_client.callProcedure("@SystemCatalog", "TABLES");
            System.out.println(resp.getResults()[0]);
            assertFalse(findTableInSystemCatalogResults("DROPME_R"));

            // Verify dropping a table that doesn't exist fails
            boolean threw = false;
            try {
                m_client.callProcedure("@AdHoc", "drop table DROPME;");
            }
            catch (ProcCallException pce) {
                assertTrue(pce.getMessage().contains("object not found: DROPME"));
                threw = true;
            }
            assertTrue("Dropping bad table should have failed", threw);

            // Verify dropping a table that doesn't exist is fine with IF EXISTS
            threw = false;
            try {
                m_client.callProcedure("@AdHoc", "drop table DROPME IF EXISTS;");
            }
            catch (ProcCallException pce) {
                threw = true;
            }
            assertFalse("Dropping bad table with IF EXISTS should not have failed", threw);
        }
        finally {
            teardownSystem();
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
            "create table VIEWBASE (" +
            "ID int default 0 not null, " +
            "VAL varchar(32) default null," +
            "PRIMARY KEY(ID));\n" +
            "create table DROPME (" +
            "ID int default 0 not null, " +
            "VAL varchar(32) default null," +
            "PRIMARY KEY(ID));\n" +
            "create assumeunique index pkey_idx on DROPME(VAL);\n" +
            "create view BLAT (VAL, TOTAL) as select VAL, COUNT(*) from VIEWBASE group by VAL;\n"
            );
        builder.addPartitionInfo("BLAH", "ID");
        builder.addPartitionInfo("DROPME", "ID");
        builder.addStmtProcedure("BLERG", "select * from BLAH where ID = ?");
        builder.setUseDDLSchema(true);
        boolean success = builder.compile(pathToCatalog, 2, 1, 0);
        assertTrue("Schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;

        try {
            startSystem(config);
            // Check basic drop of table with an index on it
            ClientResponse resp = m_client.callProcedure("@SystemCatalog", "TABLES");
            System.out.println(resp.getResults()[0]);
            assertTrue(findTableInSystemCatalogResults("DROPME"));
            resp = m_client.callProcedure("@SystemCatalog", "INDEXINFO");
            System.out.println(resp.getResults()[0]);
            assertTrue(findIndexInSystemCatalogResults("PKEY_IDX"));
            try {
                m_client.callProcedure("@AdHoc", "drop table DROPME;");
            }
            catch (ProcCallException pce) {
                fail("drop table should have succeeded");
            }
            resp = m_client.callProcedure("@SystemCatalog", "TABLES");
            System.out.println(resp.getResults()[0]);
            assertFalse(findTableInSystemCatalogResults("DROPME"));
            resp = m_client.callProcedure("@SystemCatalog", "INDEXINFO");
            System.out.println(resp.getResults()[0]);
            assertFalse(findIndexInSystemCatalogResults("PKEY_IDX"));

            // Verify that we can't drop a table that a procedure depends on
            resp = m_client.callProcedure("@SystemCatalog", "TABLES");
            System.out.println(resp.getResults()[0]);
            assertTrue(findTableInSystemCatalogResults("BLAH"));
            boolean threw = false;
            try {
                m_client.callProcedure("@AdHoc", "drop table BLAH;");
            }
            catch (ProcCallException pce) {
                // The error message is really confusing for this case, not sure
                // how to make it better though
                pce.printStackTrace();
                threw = true;
            }
            assertTrue("Shouldn't be able to drop a table used in a procedure", threw);
            resp = m_client.callProcedure("@SystemCatalog", "TABLES");
            System.out.println(resp.getResults()[0]);
            assertTrue(findTableInSystemCatalogResults("BLAH"));

            threw = false;
            try {
                m_client.callProcedure("@AdHoc", "drop table VIEWBASE;");
            }
            catch (ProcCallException pce) {
                threw = true;
            }
            assertTrue("Shouldn't be able to drop a table used in a view", threw);
            resp = m_client.callProcedure("@SystemCatalog", "TABLES");
            System.out.println(resp.getResults()[0]);
            assertTrue(findTableInSystemCatalogResults("VIEWBASE"));
            assertTrue(findTableInSystemCatalogResults("BLAT"));

            try {
                m_client.callProcedure("@AdHoc", "drop table VIEWBASE cascade;");
            }
            catch (ProcCallException pce) {
                fail("Should be able to drop table and view with cascade");
            }
            resp = m_client.callProcedure("@SystemCatalog", "TABLES");
            System.out.println(resp.getResults()[0]);
            assertFalse(findTableInSystemCatalogResults("VIEWBASE"));
            assertFalse(findTableInSystemCatalogResults("BLAT"));
        }
        finally {
            teardownSystem();
        }
    }
}
