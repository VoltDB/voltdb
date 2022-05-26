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

package org.voltdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.MiscUtils;

public class TestAdhocCreateDropView extends AdhocDDLTestBase {

    @Test
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

    @Test
    public void testMultiLineCreateView() throws Exception {

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

            // create tables
            try {
                m_client.callProcedure("@AdHoc",
                        "CREATE TABLE T1 (\n" +
                        "ID INTEGER DEFAULT 0,\n" +
                        "BIG BIGINT DEFAULT 0,\n" +
                        "VCHAR_INLINE VARCHAR(14) DEFAULT '0',\n" +
                        "VCHAR_OUTLINE_MIN VARCHAR(64 BYTES) DEFAULT '0' NOT NULL,\n" +
                        "VCHAR VARCHAR DEFAULT '0',\n" +
                        "VARBIN VARBINARY(100) DEFAULT x'00',\n" +
                        "POINT GEOGRAPHY_POINT,\n" +
                        "PRIMARY KEY (ID, VCHAR_OUTLINE_MIN)\n" +
                        ");");
                m_client.callProcedure("@AdHoc",
                        "CREATE TABLE T2 (\n" +
                        "ID INTEGER DEFAULT 0,\n" +
                        "BIG BIGINT DEFAULT 0,\n" +
                        "VCHAR_INLINE VARCHAR(14) DEFAULT '0',\n" +
                        "VCHAR_OUTLINE_MIN VARCHAR(64 BYTES) DEFAULT '0' NOT NULL,\n" +
                        "VCHAR VARCHAR DEFAULT '0',\n" +
                        "VARBIN VARBINARY(100) DEFAULT x'00',\n" +
                        "POINT GEOGRAPHY_POINT,\n" +
                        "PRIMARY KEY (ID, VCHAR_OUTLINE_MIN)\n" +
                        ");");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("create table should have succeeded");
            }

            // This create view test will throw exception because column VCHAR
            // is not a column in temp table
            ClientResponse resp = m_client.callProcedure("@SystemCatalog", "TABLES");
            assertTrue(findTableInSystemCatalogResults("T1"));
            assertTrue(findTableInSystemCatalogResults("T2"));
            System.out.println(resp.getResults()[0]);
            boolean threw = false;
            try {
                m_client.callProcedure("@AdHoc",
                        "CREATE VIEW DV1 (VCHAR_OUTLINE_MIN)\n"
                        + "AS SELECT MIN(VCHAR_OUTLINE_MIN)\n"
                        + "FROM T2\n"
                        + "WHERE NOT VCHAR_INLINE IN (\n"
                        + "    SELECT VCHAR FROM (\n"
                        + "        SELECT ALL T2.BIG CA3 FROM T1 AS T2\n"
                        + "    ) AS T2\n"
                        + ");");
            }
            catch (ProcCallException pce) {
                threw = true;
            }
            assertEquals(threw, true);
            resp = m_client.callProcedure("@SystemCatalog", "TABLES");
            System.out.println(resp.getResults()[0]);
            assertFalse(findTableInSystemCatalogResults("DV1"));
        }
        finally {
            teardownSystem();
        }
    }

    @Test
    public void testENG15870_count_distinct() throws Exception {

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
            try {
                m_client.callProcedure("@AdHoc",
                        "CREATE TABLE T1 (a int, b int);");
            } catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("create table should have succeeded");
            }

            // This create view test will throw exception because column VCHAR
            // is not a column in temp table
            ClientResponse resp = m_client.callProcedure("@SystemCatalog", "TABLES");
            assertTrue(findTableInSystemCatalogResults("T1"));
            System.out.println(resp.getResults()[0]);
            boolean threw = false;
            try {
                m_client.callProcedure("@AdHoc",
                        "CREATE VIEW DV1 AS SELECT a, COUNT(*), COUNT(DISTINCT b) FROM T1 GROUP BY A ORDER BY A;");
            } catch (ProcCallException pce) {
                threw = true;
            }
            assertEquals(threw, true);
            m_client.callProcedure("@SystemCatalog", "TABLES");
            assertFalse(findTableInSystemCatalogResults("DV1"));
        } finally {
            teardownSystem();
        }
    }
}
