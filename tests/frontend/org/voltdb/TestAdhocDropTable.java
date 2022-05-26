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

import org.junit.After;
import org.junit.Test;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.MiscUtils;

public class TestAdhocDropTable extends AdhocDDLTestBase {

    @After
    public void tearItDown() throws Exception {
        teardownSystem();
    }

    @Test
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

        startSystem(config);
        // Check basic drop of partitioned table that should work.
        ClientResponse resp = m_client.callProcedure("@SystemCatalog", "TABLES");
        System.out.println(resp.getResults()[0]);
        assertTrue(findTableInSystemCatalogResults("DROPME"));

        // eng7297, start with 6 rows in @Statistics table (one per table per site)
        VoltTable stats = getStatWaitOnRowCount("TABLE", 6);
        assertEquals(6, stats.getRowCount());
        stats = getStatWaitOnRowCount("INDEX", 6);
        assertEquals(6, stats.getRowCount());

        try {
            m_client.callProcedure("@AdHoc", "drop table DROPME;");
        } catch (ProcCallException pce) {
            fail("drop table should have succeeded");
        }
        resp = m_client.callProcedure("@SystemCatalog", "TABLES");
        System.out.println(resp.getResults()[0]);
        assertFalse(findTableInSystemCatalogResults("DROPME"));
        // eng7297, now only 4 rows in @Statistics table (one per table per site)
        stats = getStatWaitOnRowCount("TABLE", 4);
        assertEquals(4, stats.getRowCount());
        stats = getStatWaitOnRowCount("INDEX", 4);
        assertEquals(4, stats.getRowCount());

        // Check basic drop of replicated table that should work.
        resp = m_client.callProcedure("@SystemCatalog", "TABLES");
        System.out.println(resp.getResults()[0]);
        assertTrue(findTableInSystemCatalogResults("DROPME_R"));
        try {
            m_client.callProcedure("@AdHoc", "drop table DROPME_R;");
        } catch (ProcCallException pce) {
            fail("drop table should have succeeded");
        }
        resp = m_client.callProcedure("@SystemCatalog", "TABLES");
        System.out.println(resp.getResults()[0]);
        assertFalse(findTableInSystemCatalogResults("DROPME_R"));
        // eng7297, now only 2 rows in @Statistics table (one per table per site)
        stats = getStatWaitOnRowCount("TABLE", 2);
        assertEquals(2, stats.getRowCount());
        stats = getStatWaitOnRowCount("INDEX", 2);
        assertEquals(2, stats.getRowCount());

        // Verify dropping a table that doesn't exist fails
        boolean threw = false;
        try {
            m_client.callProcedure("@AdHoc", "drop table DROPME;");
        } catch (ProcCallException pce) {
            final String msg = pce.getMessage();
            final String pat = "object not found: DROPME in statement [drop table DROPME]";
            assertTrue(String.format("Message \"%s\" does not include \"%s: DROPME\"", msg, pat), msg.contains(pat));
            threw = true;
        }
        assertTrue("Dropping bad table should have failed", threw);

        // Verify dropping a table that doesn't exist is fine with IF EXISTS
        threw = false;
        try {
            m_client.callProcedure("@AdHoc", "drop table DROPME IF EXISTS;");
        } catch (ProcCallException pce) {
            threw = true;
        }
        assertFalse("Dropping bad table with IF EXISTS should not have failed", threw);

        // ENG-7297, Drop the last table and make sure that the statistics are correct
        try {
            m_client.callProcedure("@AdHoc", "drop table BLAH;");
        } catch (ProcCallException pce) {
            fail("drop table should have succeeded");
        }
        resp = m_client.callProcedure("@SystemCatalog", "TABLES");
        System.out.println(resp.getResults()[0]);
        assertFalse(findTableInSystemCatalogResults("BLAH"));
        // eng7297, now should be zero rows in the stats
        stats = getStatWaitOnRowCount("TABLE", 0);
        assertEquals(0, stats.getRowCount());
        stats = getStatWaitOnRowCount("INDEX", 0);
        assertEquals(0, stats.getRowCount());
    }

    @Test
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
        } catch (ProcCallException pce) {
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
        } catch (ProcCallException pce) {
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
        } catch (ProcCallException pce) {
            threw = true;
        }
        assertTrue("Shouldn't be able to drop a table used in a view", threw);
        resp = m_client.callProcedure("@SystemCatalog", "TABLES");
        assertTrue(findTableInSystemCatalogResults("VIEWBASE"));
        assertTrue(findTableInSystemCatalogResults("BLAT"));

        try {
            m_client.callProcedure("@AdHoc", "drop table VIEWBASE cascade;");
        } catch (ProcCallException pce) {
            fail("Should be able to drop table and view with cascade");
        }
        resp = m_client.callProcedure("@SystemCatalog", "TABLES");
        System.out.println(resp.getResults()[0]);
        assertFalse(findTableInSystemCatalogResults("VIEWBASE"));
        assertFalse(findTableInSystemCatalogResults("BLAT"));
    }

    @Test
    public void testEng12816() throws Exception {

        String pathToCatalog = Configuration.getPathToCatalogForTest("adhocddl.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("adhocddl.xml");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.setUseDDLSchema(true);
        boolean success = builder.compile(pathToCatalog, 2, 1, 0);
        assertTrue("Schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;

        startSystem(config);

        ClientResponse cr;
        cr = m_client.callProcedure("@AdHoc",
                "create table t_eng12816 ( "
                        + "i integer not null primary key, "
                        + "vc varchar(32) "
                        + "); ");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = m_client.callProcedure("@AdHoc", "create procedure proc1 as select count(*) from t_eng12816;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        try {
            m_client.callProcedure("@AdHoc", "drop table t_eng12816;");
            fail("expected an exception!");
        } catch (ProcCallException pce) {
            final String msg = pce.getMessage();
            final String pat = "object not found: T_ENG12816";
            assertTrue(String.format("Message \"%s\" does not contain \"%s\"", msg, pat), msg.contains(pat));
        }

        cr = m_client.callProcedure("@SystemCatalog", "TABLES");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        assertTrue(findTableInSystemCatalogResults("T_ENG12816"));
    }
}
