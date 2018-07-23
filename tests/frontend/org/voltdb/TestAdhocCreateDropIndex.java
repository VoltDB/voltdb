/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.MiscUtils;

public class TestAdhocCreateDropIndex extends AdhocDDLTestBase {

    // Add a test for partitioning a table

    @Test
    public void testBasicCreateIndex() throws Exception
    {
        VoltDB.Configuration config = new VoltDB.Configuration();
        String ddl = "create table FOO (" +
                     "ID integer not null," +
                     "VAL bigint, " +
                     "constraint PK_TREE primary key (ID)" +
                     ");\n" +
                     "create table FOO_R (" +
                     "ID integer not null," +
                     "VAL bigint, " +
                     "constraint PK_TREE_R primary key (ID)" +
                     ");\n" +
                     "Partition table FOO on column ID;\n";
        createSchema(config, ddl, 2, 1, 0);

        try {
            startSystem(config);

            // Create index on tables
            assertFalse(findIndexInSystemCatalogResults("FOODEX"));
            try {
                m_client.callProcedure("@AdHoc",
                        "create index FOODEX on FOO (VAL);");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to create an index on a partitioned table");
            }
            assertTrue(findIndexInSystemCatalogResults("FOODEX"));
            // Create index on replicated tables
            assertFalse(findIndexInSystemCatalogResults("FOODEX_R"));
            try {
                m_client.callProcedure("@AdHoc",
                        "create index FOODEX_R on FOO_R (VAL);");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to create an index on a replicated table");
            }
            assertTrue(findIndexInSystemCatalogResults("FOODEX_R"));

            // Create unique index on partitioned tables
            assertFalse(findIndexInSystemCatalogResults("UNIQFOODEX"));
            boolean threw = false;
            try {
                m_client.callProcedure("@AdHoc",
                        "create assumeunique index UNIQFOODEX on FOO (VAL);");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to create a unique index on a partitioned table");
            }
            assertTrue(findIndexInSystemCatalogResults("UNIQFOODEX"));
            // Can create redundant unique index on a table
            try {
                m_client.callProcedure("@AdHoc",
                        "create unique index UNIQFOODEX2 on FOO (ID);");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to create redundant unique index");
            }
            // It's going to get dropped because it's redundant, so don't expect to see it here
            assertFalse(findIndexInSystemCatalogResults("UNIQFOODEX2"));

            // drop an index we added
            try {
                m_client.callProcedure("@AdHoc",
                        "drop index FOODEX;");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to drop an index");
            }
            assertFalse(findIndexInSystemCatalogResults("FOODEX"));
            // can't drop it twice
            threw = false;
            try {
                m_client.callProcedure("@AdHoc",
                        "drop index FOODEX;");
            }
            catch (ProcCallException pce) {
                threw = true;
            }
            assertTrue("Shouldn't be able to drop bad index without if exists", threw);
            assertFalse(findIndexInSystemCatalogResults("FOODEX"));
            // unless we use if exists
            try {
                m_client.callProcedure("@AdHoc",
                        "drop index FOODEX if exists;");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to drop a bad index with if exists");
            }
            assertFalse(findIndexInSystemCatalogResults("FOODEX"));
        }
        finally {
            teardownSystem();
        }
    }

    @Test
    public void testCreatePartialIndex() throws Exception
    {
        VoltDB.Configuration config = new VoltDB.Configuration();
        String ddl = "create table FOO (" +
                     "ID integer not null," +
                     "TS timestamp, " +
                     "constraint PK_TREE primary key (ID)" +
                     ");\n" +
                     "partition table FOO on column ID;\n" +
                     "create table FOO_R (" +
                     "ID integer not null," +
                     "TS timestamp, " +
                     "constraint PK_TREE_R primary key (ID)" +
                     ");\n" +
                     "";
        createSchema(config, ddl, 2, 1, 0);

        try {
            startSystem(config);

            // Create a partial index on the partitioned table
            assertFalse(findIndexInSystemCatalogResults("partial_FOO_ts"));
            try {
                // Use a timestamp constant to validate ENG-9283
                m_client.callProcedure("@AdHoc",
                        "create index partial_FOO_ts on FOO (TS) where TS > '2000-01-01';");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to create a partial index on a partitioned table");
            }
            assertTrue(findIndexInSystemCatalogResults("partial_FOO_ts"));

            // Create a partial index on the replicated table.
            // It is unlikely that switching to use a replicated table will
            // uncover a failure when the partitioned table test apparently
            // succeeded, UNLESS that partitioned table schema change
            // succeeded BUT left the schema in a compromised state that is
            // operational but no longer mutable.
            // This has happened in the past because of issues with
            // regenerating the SQL DDL syntax that effectively recreates the
            // pre-existing schema. This kind of error will only be discovered
            // by a subsequent attempt to alter the schema.
            // Uncovering that failure mode may be the most useful role
            // of this additional test step.
            assertFalse(findIndexInSystemCatalogResults("partial_FOO_R_ts"));
            try {
                m_client.callProcedure("@AdHoc",
                        "create index partial_FOO_R_ts on FOO_R (TS) where TS > '2000-01-01';");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to create a partial index on a replicated table" +
                     " after apparently succeeding with a partitioned table.");
            }
            assertTrue(findIndexInSystemCatalogResults("partial_FOO_R_ts"));

        }
        finally {
            teardownSystem();
        }
    }

    @Test
    public void testCreateDropIndexonView() throws Exception
    {
        VoltDB.Configuration config = new VoltDB.Configuration();
        String ddl = "create table FOO (" +
                     "ID integer not null," +
                     "VAL bigint, " +
                     "VAL1 float," +
                     "constraint PK_TREE primary key (ID)" +
                     ");\n" +
                     "Partition table FOO on column ID;\n";
        createSchema(config, ddl, 2, 1, 0);

        try {
            startSystem(config);

            // create a basic view
            assertFalse(findTableInSystemCatalogResults("FOOVIEW"));
            try {
                m_client.callProcedure("@AdHoc",
                    "create view FOOVIEW (VAL, VAL1, TOTAL) as " +
                    "select VAL, VAL1, COUNT(*) from FOO group by VAL, VAL1;");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to create a view");
            }
            assertTrue(findTableInSystemCatalogResults("FOOVIEW"));

            // Create index on view
            assertFalse(findIndexInSystemCatalogResults("VALDEX"));
            try {
                m_client.callProcedure("@AdHoc",
                        "create index SimpleIndex on FOOVIEW (VAL);");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to create an index on a view");
            }
            assertTrue(findIndexInSystemCatalogResults("SimpleIndex"));

            // drop index
            try {
                m_client.callProcedure("@AdHoc",
                        "drop index SimpleIndex;");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to drop an index on a view");
            }
            assertFalse(findIndexInSystemCatalogResults("SimpleIndex"));

            // can't drop index twice
            boolean threw = false;
            try {
                m_client.callProcedure("@AdHoc",
                        "drop index SimpleIndex;");
            }
            catch (ProcCallException pce) {
                threw = true;
            }
            assertTrue("Shouldn't be able to drop bad index without if exists", threw);
            assertFalse(findIndexInSystemCatalogResults("SimpleIndex"));

            // should be able to execute drop index on non-existing index
            // with "if exists" clause
            try {
                m_client.callProcedure("@AdHoc",
                        "drop index SimpleIndex if exists;");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to drop a bad index with if exists");
            }
            assertFalse(findIndexInSystemCatalogResults("SimpleIndex"));

            // recreate index
            try {
                m_client.callProcedure("@AdHoc",
                        "create index ComplexIndex on FOOVIEW (VAL, TOTAL);");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to recreate an index on a view");
            }
            assertTrue(findIndexInSystemCatalogResults("ComplexIndex"));

            // drop index
            try {
                m_client.callProcedure("@AdHoc",
                        "drop index ComplexIndex if exists;");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to drop an index on a view");
            }
            assertFalse(findIndexInSystemCatalogResults("ComplexIndex"));
        }
        finally {
            teardownSystem();
        }
    }

    public static String unsafeIndexExprErrorMsg = "The index definition uses operations that cannot be applied";

    public void ENG12024TestHelper(String ddlTemplate, String testExpression, boolean isSafeForDDL) throws Exception {
        String ddl = String.format(ddlTemplate, testExpression);

        // Create index on empty table first.
        assertFalse(findIndexInSystemCatalogResults("IDX_ENG_12024"));
        try {
            m_client.callProcedure("@AdHoc", ddl);
        }
        catch (ProcCallException pce) {
            pce.printStackTrace();
            fail("Should be able to create an index on a empty table:\n" + ddl);
        }
        assertTrue(findIndexInSystemCatalogResults("IDX_ENG_12024"));
        try {
            m_client.callProcedure("@AdHoc", "DROP INDEX IDX_ENG_12024;");
        }
        catch (ProcCallException pce) {
            pce.printStackTrace();
            fail("Should be able to drop index IDX_ENG_12024.");
        }
        assertFalse(findIndexInSystemCatalogResults("IDX_ENG_12024"));

        // Populate the table with some data and try again.
        try {
            m_client.callProcedure("@AdHoc", "INSERT INTO T_ENG_12024 VALUES (1, 2, 'ABC');");
        }
        catch (ProcCallException pce) {
            pce.printStackTrace();
            fail("Should be able to insert data into T_ENG_12024.");
        }
        try {
            m_client.callProcedure("@AdHoc", ddl);
            if (! isSafeForDDL) {
                fail("Create index DDL on non-empty table with unsafe operators did not fail as expected:\n" + ddl);
            }
        }
        catch (ProcCallException pce) {
            if (isSafeForDDL || ! pce.getMessage().contains(unsafeIndexExprErrorMsg)) {
                fail("Unexpected create index DDL failure (isSafeForDDL = " +
                        (isSafeForDDL ? "true" : "false") + "):\n" + ddl);
            }
        }
        m_client.callProcedure("@AdHoc", "DROP INDEX IDX_ENG_12024 IF EXISTS;");
        m_client.callProcedure("@AdHoc", "DELETE FROM T_ENG_12024;");
    }

    @Test
    public void testCreateUnsafeIndexOnNonemptyTable() throws Exception
    {
        VoltDB.Configuration config = new VoltDB.Configuration();
        String ddl = "CREATE TABLE T_ENG_12024 (a INT, b INT, c VARCHAR(10));";
        createSchema(config, ddl, 2, 1, 0);

        try {
            startSystem(config);

            String ddlTemplateForColumnExpression = "CREATE INDEX IDX_ENG_12024 ON T_ENG_12024(%s);";
            String ddlTemplateForBooleanExpression = "CREATE INDEX IDX_ENG_12024 ON T_ENG_12024(a) WHERE %s;";

            ENG12024TestHelper(ddlTemplateForColumnExpression, "a", true);
            ENG12024TestHelper(ddlTemplateForColumnExpression, "a + b", false);
            ENG12024TestHelper(ddlTemplateForColumnExpression, "a - b", false);
            ENG12024TestHelper(ddlTemplateForColumnExpression, "a * b", false);
            ENG12024TestHelper(ddlTemplateForColumnExpression, "a / b", false);
            ENG12024TestHelper(ddlTemplateForColumnExpression, "c || c", false);
            ENG12024TestHelper(ddlTemplateForColumnExpression, "repeat(c, 100)", false);

            ENG12024TestHelper(ddlTemplateForBooleanExpression, "NOT a > b", true);
            ENG12024TestHelper(ddlTemplateForBooleanExpression, "a IS NULL", true);
            ENG12024TestHelper(ddlTemplateForBooleanExpression, "a = b", true);
            ENG12024TestHelper(ddlTemplateForBooleanExpression, "a <> b", true);
            ENG12024TestHelper(ddlTemplateForBooleanExpression, "a < b", true);
            ENG12024TestHelper(ddlTemplateForBooleanExpression, "a > b", true);
            ENG12024TestHelper(ddlTemplateForBooleanExpression, "a <= b", true);
            ENG12024TestHelper(ddlTemplateForBooleanExpression, "a >= b", true);
            ENG12024TestHelper(ddlTemplateForBooleanExpression, "c LIKE 'abc%'", true);
            ENG12024TestHelper(ddlTemplateForBooleanExpression, "c STARTS WITH 'abc'", true);
            ENG12024TestHelper(ddlTemplateForBooleanExpression, "a IS NOT DISTINCT FROM b", true);
            ENG12024TestHelper(ddlTemplateForBooleanExpression, "a > b AND b > 0", true);
            ENG12024TestHelper(ddlTemplateForBooleanExpression, "a > b OR b > 0", true);
        }
        finally {
            teardownSystem();
        }
    }

    private void createSchema(VoltDB.Configuration config,
                              String ddl,
                              final int sitesPerHost,
                              final int hostCount,
                              final int replication) throws Exception
    {
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(ddl);
        builder.setUseDDLSchema(true);
        config.m_pathToCatalog = Configuration.getPathToCatalogForTest("adhocddl.jar");
        boolean success = builder.compile(config.m_pathToCatalog, sitesPerHost, hostCount, replication);
        assertTrue("Schema compilation failed", success);
        config.m_pathToDeployment = Configuration.getPathToCatalogForTest("adhocddl.xml");
        MiscUtils.copyFile(builder.getPathToDeployment(), config.m_pathToDeployment);
    }
}
