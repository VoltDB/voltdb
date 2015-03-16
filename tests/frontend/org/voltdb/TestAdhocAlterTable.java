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

public class TestAdhocAlterTable extends AdhocDDLTestBase {

    public void testAlterAddColumn() throws Exception
    {
        String pathToCatalog = Configuration.getPathToCatalogForTest("adhocddl.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("adhocddl.xml");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(
                "create table FOO (" +
                "ID integer not null," +
                "VAL bigint, " +
                "constraint pk_tree primary key (ID)" +
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

            try {
                m_client.callProcedure("@AdHoc",
                        "alter table FOO add column NEWCOL varchar(50);");
            }
            catch (ProcCallException pce) {
                fail("Alter table to add column should have succeeded");
            }
            assertTrue(verifyTableColumnType("FOO", "NEWCOL", "VARCHAR"));
            assertTrue(verifyTableColumnSize("FOO", "NEWCOL", 50));
            assertTrue(isColumnNullable("FOO", "NEWCOL"));

            // second time should fail
            boolean threw = false;
            try {
                m_client.callProcedure("@AdHoc",
                        "alter table FOO add column NEWCOL varchar(50);");
            }
            catch (ProcCallException pce) {
                threw = true;
            }
            assertTrue("Adding the same column twice should fail", threw);

            // can't add another primary key
            threw = false;
            try {
                m_client.callProcedure("@AdHoc",
                        "alter table FOO add column BADPK integer primary key;");
            }
            catch (ProcCallException pce) {
                threw = true;
            }
            assertTrue("Shouldn't be able to add a second primary key", threw);

            // Can't add a not-null column with no default
            // NOTE: this could/should work with an empty table, but HSQL apparently
            // doesn't let it get that far.
            threw = false;
            try {
                m_client.callProcedure("@AdHoc",
                        "alter table FOO add column BADNOTNULL integer not null;");
            }
            catch (ProcCallException pce) {
                threw = true;
            }
            assertFalse("Should be able to add a not null column to an empty table without default", threw);

            // but we're good with a default
            try {
                m_client.callProcedure("@AdHoc",
                        "alter table FOO add column GOODNOTNULL integer default 0 not null;");
            }
            catch (ProcCallException pce) {
                fail("Should be able to add a column with not null and a default.");
            }
            assertTrue(verifyTableColumnType("FOO", "GOODNOTNULL", "INTEGER"));
            assertFalse(isColumnNullable("FOO", "GOODNOTNULL"));

            // Can't add a column with a bad definition
            threw = false;
            try {
                m_client.callProcedure("@AdHoc",
                        "alter table FOO add column BADVARCHAR varchar(0) not null;");
            }
            catch (ProcCallException pce) {
                threw = true;
            }
            assertTrue("Shouldn't be able to add a column with a bad definition", threw);
            assertFalse(doesColumnExist("FOO", "BADVARCHAR"));
        }
        finally {
            teardownSystem();
        }
    }

    public void testAlterDropColumn() throws Exception
    {
        String pathToCatalog = Configuration.getPathToCatalogForTest("adhocddl.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("adhocddl.xml");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(
                "create table FOO (" +
                "PKCOL integer not null," +
                "DROPME bigint, " +
                "PROCCOL bigint, " +
                "VIEWCOL bigint, " +
                "INDEXCOL bigint, " +
                "INDEX1COL bigint, " +
                "INDEX2COL bigint, " +
                "constraint pk_tree primary key (PKCOL)" +
                ");\n" +
                "create procedure BAR as select PROCCOL from FOO;\n" +
                "create view FOOVIEW (VIEWCOL, TOTAL) as select VIEWCOL, COUNT(*) from FOO group by VIEWCOL;\n" +
                "create index FOODEX on FOO(INDEXCOL);\n" +
                "create index FOO2DEX on FOO(INDEX1COL, INDEX2COL);\n" +
                "create table ONECOL (" +
                "SOLOCOL integer, " +
                ");\n" +
                "create table BAZ (" +
                "PKCOL1 integer not null, " +
                "PKCOL2 integer not null, " +
                "constraint pk_tree2 primary key (PKCOL1, PKCOL2)" +
                ");\n"
                );
        builder.setUseDDLSchema(true);
        boolean success = builder.compile(pathToCatalog, 2, 1, 0);
        assertTrue("Schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;

        try {
            startSystem(config);

            // Basic alter drop, should work
            assertTrue(doesColumnExist("FOO", "DROPME"));
            try {
                m_client.callProcedure("@AdHoc",
                        "alter table FOO drop column DROPME;");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to drop a bare column.");
            }
            assertFalse(doesColumnExist("FOO", "DROPME"));

            // but not twice
            boolean threw = false;
            try {
                m_client.callProcedure("@AdHoc",
                        "alter table FOO drop column DROPME; --commentfortesting");
            }
            catch (ProcCallException pce) {
                threw = true;
            }
            assertTrue("Shouldn't be able to drop a column that doesn't exist", threw);
            assertFalse(doesColumnExist("FOO", "DROPME"));

            // Can't drop column used by procedure
            assertTrue(doesColumnExist("FOO", "PROCCOL"));
            threw = false;
            try {
                m_client.callProcedure("@AdHoc",
                        "alter table FOO drop column PROCCOL;");
            }
            catch (ProcCallException pce) {
                threw = true;
            }
            assertTrue("Shouldn't be able to drop a column used by a procedure", threw);
            assertTrue(doesColumnExist("FOO", "PROCCOL"));
            try {
                m_client.callProcedure("BAR");
            }
            catch (ProcCallException pce) {
                fail("Procedure should still exist.");
            }

            // Can't drop a column used by a view
            assertTrue(doesColumnExist("FOO", "VIEWCOL"));
            threw = false;
            try {
                m_client.callProcedure("@AdHoc",
                        "alter table FOO drop column VIEWCOL;");
            }
            catch (ProcCallException pce) {
                threw = true;
            }
            assertTrue("Shouldn't be able to drop a column used by a view", threw);
            assertTrue(doesColumnExist("FOO", "VIEWCOL"));
            assertTrue(findTableInSystemCatalogResults("FOOVIEW"));

            // unless you use cascade
            assertTrue(doesColumnExist("FOO", "VIEWCOL"));
            assertTrue(findTableInSystemCatalogResults("FOOVIEW"));
            try {
                m_client.callProcedure("@AdHoc",
                        "alter table FOO drop column VIEWCOL cascade; --comment messes up cascade?");
            }
            catch (ProcCallException pce) {
                fail("Dropping a column should drop a view with cascade");
            }
            assertFalse(doesColumnExist("FOO", "VIEWCOL"));
            assertFalse(findTableInSystemCatalogResults("FOOVIEW"));

            // single-column indexes get cascaded automagically
            assertTrue(doesColumnExist("FOO", "INDEXCOL"));
            assertTrue(findIndexInSystemCatalogResults("FOODEX"));
            try {
                m_client.callProcedure("@AdHoc",
                        "alter table FOO drop column INDEXCOL;");
            }
            catch (ProcCallException pce) {
                fail("Should be able to drop a single column backing a single column index.");
            }
            assertFalse(doesColumnExist("FOO", "INDEXCOL"));
            assertFalse(findIndexInSystemCatalogResults("FOODEX"));

            // single-column primary keys get cascaded automagically
            assertTrue(doesColumnExist("FOO", "PKCOL"));
            assertTrue(findIndexInSystemCatalogResults("VOLTDB_AUTOGEN_CONSTRAINT_IDX_PK_TREE"));
            try {
                m_client.callProcedure("@AdHoc",
                        "alter table FOO drop column PKCOL;");
            }
            catch (ProcCallException pce) {
                fail("Should be able to drop a single column backing a single column primary key.");
            }
            assertFalse(doesColumnExist("FOO", "PKCOL"));
            assertFalse(findIndexInSystemCatalogResults("VOLTDB_AUTOGEN_CONSTRAINT_IDX_PK_TREE"));

            // WEIRD: this seems like weird behavior to me still --izzy
            // Dropping a column used by a multi-column index drops the index
            assertTrue(doesColumnExist("FOO", "INDEX1COL"));
            assertTrue(findIndexInSystemCatalogResults("FOO2DEX"));
            threw = false;
            try {
                m_client.callProcedure("@AdHoc",
                        "alter table FOO drop column INDEX1COL;");
            }
            catch (ProcCallException pce) {
                fail("Dropping a column used by an index should kill the index");
            }
            assertFalse(doesColumnExist("FOO", "INDEX1COL"));
            assertFalse(findIndexInSystemCatalogResults("FOO2DEX"));

            // Can't drop a column used by a multi-column primary key
            assertTrue(doesColumnExist("BAZ", "PKCOL1"));
            assertTrue(findIndexInSystemCatalogResults("VOLTDB_AUTOGEN_CONSTRAINT_IDX_PK_TREE2"));
            threw = false;
            try {
                m_client.callProcedure("@AdHoc",
                        "alter table BAZ drop column PKCOL1;");
            }
            catch (ProcCallException pce) {
                threw = true;
            }
            System.out.println("COLUMNS: " + m_client.callProcedure("@SystemCatalog", "COLUMNS").getResults()[0]);
            System.out.println("INDEXES: " + m_client.callProcedure("@SystemCatalog", "INDEXINFO").getResults()[0]);
            assertTrue("Shouldn't be able to drop a column used by a multi-column primary key", threw);
            assertTrue(doesColumnExist("BAZ", "PKCOL1"));
            assertTrue(findIndexInSystemCatalogResults("VOLTDB_AUTOGEN_CONSTRAINT_IDX_PK_TREE2"));

            // Can't drop the last column in a table
            assertTrue(doesColumnExist("ONECOL", "SOLOCOL"));
            threw = false;
            try {
                m_client.callProcedure("@AdHoc",
                        "alter table BAZ drop column PKCOL1;");
            }
            catch (ProcCallException pce) {
                threw = true;
            }
            assertTrue("Shouldn't be able to drop the last column in a table", threw);
            assertTrue(doesColumnExist("ONECOL", "SOLOCOL"));

        }
        finally {
            teardownSystem();
        }
    }

    public void testAlterColumnOther() throws Exception
    {
        System.out.println("----------------\n\n TestAlterColumnOther \n\n--------------");
        String pathToCatalog = Configuration.getPathToCatalogForTest("adhocddl.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("adhocddl.xml");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(
                "create table FOO (" +
                "ID integer not null," +
                "VAL varchar(50), " +
                "constraint PK_TREE primary key (ID)" +
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

            // Setting to not null should work if the table is empty.
            try {
                m_client.callProcedure("@AdHoc",
                        "alter table FOO alter VAL set not null;");
            }
            catch (ProcCallException pce) {
                fail(String.format(
                        "Should be able to declare not null on existing column of an empty table. "
                        + "Exception: %s", pce.getLocalizedMessage()));
            }
            assertFalse(isColumnNullable("FOO", "VAL"));

            // Make nullable again.
            try {
                m_client.callProcedure("@AdHoc",
                        "alter table FOO alter VAL set null;");
            }
            catch (ProcCallException pce) {
                fail(String.format(
                        "Should be able to make an existing column nullable. "
                        + "Exception: %s", pce.getLocalizedMessage()));
            }
            assertTrue(isColumnNullable("FOO", "VAL"));

            // Specify a default.
            assertTrue(verifyTableColumnDefault("FOO", "VAL", null));
            try {
                m_client.callProcedure("@AdHoc",
                        "alter table FOO alter VAL set default 'goats';");
            }
            catch (ProcCallException pce) {
                fail(String.format("Shouldn't fail. Exception: %s", pce.getLocalizedMessage()));
            }
            assertTrue(verifyTableColumnDefault("FOO", "VAL", "'goats'"));
            assertTrue(isColumnNullable("FOO", "VAL"));

            // Change the default back to null
            try {
                m_client.callProcedure("@AdHoc",
                        "alter table FOO alter VAL set default NULL;");
            }
            catch (ProcCallException pce) {
                fail("Shouldn't fail");
            }
            assertTrue(verifyTableColumnDefault("FOO", "VAL", null));
            assertTrue(isColumnNullable("FOO", "VAL"));

            // Setting to not null should fail if the table is not empty.
            m_client.callProcedure("FOO.insert", 0, "whatever");
            boolean threw = false;
            try {
                m_client.callProcedure("@AdHoc",
                        "alter table FOO alter VAL set not null;");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                threw = true;
            }
            assertTrue("Shouldn't be able to declare not null on existing column of a non-empty table", threw);
            assertTrue(isColumnNullable("FOO", "VAL"));

            // Clear the table and reset VAL default (by setting to NULL)
            try {
                m_client.callProcedure("@AdHoc",
                        "delete from FOO;");
            }
            catch (ProcCallException pce) {
                fail("Shouldn't fail");
            }
            try {
                m_client.callProcedure("@AdHoc",
                        "alter table FOO alter VAL set default NULL;");
            }
            catch (ProcCallException pce) {
                fail("Shouldn't fail");
            }
            assertTrue(verifyTableColumnDefault("FOO", "VAL", null));
            assertTrue(isColumnNullable("FOO", "VAL"));

            // Can't make primary key nullable
            assertFalse(isColumnNullable("FOO", "ID"));
            threw = false;
            try {
                m_client.callProcedure("@AdHoc",
                        "alter table FOO alter ID set null;");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                threw = true;
            }
            assertTrue("Shouldn't be able to make the primary key nullable", threw);
            assertFalse(isColumnNullable("FOO", "ID"));

            // magic name for PK index
            assertTrue(findIndexInSystemCatalogResults("VOLTDB_AUTOGEN_CONSTRAINT_IDX_PK_TREE"));
            assertTrue(verifyIndexUniqueness("VOLTDB_AUTOGEN_CONSTRAINT_IDX_PK_TREE", true));
            try {
                m_client.callProcedure("@AdHoc",
                        "alter table FOO add unique (ID);");
            }
            catch (ProcCallException pce) {
                fail("Shouldn't fail to add unique constraint to column with unique constraint");
            }
            // Unique constraint we added is redundant with existing constraint
            assertTrue(findIndexInSystemCatalogResults("VOLTDB_AUTOGEN_CONSTRAINT_IDX_PK_TREE"));
            assertTrue(verifyIndexUniqueness("VOLTDB_AUTOGEN_CONSTRAINT_IDX_PK_TREE", true));

            // Now, drop the PK constraint
            try {
                m_client.callProcedure("@AdHoc",
                        "alter table FOO drop constraint PK_TREE;");
            }
            catch (ProcCallException pce) {
                fail("Shouldn't fail to drop primary key constraint");
            }
            // Now we create a new named index for the unique constraint.  C'est la vie.
            assertTrue(findIndexInSystemCatalogResults("VOLTDB_AUTOGEN_IDX_CT_FOO_ID"));
            assertTrue(verifyIndexUniqueness("VOLTDB_AUTOGEN_IDX_CT_FOO_ID", true));

            // Can't add a PK constraint on the other column
            threw = false;
            try {
                m_client.callProcedure("@AdHoc",
                        "alter table FOO add constraint PK_TREE primary key (VAL);");
            }
            catch (ProcCallException pce) {
                threw = true;
            }
            assertTrue("Shouldn't be able to add a primary key on nullable column", threw);

//            // But we can add it back on the original column
//            try {
//                m_client.callProcedure("@AdHoc",
//                        "alter table FOO add constraint PK_TREE primary key (ID);");
//            }
//            catch (ProcCallException pce) {
//                pce.printStackTrace();
//                fail("Shouldn't fail to add primary key constraint");
//            }
//            System.out.println("INDEXES: " + m_client.callProcedure("@SystemCatalog", "INDEXINFO").getResults()[0]);
//            // Of course we rename this yet again, because, why not?
//            assertTrue(findIndexInSystemCatalogResults("VOLTDB_AUTOGEN_IDX_FOO_ID"));
//            assertTrue(verifyIndexUniqueness("VOLTDB_AUTOGEN_IDX_FOO_ID", true));
        }
        finally {
            teardownSystem();
        }
    }

    public void testAlterRename() throws Exception
    {
        String pathToCatalog = Configuration.getPathToCatalogForTest("adhocddl.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("adhocddl.xml");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(
                "create table FOO (" +
                "ID integer not null," +
                "VAL varchar(50), " +
                "constraint PK_TREE primary key (ID)" +
                ");\n" +
                "create table EMPTYFOO (" +
                "ID integer not null," +
                "VAL varchar(50), " +
                "constraint PK_TREE2 primary key (ID)" +
                ");\n"
                );
        builder.addPartitionInfo("FOO", "ID");
        builder.addPartitionInfo("EMPTYFOO", "ID");
        builder.setUseDDLSchema(true);
        boolean success = builder.compile(pathToCatalog, 2, 1, 0);
        assertTrue("Schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;

        try {
            startSystem(config);
            // write a couple rows to FOO so it's not empty
            m_client.callProcedure("FOO.insert", 0, "ryanloves");
            m_client.callProcedure("FOO.insert", 1, "theyankees");

            // check rename table fails
            assertEquals(2, m_client.callProcedure("@AdHoc", "select count(*) from foo;").getResults()[0].asScalarLong());
            boolean threw = false;
            try {
                m_client.callProcedure("@AdHoc",
                        "alter table FOO rename to BAR;");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                threw = true;
            }
            assertTrue("Shouldn't be able to rename a non-empty table", threw);
            assertEquals(2, m_client.callProcedure("@AdHoc", "select count(*) from foo;").getResults()[0].asScalarLong());

            // check rename column on a table fails
            VoltTable results = m_client.callProcedure("@AdHoc", "select VAL from FOO where ID = 0;").getResults()[0];
            results.advanceRow();
            assertEquals("ryanloves", results.getString("VAL"));
            threw = false;
            try {
                m_client.callProcedure("@AdHoc",
                        "alter table FOO alter column VAL rename to LAV;");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                threw = true;
            }
            assertTrue("Shouldn't be able to rename columns in a non-empty table", threw);
            results = m_client.callProcedure("@AdHoc", "select VAL from FOO where ID = 0;").getResults()[0];
            results.advanceRow();
            assertEquals("ryanloves", results.getString("VAL"));

            // After the empty table checks go in, add similar tests to EMPTYFOO that show
            // that rename on empty stuff works.
        }
        finally {
            teardownSystem();
        }
    }

    public void testAlterLimitPartitionRows() throws Exception
    {
        String pathToCatalog = Configuration.getPathToCatalogForTest("adhocddl.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("adhocddl.xml");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(
                "create table FOO (" +
                "ID integer not null," +
                "VAL varchar(50), " +
                "constraint PK_TREE primary key (ID)" +
                ");\n"
                );
        builder.addPartitionInfo("FOO", "ID");
        builder.setUseDDLSchema(true);
        boolean success = builder.compile(pathToCatalog, 1, 1, 0);
        assertTrue("Schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;

        try {
            startSystem(config);
            VoltTable results = m_client.callProcedure("@Statistics", "TABLE", 0).getResults()[0];
            while (results.getRowCount() == 0) {
                results = m_client.callProcedure("@Statistics", "TABLE", 0).getResults()[0];
            }
            System.out.println(results);
            assertEquals(1, results.getRowCount());
            results.advanceRow();
            Long limit = results.getLong("TUPLE_LIMIT");
            assertTrue(results.wasNull());
            try {
                m_client.callProcedure("@AdHoc",
                        "alter table foo add limit partition rows 10;");
            }
            catch (ProcCallException pce) {
                fail("Should be able to alter partition row limit: " + pce.toString());
            }
            do {
                results = m_client.callProcedure("@Statistics", "TABLE", 0).getResults()[0];
                results.advanceRow();
                limit = results.getLong("TUPLE_LIMIT");
            }
            while (results.wasNull());
            System.out.println(results);
            assertEquals(10L, (long)limit);

            try {
                m_client.callProcedure("@AdHoc",
                        "alter table foo drop limit partition rows;");
            }
            catch (ProcCallException pce) {
                fail("Should be able to drop partition row limit: " + pce.toString());
            }
            do {
                results = m_client.callProcedure("@Statistics", "TABLE", 0).getResults()[0];
                results.advanceRow();
                limit = results.getLong("TUPLE_LIMIT");
            }
            while (!results.wasNull());
            System.out.println(results);
        }
        finally {
            teardownSystem();
        }
    }

    public void testAlterPartitionColumn() throws Exception
    {
        String pathToCatalog = Configuration.getPathToCatalogForTest("adhocddl.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("adhocddl.xml");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(
                "create table FOO (" +
                "ID integer not null," +
                "VAL varchar(50) not null, " +
                "VAL2 bigint" +
                ");\n" +
                "create table EMPTYFOO (" +
                "ID integer not null," +
                "VAL varchar(50) not null, " +
                "VAL2 bigint" +
                ");\n"
                );
        builder.addPartitionInfo("FOO", "ID");
        builder.addPartitionInfo("EMPTYFOO", "ID");
        builder.setUseDDLSchema(true);
        boolean success = builder.compile(pathToCatalog, 2, 1, 0);
        assertTrue("Schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;

        try {
            startSystem(config);
            // write a couple rows to FOO so it's not empty
            m_client.callProcedure("FOO.insert", 0, "ryanloves", 0);
            m_client.callProcedure("FOO.insert", 1, "theyankees", 1);
            // Double-check our starting point
            assertTrue(findTableInSystemCatalogResults("FOO"));
            assertTrue(isColumnPartitionColumn("FOO", "ID"));
            assertTrue(findTableInSystemCatalogResults("EMPTYFOO"));
            assertTrue(isColumnPartitionColumn("EMPTYFOO", "ID"));

            // First, have fun with the empty table
            // alter the partition column type, should work on empty table
            try {
                m_client.callProcedure("@AdHoc",
                        "alter table EMPTYFOO alter column ID bigint not null;");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to alter partition column type on empty table");
            }
            // Make sure it's still the partition column but the new type
            assertTrue(isColumnPartitionColumn("EMPTYFOO", "ID"));
            assertTrue(verifyTableColumnType("EMPTYFOO", "ID", "BIGINT"));
            // Change the partition column, should work on an empty table
            try {
                m_client.callProcedure("@AdHoc",
                        "partition table EMPTYFOO on column VAL;");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to change partition column on empty table");
            }
            assertTrue(isColumnPartitionColumn("EMPTYFOO", "VAL"));
            assertTrue(verifyTableColumnType("EMPTYFOO", "VAL", "VARCHAR"));

            // try to change the partition to a nullable column, nothing should change
            boolean threw = false;
            try {
                m_client.callProcedure("@AdHoc",
                        "partition table EMPTYFOO on column VAL2;");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                threw = true;
            }
            assertTrue("Shouldn't be able to change partition column to nullable column", threw);
            assertTrue(isColumnPartitionColumn("EMPTYFOO", "VAL"));
            assertTrue(verifyTableColumnType("EMPTYFOO", "VAL", "VARCHAR"));
            // Now drop the partition column, should go away and end up with replicated table
            try {
                m_client.callProcedure("@AdHoc",
                        "alter table EMPTYFOO drop column VAL;");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to drop partition column on empty table");
            }
            assertFalse(isColumnPartitionColumn("EMPTYFOO", "ID"));
            assertFalse(isColumnPartitionColumn("EMPTYFOO", "VAL"));
            assertFalse(isColumnPartitionColumn("EMPTYFOO", "VAL2"));

            // repeat with non-empty table.  Most everything should fail
            // alter the partition column type wider, should work on non-empty table
            try {
                m_client.callProcedure("@AdHoc",
                        "alter table FOO alter column ID bigint not null;");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to alter partition column type to wider type on non-empty table");
            }
            assertTrue(isColumnPartitionColumn("FOO", "ID"));
            assertTrue(verifyTableColumnType("FOO", "ID", "BIGINT"));
            // alter the partition column type narrower, should fail on non-empty table
            threw = false;
            try {
                m_client.callProcedure("@AdHoc",
                        "alter table FOO alter column ID integer not null;");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                threw = true;
            }
            assertTrue("Shouldn't be able to narrow partition column on non-empty table", threw);
            // Make sure it's still the partition column and the same type
            assertTrue(isColumnPartitionColumn("FOO", "ID"));
            assertTrue(verifyTableColumnType("FOO", "ID", "BIGINT"));
            // Change the partition column, should fail on a non-empty table
            threw = false;
            try {
                m_client.callProcedure("@AdHoc",
                        "partition table FOO on column VAL;");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                threw = true;
            }
            assertTrue("Shouldn't be able to change partition column on non-empty table", threw);
            assertTrue(isColumnPartitionColumn("FOO", "ID"));
            assertTrue(verifyTableColumnType("FOO", "ID", "BIGINT"));

            // try to change the partition to a nullable column, nothing should change
            threw = false;
            try {
                m_client.callProcedure("@AdHoc",
                        "partition table FOO on column VAL2;");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                threw = true;
            }
            assertTrue("Shouldn't be able to change partition column to nullable column", threw);
            assertTrue(isColumnPartitionColumn("FOO", "ID"));
            assertTrue(verifyTableColumnType("FOO", "ID", "BIGINT"));
            // Now drop the partition column, should go away and end up with replicated table
            threw = false;
            try {
                m_client.callProcedure("@AdHoc",
                        "alter table FOO drop column ID;");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                threw = true;
            }
            assertTrue("Shouldn't be able to drop partition column on non-empty table", threw);
            assertTrue(isColumnPartitionColumn("FOO", "ID"));
            assertTrue(verifyTableColumnType("FOO", "ID", "BIGINT"));
        }
        finally {
            teardownSystem();
        }
    }

    public void testAlterConstraintAssumeUnique() throws Exception
    {
        String pathToCatalog = Configuration.getPathToCatalogForTest("adhocddl.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("adhocddl.xml");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("-- dont care");
        builder.setUseDDLSchema(true);
        boolean success = builder.compile(pathToCatalog, 1, 1, 0);
        assertTrue("Schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;
        try {
            startSystem(config);
            m_client.callProcedure("@AdHoc",
                    "create table FOO (ID integer not null, VAL bigint not null, VAL2 bigint not null);");
            m_client.callProcedure("@AdHoc",
                    "partition table foo on column ID;");
            // Should be no indexes in the system (no constraints)
            VoltTable indexes = m_client.callProcedure("@Statistics", "INDEX", 0).getResults()[0];
            assertEquals(0, indexes.getRowCount());
            // now add an ASSUMEUNIQUE constraint (ENG-7224)
            m_client.callProcedure("@AdHoc",
                    "alter table FOO add constraint blerg ASSUMEUNIQUE(VAL);");
            do {
                indexes = m_client.callProcedure("@Statistics", "INDEX", 0).getResults()[0];
            }
            while (indexes.getRowCount() != 1);
            // Only one host, one site/host, should only be one row in returned result
            indexes.advanceRow();
            assertEquals(1, indexes.getLong("IS_UNIQUE"));

            // Make sure we can drop a named one (can't drop unnamed at the moment, haha)
            m_client.callProcedure("@AdHoc",
                    "alter table FOO drop constraint blerg;");
            indexes = m_client.callProcedure("@Statistics", "INDEX", 0).getResults()[0];
            do {
                indexes = m_client.callProcedure("@Statistics", "INDEX", 0).getResults()[0];
            }
            while (indexes.getRowCount() != 0);
        }
        finally {
            teardownSystem();
        }
    }

    public void testAddNotNullColumnToEmptyTable() throws Exception
    {
        String pathToCatalog = Configuration.getPathToCatalogForTest("adhocddl.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("adhocddl.xml");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(
                "create table FOO (" +
                "ID integer not null," +
                "VAL bigint, " +
                "constraint pk_tree primary key (ID)" +
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

            try {
                m_client.callProcedure("@AdHoc",
                        "alter table FOO add column NEWCOL varchar(50) not null;");
            }
            catch (ProcCallException pce) {
                fail(pce.getLocalizedMessage());
            }
            assertTrue(verifyTableColumnType("FOO", "NEWCOL", "VARCHAR"));
            assertTrue(verifyTableColumnSize("FOO", "NEWCOL", 50));
            assertFalse(isColumnNullable("FOO", "NEWCOL"));
        }
        finally {
            teardownSystem();
        }
    }

    public void testAddNotNullColumnToNonEmptyTable() throws Exception
    {
        String pathToCatalog = Configuration.getPathToCatalogForTest("adhocddl.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("adhocddl.xml");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(
                "create table FOO (" +
                "ID integer not null," +
                "VAL bigint, " +
                "constraint pk_tree primary key (ID)" +
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

            // Adding NOT NULL column without a default fails for a non-empty table.
            m_client.callProcedure("FOO.insert", 0, 0);
            boolean threw = false;
            try {
                m_client.callProcedure("@AdHoc",
                        "alter table FOO add column NEWCOL varchar(50) not null;");
            }
            catch (ProcCallException pce) {
                assertTrue("Expected \"is not empty\" error.", pce.getMessage().contains("is not empty"));
                threw = true;
            }
            assertTrue(threw);

            // Adding NOT NULL column with a default succeeds for a non-empty table.
            try {
                m_client.callProcedure("@AdHoc",
                        "alter table FOO add column NEWCOL varchar(50) default 'default' not null;");
            }
            catch (ProcCallException pce) {
                fail("Should be able to add NOT NULL column with default to a non-empty table.");
            }
            assertTrue(verifyTableColumnType("FOO", "NEWCOL", "VARCHAR"));
            assertTrue(verifyTableColumnSize("FOO", "NEWCOL", 50));
            assertFalse(isColumnNullable("FOO", "NEWCOL"));
        }
        finally {
            teardownSystem();
        }
    }

    // Check that assumeunique constraints and rowlimit constraints are preserved
    // across ALTER TABLE
    public void testAlterTableENG7242NoExpressions() throws Exception
    {
        System.out.println("----------------\n\n TestAlterTableENG7242 \n\n--------------");
        String pathToCatalog = Configuration.getPathToCatalogForTest("adhocddl.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("adhocddl.xml");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("-- dont care");
        builder.setUseDDLSchema(true);
        boolean success = builder.compile(pathToCatalog, 1, 1, 0);
        assertTrue("Schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;

        try {
            startSystem(config);
            m_client.callProcedure("@AdHoc",
                    "create table FOO (ID integer not null, " +
                    "VAL bigint not null, " +
                    "VAL2 bigint not null, " +
                    "VAL3 bigint not null);");
            m_client.callProcedure("@AdHoc",
                    "partition table foo on column ID;");
            // Should be no indexes in the system (no constraints)
            VoltTable indexes = m_client.callProcedure("@Statistics", "INDEX", 0).getResults()[0];
            VoltTable tables = null;
            assertEquals(0, indexes.getRowCount());
            m_client.callProcedure("@AdHoc",
                    "alter table FOO add constraint blarg LIMIT PARTITION ROWS 10;");
            // and an ASSUMEUNIQUE constraint (custom VoltDB constraint)
            m_client.callProcedure("@AdHoc",
                    "alter table FOO add constraint blerg ASSUMEUNIQUE(VAL);");
            // Stall until the indexes update
            do {
                indexes = m_client.callProcedure("@Statistics", "INDEX", 0).getResults()[0];
                tables = m_client.callProcedure("@Statistics", "TABLE", 0).getResults()[0];
            }
            while (indexes.getRowCount() != 1);
            // Only one host, one site/host, should only be one row in returned result
            indexes.advanceRow();
            assertEquals(1, indexes.getLong("IS_UNIQUE"));
            tables.advanceRow();
            assertEquals(10, tables.getLong("TUPLE_LIMIT"));

            // ENG-7242 - check that VoltDB constraints are preserved across alter table
            try {
                m_client.callProcedure("@AdHoc", "alter table FOO drop column VAL2;");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                // We just compile-fail here when assumeunique goes away.  Unclear that
                // there's a better way to programatically check the difference
                // between ASSUMEUNIQUE and UNIQUE?
                fail("ALTER TABLE shouldn't drop ASSUMEUNIQUE from constraint blerg");
            }

            // ENG-7242 - check the row limits on the table
            // Spin until stats updates the table
            do {
                tables = m_client.callProcedure("@Statistics", "TABLE", 0).getResults()[0];
            }
            while (tables.getColumnCount() == 3);
            tables.advanceRow();
            assertEquals(10, tables.getLong("TUPLE_LIMIT"));

            // Make sure we can drop a named one (can't drop unnamed at the moment, haha)
            m_client.callProcedure("@AdHoc",
                    "alter table FOO drop constraint blerg;");
            indexes = m_client.callProcedure("@Statistics", "INDEX", 0).getResults()[0];
            do {
                indexes = m_client.callProcedure("@Statistics", "INDEX", 0).getResults()[0];
                tables = m_client.callProcedure("@Statistics", "TABLE", 0).getResults()[0];
            }
            while (indexes.getRowCount() != 0);
            // Check row limits again...if we failed to copy it on the first alter table,
            // it's definitely going to be bad here
            tables.advanceRow();
            assertEquals(10, tables.getLong("TUPLE_LIMIT"));
        }
        finally {
            teardownSystem();
        }
    }

    // Will also test the constraint with expression part of ENG-7242
    // Currently commented out because it fails, just wanted to write it while I was here --izzy
    public void testAlterTableENG7304ENG7305() throws Exception
    {
        System.out.println("----------------\n\n TestAlterTableENG7304ENG7305 \n\n--------------");
        String pathToCatalog = Configuration.getPathToCatalogForTest("adhocddl.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("adhocddl.xml");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("-- dont care");
        builder.setUseDDLSchema(true);
        boolean success = builder.compile(pathToCatalog, 1, 1, 0);
        assertTrue("Schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;
        try {
            startSystem(config);
            m_client.callProcedure("@AdHoc",
                    "create table FOO (ID integer not null, " +
                    "VAL bigint not null, " +
                    "VAL2 bigint not null, " +
                    "VAL3 bigint not null);");
            m_client.callProcedure("@AdHoc",
                    "partition table foo on column ID;");
            // Add a unique function constraint (custom VoltDB constraint)
            // TESTS ENG-7304
            m_client.callProcedure("@AdHoc",
                    "alter table FOO add constraint blurg ASSUMEUNIQUE(abs(VAL3));");

            // Check that the unique absolute value constraint applies
            m_client.callProcedure("FOO.insert", 1, 1, 1, 1);
            boolean threw = true;
            try {
                m_client.callProcedure("FOO.insert", -1, -1, -1, -1);
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                threw = true;
            }
            assertTrue("Unique absolute value constraint on FOO never applied", threw);

            // ENG-7305: Verify that we can't alter table that messes with
            // expression index/constraint when table has data but that we can
            // when table is empty
            threw = false;
            try {
                m_client.callProcedure("@AdHoc", "alter table FOO drop column VAL2;");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                threw = true;
            }
            assertTrue("Shouldn't be able to drop column VAL2 when table has data", threw);

            // Now empty the table and try again
            try {
                m_client.callProcedure("@AdHoc", "truncate table FOO;");
                m_client.callProcedure("@AdHoc", "alter table FOO drop column VAL2;");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to drop a column on empty table in presence of expression-based index: " + pce.getMessage());
            }

            // Check that the unique absolute value constraint still applies (ENG-7242)
            m_client.callProcedure("FOO.insert", 2, 2, 2);
            threw = true;
            try {
                m_client.callProcedure("FOO.insert", -2, -2, -2);
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                threw = true;
            }
            assertTrue("Unique absolute value constraint on FOO has gone missing", threw);
        }
        finally {
            teardownSystem();
        }
    }
}
