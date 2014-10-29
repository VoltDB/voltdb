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
            assertTrue("Shouldn't be able to add a not null column without default", threw);

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
                        "alter table FOO drop column DROPME;");
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
                        "alter table FOO drop column VIEWCOL cascade;");
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

            assertTrue(verifyTableColumnDefault("FOO", "VAL", null));
            try {
                m_client.callProcedure("@AdHoc",
                        "alter table FOO alter VAL set default 'goats';");
            }
            catch (ProcCallException pce) {
                fail("Shouldn't fail");
            }
            assertTrue(verifyTableColumnDefault("FOO", "VAL", "'goats'"));
            assertTrue(isColumnNullable("FOO", "VAL"));
            // Can't just make something not null (for the moment, empty table
            // check to come later?)
            boolean threw = false;
            try {
                m_client.callProcedure("@AdHoc",
                        "alter table FOO alter VAL set not null;");
            }
            catch (ProcCallException pce) {
                threw = true;
            }
            assertTrue("Shouldn't be able to declare not null on existing column", threw);
            assertTrue(isColumnNullable("FOO", "VAL"));
            // Now change the default back to null
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
}
