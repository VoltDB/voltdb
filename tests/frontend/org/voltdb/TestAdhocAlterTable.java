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
    /**
     * Launch an AdHoc ddl command and expect it to succeed.
     * Fail the test if it gets a ProcCallException.
     * @param command
     * @throws Exception
     */
    private void tryAsShouldBeAble(String command) throws Exception {
        try {
            m_client.callProcedure("@AdHoc", command);
        }
        catch (ProcCallException pce) {
            fail("Should be able to '" + command + "' but caught: " +
                    pce.getLocalizedMessage());
        }
    }

    /**
     * Launch an AdHoc ddl command and expect it to fail.
     * fail the test if the command returns normally.
     * Make sure the ProcCallException contains the appropriate text
     * in complaint, or if complaint is null rant to standard out
     * that it really shouldn't be. Long term, that's just being sloppy.
     * @param condition describe any specific context that contributes to the failure
     * @param command should be valid ddl or something like it
     * @param complaint part of the expected ProcCallException message
     * @throws Exception
     */
    private void tryButShouldNotBeAble(String condition, String command,
            String complaint) throws Exception {
        try {
            m_client.callProcedure("@AdHoc", command);
            fail("Shouldn't be able to '" + command + "' " + condition);
        }
        catch (ProcCallException pce) {
            if (complaint == null) {
                pce.printStackTrace();
                System.out.println(
                        "TODO: pick some part of the message at the top " +
                        " of that stack trace to pass " +
                        "instead of null as a pattern filter to " +
                        "tryButShouldNotBeAble from the caller " +
                        "found in the stack trace");
            }
            else {
                assertTrue("Unexpected message from exception: " + pce.getLocalizedMessage(),
                        pce.getLocalizedMessage().contains(complaint));
            }
        }
    }
    
    /**
     * Build a simple configuration.
     * @param initialSchema
     * @return A 2-node server configuaration initially serving initialSchema.
     * @throws Exception
     */
    private Configuration configure(String initialSchema) throws Exception {
        String pathToCatalog = Configuration.getPathToCatalogForTest("adhocddl.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("adhocddl.xml");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(initialSchema);
        builder.setUseDDLSchema(true);
        boolean success = builder.compile(pathToCatalog, 2, 1, 0);
        assertTrue("Schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        Configuration config = new Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;
        return config;
    }

    /**
     * Poll a stats selector, waiting for the given row count, up to about 1 minute.
     * @param targetCount
     * @param selector
     * @return
     * @throws Exception
     */
    private VoltTable pollStatsWithTimeout(int targetCount, String selector)
            throws Exception {
        VoltTable stats = m_client.callProcedure("@Statistics", selector, 0).getResults()[0];
        int patience = 600; // Wait just a minute -- at most -- to see the schema change
        while (stats.getRowCount() != targetCount) {
            if (patience-- == 0) {
                fail("It should have taken less than a minute for the " +
                        selector + " count now at " + stats.getRowCount() +
                        " to hit " + targetCount);
            }
            Thread.sleep(100);
            stats = m_client.callProcedure("@Statistics", selector, 0).getResults()[0];
        }
        return stats;
    }

    private VoltTable pollStatsWithTimeout(int targetCount, String selector,
            String column, Object targetValue) throws Exception {
        VoltTable stats = m_client.callProcedure("@Statistics", selector, 0).getResults()[0];
        VoltType type = stats.getColumnType(stats.getColumnIndex(column));
        Object lastBadValue = null;
        int patience = 600; // Wait just a minute -- at most -- to see the schema change
        while (true) {
            long lastRowCount = stats.getRowCount();
            if (lastRowCount == targetCount) {
                boolean ready = true;
                while (stats.advanceRow()) {
                    Object value = stats.get(column, type);
                    if (stats.wasNull()) {
                        if (targetValue != null) {
                            lastBadValue = targetValue;
                            ready = false;
                            break;
                        }
                        continue;
                    }
                    if (targetValue == null || ! targetValue.equals(value)) {
                        lastBadValue = targetValue;
                        ready = false;
                        break;
                    }
                }
                if (ready) {
                    break;
                }
            }
            if (patience-- == 0) {
                fail("It should have taken less than a minute for all " +
                        targetCount + " " + selector + " row " +
                        column + " values to become " +
                        (targetValue == null ? "null" : targetValue.toString()) +
                        " vs. at last check " + lastRowCount + " rows" +
                        ((lastRowCount != targetCount) ? "" :
                                " with at least one " +
                                column + " value set to " +
                                (lastBadValue == null ? "null" : lastBadValue.toString())));
            }
            Thread.sleep(100);
            stats = m_client.callProcedure("@Statistics", selector, 0).getResults()[0];
        }
        return stats;
    }

    public void testAlterAddColumn() throws Exception
    {
        String initialSchema =
                "create table FOO (" +
                "ID integer not null," +
                "VAL bigint, " +
                "constraint pk_tree primary key (ID)" +
                ");\n" +
                "partition table FOO on column ID;\n" +
                "";
        Configuration config = configure(initialSchema);

        try {
            startSystem(config);

            tryAsShouldBeAble("alter table FOO add column NEWCOL varchar(50);");
            assertTrue(verifyTableColumnType("FOO", "NEWCOL", "VARCHAR"));
            assertTrue(verifyTableColumnSize("FOO", "NEWCOL", 50));
            assertTrue(isColumnNullable("FOO", "NEWCOL"));

            // second time should fail
            tryButShouldNotBeAble("on an existing column",
                    "alter table FOO add column NEWCOL varchar(50);",
                    "object name already exists");

            // can't add another primary key
            tryButShouldNotBeAble("on a table with an existing primary key",
                    "alter table FOO add column BADPK integer primary key;",
                    "primary key definition not allowed in statement"); // message could be better

            // Should be able to add a not-null column with no default
            // with an empty table.
            tryAsShouldBeAble("alter table FOO add column YESNOTNULL integer not null;");
            assertTrue(verifyTableColumnType("FOO", "YESNOTNULL", "INTEGER"));
            assertFalse(isColumnNullable("FOO", "YESNOTNULL"));

            // but we're always good with a default
            tryAsShouldBeAble("alter table FOO add column GOODNOTNULL integer default 0 not null;");
            assertTrue(verifyTableColumnType("FOO", "GOODNOTNULL", "INTEGER"));
            assertFalse(isColumnNullable("FOO", "GOODNOTNULL"));

            // Can't add a column with a bad definition
            tryButShouldNotBeAble("ever",
                        "alter table FOO add column BADVARCHAR varchar(0) not null;",
                        "precision or scale out of range");
            assertFalse(doesColumnExist("FOO", "BADVARCHAR"));
        }
        finally {
            teardownSystem();
        }
    }

    public void testAlterDropColumn() throws Exception
    {
        String initialSchema =
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
                ");\n" +
                "";
        Configuration config = configure(initialSchema);
        try {
            startSystem(config);

            // Basic alter drop, should work
            assertTrue(doesColumnExist("FOO", "DROPME"));
            tryAsShouldBeAble("alter table FOO drop column DROPME;");
            assertFalse(doesColumnExist("FOO", "DROPME"));

            tryButShouldNotBeAble("for a column already dropped",
                    "alter table FOO drop column DROPME; --commentfortesting",
                    "object not found: DROPME");
            assertFalse(doesColumnExist("FOO", "DROPME"));

            // Can't drop column used by procedure
            assertTrue(doesColumnExist("FOO", "PROCCOL"));
            tryButShouldNotBeAble("for a column in use by a view",
                    "alter table FOO drop column PROCCOL;",
                    "Failed to plan for statement (sql) select PROCCOL "); // This could be improved.
            assertTrue(doesColumnExist("FOO", "PROCCOL"));
            try {
                m_client.callProcedure("BAR");
            }
            catch (ProcCallException pce) {
                fail("Procedure should still exist.");
            }

            // Can't drop a column used by a view
            assertTrue(doesColumnExist("FOO", "VIEWCOL"));
            tryButShouldNotBeAble("for a column in use by a view",
                    "alter table FOO drop column VIEWCOL;",
                    "column is referenced in: PUBLIC.FOOVIEW");
            assertTrue(doesColumnExist("FOO", "VIEWCOL"));
            assertTrue(findTableInSystemCatalogResults("FOOVIEW"));

            // unless you use cascade
            assertTrue(doesColumnExist("FOO", "VIEWCOL"));
            assertTrue(findTableInSystemCatalogResults("FOOVIEW"));
            tryAsShouldBeAble("alter table FOO drop column VIEWCOL cascade; " + 
                    "--comment messes up cascade?");
            assertFalse(doesColumnExist("FOO", "VIEWCOL"));
            assertFalse(findTableInSystemCatalogResults("FOOVIEW"));

            // single-column indexes get cascaded automagically
            assertTrue(doesColumnExist("FOO", "INDEXCOL"));
            assertTrue(findIndexInSystemCatalogResults("FOODEX"));
            tryAsShouldBeAble("alter table FOO drop column INDEXCOL;");
            assertFalse(doesColumnExist("FOO", "INDEXCOL"));
            assertFalse(findIndexInSystemCatalogResults("FOODEX"));

            // single-column primary keys get cascaded automagically
            assertTrue(doesColumnExist("FOO", "PKCOL"));
            assertTrue(findIndexInSystemCatalogResults("VOLTDB_AUTOGEN_CONSTRAINT_IDX_PK_TREE"));
            tryAsShouldBeAble("alter table FOO drop column PKCOL;");
            assertFalse(doesColumnExist("FOO", "PKCOL"));
            assertFalse(findIndexInSystemCatalogResults("VOLTDB_AUTOGEN_CONSTRAINT_IDX_PK_TREE"));

            // WEIRD: this seems like weird behavior to me still --izzy
            // Dropping a column used by a multi-column index drops the index
            assertTrue(doesColumnExist("FOO", "INDEX1COL"));
            assertTrue(findIndexInSystemCatalogResults("FOO2DEX"));
            tryAsShouldBeAble("alter table FOO drop column INDEX1COL;");
            assertFalse(doesColumnExist("FOO", "INDEX1COL"));
            assertFalse("Dropping a column used by an index should kill the index",
                    findIndexInSystemCatalogResults("FOO2DEX"));

            // Can't drop a column used by a multi-column primary key
            assertTrue(doesColumnExist("BAZ", "PKCOL1"));
            assertTrue(findIndexInSystemCatalogResults("VOLTDB_AUTOGEN_CONSTRAINT_IDX_PK_TREE2"));
            tryButShouldNotBeAble("ever",
                    "alter table BAZ drop column PKCOL1;",
                    "column is referenced in: PUBLIC.PK_TREE2");
            System.out.println("COLUMNS: " + m_client.callProcedure("@SystemCatalog", "COLUMNS").getResults()[0]);
            System.out.println("INDEXES: " + m_client.callProcedure("@SystemCatalog", "INDEXINFO").getResults()[0]);
            assertTrue(doesColumnExist("BAZ", "PKCOL1"));
            assertTrue(findIndexInSystemCatalogResults("VOLTDB_AUTOGEN_CONSTRAINT_IDX_PK_TREE2"));

            // Can't drop the last column in a table
            assertTrue(doesColumnExist("ONECOL", "SOLOCOL"));
            tryButShouldNotBeAble("for last column remaining in table",
                    "alter table ONECOL drop column SOLOCOL;",
                    "cannot drop sole column of table");
            assertTrue(doesColumnExist("ONECOL", "SOLOCOL"));
        }
        finally {
            teardownSystem();
        }
    }

    public void testAlterColumnOther() throws Exception
    {
        System.out.println("----------------\n\n testAlterColumnOther \n\n--------------");
        String initialSchema =
                "create table FOO (" +
                "ID integer not null," +
                "VAL varchar(50), " +
                "constraint PK_TREE primary key (ID)" +
                ");\n" +
                "partition table FOO on column ID;\n" +
                "";
        Configuration config = configure(initialSchema);
        try {
            startSystem(config);

            // Setting to not null should work if the table is empty.
            tryAsShouldBeAble("alter table FOO alter VAL set not null;");
            assertFalse(isColumnNullable("FOO", "VAL"));

            // Make nullable again.
            tryAsShouldBeAble("alter table FOO alter VAL set null;");
            assertTrue(isColumnNullable("FOO", "VAL"));

            // Specify a default.
            assertTrue(verifyTableColumnDefault("FOO", "VAL", null));
            tryAsShouldBeAble("alter table FOO alter VAL set default 'goats';");
            assertTrue(verifyTableColumnDefault("FOO", "VAL", "'goats'"));
            assertTrue(isColumnNullable("FOO", "VAL"));

            // Change the default back to null
            tryAsShouldBeAble("alter table FOO alter VAL set default NULL;");
            assertTrue(verifyTableColumnDefault("FOO", "VAL", null));
            assertTrue(isColumnNullable("FOO", "VAL"));

            // Setting to not null should fail if the table is not empty.
            m_client.callProcedure("FOO.insert", 0, "whatever");
            tryButShouldNotBeAble("on a non-empty table",
                    "alter table FOO alter VAL set not null;",
                    "Unable to change column VAL null constraint to NOT NULL in table FOO because it is not empty");
            assertTrue(isColumnNullable("FOO", "VAL"));

            // Clear the table and reset VAL default (by setting to NULL)
            m_client.callProcedure("@AdHoc", "delete from FOO;");

            tryAsShouldBeAble("alter table FOO alter VAL set default NULL;");
            assertTrue(verifyTableColumnDefault("FOO", "VAL", null));
            assertTrue(isColumnNullable("FOO", "VAL"));

            // Can't make primary key nullable
            assertFalse(isColumnNullable("FOO", "ID"));
            tryButShouldNotBeAble("on a primary key",
                    "alter table FOO alter ID set null;",
                    "column is in primary key in statement");
            assertFalse(isColumnNullable("FOO", "ID"));

            // magic name for PK index
            /* not yet hsql232 -- running afoul of ddl roundtrip
            // FATAL [Ad Hoc Planner - 0] HOST: Catalog Verification from Generated DDL failed! The offending diffcmds were: delete /clusters#cluster/databases#database/tables#FOO indexes VOLTDB_AUTOGEN_IDX_CT_FOO_ID_43496
//             [junit] VoltDB has encountered an unrecoverable error and is exiting.
//             [junit] delete /clusters#cluster/databases#database/tables#FOO constraints VOLTDB_AUTOGEN_IDX_CT_FOO_ID_43496
//             [junit] The log may contain additional information.
//             [junit] add /clusters#cluster/databases#database/tables#FOO constraints VOLTDB_AUTOGEN_IDX_CT_FOO_ID
//             [junit] set /clusters#cluster/databases#database/tables#FOO/constraints#VOLTDB_AUTOGEN_IDX_CT_FOO_ID type 2
//             [junit] set $PREV oncommit ""
//             [junit] set $PREV index /clusters#cluster/databases#database/tables#FOO/indexes#VOLTDB_AUTOGEN_CONSTRAINT_IDX_PK_TREE
//             [junit] set $PREV foreignkeytable null
            assertTrue(findIndexInSystemCatalogResults("VOLTDB_AUTOGEN_CONSTRAINT_IDX_PK_TREE"));
            assertTrue(verifyIndexUniqueness("VOLTDB_AUTOGEN_CONSTRAINT_IDX_PK_TREE", true));
            tryAsShouldBeAble("alter table FOO add unique (ID);");
            // Unique constraint we added is redundant with existing constraint
            assertTrue(findIndexInSystemCatalogResults("VOLTDB_AUTOGEN_CONSTRAINT_IDX_PK_TREE"));
            assertTrue(verifyIndexUniqueness("VOLTDB_AUTOGEN_CONSTRAINT_IDX_PK_TREE", true));

            tryAsShouldBeAble("alter table FOO drop constraint PK_TREE;");
            // Now we create a new named index for the unique constraint.  C'est la vie.
            assertTrue(findIndexInSystemCatalogResults("VOLTDB_AUTOGEN_IDX_CT_FOO_ID"));
            assertTrue(verifyIndexUniqueness("VOLTDB_AUTOGEN_IDX_CT_FOO_ID", true));

            // Can't add a PK constraint on a non-partition key.
            tryButShouldNotBeAble("on a non-partition key",
                    "alter table FOO add constraint PK_TREE primary key (VAL);",
                    "does not include the partitioning column ID");
            // But we can add it back on the original column
            //tryAsShouldBeAble("alter table FOO add constraint PK_TREE primary key (ID);");

            /* enable to debug *-/ System.out.println("INDEXES: " + m_client.callProcedure("@SystemCatalog", "INDEXINFO").getResults()[0]);
            // Of course we rename this yet again, because, why not?
            //TODO: fix how a later-added pk constraint/index does not get the
            // same name as the original inline definition. Accident? 
            // Or some kind of shortfall in the canonicalizer? 
            //assertTrue(findIndexInSystemCatalogResults("VOLTDB_AUTOGEN_IDX_FOO_ID"));
            //assertTrue(verifyIndexUniqueness("VOLTDB_AUTOGEN_IDX_FOO_ID", true));
            // not yet hsql232 */
        }
        finally {
            teardownSystem();
        }
    }

    public void testAlterRename() throws Exception
    {
        String initialSchema =
                "create table FOO (" +
                "ID integer not null," +
                "VAL varchar(50), " +
                "constraint PK_TREE primary key (ID)" +
                ");\n" +
                "create table EMPTYFOO (" +
                "ID integer not null," +
                "VAL varchar(50), " +
                "constraint PK_TREE2 primary key (ID)" +
                ");\n" +
                "partition table FOO on column ID;\n" +
                "partition table EMPTYFOO on column ID;\n" +
                "";
        Configuration config = configure(initialSchema);
        try {
            startSystem(config);
            // write a couple rows to FOO so it's not empty
            m_client.callProcedure("FOO.insert", 0, "ryanloves");
            m_client.callProcedure("FOO.insert", 1, "theyankees");

            // check rename table fails
            assertEquals(2, m_client.callProcedure("@AdHoc", "select count(*) from foo;").getResults()[0].asScalarLong());
            tryButShouldNotBeAble("in this release",
                        "alter table FOO rename to BAR;",
                        "ALTER/RENAME is not yet supported");
            assertEquals(2, m_client.callProcedure("@AdHoc", "select count(*) from foo;").getResults()[0].asScalarLong());

            // check rename column on a table fails
            VoltTable results = m_client.callProcedure("@AdHoc", "select VAL from FOO where ID = 0;").getResults()[0];
            results.advanceRow();
            assertEquals("ryanloves", results.getString("VAL"));
            tryButShouldNotBeAble("in this release",
                        "alter table FOO alter column VAL rename to LAV;",
                        "ALTER/RENAME is not yet supported");
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
        String initialSchema =
                "create table FOO (" +
                "ID integer not null," +
                "VAL varchar(50), " +
                "constraint PK_TREE primary key (ID)" +
                ");\n" +
                "partition table FOO on column ID;\n" +
                "";
        Configuration config = configure(initialSchema);
        try {
            startSystem(config);
            pollStatsWithTimeout(2, "TABLE", "TUPLE_LIMIT", null);
            tryAsShouldBeAble("alter table foo add limit partition rows 10;");
            pollStatsWithTimeout(2, "TABLE", "TUPLE_LIMIT", 10);
            tryAsShouldBeAble("alter table foo drop limit partition rows;");
            pollStatsWithTimeout(2, "TABLE", "TUPLE_LIMIT", null);
        }
        finally {
            teardownSystem();
        }
    }

    public void testAlterPartitionColumn() throws Exception
    {
        String initialSchema =
                "create table FOO (" +
                "ID integer not null," +
                "VAL varchar(50) not null, " +
                "VAL2 bigint" +
                ");\n" +
                "create table EMPTYFOO (" +
                "ID integer not null," +
                "VAL varchar(50) not null, " +
                "VAL2 bigint" +
                ");\n" +
                "partition table FOO on column ID;\n" +
                "partition table EMPTYFOO on column ID;\n" +
                "";
        Configuration config = configure(initialSchema);
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
            tryAsShouldBeAble("alter table EMPTYFOO alter column ID bigint not null;");
            // Make sure it's still the partition column but the new type
            assertTrue(isColumnPartitionColumn("EMPTYFOO", "ID"));
            assertTrue(verifyTableColumnType("EMPTYFOO", "ID", "BIGINT"));
            // Change the partition column, should work on an empty table
            tryAsShouldBeAble("partition table EMPTYFOO on column VAL;");
            assertTrue(isColumnPartitionColumn("EMPTYFOO", "VAL"));
            assertTrue(verifyTableColumnType("EMPTYFOO", "VAL", "VARCHAR"));

            // try to change the partition to a nullable column, nothing should change
            tryButShouldNotBeAble("on a non-nullablecolumn",
                    "partition table EMPTYFOO on column VAL2;",
                    "Partition columns must be constrained \"NOT NULL\"");
            assertTrue(isColumnPartitionColumn("EMPTYFOO", "VAL"));
            assertTrue(verifyTableColumnType("EMPTYFOO", "VAL", "VARCHAR"));

            // drop the partition column, should go away and end up with replicated table
            tryAsShouldBeAble("alter table EMPTYFOO drop column VAL;");
            assertFalse(isColumnPartitionColumn("EMPTYFOO", "ID"));
            assertFalse(isColumnPartitionColumn("EMPTYFOO", "VAL"));
            assertFalse(isColumnPartitionColumn("EMPTYFOO", "VAL2"));

            // repeat with non-empty table.  Most everything should fail
            // alter the partition column type wider, should work on non-empty table
            tryAsShouldBeAble("alter table FOO alter column ID bigint not null;");
            assertTrue(isColumnPartitionColumn("FOO", "ID"));
            assertTrue(verifyTableColumnType("FOO", "ID", "BIGINT"));

            // alter the partition column type narrower, should fail on non-empty table
            tryButShouldNotBeAble("on a non-empty table",
                    "alter table FOO alter column ID integer not null;",
                    "Unable to narrow the width of column ID");
            // Make sure it's still the partition column and the same type
            assertTrue(isColumnPartitionColumn("FOO", "ID"));
            assertTrue(verifyTableColumnType("FOO", "ID", "BIGINT"));

            // Change the partition column, should fail on a non-empty table
            tryButShouldNotBeAble("on a non-empty table",
                    "partition table FOO on column VAL;",
                    "Unable to change the partition column of table FOO");
            assertTrue(isColumnPartitionColumn("FOO", "ID"));
            assertTrue(verifyTableColumnType("FOO", "ID", "BIGINT"));

            // try to change the partition to a nullable column, nothing should change
            tryButShouldNotBeAble("on a non-empty table",
                        "partition table FOO on column VAL2;",
                        "Partition column 'FOO.val2' is nullable. " +
                        "Partition columns must be constrained \"NOT NULL\"");
            assertTrue(isColumnPartitionColumn("FOO", "ID"));
            assertTrue(verifyTableColumnType("FOO", "ID", "BIGINT"));

            // try to drop the partition column,
            // for the non-empty table, it should NOT go away
            // to end up with a replicated table
            tryButShouldNotBeAble("on a non-empty table",
                    "alter table FOO drop column ID;",
                    "Unable to change whether table FOO is replicated");
            assertTrue(isColumnPartitionColumn("FOO", "ID"));
            assertTrue(verifyTableColumnType("FOO", "ID", "BIGINT"));
        }
        finally {
            teardownSystem();
        }
    }

    public void testAlterConstraintAssumeUnique() throws Exception
    {
        Configuration config = configure("--don't care");
        try {
            startSystem(config);
            m_client.callProcedure("@AdHoc",
                    "create table FOO (ID integer not null, VAL bigint not null, VAL2 bigint not null);");
            m_client.callProcedure("@AdHoc",
                    "partition table foo on column ID;");
            // Should be no indexes in the system (no constraints)
            pollStatsWithTimeout(0, "INDEX");
            // now add an ASSUMEUNIQUE constraint (ENG-7224)
    /* not yet hsql232 -- lacks support for dynamically adding ASSUMEUNIQUE constraints? maybe also for row limit and preservation of assumeunique
            tryAsShouldBeAble("alter table FOO add constraint blerg ASSUMEUNIQUE(VAL);");
            Byte yes = Byte.valueOf((byte) 1);
            pollStatsWithTimeout(2, "INDEX", "IS_UNIQUE", yes);

            // Make sure we can drop a named one (can't drop unnamed at the moment, haha)
            tryAsShouldBeAble("alter table FOO drop constraint blerg;");
            pollStatsWithTimeout(0, "INDEX");
    //not yet hsql232 */
        }
        finally {
            teardownSystem();
        }
    }

    public void testAddNotNullColumnToEmptyTable() throws Exception
    {
        String initialSchema =
                "create table FOO (" +
                "ID integer not null," +
                "VAL bigint, " +
                "constraint pk_tree primary key (ID)" +
                ");\n" +
                "partition table FOO on column ID;\n" +
                "";
        Configuration config = configure(initialSchema);
        try {
            startSystem(config);

            tryAsShouldBeAble("alter table FOO add column NEWCOL varchar(50) not null;");
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
        String initialSchema =
                "create table FOO (" +
                "ID integer not null," +
                "VAL bigint, " +
                "constraint pk_tree primary key (ID)" +
                ");\n" +
                "partition table FOO on column ID;\n" +
                "";
        Configuration config = configure(initialSchema);
        try {
            startSystem(config);

            // Adding NOT NULL column without a default fails for a non-empty table.
            m_client.callProcedure("FOO.insert", 0, 0);
            tryButShouldNotBeAble("on a non-empty table",
                    "alter table FOO add column NEWCOL varchar(50) not null;",
                    "is not empty");
            // Adding NOT NULL column with a default succeeds for a non-empty table.
            tryAsShouldBeAble("alter table FOO add column NEWCOL varchar(50) default 'default' not null;");
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
        Configuration config = configure("-- don't care");
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
            pollStatsWithTimeout(0, "INDEX");
            tryAsShouldBeAble("alter table FOO add constraint blarg LIMIT PARTITION ROWS 10;");
            // and an ASSUMEUNIQUE constraint (custom VoltDB constraint)
    /* not yet hsql232 -- lacks support for dynamically adding ASSUMEUNIQUE constraints? maybe also for row limit and preservation of assumeunique
    // FATAL [Ad Hoc Planner - 0] HOST: Catalog Verification from Generated DDL failed! The offending diffcmds were: set /clusters#cluster/databases#database/tables#FOO/indexes#VOLTDB_AUTOGEN_CONSTRAINT_IDX_BLERG expressionsjson ""
            tryAsShouldBeAble("alter table FOO add constraint blerg ASSUMEUNIQUE(VAL);");
            // Stall until the indexes update
            Byte yes = Byte.valueOf((byte) 1);
            pollStatsWithTimeout(2, "INDEX", "IS_UNIQUE", yes);
            pollStatsWithTimeout(2, "TABLE", "TUPLE_LIMIT", 10);
            // ENG-7242 - check that VoltDB constraints are preserved across alter table
            // We would just compile-fail here when assumeunique goes away.  Unclear that
            // there's a better way to programmatically check the difference
            // between ASSUMEUNIQUE and UNIQUE?
            tryAsShouldBeAble("alter table FOO drop column VAL2;");
            // How to validate that an eventual change never actually comes?
            // One way is to push another schema update and assume that when it becomes
            // visible, all will be visible.
            tryAsShouldBeAble(
                    "create table FLUSH (ID integer not null, LIMIT PARTITION ROWS 10);");
            // Spin until stats updates the table
            // ENG-7242 - check the row limits on the table
            pollStatsWithTimeout(4, "TABLE", "TUPLE_LIMIT", 10);
            pollStatsWithTimeout(2, "INDEX", "IS_UNIQUE", yes);

            // Make sure we can drop a named constraint without interfering with others
            tryAsShouldBeAble("alter table FOO drop constraint blerg;");
            pollStatsWithTimeout(0, "INDEX");
            // Check row limits again
            pollStatsWithTimeout(4, "TABLE", "TUPLE_LIMIT", 10);
    //not yet hsql232 */
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
        Configuration config = configure("-- don't care");
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
            tryAsShouldBeAble("alter table FOO add constraint blurg ASSUMEUNIQUE(abs(VAL3));");

            // Check that the unique absolute value constraint applies
            m_client.callProcedure("FOO.insert", 1, 1, 1, 1);
            try {
                m_client.callProcedure("FOO.insert", 1, -1, -1, -1);
                fail("Unique absolute value constraint on FOO failed to apply");
            }
            catch (ProcCallException pce) {
                assertTrue(pce.getLocalizedMessage().contains(
                        "Constraint Type UNIQUE"));
            }

    /* not yet hsql232 -- lacks support for dynamically adding ASSUMEUNIQUE constraints? preservation of assumeunique vs. unique
            // ENG-7305: Verify that we can't alter table that messes with
            // expression index/constraint when table has data but that we can
            // when table is empty
            tryButShouldNotBeAble("on a non-empty table",
                    "alter table FOO drop column VAL2;",
                    "Unable to alter table FOO with expression-based index");
            // Now empty the table and try again
            m_client.callProcedure("@AdHoc", "truncate table FOO;");
            tryAsShouldBeAble("alter table FOO drop column VAL2;");

            // Check that the unique absolute value constraint still applies (ENG-7242)
            m_client.callProcedure("FOO.insert", 2, 2, 2);
            try {
                m_client.callProcedure("FOO.insert", 2, -2, -2);
                fail("Unique absolute value constraint on FOO has gone missing");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                assertTrue(pce.getLocalizedMessage().contains(
                        "")); //FIXME put sample text from pce here and remove stack trace.
            }
    //not yet hsql232 */
        }
        finally {
            teardownSystem();
        }
    }
}
