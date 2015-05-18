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

package org.voltdb.compiler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;

import junit.framework.TestCase;

import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Table;
import org.voltdb.utils.BuildDirectoryUtils;

public class TestVoltCompilerAlterDropTable extends TestCase {

    String testout_jar;

    String myproc = "CREATE PROCEDURE MyTableProcedure AS SELECT column2_integer FROM mytable;\n";

    @Override
    public void setUp() {
        testout_jar = BuildDirectoryUtils.getBuildDirectoryPath() + File.pathSeparator + "testout.jar";
    }

    @Override
    public void tearDown() {
        File tjar = new File(testout_jar);
        tjar.delete();
    }

    //Utility to get projectPath for ddl
    private String getSimpleProjectPathForDDL(String ddl) {
        final File schemaFile = VoltProjectBuilder.writeStringToTempFile(ddl);
        final String schemaPath = schemaFile.getPath();

        final String simpleProject =
                "<?xml version=\"1.0\"?>\n" +
                "<project>" +
                "<database name='database'>" +
                "<schemas>" +
                "<schema path='" + schemaPath + "' />" +
                "</schemas>" +
                "<procedures/>" +
                "</database>" +
                "</project>";

        final File projectFile = VoltProjectBuilder.writeStringToTempFile(simpleProject);
        final String projectPath = projectFile.getPath();
        return projectPath;
    }

    private VoltCompiler compileAlteredSchema(String ddl) {
        final String projectPath = getSimpleProjectPathForDDL(ddl);
        final VoltCompiler compiler = new VoltCompiler();
        final boolean success = compiler.compileWithProjectXML(projectPath, testout_jar);
        assertTrue(success);
        return compiler;
    }

    private VoltCompiler failToCompileAlteredSchema(String ddl,
            String pattern) {
        final String projectPath = getSimpleProjectPathForDDL(ddl);
        final VoltCompiler compiler = new VoltCompiler();
        boolean status = compiler.compileWithProjectXML(projectPath, testout_jar);
        assertFalse(status);
        int foundPlanError = 0;
        for (VoltCompiler.Feedback fbLine : compiler.m_errors) {
            String fbMsg = fbLine.message.toLowerCase();
            if (fbMsg.contains(pattern.toLowerCase())) {
                foundPlanError++;
            }
        }
        assertEquals(1, foundPlanError);
        return compiler;
    }

    //Check given col type if catalog was compiled.
    private void verifyTableColumnType(VoltCompiler compiler, String tableName, String colName, VoltType type) {
        Database db = compiler.m_catalog.getClusters().get("cluster").getDatabases().get("database");
        Table table = db.getTables().get(tableName);
        Column col = table.getColumns().get(colName);
        assertNotNull(col);
        assertEquals(0, type.compareTo(VoltType.get((byte) col.getType())));
    }

    //Check given partitioning col
    private void verifyTablePartitioningColumn(VoltCompiler compiler, String tableName, String colName) {
        Database db = compiler.m_catalog.getClusters().get("cluster").getDatabases().get("database");
        Table table = db.getTables().get(tableName);
        Column col = table.getColumns().get(colName);
        assertNotNull(col);
        assertFalse(table.getIsreplicated());
        Column partitionCol = table.getPartitioncolumn();
        assertNotNull(partitionCol);
        assertEquals(colName.toLowerCase(), partitionCol.getName().toLowerCase());
    }

    //Check for no vestiges of partitioning.
    private void verifyTableNotPartitioned(VoltCompiler compiler, String tableName) {
        Database db = compiler.m_catalog.getClusters().get("cluster").getDatabases().get("database");
        Table table = db.getTables().get(tableName);
        assertTrue(table.getIsreplicated());
        Column partitionCol = table.getPartitioncolumn();
        assertNull(partitionCol);
    }

    //Check given col nullability if catalog was compiled.
    private void verifyTableColumnNullable(VoltCompiler compiler, String tableName, String colName, boolean shouldBeNullable) {
        Database db = compiler.m_catalog.getClusters().get("cluster").getDatabases().get("database");
        Table table = db.getTables().get(tableName);
        Column col = table.getColumns().get(colName);
        assertNotNull(col);
        assertEquals(shouldBeNullable, col.getNullable());
    }

    //Check given col size
    private void verifyTableColumnSize(VoltCompiler compiler, String tableName, String colName, int sz) {
        Database db = compiler.m_catalog.getClusters().get("cluster").getDatabases().get("database");
        Table table = db.getTables().get(tableName);
        Column col = table.getColumns().get(colName);
        assertNotNull(col);
        assertEquals(sz, col.getSize());
    }

    //Check given col does exists
    private void verifyTableColumnExists(VoltCompiler compiler, String tableName, String colName) {
        Database db = compiler.m_catalog.getClusters().get("cluster").getDatabases().get("database");
        Table table = db.getTables().get(tableName);
        Column col = table.getColumns().get(colName);
        assertNotNull(col);
    }

    //Check given col does not exists
    private void verifyTableColumnGone(VoltCompiler compiler, String tableName, String colName) {
        Database db = compiler.m_catalog.getClusters().get("cluster").getDatabases().get("database");
        Table table = db.getTables().get(tableName);
        Column col = table.getColumns().get(colName);
        assertNull(col);
    }

    //Check given index exists
    private void verifyIndexExists(VoltCompiler compiler, String tableName, String idxName) {
        Database db = compiler.m_catalog.getClusters().get("cluster").getDatabases().get("database");
        Table table = db.getTables().get(tableName);
        Index idx = table.getIndexes().get(idxName);
        assertNotNull(idx);
    }

    //Check given index exists
    private void verifyIndexGone(VoltCompiler compiler, String tableName, String idxName) {
        Database db = compiler.m_catalog.getClusters().get("cluster").getDatabases().get("database");
        Table table = db.getTables().get(tableName);
        if (table == null) {
            //Index must be gone.
            return;
        }
        Index idx = table.getIndexes().get(idxName);
        assertNull(idx);
    }

    //Check given table is gone
    private void verifyTableGone(VoltCompiler compiler, String tableName) {
        Database db = compiler.m_catalog.getClusters().get("cluster").getDatabases().get("database");
        Table table = db.getTables().get(tableName);
        assertNull(table);
    }

    //Check given table exists
    private void verifyTableExists(VoltCompiler compiler, String tableName) {
        Database db = compiler.m_catalog.getClusters().get("cluster").getDatabases().get("database");
        Table table = db.getTables().get(tableName);
        assertNotNull(table);
    }

    public void testAlterTable() throws IOException {
        final String simpleSchema1 =
                "create table mytable (pkey integer, column2_integer integer);\n" +
                "alter table mytable add column newcol varchar(50);\n";

        final VoltCompiler compiler = compileAlteredSchema(simpleSchema1);
        verifyTableColumnType(compiler, "mytable", "newcol", VoltType.STRING);
        verifyTableColumnSize(compiler, "mytable", "newcol", 50);
    }

    public void testAlterTableWithProcedure() throws IOException {
        final String simpleSchema1 =
                "create table mytable (pkey integer, column2_integer integer);\n" +
                "alter table mytable add column newcol varchar(50);\n" +
                myproc;

        final VoltCompiler compiler = compileAlteredSchema(simpleSchema1);
        verifyTableColumnType(compiler, "mytable", "newcol", VoltType.STRING);
        verifyTableColumnSize(compiler, "mytable", "newcol", 50);
    }

    public void testAlterTableWithProcedureUsingWrongColumn() throws IOException {
        final String simpleSchema1 =
                "create table mytable (pkey integer, column2_integer integer);\n" +
                "alter table mytable drop column column2_integer;\n" +
                myproc;
        failToCompileAlteredSchema(simpleSchema1, "object not found: COLUMN2_INTEGER");
    }

    public void testAlterTableWithProcedureUsingDroppableColumn() throws IOException {
        final String simpleSchema1 =
                "create table mytable (pkey integer, column2_integer integer);\n" +
                "alter table mytable drop column pkey;\n" +
                myproc;
        final VoltCompiler compiler = compileAlteredSchema(simpleSchema1);
        verifyTableColumnGone(compiler, "mytable", "pkey");
    }

    public void testAlterTableOnView() throws IOException {
        final String baselineSchema =
                "create table mytable (pkey integer, column2_integer integer);\n" +
                "CREATE VIEW v_mytable (pkey, num_cont, col2_sum)" +
                "   AS SELECT pkey, COUNT(*), SUM(column2_integer)" +
                "      FROM mytable" +
                "      GROUP BY pkey" +
                ";\n";
        // Not allowing "alter table ... drop column" on view.
        // For now, every view column is considered a "needed" column.
        failToCompileAlteredSchema(baselineSchema +
                "alter table v_mytable drop column col2_sum;\n",
                "object not found: v_mytable");

        // Not allowing "alter table ... add column" on view.
        failToCompileAlteredSchema(baselineSchema +
                "alter table v_mytable add column dummy bigint;\n",
                "object not found: v_mytable");

        // Not allowing "alter table ... alter column" on view.
        failToCompileAlteredSchema(baselineSchema +
                "alter table v_mytable alter column col2_sum float;\n",
                "object not found: v_mytable");

        // Not allowing "alter table ... drop"
        // for non-existent column on view.
        failToCompileAlteredSchema(baselineSchema +
                "alter table v_mytable drop column column2_integer;\n",
                "object not found: v_mytable");

    }

    public void testAlterTableDropUnrelatedColumnWithView() throws IOException {
        final String simpleSchema1 =
                "create table mytable (pkey integer, column2_integer integer);\n" +
                "CREATE VIEW v_mytable(pkey, num_cont)" +
                "   AS SELECT pkey, COUNT(*)" +
                "      FROM mytable" +
                "      GROUP BY pkey" +
                ";\n" +
                "alter table mytable drop column column2_integer;\n";

        final VoltCompiler compiler = compileAlteredSchema(simpleSchema1);
        verifyTableColumnGone(compiler, "mytable", "column2_integer");
    }

    public void testAlterTableDropColumnWithView() throws IOException {
        final String simpleSchema1 =
                "create table mytable (pkey integer, column2_integer integer);\n" +
                "CREATE VIEW v_mytable (pkey, num_cont)" +
                "   AS SELECT pkey, COUNT(*)" +
                "      FROM mytable" +
                "      GROUP BY pkey" +
                ";\n" +
                "alter table mytable drop column pkey;\n";

        failToCompileAlteredSchema(simpleSchema1,
                "column is referenced in: PUBLIC.V_MYTABLE");
    }

    public void testAlterTableDropColumnWithViewCascade() throws IOException {
        final String simpleSchema1 =
                "create table mytable (pkey integer, column2_integer integer);\n" +
                "CREATE VIEW v_mytable(pkey, num_cont)" +
                "   AS SELECT pkey, COUNT(*)" +
                "      FROM mytable" +
                "      GROUP BY pkey" +
                ";\n" +
                "alter table mytable drop column pkey cascade;\n";

        final VoltCompiler compiler = compileAlteredSchema(simpleSchema1);
        verifyTableColumnGone(compiler, "mytable", "pkey");
    }

    public void testAlterTableAlterColumnWithCountView() throws IOException {
        final String simpleSchema1 =
                "create table mytable (pkey integer, column2_integer integer);\n" +
                "CREATE VIEW v_mytable(pkey, num_cont, countpkey)" +
                "   AS SELECT pkey, COUNT(*), COUNT(pkey)" +
                "      FROM mytable" +
                "      GROUP BY pkey" +
                ";\n" +
                "alter table mytable alter column pkey VARCHAR(20);\n";
        failToCompileAlteredSchema(simpleSchema1, "dependent objects exist");
    }

    public void testAlterTableAlterColumnWithSumViewToNonSummableType() throws IOException {
        final String simpleSchema1 =
                "create table mytable (pkey integer, column2_integer integer);\n" +
                "CREATE VIEW v_mytable(pkey, num_cont, sumpkey)" +
                "   AS SELECT pkey, COUNT(*), SUM(pkey)" +
                "      FROM mytable" +
                "      GROUP BY pkey" +
                ";\n" +
                "alter table mytable alter column pkey VARCHAR(20);\n";
        failToCompileAlteredSchema(simpleSchema1, "dependent objects exist");
    }

    /* hsql232 not yet working -- compile succeeds regardless of dependent view.
     * This reasonably may require a VoltDB extension since only VoltDB materializes views,
     * but did it have and lose one from the original VoltDB extensions?
    public void testAlterTableAlterColumnPossibleWithSumView() throws IOException {
        final String simpleSchema1 =
                "create table mytable (pkey integer, column2_integer integer);\n" +
                "CREATE VIEW v_mytable(pkey, num_cont, sumpkey)" +
                "   AS SELECT pkey, COUNT(*), SUM(pkey)" +
                "      FROM mytable" +
                "      GROUP BY pkey" +
                ";\n" +
                "alter table mytable alter column pkey smallint;\n";
        failToCompileAlteredSchema(simpleSchema1, "dependent objects exist");
    }
    // hsql232 not yet working */

    //Index
    public void testAlterTableDropColumnWithIndex() throws IOException {
        final String simpleSchema1 =
                "create table mytable (pkey integer, column2_integer integer);\n" +
                "create unique index pkey_idx on mytable(pkey);\n" +
                "alter table mytable drop column column2_integer;\n";
        final VoltCompiler compiler = compileAlteredSchema(simpleSchema1);
        verifyTableColumnGone(compiler, "mytable", "column2_integer");
        verifyIndexExists(compiler, "mytable", "pkey_idx");
    }

    public void testAlterTableDropColumnWithIndexRecreateColumn() throws IOException {
        final String simpleSchema1 =
                "create table mytable (pkey integer, column2_integer integer);\n" +
                "create unique index pkey_idx on mytable(pkey);\n" +
                "alter table mytable drop column pkey;\n" +
                "alter table mytable add column pkey integer;\n";
        final VoltCompiler compiler = compileAlteredSchema(simpleSchema1);
        verifyTableColumnExists(compiler, "mytable", "pkey");
        verifyIndexGone(compiler, "mytable", "pkey_idx");
    }

    public void testAlterTableDropRelatedColumnWithIndex() throws IOException {
        final String simpleSchema1 =
                "create table mytable (pkey integer, column2_integer integer);\n" +
                // This index should go:
                "create unique index pkey_idx on mytable(pkey, column2_integer);\n" +
                // This index should remain
                "create unique index pkey_idxx on mytable(column2_integer);\n" +
                "alter table mytable drop column pkey;\n";
        final VoltCompiler compiler = compileAlteredSchema(simpleSchema1);
        verifyTableColumnGone(compiler, "mytable", "pkey");
        verifyIndexExists(compiler, "mytable", "pkey_idxx");
        verifyIndexGone(compiler, "mytable", "pkey_idx");
    }

    public void testAlterTableZeroVarcharLength() throws IOException {
        final String simpleSchema1 =
                "create table mytable (pkey integer, column2_integer integer);\n" +
                "alter table mytable add column newcol varchar(0);\n";
        failToCompileAlteredSchema(simpleSchema1,
                "precision or scale out of range");
    }

    public void testAlterTableAddNonNullConstraintViaRedef() throws IOException {
        final String simpleSchema1 =
                "create table mytable (pkey varchar(50), column2_integer integer);\n" +
                "alter table mytable alter column pkey varchar(50) NOT NULL;\n";
        VoltCompiler compiler = compileAlteredSchema(simpleSchema1);
        // Check that constraint is now valid.
        /* // hsql232 not yet working -- even the basics
         * alter appears to succeed but has no effect?
        verifyTableColumnNullable(compiler, "mytable", "pkey", false);

        // Since we can add the constraint,
        // verify that it can be used for a partition key.
        compiler = compileAlteredSchema(simpleSchema1 +
                "PARTITION TABLE mytable ON COLUMN pkey;\n");
        // Check that constraint is valid.
        verifyTableColumnNullable(compiler, "mytable", "pkey", false);
        verifyTablePartitioningColumn(compiler, "mytable", "pkey");

        // Order of altering/partitioning should not matter?
        // That is, the partitioning should only depend on the FINAL state
        // of the column.
        compiler = compileAlteredSchema(
                "create table mytable (pkey varchar(50), column2_integer integer);\n" +
                "PARTITION TABLE mytable ON COLUMN pkey;\n" + // normally invalid
                // alter table to be retroactively valid for partitioning?
                "alter table mytable alter column pkey varchar(50) NOT NULL;\n");
        verifyTableColumnNullable(compiler, "mytable", "pkey", false);
        verifyTablePartitioningColumn(compiler, "mytable", "pkey");
        // hsql232 not yet working -- even the basics */
    }

    public void testAlterTableAddNonNullConstraint() throws IOException {
        final String simpleSchema1 =
                "create table mytable (pkey varchar(50), column2_integer integer);\n" +
                "alter table mytable alter column pkey set NOT NULL;\n";
        VoltCompiler compiler = compileAlteredSchema(simpleSchema1);
        // Check that constraint is now valid.
        verifyTableColumnNullable(compiler, "mytable", "pkey", false);

        // Since we can add the constraint,
        // verify that it can be used for a partition key.
        compiler = compileAlteredSchema(simpleSchema1 +
                "PARTITION TABLE mytable ON COLUMN pkey;\n");
        // Check that constraint is valid.
        verifyTableColumnNullable(compiler, "mytable", "pkey", false);
        verifyTablePartitioningColumn(compiler, "mytable", "pkey");

        // Order of altering/partitioning should not matter?
        // That is, the partitioning should only depend on the FINAL state
        // of the column.
        compiler = compileAlteredSchema(
                "create table mytable (pkey varchar(50), column2_integer integer);\n" +
                "PARTITION TABLE mytable ON COLUMN pkey;\n" + // normally invalid
                // alter table to be retroactively valid for partitioning?
                "alter table mytable alter column pkey set NOT NULL;\n");
        verifyTableColumnNullable(compiler, "mytable", "pkey", false);
        verifyTablePartitioningColumn(compiler, "mytable", "pkey");
    }

    public void testAlterTableRemoveNonNullConstraintViaRedef() throws IOException {
        final String simpleSchema1 =
                "create table mytable (pkey varchar(50) NOT NULL, column2_integer integer);\n" +
                "alter table mytable alter column pkey varchar(50);\n";
        final VoltCompiler compiler = compileAlteredSchema(simpleSchema1);
        // Check that constraint is still valid.
        /* // hsql232 not yet working -- even the basics
         * alter appears to succeed but has no effect?
        verifyTableColumnNullable(compiler, "mytable", "pkey", true);

        // Since we can normally drop the constraint,
        // verify the guard that keeps this operation from
        // corrupting a partition key (partition keys require NOT NULL).
        failToCompileAlteredSchema(simpleSchema1 +
                "PARTITION TABLE mytable ON COLUMN pkey;\n",
                "partition columns must be constrained \"NOT NULL\"");

        // Order of altering/partitioning should not matter.
        // That is, the partitioning should only depend on the FINAL state
        // of the column.
        failToCompileAlteredSchema(
                "create table mytable (pkey varchar(50) NOT NULL, column2_integer integer);\n" +
                "PARTITION TABLE mytable ON COLUMN pkey;\n" +
                "alter table mytable alter column pkey varchar(50);\n",
                "partition columns must be constrained \"NOT NULL\"");
        // hsql232 not yet working -- even the basics */
    }

    public void testAlterTableOverflowVarchar() throws IOException {
        final String simpleSchema1 =
                "create table mytable (pkey integer, column2_integer integer);\n" +
                "alter table mytable add column newcol varchar(" +
                String.valueOf(VoltType.MAX_VALUE_LENGTH + 1) + ");\n";
        failToCompileAlteredSchema(simpleSchema1, "> 1048576 char maximum.");
    }

    public void testAlterTableOverflowVarcharExisting() throws IOException {
        final String simpleSchema1 =
                "create table mytable (pkey integer, column2 varchar(50));\n" +
                "alter table mytable alter column column2 varchar(" +
                String.valueOf(VoltType.MAX_VALUE_LENGTH + 1) + ");\n";
        failToCompileAlteredSchema(simpleSchema1, "> 1048576 char maximum.");
    }

    public void testTooManyColumnTable() throws IOException {
        StringBuilder builder = new StringBuilder(
                "CREATE TABLE JUST_ENOUGH_WIDE_COLUMNS ( \n");
        for (int j = 1; j < 1024; ++j) {
            builder.append("   COL" + j + " INTEGER DEFAULT 0 NOT NULL, \n");
        }
        // Close out the table definition with the last of 1024 columns.
        builder.append("   COL1024 INTEGER DEFAULT 0 NOT NULL);\n");
        builder.append("alter table JUST_ENOUGH_WIDE_COLUMNS add column COL1025 INTEGER DEFAULT '0' NOT NULL;");
        String ddl = builder.toString();
        failToCompileAlteredSchema(ddl, "(max is 1024)");
    }

    public void testTooManyColumnThenDropBelowLimit() throws IOException {
        String schemaPath = "";
        URL url = TestVoltCompilerAlterDropTable.class.getResource("toowidetable-ddl.sql");
        schemaPath = URLDecoder.decode(url.getPath(), "UTF-8");
        FileReader fr = new FileReader(new File(schemaPath));
        BufferedReader br = new BufferedReader(fr);
        StringBuilder ddl1 = new StringBuilder("");
        String line;
        while ((line = br.readLine()) != null) {
            ddl1.append(line).append("\n");
        }
        br.close();
        ddl1.append("alter table MANY_COLUMNS drop column COL01022;");

        final String projectPath = getSimpleProjectPathForDDL(ddl1.toString());
        final VoltCompiler compiler = new VoltCompiler();
        final boolean success = compiler.compileWithProjectXML(projectPath, testout_jar);
        assertTrue(success);
        verifyTableColumnGone(compiler, "MANY_COLUMNS", "COL01022");
    }

    public void testAlterTableRowLimit() throws IOException {
        //Just enough big row one byte over and we will fail to compile.
        String simpleSchema1 = "create table mytable (val1 varchar(1048572), val2 varchar(1048572));\n";
        String projectPath = getSimpleProjectPathForDDL(simpleSchema1);
        VoltCompiler compiler = new VoltCompiler();
        boolean success = compiler.compileWithProjectXML(projectPath, testout_jar);
        assertTrue(success);

        //Still over after alter
        simpleSchema1 = "create table mytable (val1 varchar(1048576), val2 varchar(1048576));\n" +
                "alter table mytable alter column val1 varchar(1048572);\n";
        projectPath = getSimpleProjectPathForDDL(simpleSchema1);
        compiler = new VoltCompiler();
        success = compiler.compileWithProjectXML(projectPath, testout_jar);
        assertFalse(success);

        //Going under after alter
        simpleSchema1 = "create table mytable (val1 varchar(1048576), val2 varchar(1048576));\n" +
                "alter table mytable alter column val1 varchar(1048572);\n" +
                "alter table mytable alter column val2 varchar(1048572);\n";
        projectPath = getSimpleProjectPathForDDL(simpleSchema1);
        compiler = new VoltCompiler();
        success = compiler.compileWithProjectXML(projectPath, testout_jar);
        assertTrue(success);

        //Going over: after alter going under and back over
        simpleSchema1 = "create table mytable (val1 varchar(1048576), val2 varchar(1048576));\n" +
                "alter table mytable alter column val1 varchar(1048572);\n" +
                "alter table mytable alter column val2 varchar(1048572);\n" +
                "alter table mytable alter column val1 varchar(1048576);\n";
        projectPath = getSimpleProjectPathForDDL(simpleSchema1);
        compiler = new VoltCompiler();
        success = compiler.compileWithProjectXML(projectPath, testout_jar);
        assertFalse(success);
    }

    //We will be successful in compiling but a WARN should appear.
    public void testAlterTableAddForeignKey() throws IOException {
        final String simpleSchema1 =
                "create table mytable (pkey integer, column2_integer integer);\n" +
                "create table ftable (pkey integer, column2_integer integer UNIQUE);\n" +
                "ALTER TABLE mytable ADD FOREIGN KEY (column2_integer) REFERENCES ftable(column2_integer);\n";

        final VoltCompiler compiler = compileAlteredSchema(simpleSchema1);
        verifyTableColumnType(compiler, "mytable", "pkey", VoltType.INTEGER);
        verifyTableColumnType(compiler, "ftable", "pkey", VoltType.INTEGER);

        // verify that warnings exist for foreign key
        int foundFKWarnings = 0;
        for (VoltCompiler.Feedback f : compiler.m_warnings) {
            if (f.message.toLowerCase().contains("foreign")) {
                foundFKWarnings++;
            }
        }
        assertEquals(1, foundFKWarnings);
    }

    public void testAlterUnknownTable() throws IOException {
        final String simpleSchema1 =
                "create table mytable (pkey integer, column2_integer integer);\n" +
                "alter table mytablenonexistent add column newcol varchar(50);\n";
        failToCompileAlteredSchema(simpleSchema1,
                "object not found: mytablenonexistent");
    }

    public void testCreateDropAlterTable() throws IOException {
        final String simpleSchema1 =
                "create table mytable (pkey integer, column2_integer integer);\n" +
                "drop table mytable;\n" +
                "alter table mytable add column newcol varchar(50);\n";
        failToCompileAlteredSchema(simpleSchema1, "object not found: mytable");
    }

    public void testCreateDropCreateAlterTable() throws IOException {
        final String simpleSchema1 =
                "create table mytable (pkey integer, column2_integer integer);\n" +
                "drop table mytable;\n" +
                "create table mytable (pkey integer, column2_integer integer);\n" +
                "alter table mytable add column newcol varchar(50);\n";
        final VoltCompiler compiler = compileAlteredSchema(simpleSchema1);
        verifyTableColumnType(compiler, "mytable", "pkey", VoltType.INTEGER);
    }

    public void testAlterTableBadDowngradeOfPartitionColumn() throws IOException {
        final String simpleSchema1 =
                "create table mytable (pkey integer NOT NULL, column2_integer integer);\n" +
                "PARTITION TABLE mytable ON COLUMN pkey;\n" +
                "alter table mytable alter column pkey NULL;\n";

        failToCompileAlteredSchema(simpleSchema1,
                "type not found or user lacks privilege:");
    }

    public void testAlterTableDropOfPartitionColumn() throws IOException {
        final String simpleSchema1 =
                "create table mytable (pkey integer NOT NULL, column2_integer integer);\n" +
                "PARTITION TABLE mytable ON COLUMN pkey;\n" +
                "alter table mytable drop column pkey;\n";

        final VoltCompiler compiler = compileAlteredSchema(simpleSchema1);
        verifyTableColumnGone(compiler, "mytable", "pkey");
        verifyTableNotPartitioned(compiler, "mytable");
    }

    public void testAlterTableSizeChangeAndConstraintChangeOfPartitionColumn() throws IOException {
        final String simpleSchema1 =
                "create table mytable (pkey varchar(20) NOT NULL, column2_integer integer);\n" +
                "PARTITION TABLE mytable ON COLUMN pkey;\n" +
                "alter table mytable alter column pkey varchar(50);\n";

        failToCompileAlteredSchema(simpleSchema1,
                "Partition columns must be constrained \"NOT NULL\"");
    }

    public void testAlterTableSizeChangeAndKeepConstraintOfPartitionColumn() throws IOException {
        final String simpleSchema1 =
                "create table mytable (pkey varchar(20) NOT NULL, column2_integer integer);\n" +
                "PARTITION TABLE mytable ON COLUMN pkey;\n" +
                "alter table mytable alter column pkey varchar(50) NOT NULL;\n";

        final VoltCompiler compiler = compileAlteredSchema(simpleSchema1);
        // Check that constraint is still valid.
        verifyTableColumnNullable(compiler, "mytable", "pkey", false);
    }

    public void testAlterTableSizeChangeAndImplicitlyKeepConstraintOfPartitionColumn() throws IOException {
        final String simpleSchema1 =
                "create table mytable (pkey varchar(20) NOT NULL, column2_integer integer);\n" +
                "PARTITION TABLE mytable ON COLUMN pkey;\n" +
                "alter table mytable alter column pkey set data type varchar(50);\n";

        final VoltCompiler compiler = compileAlteredSchema(simpleSchema1);
        // Check that constraint is still valid.
        verifyTableColumnNullable(compiler, "mytable", "pkey", false);
    }

    public void testAlterTableAddPartitionColumn() throws IOException {
        final String simpleSchema1 =
                "create table mytable (pkey varchar(20) NOT NULL, column2_integer integer);\n" +
                "alter table mytable add column pkey2 varchar(500);\n" +
                "PARTITION TABLE mytable ON COLUMN pkey2;\n";

        failToCompileAlteredSchema(simpleSchema1,
                "Partition columns must be constrained");
    }

    //This is create procedure on valid column add after alter...should be successful.
    public void testAlterTableAddNonNULLPartitionColumn() throws IOException {
        final String simpleSchema1 =
                "create table mytable (pkey varchar(20) NOT NULL, column2_integer integer);\n" +
                "alter table mytable add column pkey2 varchar(500) NOT NULL;\n" +
                "PARTITION TABLE mytable ON COLUMN pkey2;\n";

        final VoltCompiler compiler = compileAlteredSchema(simpleSchema1);
        verifyTableColumnSize(compiler, "mytable", "pkey2", 500);
        // Check that constraint is valid.
        verifyTableColumnNullable(compiler, "mytable", "pkey2", false);
    }

    public void testAlterTableRedundantlySetColNonNULLThenPartitionOnIt() throws IOException {
        final String simpleSchema1 =
                "create table mytable (pkey varchar(20) NOT NULL, column2_integer integer);\n" +
                "alter table mytable alter column pkey set NOT NULL;\n" +
                "PARTITION TABLE mytable ON COLUMN pkey;\n";

        final VoltCompiler compiler = compileAlteredSchema(simpleSchema1);
        // Check that constraint is valid.
        verifyTableColumnNullable(compiler, "mytable", "pkey", false);
    }

    public void testAlterTableFailToAddDuplicateColumnName() throws IOException {
        final String simpleSchema1 =
                "create table mytable (pkey varchar(20) NOT NULL, column2_integer integer);\n" +
                "alter table mytable add column pkey varchar(20);\n";
        failToCompileAlteredSchema(simpleSchema1,
                "name already exists ");
    }

    //DROP TABLE TESTS from here
    public void testDropTable() throws IOException {
        final String simpleSchema1 =
                "create table mytable (pkey integer, column2_integer integer);\n" +
                "drop table mytable;\n";
        final VoltCompiler compiler = compileAlteredSchema(simpleSchema1);
        verifyTableGone(compiler, "mytable");
    }

    public void testDropTableThatDoesNotExist() throws IOException {
        final String simpleSchema1 =
                "create table mytable (pkey integer, column2_integer integer);\n" +
                "drop table mytablenonexistant;\n";
        failToCompileAlteredSchema(simpleSchema1,
                "object not found: mytablenonexistant");
        // The same operation succeeds with "if exists" qualifier.
        compileAlteredSchema(
                "create table mytable (pkey integer, column2_integer integer);\n" +
                "drop table mytablenonexistant if exists;\n");
    }

    public void testDropTableWithIndex() throws IOException {
        final String simpleSchema1 =
                "create table mytable (pkey integer, column2_integer integer);\n" +
                "create unique index pkey_idx on mytable(pkey);\n" +
                "drop table mytable;\n";
        final VoltCompiler compiler = compileAlteredSchema(simpleSchema1);
        verifyTableGone(compiler, "mytable");
        verifyIndexGone(compiler, "mytable", "pkey_idx");
    }

    public void testDropTableWithIndexAndReCreateTable() throws IOException {
        final String simpleSchema1 =
                "create table mytable (pkey integer, column2_integer integer);\n" +
                "create unique index pkey_idx on mytable(pkey);\n" +
                "drop table mytable;\n" +
                "create table mytable (pkey integer, column2_integer integer);\n";
        final VoltCompiler compiler = compileAlteredSchema(simpleSchema1);
        verifyTableExists(compiler, "mytable");
        verifyIndexGone(compiler, "mytable", "pkey_idx");
    }

    //Should fail as table should not exist
    public void testDropTableWithProcedure() throws IOException {
        final String simpleSchema1 =
                "create table mytable (pkey integer, column2_integer integer);\n" +
                "drop table mytable;\n" +
                myproc;
        // dropping the table would strand the proc -- this is not allowed.
        failToCompileAlteredSchema(simpleSchema1, "object not found: mytable");
    }
}
