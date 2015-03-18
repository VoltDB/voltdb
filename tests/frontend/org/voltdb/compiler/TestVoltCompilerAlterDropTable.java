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
import org.voltdb.utils.MiscUtils;

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

    //Check given col type if catalog was compiled.
    private void verifyTableColumnType(VoltCompiler compiler, String tableName, String colName, VoltType type) {
        Database db = compiler.m_catalog.getClusters().get("cluster").getDatabases().get("database");
        Table table = db.getTables().get(tableName);
        Column col = table.getColumns().get(colName);
        assertNotNull(col);
        assertEquals(0, type.compareTo(VoltType.get((byte) col.getType())));
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

    private VoltCompiler compileSimpleDdl(final String simpleSchema) {
        final String schemaPath = MiscUtils.writeStringToTempFilePath(simpleSchema);
        final VoltCompiler compiler = new VoltCompiler();
        final boolean success = compiler.compileFromDDL(testout_jar, schemaPath);
        assertTrue(success);
        return compiler;
    }

    //TDOD: deprecate this call after strengthening all callers to use something stronger that
    // pattern matches expected error message text -- consider delegating to TestVoltCompiler methods.
    private void failToCompileSimpleDdl(final String simpleSchema) {
        final String schemaPath = MiscUtils.writeStringToTempFilePath(simpleSchema);
        final VoltCompiler compiler = new VoltCompiler();
        final boolean success = compiler.compileFromDDL(testout_jar, schemaPath);
        assertFalse(success);
    }

    public void testAlterTable() throws IOException {
        final String simpleSchema =
                "create table mytable  (pkey integer, column2_integer integer);\n" +
                "alter table mytable add column newcol varchar(50);\n" +
                "";
        final VoltCompiler compiler = compileSimpleDdl(simpleSchema);
        verifyTableColumnType(compiler, "mytable", "newcol", VoltType.STRING);
        verifyTableColumnSize(compiler, "mytable", "newcol", 50);
    }

    public void testAlterTableWithProcedure() throws IOException {
        final String simpleSchema =
                "create table mytable  (pkey integer, column2_integer integer);\n" +
                "alter table mytable add column newcol varchar(50);\n"
                + myproc;

        final VoltCompiler compiler = compileSimpleDdl(simpleSchema);
        verifyTableColumnType(compiler, "mytable", "newcol", VoltType.STRING);
        verifyTableColumnSize(compiler, "mytable", "newcol", 50);
    }

    public void testAlterTableWithProcedureUsingWrongColumn() throws IOException {
        TestVoltCompiler.checkDDLErrorMessage(testout_jar,
                "create table mytable  (pkey integer, column2_integer integer);\n" +
                "alter table mytable drop column column2_integer;\n" +
                myproc
                , "Failed to plan for statement");
    }

    public void testAlterTableWithProcedureUsingDroppableColumn() throws IOException {
        final String simpleSchema =
                "create table mytable  (pkey integer, column2_integer integer);\n" +
                "alter table mytable drop column pkey;\n"
                + myproc;

        final VoltCompiler compiler = compileSimpleDdl(simpleSchema);
        verifyTableColumnGone(compiler, "mytable", "pkey");
    }

    public void testAlterTableOnView() throws IOException {
        TestVoltCompiler.checkDDLErrorMessage(testout_jar,
                "create table mytable  (pkey integer, column2_integer integer);\n" +
                "CREATE VIEW v_mytable\n" +
                "(\n" +
                "  pkey\n" +
                ", num_cont\n" +
                ")\n" +
                "AS\n" +
                "   SELECT pkey\n" +
                "        , COUNT(*)\n" +
                "     FROM mytable\n" +
                " GROUP BY pkey\n" +
                ";" +
                "alter table v_mytable drop column column2_integer;\n"
                , "user lacks privilege or object not found: V_MYTABLE");
    }

    public void testAlterTableDropUnrelatedColumnWithView() throws IOException {
        final String simpleSchema =
                "create table mytable  (pkey integer, column2_integer integer);\n" +
                "CREATE VIEW v_mytable\n" +
                "(\n" +
                "  pkey\n" +
                ", num_cont\n" +
                ")\n" +
                "AS\n" +
                "   SELECT pkey\n" +
                "        , COUNT(*)\n" +
                "     FROM mytable\n" +
                " GROUP BY pkey\n" +
                ";" +
                "alter table mytable drop column column2_integer;\n" +
                "";
        final VoltCompiler compiler = compileSimpleDdl(simpleSchema);
        verifyTableColumnGone(compiler, "mytable", "column2_integer");
    }

    public void testAlterTableDropColumnWithView() throws IOException {
        TestVoltCompiler.checkDDLErrorMessage(testout_jar,
                "create table mytable  (pkey integer, column2_integer integer);\n" +
                "CREATE VIEW v_mytable\n" +
                "(\n" +
                "  pkey\n" +
                ", num_cont\n" +
                ")\n" +
                "AS\n" +
                "   SELECT pkey\n" +
                "        , COUNT(*)\n" +
                "     FROM mytable\n" +
                " GROUP BY pkey\n" +
                ";" +
                "alter table mytable drop column pkey;\n"
                , "column is referenced in: PUBLIC.V_MYTABLE");
    }

    public void testAlterTableDropColumnWithViewCascade() throws IOException {
        final String simpleSchema =
                "create table mytable  (pkey integer, column2_integer integer);\n" +
                "CREATE VIEW v_mytable\n" +
                "(\n" +
                "  pkey\n" +
                ", num_cont\n" +
                ")\n" +
                "AS\n" +
                "   SELECT pkey\n" +
                "        , COUNT(*)\n" +
                "     FROM mytable\n" +
                " GROUP BY pkey\n" +
                ";" +
                "alter table mytable drop column pkey cascade;\n" +
                "";
        final VoltCompiler compiler = compileSimpleDdl(simpleSchema);
        verifyTableColumnGone(compiler, "mytable", "pkey");
    }

    public void testAlterTableAlterColumnWithCountView() throws IOException {
        TestVoltCompiler.checkDDLErrorMessage(testout_jar,
                "create table mytable  (pkey integer, column2_integer integer);\n" +
                "CREATE VIEW v_mytable\n" +
                "(\n" +
                "  pkey\n" +
                ", num_cont\n" +
                ")\n" +
                "AS\n" +
                "   SELECT pkey\n" +
                "        , COUNT(pkey)\n" +
                "     FROM mytable\n" +
                " GROUP BY pkey\n" +
                ";" +
                "alter table mytable alter column pkey VARCHAR(20);\n"
                , "dependent objects exist");
    }

    public void testAlterTableAlterColumnWithSumViewToNonSummableType() throws IOException {
        TestVoltCompiler.checkDDLErrorMessage(testout_jar,
                "create table mytable  (pkey integer, column2_integer integer);\n" +
                "CREATE VIEW v_mytable\n" +
                "(\n" +
                "  pkey\n" +
                ", num_cont\n" +
                ")\n" +
                "AS\n" +
                "   SELECT pkey\n" +
                "        , SUM(pkey)\n" +
                "     FROM mytable\n" +
                " GROUP BY pkey\n" +
                ";" +
                "alter table mytable alter column pkey VARCHAR(20);\n"
                , "dependent objects exist");
    }

    public void testAlterTableAlterColumnPossibleWithSumView() throws IOException {
        TestVoltCompiler.checkDDLErrorMessage(testout_jar,
                "create table mytable  (pkey integer, column2_integer integer);\n" +
                "CREATE VIEW v_mytable\n" +
                "(\n" +
                "  pkey\n" +
                ", num_cont\n" +
                ")\n" +
                "AS\n" +
                "   SELECT pkey\n" +
                "        , SUM(pkey)\n" +
                "     FROM mytable\n" +
                " GROUP BY pkey\n" +
                ";" +
                "alter table mytable alter column pkey smallint;\n"
                , "dependent objects exist");
    }

    //Index
    public void testAlterTableDropColumnWithIndex() throws IOException {
        final String simpleSchema =
                "create table mytable (pkey integer, column2_integer integer);\n" +
                "create unique index pkey_idx on mytable(pkey);\n" +
                "alter table mytable drop column column2_integer;\n" +
                "";
        final VoltCompiler compiler = compileSimpleDdl(simpleSchema);
        verifyTableColumnGone(compiler, "mytable", "column2_integer");
        verifyIndexExists(compiler, "mytable", "pkey_idx");
    }

    public void testAlterTableDropColumnWithIndexRecreateColumn() throws IOException {
        final String simpleSchema =
                "create table mytable (pkey integer, column2_integer integer);\n" +
                "create unique index pkey_idx on mytable(pkey);\n" +
                "alter table mytable drop column pkey;\n" +
                "alter table mytable add column pkey integer;\n" +
                "";
        final VoltCompiler compiler = compileSimpleDdl(simpleSchema);
        verifyTableColumnExists(compiler, "mytable", "pkey");
        verifyIndexGone(compiler, "mytable", "pkey_idx");
    }

    public void testAlterTableDropRelatedColumnWithIndex() throws IOException {
        final String simpleSchema =
                "create table mytable  (pkey integer, column2_integer integer);\n" +
                "create unique index pkey_idx on mytable(pkey, column2_integer);\n" + // This index should go +
                "create unique index pkey_idxx on mytable(column2_integer);\n" + //This index should remain +
                "alter table mytable drop column pkey;\n" +
                "";
        final VoltCompiler compiler = compileSimpleDdl(simpleSchema);
        verifyTableColumnGone(compiler, "mytable", "pkey");
        verifyIndexExists(compiler, "mytable", "pkey_idxx");
        verifyIndexGone(compiler, "mytable", "pkey_idx");
    }

    public void testAlterTableBadVarchar() throws IOException {
        TestVoltCompiler.checkDDLErrorMessage(testout_jar,
                "create table mytable  (pkey integer, column2_integer integer);\n" +
                "alter table mytable add column newcol varchar(0);\n"
                , "precision or scale out of range");
    }

    public void testAlterTableAddConstraint() throws IOException {
        final String simpleSchema =
                "create table mytable  (pkey varchar(50), column2_integer integer);\n" +
                "alter table mytable alter column pkey varchar(50) NOT NULL;\n" +
                "";
        final VoltCompiler compiler = compileSimpleDdl(simpleSchema);
        // Check that constraint is now valid.
        verifyTableColumnNullable(compiler, "mytable", "pkey", false);
    }

    public void testAlterTableRemoveConstraint() throws IOException {
        final String simpleSchema =
                "create table mytable  (pkey varchar(50) NOT NULL, column2_integer integer);\n" +
                "alter table mytable alter column pkey varchar(50) NOT NULL;\n" +
                "";
        final VoltCompiler compiler = compileSimpleDdl(simpleSchema);
        // Check that constraint is still valid.
        verifyTableColumnNullable(compiler, "mytable", "pkey", false);
    }

    public void testAlterTableOverflowVarchar() throws IOException {
        TestVoltCompiler.checkDDLErrorMessage(testout_jar,
                "create table mytable  (pkey integer, column2_integer integer);\n" +
                "alter table mytable add column newcol varchar(" +
                String.valueOf(VoltType.MAX_VALUE_LENGTH + 1) + ");\n"
                , "> 1048576 char maximum.");
    }

    public void testAlterTableOverflowVarcharExisting() throws IOException {
        TestVoltCompiler.checkDDLErrorMessage(testout_jar,
                "create table mytable  (pkey integer, column2 varchar(50));\n" +
                "alter table mytable alter column column2 varchar(" +
                String.valueOf(VoltType.MAX_VALUE_LENGTH + 1) + ");\n"
                , "> 1048576 char maximum.");
    }

    public void testTooManyColumnTable() throws IOException {
        String schemaPath = "";
        URL url = TestVoltCompilerAlterDropTable.class.getResource("justwidetable-ddl.sql");
        schemaPath = URLDecoder.decode(url.getPath(), "UTF-8");
        FileReader fr = new FileReader(new File(schemaPath));
        BufferedReader br = new BufferedReader(fr);
        StringBuilder ddl1 = new StringBuilder("");
        String line;
        while ((line = br.readLine()) != null) {
            ddl1.append(line).append("\n");
        }
        br.close();
        ddl1.append("alter table JUST_ENOUGH_WIDE_COLUMNS add column COL01022 INTEGER DEFAULT '0' NOT NULL;");
        TestVoltCompiler.checkDDLErrorMessage(testout_jar, ddl1.toString(), "(max is 1024)");
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

        final VoltCompiler compiler = compileSimpleDdl(ddl1.toString());
        verifyTableColumnGone(compiler, "MANY_COLUMNS", "COL01022");
    }

    public void testAlterTableRowLimit() throws IOException {
        //Just enough big row one byte over and we will fail to compile.
        String simpleSchema = "create table mytable (val1 varchar(1048572), val2 varchar(1048572));\n";
        compileSimpleDdl(simpleSchema);

        //Still over after alter
        simpleSchema = "create table mytable (val1 varchar(1048576), val2 varchar(1048576));" +
                "alter table mytable alter column val1 varchar(1048572);\n";
        failToCompileSimpleDdl(simpleSchema);

        //Going under after alter
        simpleSchema = "create table mytable (val1 varchar(1048576), val2 varchar(1048576));" +
                "alter table mytable alter column val1 varchar(1048572);\n" +
                "alter table mytable alter column val2 varchar(1048572);\n";
        compileSimpleDdl(simpleSchema);

        //Going over: after alter going under and back over
        simpleSchema = "create table mytable (val1 varchar(1048576), val2 varchar(1048576));" +
                "alter table mytable alter column val1 varchar(1048572);\n" +
                "alter table mytable alter column val2 varchar(1048572);\n" +
                "alter table mytable alter column val1 varchar(1048576);\n";
        failToCompileSimpleDdl(simpleSchema);
    }

    //We will be successful in compiling but a WARN should appear.
    public void testAlterTableAddForeignKey() throws IOException {
        // Special case looking for successful compile with a warning message.
        final VoltCompiler compiler = TestVoltCompiler.checkDDLWarningMessage(testout_jar,
                "create table mytable  (pkey integer, column2_integer integer);\n" +
                "create table ftable  (pkey integer, column2_integer integer UNIQUE);\n" +
                "ALTER TABLE mytable ADD FOREIGN KEY (column2_integer) REFERENCES ftable(column2_integer);\n"
                , "foreign");
        verifyTableColumnType(compiler, "mytable", "pkey", VoltType.INTEGER);
        verifyTableColumnType(compiler, "ftable", "pkey", VoltType.INTEGER);
    }

    public void testAlterUnknownTable() throws IOException {
        TestVoltCompiler.checkDDLErrorMessage(testout_jar,
                "create table mytable  (pkey integer, column2_integer integer);\n" +
                "alter table mytablenonexistent add column newcol varchar(50);\n"
                , "user lacks privilege or object not found:");
    }

    public void testCreateDropAlterTable() throws IOException {
        TestVoltCompiler.checkDDLErrorMessage(testout_jar,
                "create table mytable  (pkey integer, column2_integer integer);\n" +
                "drop table mytable;\n" +
                "alter table mytable add column newcol varchar(50);\n"
                , "user lacks privilege or object not found:");
    }

    public void testCreateDropCreateAlterTable() throws IOException {
        final String simpleSchema =
                "create table mytable  (pkey integer, column2_integer integer);\n" +
                "drop table mytable;\n" +
                "create table mytable  (pkey integer, column2_integer integer);\n" +
                "alter table mytable add column newcol varchar(50);\n" +
                "";
        final VoltCompiler compiler = compileSimpleDdl(simpleSchema);
        verifyTableColumnType(compiler, "mytable", "pkey", VoltType.INTEGER);
    }

    public void testAlterTableBadDowngradeOfPartitionColumn() throws IOException {
        TestVoltCompiler.checkDDLErrorMessage(testout_jar,
                "create table mytable  (pkey integer NOT NULL, column2_integer integer);\n" +
                "PARTITION TABLE mytable ON COLUMN pkey;" +
                "alter table mytable alter column pkey NULL;\n"
                , "type not found or user lacks privilege:");
    }

    public void testAlterTableDropOfPartitionColumn() throws IOException {
        final String simpleSchema =
                "create table mytable  (pkey integer NOT NULL, column2_integer integer);\n" +
                "PARTITION TABLE mytable ON COLUMN pkey;" +
                "alter table mytable drop column pkey;\n" +
                "";
        compileSimpleDdl(simpleSchema);
    }

    public void testAlterTableSizeChangeAndConstraintChangeOfPartitionColumn() throws IOException {
        TestVoltCompiler.checkDDLErrorMessage(testout_jar,
                "create table mytable  (pkey varchar(20) NOT NULL, column2_integer integer);\n" +
                "PARTITION TABLE mytable ON COLUMN pkey;" +
                "alter table mytable alter column pkey varchar(50);\n"
                , "Partition columns must be constrained \"NOT NULL\"");
    }

    public void testAlterTableSizeChangeAndKeepConstraintOfPartitionColumn() throws IOException {
        final String simpleSchema =
                "create table mytable  (pkey varchar(20) NOT NULL, column2_integer integer);\n" +
                "PARTITION TABLE mytable ON COLUMN pkey;" +
                "alter table mytable alter column pkey varchar(50) NOT NULL;\n" +
                "";
        final VoltCompiler compiler = compileSimpleDdl(simpleSchema);
        // Check that constraint is still valid.
        verifyTableColumnNullable(compiler, "mytable", "pkey", false);
    }

    public void testAlterTableSizeChangeAndImplicitlyKeepConstraintOfPartitionColumn() throws IOException {
        final String simpleSchema =
                "create table mytable  (pkey varchar(20) NOT NULL, column2_integer integer);\n" +
                "PARTITION TABLE mytable ON COLUMN pkey;" +
                "alter table mytable alter column pkey set data type varchar(50);\n" +
                "";
        final VoltCompiler compiler = compileSimpleDdl(simpleSchema);
        // Check that constraint is still valid.
        verifyTableColumnNullable(compiler, "mytable", "pkey", false);
    }

    public void testAlterTableAddPartitionColumn() throws IOException {
        TestVoltCompiler.checkDDLErrorMessage(testout_jar,
                "create table mytable  (pkey varchar(20) NOT NULL, column2_integer integer);\n" +
                "alter table mytable add column pkey2 varchar(500);\n" +
                "PARTITION TABLE mytable ON COLUMN pkey2;\n"
                , "Partition columns must be constrained");
    }

    //This is create procedure on valid column add after alter...should be successful.
    public void testAlterTableAddNonNULLPartitionColumn() throws IOException {
        final String simpleSchema =
                "create table mytable  (pkey varchar(20) NOT NULL, column2_integer integer);\n" +
                "alter table mytable add column pkey2 varchar(500) NOT NULL;\n" +
                "PARTITION TABLE mytable ON COLUMN pkey2;\n" +
                "";
        final VoltCompiler compiler = compileSimpleDdl(simpleSchema);
        verifyTableColumnSize(compiler, "mytable", "pkey2", 500);
        // Check that constraint is valid.
        verifyTableColumnNullable(compiler, "mytable", "pkey2", false);
    }

    public void testAlterTableRedefColWithNonNULLThenPartitionOnIt() throws IOException {
        final String simpleSchema =
                "create table mytable  (pkey varchar(20), column2_integer integer);\n" +
                "alter table mytable alter column pkey varchar(20) NOT NULL;\n" +
                "PARTITION TABLE mytable ON COLUMN pkey;\n" +
                "";
        final VoltCompiler compiler = compileSimpleDdl(simpleSchema);
        // Check that constraint is valid.
        verifyTableColumnNullable(compiler, "mytable", "pkey", false);
    }

    public void testAlterTableSetColNonNULLThenPartitionOnIt() throws IOException {
        final String simpleSchema =
                "create table mytable  (pkey varchar(20), column2_integer integer);\n" +
                "alter table mytable alter column pkey set NOT NULL;\n" +
                "PARTITION TABLE mytable ON COLUMN pkey;\n" +
                "";
        final VoltCompiler compiler = compileSimpleDdl(simpleSchema);
        // Check that constraint is valid.
        verifyTableColumnNullable(compiler, "mytable", "pkey", false);
    }

    public void testAlterTableRedundantlySetColNonNULLThenPartitionOnIt() throws IOException {
        final String simpleSchema =
                "create table mytable  (pkey varchar(20) NOT NULL, column2_integer integer);\n" +
                "alter table mytable alter column pkey set NOT NULL;\n" +
                "PARTITION TABLE mytable ON COLUMN pkey;\n" +
                "";
        final VoltCompiler compiler = compileSimpleDdl(simpleSchema);
        // Check that constraint is valid.
        verifyTableColumnNullable(compiler, "mytable", "pkey", false);
    }

    public void testAlterTableRedefColToDropNonNULLThenFailToPartitionOnIt() throws IOException {
        TestVoltCompiler.checkDDLErrorMessage(testout_jar,
                "create table mytable  (pkey varchar(20) NOT NULL, column2_integer integer);\n" +
                "alter table mytable alter column pkey varchar(20);\n" +
                "PARTITION TABLE mytable ON COLUMN pkey;\n"
                , "Partition columns must be constrained ");
    }

    public void testAlterTableFailToAddDuplicateColumnName() throws IOException {
        TestVoltCompiler.checkDDLErrorMessage(testout_jar,
                "create table mytable  (pkey varchar(20) NOT NULL, column2_integer integer);\n" +
                "alter table mytable add column pkey varchar(20);\n"
                , "name already exists ");
    }

    //DROP TABLE TESTS from here
    public void testDropTable() throws IOException {
        final String simpleSchema =
                "create table mytable  (pkey integer, column2_integer integer);\n" +
                "drop table mytable;\n" +
                "";
        final VoltCompiler compiler = compileSimpleDdl(simpleSchema);
        verifyTableGone(compiler, "mytable");
    }

    public void testDropTableThatDoesNotExists() throws IOException {
        final String simpleSchema =
                "create table mytable  (pkey integer, column2_integer integer);\n" +
                "drop table mytablefoo;\n";
        failToCompileSimpleDdl(simpleSchema);
    }

    public void testDropTableIfExists() throws IOException {
        final String simpleSchema =
                "create table mytable  (pkey integer, column2_integer integer);\n" +
                "drop table mytablenonexistant if exists;\n" +
                "";
        final VoltCompiler compiler = compileSimpleDdl(simpleSchema);
        verifyTableColumnType(compiler, "mytable", "pkey", VoltType.INTEGER);
    }

    public void testDropTableWithIndex() throws IOException {
        final String simpleSchema =
                "create table mytable  (pkey integer, column2_integer integer);\n" +
                "create unique index pkey_idx on mytable(pkey);\n" +
                "drop table mytable;\n";
        final VoltCompiler compiler = compileSimpleDdl(simpleSchema);
        verifyTableGone(compiler, "mytable");
        verifyIndexGone(compiler, "mytable", "pkey_idx");
    }

    public void testDropTableWithIndexAndReCreateTable() throws IOException {
        final String simpleSchema =
                "create table mytable  (pkey integer, column2_integer integer);\n" +
                "create unique index pkey_idx on mytable(pkey);\n" +
                "drop table mytable;\n" +
                "create table mytable  (pkey integer, column2_integer integer);\n";
        final VoltCompiler compiler = compileSimpleDdl(simpleSchema);
        verifyTableExists(compiler, "mytable");
        verifyIndexGone(compiler, "mytable", "pkey_idx");
    }

    //Should fail as the table underlying the new procedure should not exist
    public void testDropTableWithProcedure() throws IOException {
        final String simpleSchema =
                "create table mytable  (pkey integer, column2_integer integer);\n" +
                "drop table mytable;\n" +
                myproc;
        failToCompileSimpleDdl(simpleSchema);
    }
}
