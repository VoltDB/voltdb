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

package org.voltdb.compiler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import static junit.framework.Assert.assertTrue;

import junit.framework.TestCase;

import org.hsqldb_voltpatches.HSQLInterface;
import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
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
    public String getSimpleProjectPathForDDL(String ddl) {
        final File schemaFile = VoltProjectBuilder.writeStringToTempFile(ddl);
        final String schemaPath = schemaFile.getPath();

        final String simpleProject
                = "<?xml version=\"1.0\"?>\n"
                + "<project>"
                + "<database name='database'>"
                + "<schemas>"
                + "<schema path='" + schemaPath + "' />"
                + "</schemas>"
                + "<procedures/>"
                + "</database>"
                + "</project>";

        final File projectFile = VoltProjectBuilder.writeStringToTempFile(simpleProject);
        final String projectPath = projectFile.getPath();
        return projectPath;
    }

    //Check given col type if catalog was compiled.
    public void verifyTableColumnTypes(VoltCompiler compiler, String tableName, String colName, VoltType type) {
        Database db = compiler.m_catalog.getClusters().get("cluster").getDatabases().get("database");
        Table table = db.getTables().get(tableName);
        Column col = table.getColumns().get(colName);
        assertEquals(0, type.compareTo(VoltType.get((byte) col.getType())));
    }

    //Check given col size
    public void verifyTableColumnSize(VoltCompiler compiler, String tableName, String colName, int sz) {
        Database db = compiler.m_catalog.getClusters().get("cluster").getDatabases().get("database");
        Table table = db.getTables().get(tableName);
        Column col = table.getColumns().get(colName);
        assertEquals(col.getSize(), sz);
    }

    //Check given col does not exists
    public void verifyTableColumnGone(VoltCompiler compiler, String tableName, String colName) {
        Database db = compiler.m_catalog.getClusters().get("cluster").getDatabases().get("database");
        Table table = db.getTables().get(tableName);
        Column col = table.getColumns().get(colName);
        assertNull(col);
    }

    //Check given table is gone
    public void verifyTableGone(VoltCompiler compiler, String tableName) {
        Database db = compiler.m_catalog.getClusters().get("cluster").getDatabases().get("database");
        Table table = db.getTables().get(tableName);
        assertNull(table);
    }

    public void testAlterTable() throws IOException {
        final String simpleSchema1
                = "create table mytable  (pkey integer, column2_integer integer);\n"
                + "alter table mytable add column newcol varchar(50);\n";

        final String projectPath = getSimpleProjectPathForDDL(simpleSchema1);
        final VoltCompiler compiler = new VoltCompiler();
        final boolean success = compiler.compileWithProjectXML(projectPath, testout_jar);
        assertTrue(success);
        verifyTableColumnTypes(compiler, "mytable", "newcol", VoltType.STRING);
        verifyTableColumnSize(compiler, "mytable", "newcol", 50);
    }

    public void testAlterTableWithProcedure() throws IOException {
        final String simpleSchema1
                = "create table mytable  (pkey integer, column2_integer integer);\n"
                + "alter table mytable add column newcol varchar(50);\n"
                + myproc;

        final String projectPath = getSimpleProjectPathForDDL(simpleSchema1);
        final VoltCompiler compiler = new VoltCompiler();
        final boolean success = compiler.compileWithProjectXML(projectPath, testout_jar);
        assertTrue(success);
        verifyTableColumnTypes(compiler, "mytable", "newcol", VoltType.STRING);
        verifyTableColumnSize(compiler, "mytable", "newcol", 50);
    }

    public void testAlterTableWithProcedureUsingWrongColumn() throws IOException {
        final String simpleSchema1
                = "create table mytable  (pkey integer, column2_integer integer);\n"
                + "alter table mytable drop column column2_integer;\n"
                + myproc;

        final String projectPath = getSimpleProjectPathForDDL(simpleSchema1);
        final VoltCompiler compiler = new VoltCompiler();
        final boolean success = compiler.compileWithProjectXML(projectPath, testout_jar);
        assertFalse(success);
        //Failed to plan for statement
        int foundPlanError = 0;
        for (VoltCompiler.Feedback f : compiler.m_errors) {
            if (f.message.contains("Failed to plan for statement")) {
                foundPlanError++;
            }
        }
        assertEquals(1, foundPlanError);
    }

    public void testAlterTableWithProcedureUsingDroppableColumn() throws IOException {
        final String simpleSchema1
                = "create table mytable  (pkey integer, column2_integer integer);\n"
                + "alter table mytable drop column pkey;\n"
                + myproc;

        final String projectPath = getSimpleProjectPathForDDL(simpleSchema1);
        final VoltCompiler compiler = new VoltCompiler();
        final boolean success = compiler.compileWithProjectXML(projectPath, testout_jar);
        assertTrue(success);
        verifyTableColumnGone(compiler, "mytable", "pkey");
    }

    public void testAlterTableOnView() throws IOException {
        final String simpleSchema1
                = "create table mytable  (pkey integer, column2_integer integer);\n"
                + "CREATE VIEW v_mytable\n"
                + "(\n"
                + "  pkey\n"
                + ", num_cont\n"
                + ")\n"
                + "AS\n"
                + "   SELECT pkey\n"
                + "        , COUNT(*)\n"
                + "     FROM mytable\n"
                + " GROUP BY pkey\n"
                + ";"
                + "alter table v_mytable drop column column2_integer;\n";

        final String projectPath = getSimpleProjectPathForDDL(simpleSchema1);
        final VoltCompiler compiler = new VoltCompiler();
        final boolean success = compiler.compileWithProjectXML(projectPath, testout_jar);
        assertFalse(success);
        //user lacks privilege or object not found: V_MYTABLE
        int foundDropError = 0;
        for (VoltCompiler.Feedback f : compiler.m_errors) {
            if (f.message.contains("user lacks privilege or object not found: V_MYTABLE")) {
                foundDropError++;
            }
        }
        assertEquals(1, foundDropError);
    }

    public void testAlterTableDropUnrelatedColumnWithView() throws IOException {
        final String simpleSchema1
                = "create table mytable  (pkey integer, column2_integer integer);\n"
                + "CREATE VIEW v_mytable\n"
                + "(\n"
                + "  pkey\n"
                + ", num_cont\n"
                + ")\n"
                + "AS\n"
                + "   SELECT pkey\n"
                + "        , COUNT(*)\n"
                + "     FROM mytable\n"
                + " GROUP BY pkey\n"
                + ";"
                + "alter table mytable drop column column2_integer;\n";

        final String projectPath = getSimpleProjectPathForDDL(simpleSchema1);
        final VoltCompiler compiler = new VoltCompiler();
        final boolean success = compiler.compileWithProjectXML(projectPath, testout_jar);
        assertTrue(success);
        verifyTableColumnGone(compiler, "mytable", "column2_integer");
    }

    public void testAlterTableDropColumnWithView() throws IOException {
        final String simpleSchema1
                = "create table mytable  (pkey integer, column2_integer integer);\n"
                + "CREATE VIEW v_mytable\n"
                + "(\n"
                + "  pkey\n"
                + ", num_cont\n"
                + ")\n"
                + "AS\n"
                + "   SELECT pkey\n"
                + "        , COUNT(*)\n"
                + "     FROM mytable\n"
                + " GROUP BY pkey\n"
                + ";"
                + "alter table mytable drop column pkey;\n";

        final String projectPath = getSimpleProjectPathForDDL(simpleSchema1);
        final VoltCompiler compiler = new VoltCompiler();
        final boolean success = compiler.compileWithProjectXML(projectPath, testout_jar);
        assertFalse(success);
        //column is referenced in: PUBLIC.V_MYTABLE
        int foundDropError = 0;
        for (VoltCompiler.Feedback f : compiler.m_errors) {
            if (f.message.contains("column is referenced in: PUBLIC.V_MYTABLE")) {
                foundDropError++;
            }
        }
        assertEquals(1, foundDropError);
    }

    public void testAlterTableDropColumnWithViewCascade() throws IOException {
        final String simpleSchema1
                = "create table mytable  (pkey integer, column2_integer integer);\n"
                + "CREATE VIEW v_mytable\n"
                + "(\n"
                + "  pkey\n"
                + ", num_cont\n"
                + ")\n"
                + "AS\n"
                + "   SELECT pkey\n"
                + "        , COUNT(*)\n"
                + "     FROM mytable\n"
                + " GROUP BY pkey\n"
                + ";"
                + "alter table mytable drop column pkey cascade;\n";

        final String projectPath = getSimpleProjectPathForDDL(simpleSchema1);
        final VoltCompiler compiler = new VoltCompiler();
        final boolean success = compiler.compileWithProjectXML(projectPath, testout_jar);
        assertTrue(success);
        verifyTableColumnGone(compiler, "mytable", "pkey");
    }

    public void testAlterTableAlterColumnWithCountView() throws IOException {
        final String simpleSchema1
                = "create table mytable  (pkey integer, column2_integer integer);\n"
                + "CREATE VIEW v_mytable\n"
                + "(\n"
                + "  pkey\n"
                + ", num_cont\n"
                + ")\n"
                + "AS\n"
                + "   SELECT pkey\n"
                + "        , COUNT(pkey)\n"
                + "     FROM mytable\n"
                + " GROUP BY pkey\n"
                + ";"
                + "alter table mytable alter column pkey VARCHAR(20);\n";

        final String projectPath = getSimpleProjectPathForDDL(simpleSchema1);
        final VoltCompiler compiler = new VoltCompiler();
        final boolean success = compiler.compileWithProjectXML(projectPath, testout_jar);
        assertFalse(success);
        //dependent objects exist
        int foundDepError = 0;
        for (VoltCompiler.Feedback f : compiler.m_errors) {
            if (f.message.toLowerCase().contains("dependent objects exist")) {
                foundDepError++;
            }
        }
        assertEquals(1, foundDepError);
    }

    public void testAlterTableAlterColumnWithSumViewToNonSummableType() throws IOException {
        final String simpleSchema1
                = "create table mytable  (pkey integer, column2_integer integer);\n"
                + "CREATE VIEW v_mytable\n"
                + "(\n"
                + "  pkey\n"
                + ", num_cont\n"
                + ")\n"
                + "AS\n"
                + "   SELECT pkey\n"
                + "        , SUM(pkey)\n"
                + "     FROM mytable\n"
                + " GROUP BY pkey\n"
                + ";"
                + "alter table mytable alter column pkey VARCHAR(20);\n";

        final String projectPath = getSimpleProjectPathForDDL(simpleSchema1);
        final VoltCompiler compiler = new VoltCompiler();
        final boolean success = compiler.compileWithProjectXML(projectPath, testout_jar);
        assertFalse(success);
        //dependent objects exist
        int foundDepError = 0;
        for (VoltCompiler.Feedback f : compiler.m_errors) {
            if (f.message.toLowerCase().contains("dependent objects exist")) {
                foundDepError++;
            }
        }
        assertEquals(1, foundDepError);
    }

    public void testAlterTableAlterColumnPossibleWithSumView() throws IOException {
        final String simpleSchema1
                = "create table mytable  (pkey integer, column2_integer integer);\n"
                + "CREATE VIEW v_mytable\n"
                + "(\n"
                + "  pkey\n"
                + ", num_cont\n"
                + ")\n"
                + "AS\n"
                + "   SELECT pkey\n"
                + "        , SUM(pkey)\n"
                + "     FROM mytable\n"
                + " GROUP BY pkey\n"
                + ";"
                + "alter table mytable alter column pkey smallint;\n";

        final String projectPath = getSimpleProjectPathForDDL(simpleSchema1);
        final VoltCompiler compiler = new VoltCompiler();
        final boolean success = compiler.compileWithProjectXML(projectPath, testout_jar);
        assertFalse(success);
        //dependent objects exist - This should technically be ok as int to smallint with count should work....
        int foundDepError = 0;
        for (VoltCompiler.Feedback f : compiler.m_errors) {
            if (f.message.toLowerCase().contains("dependent objects exist")) {
                foundDepError++;
            }
        }
        assertEquals(1, foundDepError);
    }

    //Index
    public void testAlterTableDropColumnWithIndex() throws IOException {
        final String simpleSchema1
                = "create table mytable  (pkey integer, column2_integer integer);\n"
                + "create unique index pkey_idx on mytable(pkey);\n"
                + "alter table mytable drop column column2_integer;\n";

        final String projectPath = getSimpleProjectPathForDDL(simpleSchema1);
        final VoltCompiler compiler = new VoltCompiler();
        final boolean success = compiler.compileWithProjectXML(projectPath, testout_jar);
        assertTrue(success);
        verifyTableColumnGone(compiler, "mytable", "column2_integer");
    }

    public void testAlterTableDropRelatedColumnWithIndex() throws IOException {
        final String simpleSchema1
                = "create table mytable  (pkey integer, column2_integer integer);\n"
                + "create unique index pkey_idx on mytable(pkey, column2_integer);\n" // This index should go
                + "create unique index pkey_idxx on mytable(column2_integer);\n" //This index should remain
                + "alter table mytable drop column pkey;\n";

        final String projectPath = getSimpleProjectPathForDDL(simpleSchema1);
        final VoltCompiler compiler = new VoltCompiler();
        final boolean success = compiler.compileWithProjectXML(projectPath, testout_jar);
        assertTrue(success);
        verifyTableColumnGone(compiler, "mytable", "pkey");
    }

    public void testAlterTableBadVarchar() throws IOException {
        final String simpleSchema1
                = "create table mytable  (pkey integer, column2_integer integer);\n"
                + "alter table mytable add column newcol varchar(0);\n";

        final String projectPath = getSimpleProjectPathForDDL(simpleSchema1);
        final VoltCompiler compiler = new VoltCompiler();
        final boolean success = compiler.compileWithProjectXML(projectPath, testout_jar);
        assertFalse(success);
        //precision or scale out of range
        int foundSZError = 0;
        for (VoltCompiler.Feedback f : compiler.m_errors) {
            if (f.message.toLowerCase().contains("precision or scale out of range")) {
                foundSZError++;
            }
        }
        assertEquals(1, foundSZError);
    }

    public void testAlterTableOverflowVarchar() throws IOException {
        final String simpleSchema1
                = "create table mytable  (pkey integer, column2_integer integer);\n"
                + "alter table mytable add column newcol varchar(" + String.valueOf(VoltType.MAX_VALUE_LENGTH + 1) + ");\n";

        final String projectPath = getSimpleProjectPathForDDL(simpleSchema1);
        final VoltCompiler compiler = new VoltCompiler();
        final boolean success = compiler.compileWithProjectXML(projectPath, testout_jar);
        assertFalse(success);
        //> 1048576 char maximum.
        int foundSZError = 0;
        for (VoltCompiler.Feedback f : compiler.m_errors) {
            if (f.message.toLowerCase().contains("> 1048576 char maximum.")) {
                foundSZError++;
            }
        }
        assertEquals(1, foundSZError);
    }

    public void testAlterTableOverflowVarcharExisting() throws IOException {
        final String simpleSchema1
                = "create table mytable  (pkey integer, column2 varchar(50));\n"
                + "alter table mytable alter column column2 varchar(" + String.valueOf(VoltType.MAX_VALUE_LENGTH + 1) + ");\n";

        final String projectPath = getSimpleProjectPathForDDL(simpleSchema1);
        final VoltCompiler compiler = new VoltCompiler();
        final boolean success = compiler.compileWithProjectXML(projectPath, testout_jar);
        assertFalse(success);
        //> 1048576 char maximum.
        int foundSZError = 0;
        for (VoltCompiler.Feedback f : compiler.m_errors) {
            if (f.message.toLowerCase().contains("> 1048576 char maximum.")) {
                foundSZError++;
            }
        }
        assertEquals(1, foundSZError);
    }

    public void testTooManyColumnTable() throws IOException, HSQLInterface.HSQLParseException {
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
        ddl1.append("alter table JUST_ENOUGH_WIDE_COLUMNS add column COL01022 INTEGER DEFAULT '0' NOT NULL;");

        final String projectPath = getSimpleProjectPathForDDL(ddl1.toString());
        final VoltCompiler compiler = new VoltCompiler();
        final boolean success = compiler.compileWithProjectXML(projectPath, testout_jar);
        assertFalse(success);
        //(max is 1024)
        int foundSZError = 0;
        for (VoltCompiler.Feedback f : compiler.m_errors) {
            if (f.message.toLowerCase().contains("(max is 1024)")) {
                foundSZError++;
            }
        }
        assertEquals(1, foundSZError);
    }

    public void testTooManyColumnThenLessColumnTable() throws IOException, HSQLInterface.HSQLParseException {
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
        ddl1.append("alter table MANY_COLUMNS drop column COL01022;");

        final String projectPath = getSimpleProjectPathForDDL(ddl1.toString());
        final VoltCompiler compiler = new VoltCompiler();
        final boolean success = compiler.compileWithProjectXML(projectPath, testout_jar);
        assertTrue(success);
        verifyTableColumnGone(compiler, "MANY_COLUMNS", "COL01022");
    }

    //We will be successful in compiling but a WARN should appear.
    public void testAlterTableAddForeignKey() throws IOException {
        final String simpleSchema1
                = "create table mytable  (pkey integer, column2_integer integer);\n"
                + "create table ftable  (pkey integer, column2_integer integer UNIQUE);\n"
                + "ALTER TABLE mytable ADD FOREIGN KEY (column2_integer) REFERENCES ftable(column2_integer);\n";

        final String projectPath = getSimpleProjectPathForDDL(simpleSchema1);
        final VoltCompiler compiler = new VoltCompiler();
        final boolean success = compiler.compileWithProjectXML(projectPath, testout_jar);
        assertTrue(success);
        verifyTableColumnTypes(compiler, "mytable", "pkey", VoltType.INTEGER);
        verifyTableColumnTypes(compiler, "ftable", "pkey", VoltType.INTEGER);

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
        final String simpleSchema1
                = "create table mytable  (pkey integer, column2_integer integer);\n"
                + "alter table mytablenonexistent add column newcol varchar(50);\n";

        final String projectPath = getSimpleProjectPathForDDL(simpleSchema1);
        final VoltCompiler compiler = new VoltCompiler();
        final boolean success = compiler.compileWithProjectXML(projectPath, testout_jar);
        assertFalse(success);
        //type not found or user lacks privilege:
        int foundMissingError = 0;
        for (VoltCompiler.Feedback f : compiler.m_errors) {
            if (f.message.contains("user lacks privilege or object not found:")) {
                foundMissingError++;
            }
        }
        assertEquals(1, foundMissingError);
    }

    public void testCreateDropAlterTable() throws IOException {
        final String simpleSchema1
                = "create table mytable  (pkey integer, column2_integer integer);\n"
                + "drop table mytable;\n"
                + "alter table mytable add column newcol varchar(50);\n";

        final String projectPath = getSimpleProjectPathForDDL(simpleSchema1);
        final VoltCompiler compiler = new VoltCompiler();
        final boolean success = compiler.compileWithProjectXML(projectPath, testout_jar);
        assertFalse(success);
        //type not found or user lacks privilege:
        int foundMissingError = 0;
        for (VoltCompiler.Feedback f : compiler.m_errors) {
            if (f.message.contains("user lacks privilege or object not found:")) {
                foundMissingError++;
            }
        }
        assertEquals(1, foundMissingError);
    }

    public void testCreateDropCreateAlterTable() throws IOException {
        final String simpleSchema1
                = "create table mytable  (pkey integer, column2_integer integer);\n"
                + "drop table mytable;\n"
                + "create table mytable  (pkey integer, column2_integer integer);\n"
                + "alter table mytable add column newcol varchar(50);\n";

        final String projectPath = getSimpleProjectPathForDDL(simpleSchema1);
        final VoltCompiler compiler = new VoltCompiler();
        final boolean success = compiler.compileWithProjectXML(projectPath, testout_jar);
        assertTrue(success);
        verifyTableColumnTypes(compiler, "mytable", "pkey", VoltType.INTEGER);
    }

    public void testAlterTableBadDowngradeOfPartitionColumn() throws IOException {
        final String simpleSchema1
                = "create table mytable  (pkey integer NOT NULL, column2_integer integer);\n"
                + "PARTITION TABLE mytable ON COLUMN pkey;"
                + "alter table mytable alter column pkey NULL;\n";

        final String projectPath = getSimpleProjectPathForDDL(simpleSchema1);
        final VoltCompiler compiler = new VoltCompiler();
        final boolean success = compiler.compileWithProjectXML(projectPath, testout_jar);
        assertFalse(success);
        //type not found or user lacks privilege:
        int foundMissingError = 0;
        for (VoltCompiler.Feedback f : compiler.m_errors) {
            if (f.message.contains("type not found or user lacks privilege:")) {
                foundMissingError++;
            }
        }
        assertEquals(1, foundMissingError);
    }

    public void testAlterTableSizeChangeAndConstraintChangeOfPartitionColumn() throws IOException {
        final String simpleSchema1
                = "create table mytable  (pkey varchar(20) NOT NULL, column2_integer integer);\n"
                + "PARTITION TABLE mytable ON COLUMN pkey;"
                + "alter table mytable alter column pkey varchar(50);\n";

        final String projectPath = getSimpleProjectPathForDDL(simpleSchema1);
        final VoltCompiler compiler = new VoltCompiler();
        final boolean success = compiler.compileWithProjectXML(projectPath, testout_jar);
        assertFalse(success);
        //Partition columns must be constrained "NOT NULL"
        int foundConstraintError = 0;
        for (VoltCompiler.Feedback f : compiler.m_errors) {
            if (f.message.contains("Partition columns must be constrained \"NOT NULL\"")) {
                foundConstraintError++;
            }
        }
        assertEquals(1, foundConstraintError);
    }

    public void testAlterTableSizeChangeAndAddConstraintOfPartitionColumn() throws IOException {
        final String simpleSchema1
                = "create table mytable  (pkey varchar(20) NOT NULL, column2_integer integer);\n"
                + "PARTITION TABLE mytable ON COLUMN pkey;"
                + "alter table mytable alter column pkey varchar(50) NOT NULL;\n";

        final String projectPath = getSimpleProjectPathForDDL(simpleSchema1);
        final VoltCompiler compiler = new VoltCompiler();
        final boolean success = compiler.compileWithProjectXML(projectPath, testout_jar);
        assertFalse(success);
        //constraint definition not allowed in statement
        int foundConstraintError = 0;
        for (VoltCompiler.Feedback f : compiler.m_errors) {
            if (f.message.contains("constraint definition not allowed in statement")) {
                foundConstraintError++;
            }
        }
        assertEquals(1, foundConstraintError);
    }

    public void testAlterTableAddPartitionColumn() throws IOException {
        final String simpleSchema1
                = "create table mytable  (pkey varchar(20) NOT NULL, column2_integer integer);\n"
                + "alter table mytable add column pkey2 varchar(500);\n"
                + "PARTITION TABLE mytable ON COLUMN pkey2;\n";

        final String projectPath = getSimpleProjectPathForDDL(simpleSchema1);
        final VoltCompiler compiler = new VoltCompiler();
        final boolean success = compiler.compileWithProjectXML(projectPath, testout_jar);
        assertFalse(success);
        //Partition columns must be constrained
        int foundConstraintError = 0;
        for (VoltCompiler.Feedback f : compiler.m_errors) {
            if (f.message.contains("Partition columns must be constrained")) {
                foundConstraintError++;
            }
        }
        assertEquals(1, foundConstraintError);
    }

    //This is create procedure on valid column add after alter...should be successful.
    public void testAlterTableAddNonNULLPartitionColumn() throws IOException {
        final String simpleSchema1
                = "create table mytable  (pkey varchar(20) NOT NULL, column2_integer integer);\n"
                + "alter table mytable add column pkey2 varchar(500) NOT NULL;\n"
                + "PARTITION TABLE mytable ON COLUMN pkey2;\n";

        final String projectPath = getSimpleProjectPathForDDL(simpleSchema1);
        final VoltCompiler compiler = new VoltCompiler();
        final boolean success = compiler.compileWithProjectXML(projectPath, testout_jar);
        assertTrue(success);
        verifyTableColumnSize(compiler, "mytable", "pkey2", 500);
    }

    //DROP TABLE TESTS from here
    public void testDropTable() throws IOException {
        final String simpleSchema1
                = "create table mytable  (pkey integer, column2_integer integer);\n"
                + "drop table mytable;\n";

        final String projectPath = getSimpleProjectPathForDDL(simpleSchema1);
        final VoltCompiler compiler = new VoltCompiler();
        final boolean success = compiler.compileWithProjectXML(projectPath, testout_jar);
        assertTrue(success);
        verifyTableGone(compiler, "mytable");
    }

    public void testDropTableThatDoesNotExists() throws IOException {
        final String simpleSchema1
                = "create table mytable  (pkey integer, column2_integer integer);\n"
                + "drop table mytablefoo;\n";

        final String projectPath = getSimpleProjectPathForDDL(simpleSchema1);
        final VoltCompiler compiler = new VoltCompiler();
        final boolean success = compiler.compileWithProjectXML(projectPath, testout_jar);
        assertFalse(success);
    }

    public void testDropTableIfExists() throws IOException {
        final String simpleSchema1
                = "create table mytable  (pkey integer, column2_integer integer);\n"
                + "drop table mytablenonexistant if exists;\n";

        final String projectPath = getSimpleProjectPathForDDL(simpleSchema1);
        final VoltCompiler compiler = new VoltCompiler();
        final boolean success = compiler.compileWithProjectXML(projectPath, testout_jar);
        assertTrue(success);
        verifyTableColumnTypes(compiler, "mytable", "pkey", VoltType.INTEGER);
    }

    //Should fail as table should not exist
    public void testDropTableWithProcedure() throws IOException {
        final String simpleSchema1
                = "create table mytable  (pkey integer, column2_integer integer);\n"
                + "drop table mytable;\n"
                + myproc;

        final String projectPath = getSimpleProjectPathForDDL(simpleSchema1);
        final VoltCompiler compiler = new VoltCompiler();
        final boolean success = compiler.compileWithProjectXML(projectPath, testout_jar);
        assertFalse(success);
    }
}
