/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.voltdb.compiler.VoltCompiler;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.types.VoltDecimalHelper;
import org.voltdb.utils.BuildDirectoryUtils;

import junit.framework.TestCase;

public class TestJdbcDatabaseMetaDataGenerator extends TestCase
{
    String testout_jar;

    private VoltCompiler compileForDDLTest(String project) {
        final File projectFile = VoltProjectBuilder.writeStringToTempFile(project);
        projectFile.deleteOnExit();
        final String projectPath = projectFile.getPath();
        final VoltCompiler compiler = new VoltCompiler();
        boolean success = compiler.compile(projectPath, testout_jar, System.out, null);
        assertTrue("Catalog compile failed!", success);
        return compiler;
    }

    private String getPathForSchema(String s) {
        final File schemaFile = VoltProjectBuilder.writeStringToTempFile(s);
        schemaFile.deleteOnExit();
        return schemaFile.getPath();
    }

    public void setUp() throws Exception
    {
        testout_jar = BuildDirectoryUtils.getBuildDirectoryPath() + File.pathSeparator + "jdbctestout.jar";
    }

    public void tearDown() throws Exception
    {
        File tjar = new File(testout_jar);
        tjar.delete();
    }

    boolean moveToMatchingRow(VoltTable table, String columnName,
                              String columnValue)
    {
        boolean found = false;
        table.resetRowPosition();
        while (table.advanceRow())
        {
            if (table.get(columnName, VoltType.STRING).equals(columnValue.toUpperCase()))
            {
                found = true;
                break;
            }
        }
        return found;
    }

    public void testGetTables()
    {
        String schema =
            "create table Table1 (Column1 varchar(10), Column2 integer);" +
            "create table Table2 (Column1 integer);" +
            "create view View1 (Column1, num) as select Column1, count(*) from Table1 group by Column1;" +
            "create table Export1 (Column1 integer);";
        String project =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "  <database name='database'>" +
            "    <schemas><schema path='" + getPathForSchema(schema) + "' /></schemas>" +
            "    <procedures><procedure class='sample'><sql>select * from Table1</sql></procedure></procedures>" +
            "    <export><tables><table name = \"Export1\"/></tables></export>" +
            "  </database>" +
            "</project>";

        VoltCompiler c = compileForDDLTest(project);
        System.out.println(c.getCatalog().serialize());
        JdbcDatabaseMetaDataGenerator dut =
            new JdbcDatabaseMetaDataGenerator(c.getCatalog());
        VoltTable tables = dut.getTables();
        System.out.println(tables);
        assertEquals(10, tables.getColumnCount());
        assertEquals(4, tables.getRowCount());
        assertTrue(moveToMatchingRow(tables, "TABLE_NAME", "Table1"));
        assertTrue(tables.get("TABLE_TYPE", VoltType.STRING).equals("TABLE"));
        assertTrue(moveToMatchingRow(tables, "TABLE_NAME", "Table2"));
        assertTrue(tables.get("TABLE_TYPE", VoltType.STRING).equals("TABLE"));
        assertTrue(moveToMatchingRow(tables, "TABLE_NAME", "View1"));
        assertTrue(tables.get("TABLE_TYPE", VoltType.STRING).equals("VIEW"));
        assertTrue(moveToMatchingRow(tables, "TABLE_NAME", "Export1"));
        assertTrue(tables.get("TABLE_TYPE", VoltType.STRING).equals("EXPORT"));
        assertFalse(moveToMatchingRow(tables, "TABLE_NAME", "NotATable"));
    }

    private void assertWithNullCheck(Object expected, Object value, VoltTable table)
    {
        if (table.wasNull())
        {
            assertTrue(expected == null);
        }
        else if (expected == null)
        {
            assertTrue(table.wasNull());
        }
        else
        {
            if (expected instanceof String)
            {
                assertTrue(((String)expected).equals((String)value));
            }
            else
            {
                assertEquals(expected, value);
            }
        }
    }

    private void verifyColumnData(String columnName,
                                  VoltTable columns,
                                  Object[] expected)
    {
        assertTrue(moveToMatchingRow(columns, "COLUMN_NAME", columnName));
        assertEquals(expected[0], columns.get("DATA_TYPE", VoltType.INTEGER));
        assertTrue("SQL Typename mismatch",
                   columns.get("TYPE_NAME", VoltType.STRING).equals(expected[1]));
        assertWithNullCheck(expected[2], columns.get("COLUMN_SIZE", VoltType.INTEGER), columns);
        assertWithNullCheck(expected[3], columns.get("DECIMAL_DIGITS", VoltType.INTEGER), columns);
        assertWithNullCheck(expected[4], columns.get("NUM_PREC_RADIX", VoltType.INTEGER), columns);
        assertEquals(expected[5], columns.get("NULLABLE", VoltType.INTEGER));
    }

    public void testGetColumns()
    {
        HashMap<String, Object[]> refcolumns = new HashMap<String, Object[]>();
        refcolumns.put("Column1", new Object[] {java.sql.Types.VARCHAR,
                                                "VARCHAR",
                                                200,
                                                null,
                                                null,
                                                java.sql.DatabaseMetaData.columnNoNulls});
        refcolumns.put("Column2", new Object[] {java.sql.Types.TINYINT,
                                                "TINYINT",
                                                7,
                                                null,
                                                2,
                                                java.sql.DatabaseMetaData.columnNullable});
        refcolumns.put("Column3", new Object[] {java.sql.Types.SMALLINT,
                                                "SMALLINT",
                                                15,
                                                null,
                                                2,
                                                java.sql.DatabaseMetaData.columnNoNulls});
        refcolumns.put("Column4", new Object[] {java.sql.Types.INTEGER,
                                                "INTEGER",
                                                31,
                                                null,
                                                2,
                                                java.sql.DatabaseMetaData.columnNullable});
        refcolumns.put("Column5", new Object[] {java.sql.Types.BIGINT,
                                                "BIGINT",
                                                63,
                                                null,
                                                2,
                                                java.sql.DatabaseMetaData.columnNoNulls});
        refcolumns.put("Column6", new Object[] {java.sql.Types.DOUBLE,
                                                "DOUBLE",
                                                53,
                                                null,
                                                2,
                                                java.sql.DatabaseMetaData.columnNullable});
        refcolumns.put("Column7", new Object[] {java.sql.Types.TIMESTAMP,
                                                "TIMESTAMP",
                                                63,
                                                null,
                                                2,
                                                java.sql.DatabaseMetaData.columnNoNulls});
        refcolumns.put("Column8", new Object[] {java.sql.Types.DECIMAL,
                                                "DECIMAL",
                                                VoltDecimalHelper.kDefaultPrecision,
                                                VoltDecimalHelper.kDefaultScale,
                                                10,
                                                java.sql.DatabaseMetaData.columnNullable});
        refcolumns.put("Column9", new Object[] {java.sql.Types.VARBINARY,
                                                "VARBINARY",
                                                250,
                                                null,
                                                null,
                                                java.sql.DatabaseMetaData.columnNoNulls});
        refcolumns.put("Column10", new Object[] {java.sql.Types.VARCHAR,
                                                 "VARCHAR",
                                                 200,
                                                 null,
                                                 null,
                                                 java.sql.DatabaseMetaData.columnNullable});
        refcolumns.put("Column11", new Object[] {java.sql.Types.INTEGER,
                                                 "INTEGER",
                                                 31,
                                                 null,
                                                 2,
                                                 java.sql.DatabaseMetaData.columnNullable});

        String schema =
            "create table Table1 (Column1 varchar(200) not null, Column2 tinyint);" +
            "create table Table2 (Column3 smallint not null, Column4 integer, Column5 bigint not null);" +
            "create table Table3 (Column6 float, Column7 timestamp not null, Column8 decimal);" +
            "create table Table4 (Column9 varbinary(250) not null);" +
            "create view View1 (Column10, Column11) as select Column1, count(*) from Table1 group by Column1;";
        String project =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "  <database name='database'>" +
            "    <schemas><schema path='" + getPathForSchema(schema) + "' /></schemas>" +
            "    <procedures><procedure class='sample'><sql>select * from Table1</sql></procedure></procedures>" +
            "  </database>" +
            "</project>";

        VoltCompiler c = compileForDDLTest(project);
        System.out.println(c.getCatalog().serialize());
        JdbcDatabaseMetaDataGenerator dut =
            new JdbcDatabaseMetaDataGenerator(c.getCatalog());
        VoltTable columns = dut.getColumns();
        System.out.println(columns);
        assertEquals(23, columns.getColumnCount());
        assertEquals(11, columns.getRowCount());
        for (Map.Entry<String, Object[]> entry : refcolumns.entrySet())
        {
            verifyColumnData(entry.getKey(), columns, entry.getValue());
        }
    }
}