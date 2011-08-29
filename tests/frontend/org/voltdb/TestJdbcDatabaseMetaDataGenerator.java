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
        testout_jar = BuildDirectoryUtils.getBuildDirectoryPath() + File.separator + "jdbctestout.jar";
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
            if (((String)table.get(columnName, VoltType.STRING)).
                    equalsIgnoreCase(columnValue.toUpperCase()))
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
        VoltTable tables = dut.getMetaData("tables");
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
        assertWithNullCheck(expected[2],
                            columns.get("COLUMN_SIZE", VoltType.INTEGER),
                            columns);
        assertWithNullCheck(expected[3],
                            columns.get("DECIMAL_DIGITS", VoltType.INTEGER),
                            columns);
        assertWithNullCheck(expected[4],
                            columns.get("NUM_PREC_RADIX", VoltType.INTEGER),
                            columns);
        assertEquals("Null mismatch for column " + columnName,
                     expected[5], columns.get("NULLABLE", VoltType.INTEGER));
        assertWithNullCheck(expected[6], columns.get("REMARKS", VoltType.STRING),
                            columns);
        assertWithNullCheck(expected[7],
                            columns.get("COLUMN_DEF", VoltType.STRING),
                            columns);
        assertWithNullCheck(expected[8],
                            columns.get("CHAR_OCTET_LENGTH", VoltType.INTEGER),
                            columns);
        assertEquals(expected[9],
                     columns.get("ORDINAL_POSITION", VoltType.INTEGER));
    }

    public void testGetColumns()
    {
        HashMap<String, Object[]> refcolumns = new HashMap<String, Object[]>();
        refcolumns.put("Column1", new Object[] {java.sql.Types.VARCHAR,
                                                "VARCHAR",
                                                200,
                                                null,
                                                null,
                                                java.sql.DatabaseMetaData.columnNoNulls,
                                                null,
                                                null,
                                                200,
                                                1,
                                                "NO"});
        refcolumns.put("Column2", new Object[] {java.sql.Types.TINYINT,
                                                "TINYINT",
                                                7,
                                                null,
                                                2,
                                                java.sql.DatabaseMetaData.columnNullable,
                                                null,
                                                null,
                                                null,
                                                2,
                                                "YES"});
        refcolumns.put("Column3", new Object[] {java.sql.Types.SMALLINT,
                                                "SMALLINT",
                                                15,
                                                null,
                                                2,
                                                java.sql.DatabaseMetaData.columnNoNulls,
                                                "PARTITION_COLUMN",
                                                null,
                                                null,
                                                1,
                                                "NO"});
        refcolumns.put("Column4", new Object[] {java.sql.Types.INTEGER,
                                                "INTEGER",
                                                31,
                                                null,
                                                2,
                                                java.sql.DatabaseMetaData.columnNullable,
                                                null,
                                                null,
                                                null,
                                                2,
                                                "YES"});
        refcolumns.put("Column5", new Object[] {java.sql.Types.BIGINT,
                                                "BIGINT",
                                                63,
                                                null,
                                                2,
                                                java.sql.DatabaseMetaData.columnNoNulls,
                                                null,
                                                null,
                                                null,
                                                3,
                                                "NO"});
        refcolumns.put("Column6", new Object[] {java.sql.Types.DOUBLE,
                                                "DOUBLE",
                                                53,
                                                null,
                                                2,
                                                java.sql.DatabaseMetaData.columnNullable,
                                                null,
                                                null,
                                                null,
                                                1,
                                                "YES"});
        refcolumns.put("Column7", new Object[] {java.sql.Types.TIMESTAMP,
                                                "TIMESTAMP",
                                                63,
                                                null,
                                                2,
                                                java.sql.DatabaseMetaData.columnNoNulls,
                                                null,
                                                null,
                                                null,
                                                2,
                                                "NO"});
        refcolumns.put("Column8", new Object[] {java.sql.Types.DECIMAL,
                                                "DECIMAL",
                                                VoltDecimalHelper.kDefaultPrecision,
                                                VoltDecimalHelper.kDefaultScale,
                                                10,
                                                java.sql.DatabaseMetaData.columnNullable,
                                                null,
                                                null,
                                                null,
                                                3,
                                                "YES"});
        refcolumns.put("Column9", new Object[] {java.sql.Types.VARBINARY,
                                                "VARBINARY",
                                                250,
                                                null,
                                                null,
                                                java.sql.DatabaseMetaData.columnNoNulls,
                                                null,
                                                null,
                                                250,
                                                1,
                                                "NO"});
        refcolumns.put("Column10", new Object[] {java.sql.Types.VARCHAR,
                                                 "VARCHAR",
                                                 200,
                                                 null,
                                                 null,
                                                 java.sql.DatabaseMetaData.columnNullable,
                                                 null,
                                                 null,
                                                 200,
                                                 1,
                                                 "YES"});
        refcolumns.put("Column11", new Object[] {java.sql.Types.INTEGER,
                                                 "INTEGER",
                                                 31,
                                                 null,
                                                 2,
                                                 java.sql.DatabaseMetaData.columnNullable,
                                                 null,
                                                 null,
                                                 null,
                                                 2,
                                                 "YES"});
        refcolumns.put("Default1", new Object[] {java.sql.Types.TINYINT,
                                                 "TINYINT",
                                                 7,
                                                 null,
                                                 2,
                                                 java.sql.DatabaseMetaData.columnNullable,
                                                 null,
                                                 "10",
                                                 null,
                                                 1,
                                                 "YES"});
        refcolumns.put("Default2", new Object[] {java.sql.Types.VARCHAR,
                                                 "VARCHAR",
                                                 50,
                                                 null,
                                                 null,
                                                 java.sql.DatabaseMetaData.columnNullable,
                                                 null,
                                                 "'DUDE'",
                                                 50,
                                                 2,
                                                 "YES"});

        String schema =
            "create table Table1 (Column1 varchar(200) not null, Column2 tinyint);" +
            "create table Table2 (Column3 smallint not null, Column4 integer, Column5 bigint not null);" +
            "create table Table3 (Column6 float, Column7 timestamp not null, Column8 decimal);" +
            "create table Table4 (Column9 varbinary(250) not null);" +
            "create view View1 (Column10, Column11) as select Column1, count(*) from Table1 group by Column1;" +
            "create table Table5 (Default1 tinyint default 10, Default2 varchar(50) default 'DUDE');";
        String project =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "  <database name='database'>" +
            "    <schemas><schema path='" + getPathForSchema(schema) + "' /></schemas>" +
            "    <procedures><procedure class='sample'><sql>select * from Table1</sql></procedure></procedures>" +
            "    <partitions><partition table='Table2' column='Column3'/></partitions>" +
            "  </database>" +
            "</project>";

        VoltCompiler c = compileForDDLTest(project);
        System.out.println(c.getCatalog().serialize());
        JdbcDatabaseMetaDataGenerator dut =
            new JdbcDatabaseMetaDataGenerator(c.getCatalog());
        VoltTable columns = dut.getMetaData("ColUmns");
        System.out.println(columns);
        assertEquals(23, columns.getColumnCount());
        assertEquals(13, columns.getRowCount());
        for (Map.Entry<String, Object[]> entry : refcolumns.entrySet())
        {
            verifyColumnData(entry.getKey(), columns, entry.getValue());
        }
    }

    public void testGetIndexInfo()
    {
        String schema =
            "create table Table1 (Column1 smallint, Column2 integer, Column3 bigint not null, Column4 integer, Column5 integer, " +
            "  constraint pk_tree primary key (Column1));" +
            "create index Index1_tree on Table1 (Column2, Column3);" +
            "create index Index2_hash on Table1 (Column4, Column5);";
        String project =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "  <database name='database'>" +
            "    <schemas><schema path='" + getPathForSchema(schema) + "' /></schemas>" +
            "    <procedures><procedure class='sample'><sql>select * from Table1</sql></procedure></procedures>" +
            "    <partitions><partition table='Table1' column='Column3'/></partitions>" +
            "  </database>" +
            "</project>";

        VoltCompiler c = compileForDDLTest(project);
        System.out.println(c.getCatalog().serialize());
        JdbcDatabaseMetaDataGenerator dut =
            new JdbcDatabaseMetaDataGenerator(c.getCatalog());
        VoltTable indexes = dut.getMetaData("IndexInfo");
        System.out.println(indexes);
        assertEquals(13, indexes.getColumnCount());
        assertEquals(5, indexes.getRowCount());
        assertTrue(moveToMatchingRow(indexes, "COLUMN_NAME", "Column2"));
        assertEquals("TABLE1", indexes.get("TABLE_NAME", VoltType.STRING));
        assertEquals((byte)1, indexes.get("NON_UNIQUE", VoltType.TINYINT));
        assertEquals("INDEX1_TREE", indexes.get("INDEX_NAME", VoltType.STRING));
        assertEquals((short)java.sql.DatabaseMetaData.tableIndexOther,
                     indexes.get("TYPE", VoltType.SMALLINT));
        assertEquals((short)1, indexes.get("ORDINAL_POSITION", VoltType.SMALLINT));
        assertEquals("A", indexes.get("ASC_OR_DESC", VoltType.STRING));
        assertTrue(moveToMatchingRow(indexes, "COLUMN_NAME", "Column3"));
        assertEquals("TABLE1", indexes.get("TABLE_NAME", VoltType.STRING));
        assertEquals((byte)1, indexes.get("NON_UNIQUE", VoltType.TINYINT));
        assertEquals("INDEX1_TREE", indexes.get("INDEX_NAME", VoltType.STRING));
        assertEquals((short)java.sql.DatabaseMetaData.tableIndexOther,
                     indexes.get("TYPE", VoltType.SMALLINT));
        assertEquals((short)2, indexes.get("ORDINAL_POSITION", VoltType.SMALLINT));
        assertEquals("A", indexes.get("ASC_OR_DESC", VoltType.STRING));
        assertTrue(moveToMatchingRow(indexes, "COLUMN_NAME", "Column4"));
        assertEquals("TABLE1", indexes.get("TABLE_NAME", VoltType.STRING));
        assertEquals((byte)1, indexes.get("NON_UNIQUE", VoltType.TINYINT));
        assertEquals("INDEX2_HASH", indexes.get("INDEX_NAME", VoltType.STRING));
        assertEquals((short)java.sql.DatabaseMetaData.tableIndexHashed,
                     indexes.get("TYPE", VoltType.SMALLINT));
        assertEquals((short)1, indexes.get("ORDINAL_POSITION", VoltType.SMALLINT));
        assertEquals(null, indexes.get("ASC_OR_DESC", VoltType.STRING));
        assertTrue(moveToMatchingRow(indexes, "COLUMN_NAME", "Column5"));
        assertEquals("TABLE1", indexes.get("TABLE_NAME", VoltType.STRING));
        assertEquals((byte)1, indexes.get("NON_UNIQUE", VoltType.TINYINT));
        assertEquals("INDEX2_HASH", indexes.get("INDEX_NAME", VoltType.STRING));
        assertEquals((short)java.sql.DatabaseMetaData.tableIndexHashed,
                     indexes.get("TYPE", VoltType.SMALLINT));
        assertEquals((short)2, indexes.get("ORDINAL_POSITION", VoltType.SMALLINT));
        assertEquals(null, indexes.get("ASC_OR_DESC", VoltType.STRING));
        assertTrue(moveToMatchingRow(indexes, "COLUMN_NAME", "Column1"));
        assertEquals("TABLE1", indexes.get("TABLE_NAME", VoltType.STRING));
        assertEquals((byte)0, indexes.get("NON_UNIQUE", VoltType.TINYINT));
        assertTrue(((String)indexes.get("INDEX_NAME", VoltType.STRING)).contains("PK_TREE"));
        assertEquals((short)java.sql.DatabaseMetaData.tableIndexOther,
                     indexes.get("TYPE", VoltType.SMALLINT));
        assertEquals((short)1, indexes.get("ORDINAL_POSITION", VoltType.SMALLINT));
        assertEquals("A", indexes.get("ASC_OR_DESC", VoltType.STRING));
        assertFalse(moveToMatchingRow(indexes, "COLUMN_NAME", "NotAColumn"));
    }

    public void testGetPrimaryKeys()
    {
        String schema =
            "create table Table1 (Column1 smallint not null, constraint primary1 primary key (Column1));" +
            "create table Table2 (Column2 smallint not null, Column3 smallint not null, Column4 smallint not null, " +
            "  constraint primary2 primary key (Column2, Column3, Column4));";
        String project =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "  <database name='database'>" +
            "    <schemas><schema path='" + getPathForSchema(schema) + "' /></schemas>" +
            "    <procedures><procedure class='sample'><sql>select * from Table1</sql></procedure></procedures>" +
            "    <partitions><partition table='Table1' column='Column1'/></partitions>" +
            "  </database>" +
            "</project>";

        VoltCompiler c = compileForDDLTest(project);
        System.out.println(c.getCatalog().serialize());
        JdbcDatabaseMetaDataGenerator dut =
            new JdbcDatabaseMetaDataGenerator(c.getCatalog());
        VoltTable pkeys = dut.getMetaData("PrimaryKeys");
        System.out.println(pkeys);
        assertEquals(6, pkeys.getColumnCount());
        assertEquals(4, pkeys.getRowCount());
        assertTrue(moveToMatchingRow(pkeys, "COLUMN_NAME", "Column1"));
        assertEquals("TABLE1", pkeys.get("TABLE_NAME", VoltType.STRING));
        assertEquals((short)1, pkeys.get("KEY_SEQ", VoltType.SMALLINT));
        assertEquals("PRIMARY1", pkeys.get("PK_NAME", VoltType.STRING));
        assertTrue(moveToMatchingRow(pkeys, "COLUMN_NAME", "Column2"));
        assertEquals("TABLE2", pkeys.get("TABLE_NAME", VoltType.STRING));
        assertEquals((short)1, pkeys.get("KEY_SEQ", VoltType.SMALLINT));
        assertEquals("PRIMARY2", pkeys.get("PK_NAME", VoltType.STRING));
        assertTrue(moveToMatchingRow(pkeys, "COLUMN_NAME", "Column3"));
        assertEquals("TABLE2", pkeys.get("TABLE_NAME", VoltType.STRING));
        assertEquals((short)2, pkeys.get("KEY_SEQ", VoltType.SMALLINT));
        assertEquals("PRIMARY2", pkeys.get("PK_NAME", VoltType.STRING));
        assertTrue(moveToMatchingRow(pkeys, "COLUMN_NAME", "Column4"));
        assertEquals("TABLE2", pkeys.get("TABLE_NAME", VoltType.STRING));
        assertEquals((short)3, pkeys.get("KEY_SEQ", VoltType.SMALLINT));
        assertEquals("PRIMARY2", pkeys.get("PK_NAME", VoltType.STRING));
    }

    public void testGetProcedureColumns()
    {
        String schema =
            "create table Table1 (Column1 varchar(200) not null, Column2 integer);";
        String project =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "  <database name='database'>" +
            "    <schemas><schema path='" + getPathForSchema(schema) + "' /></schemas>" +
            "    <procedures>" +
            "      <procedure class='proc1' partitioninfo=\"Table1.Column1:0\"><sql>select * from Table1 where Column1=?</sql></procedure>" +
            "      <procedure class='proc2'><sql>select * from Table1 where Column2=?</sql></procedure>" +
            "    </procedures>" +
            "    <partitions><partition table='Table1' column='Column1'/></partitions>" +
            "  </database>" +
            "</project>";

        VoltCompiler c = compileForDDLTest(project);
        System.out.println(c.getCatalog().serialize());
        JdbcDatabaseMetaDataGenerator dut =
            new JdbcDatabaseMetaDataGenerator(c.getCatalog());
        VoltTable params = dut.getMetaData("ProcedureColumns");
        System.out.println(params);
        assertEquals(20, params.getColumnCount());
        assertEquals(2, params.getRowCount());
        assertTrue(moveToMatchingRow(params, "PROCEDURE_NAME", "proc1"));
        assertEquals("param0", params.get("COLUMN_NAME", VoltType.STRING));
        assertEquals(VoltType.MAX_VALUE_LENGTH, params.get("PRECISION", VoltType.INTEGER));
        assertEquals(VoltType.MAX_VALUE_LENGTH, params.get("LENGTH", VoltType.INTEGER));
        assertEquals(VoltType.MAX_VALUE_LENGTH, params.get("CHAR_OCTET_LENGTH", VoltType.INTEGER));
        assertEquals("PARTITION_PARAMETER", params.get("REMARKS", VoltType.STRING));
        assertTrue(moveToMatchingRow(params, "PROCEDURE_NAME", "proc2"));
        assertEquals("param0", params.get("COLUMN_NAME", VoltType.STRING));
        assertEquals(VoltType.INTEGER.getLengthInBytesForFixedTypes() * 8 - 1,
                     params.get("PRECISION", VoltType.INTEGER));
        assertEquals(VoltType.INTEGER.getLengthInBytesForFixedTypes(),
                     params.get("LENGTH", VoltType.INTEGER));
        assertWithNullCheck(null, params.get("CHAR_OCTET_LENGTH", VoltType.INTEGER), params);
        assertWithNullCheck(null, params.get("REMARKS", VoltType.STRING), params);
    }
}