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

import java.util.Random;

import junit.framework.TestCase;

public class TestTableHelper extends TestCase {

    public void testShorthand() {
        VoltTable t = TableHelper.quickTable("FOO (BIGINT-N, BAR:TINYINT, A:VARCHAR12-U/'foo') PK(2,BAR)");
        assertEquals("C0", t.getColumnName(0));
        assertEquals("BAR", t.getColumnName(1));
        assertEquals("A", t.getColumnName(2));
        assertEquals(VoltType.BIGINT, t.getColumnType(0));
        assertEquals(VoltType.TINYINT, t.getColumnType(1));
        assertEquals(VoltType.STRING, t.getColumnType(2));
        assertEquals(false, t.getColumnUniqueness(0));
        assertEquals(false, t.getColumnUniqueness(1));
        assertEquals(true, t.getColumnUniqueness(2));
        assertEquals(VoltTable.ColumnInfo.NO_DEFAULT_VALUE, t.getColumnDefaultValue(0));
        assertEquals(VoltTable.ColumnInfo.NO_DEFAULT_VALUE, t.getColumnDefaultValue(1));
        assertEquals("foo", t.getColumnDefaultValue(2));
        assertEquals(false, t.getColumnNullable(0));
        assertEquals(true, t.getColumnNullable(1));
        assertEquals(true, t.getColumnNullable(2));
        assertEquals(12, t.getColumnMaxSize(2));
        assertEquals(-1, t.m_extraMetadata.partitionColIndex);

        System.out.println(TableHelper.ddlForTable(t));

        // try the same thing, but partitioned
        t = TableHelper.quickTable("FOO (BIGINT-N, BAR:TINYINT, A:VARCHAR12-U/'foo') P(A) PK(2,BAR)");
        assertEquals("C0", t.getColumnName(0));
        assertEquals("BAR", t.getColumnName(1));
        assertEquals("A", t.getColumnName(2));
        assertEquals(VoltType.BIGINT, t.getColumnType(0));
        assertEquals(VoltType.TINYINT, t.getColumnType(1));
        assertEquals(VoltType.STRING, t.getColumnType(2));
        assertEquals(false, t.getColumnUniqueness(0));
        assertEquals(false, t.getColumnUniqueness(1));
        assertEquals(true, t.getColumnUniqueness(2));
        assertEquals(VoltTable.ColumnInfo.NO_DEFAULT_VALUE, t.getColumnDefaultValue(0));
        assertEquals(VoltTable.ColumnInfo.NO_DEFAULT_VALUE, t.getColumnDefaultValue(1));
        assertEquals("foo", t.getColumnDefaultValue(2));
        assertEquals(false, t.getColumnNullable(0));
        assertEquals(true, t.getColumnNullable(1));
        assertEquals(true, t.getColumnNullable(2));
        assertEquals(12, t.getColumnMaxSize(2));
        assertEquals(2, t.m_extraMetadata.partitionColIndex);

        t = TableHelper.quickTable("Ryan (likes:smallint, TINYINT/'8', A:VARBINARY/'ABCD')");
        assertEquals("likes", t.getColumnName(0));
        assertEquals("C1", t.getColumnName(1));
        assertEquals("A", t.getColumnName(2));
        assertEquals(VoltType.SMALLINT, t.getColumnType(0));
        assertEquals(VoltType.TINYINT, t.getColumnType(1));
        assertEquals(VoltType.VARBINARY, t.getColumnType(2));
        assertEquals(false, t.getColumnUniqueness(0));
        assertEquals(false, t.getColumnUniqueness(1));
        assertEquals(false, t.getColumnUniqueness(2));
        assertEquals(VoltTable.ColumnInfo.NO_DEFAULT_VALUE, t.getColumnDefaultValue(0));
        assertEquals("8", t.getColumnDefaultValue(1));
        assertEquals("ABCD", t.getColumnDefaultValue(2));
        assertEquals(true, t.getColumnNullable(0));
        assertEquals(true, t.getColumnNullable(1));
        assertEquals(true, t.getColumnNullable(2));
        assertEquals(255, t.getColumnMaxSize(2));
        assertEquals(-1, t.m_extraMetadata.partitionColIndex);

        System.out.println(TableHelper.ddlForTable(t));

        // try the same thing, but partitioned
        t = TableHelper.quickTable("Ryan (likes:smallint, TINYINT/'8', A:VARBINARY/'ABCD') P(0)");
        assertEquals("likes", t.getColumnName(0));
        assertEquals("C1", t.getColumnName(1));
        assertEquals("A", t.getColumnName(2));
        assertEquals(VoltType.SMALLINT, t.getColumnType(0));
        assertEquals(VoltType.TINYINT, t.getColumnType(1));
        assertEquals(VoltType.VARBINARY, t.getColumnType(2));
        assertEquals(false, t.getColumnUniqueness(0));
        assertEquals(false, t.getColumnUniqueness(1));
        assertEquals(false, t.getColumnUniqueness(2));
        assertEquals(VoltTable.ColumnInfo.NO_DEFAULT_VALUE, t.getColumnDefaultValue(0));
        assertEquals("8", t.getColumnDefaultValue(1));
        assertEquals("ABCD", t.getColumnDefaultValue(2));
        assertEquals(true, t.getColumnNullable(0));
        assertEquals(true, t.getColumnNullable(1));
        assertEquals(true, t.getColumnNullable(2));
        assertEquals(255, t.getColumnMaxSize(2));
        assertEquals(0, t.m_extraMetadata.partitionColIndex);

        System.out.println(TableHelper.ddlForTable(t));
    }

    /**
     * Since this is test code for test code, some of this is verified manually. Should
     * be more automated.
     * @throws Exception
     */
    public void testDataGeneration() throws Exception {
        TableHelper.Configuration helperConfig = new TableHelper.Configuration();
        helperConfig.rand = new Random();
        TableHelper helper = new TableHelper(helperConfig);

        VoltTable t = TableHelper.quickTable("Ryan (likes:smallint, TINYINT/'8', A:VARBINARY/'ABCD')");
        helper.randomFill(t, 10, 128);

        t = TableHelper.quickTable("FOO (BIGINT-N, BAR:TINYINT, A:VARCHAR12-U/'foo') PK(2,BAR)");
        helper.randomFill(t, 10, 128);
        System.out.println(t.toString());

        VoltTable t2 = TableHelper.quickTable("FOO (BAR:DOUBLE, C0:BIGINT-N, A:VARCHAR14/'foo') PK(2,BAR)");

        TableHelper.migrateTable(t, t2);
        System.out.println(t2.toString());
    }

    /**
     * Create and mutate random tables. Just try to not crash here.
     * TestLiveTableSchemaMigration does more *real* stuff.
     * @throws Exception
     */
    public void testRandomTables() throws Exception {
        TableHelper helper = new TableHelper();
        for (int i = 0; i < 1000; i++) {
            TableHelper.RandomTable trt = helper.getTotallyRandomTable("foo");
            VoltTable t1 = trt.table;
            VoltTable t2 = helper.mutateTable(t1, true);
            TableHelper.getAlterTableDDLToMigrate(t1, t2);
            helper.randomFill(t1, 50, 32);
            TableHelper.migrateTable(t1, t2);
        }
    }

}
