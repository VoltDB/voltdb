/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

package org.voltdb.sysprocs.saverestore;

import java.math.BigDecimal;

import org.voltdb.MockVoltDB;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.catalog.Table;
import org.voltdb.utils.CatalogUtil;

import junit.framework.TestCase;

public class TestSavedTableConverter extends TestCase {
    private static final String TABLE_NAME = "TEST_TABLE";
    private static int DEFAULT_INT = 1234;

    @Override
    public void tearDown() throws Exception {
        m_catalogCreator.shutdown(null);
        m_catalogTable = null;
    }

    @Override
    public void setUp() {
        m_catalogCreator = new MockVoltDB();
        m_catalogCreator.addTable(TABLE_NAME, false);
        m_catalogCreator.addColumnToTable(TABLE_NAME, "HAS_DEFAULT",
                                          VoltType.INTEGER, true,
                                          Integer.toString(DEFAULT_INT),
                                          VoltType.INTEGER);
        m_catalogCreator.addColumnToTable(TABLE_NAME, "HAS_NULLABLE_STRING",
                                          VoltType.STRING, true, "",
                                          VoltType.INVALID);
        m_catalogCreator.addColumnToTable(TABLE_NAME, "HAS_NULLABLE_FLOAT",
                                          VoltType.FLOAT, true, "",
                                          VoltType.INVALID);
        m_catalogCreator.addColumnToTable(TABLE_NAME, "HAS_NADA",
                VoltType.FLOAT, false, "",
                VoltType.INVALID);
        m_catalogTable = m_catalogCreator.getTable(TABLE_NAME);
    }

    // Test cases:
    // NON-TYPE-CAST-RELATED:
    // unchanged table copies correctly
    public void testUnchangedTable() {
        VoltTable inputTable =  CatalogUtil.getVoltTable(m_catalogTable);
        for (int i = 0; i < 10; i++) {
            inputTable.addRow(i, "name_" + i, new Double(i), new Double(i));
        }

        VoltTable result = null;
        try {
            result = SavedTableConverter.convertTable(inputTable,
                                                      m_catalogTable,
                                                      false,
                                                      false,
                                                      false,
                                                      true);
        } catch (Exception e) {
            e.printStackTrace();
            fail("SavedTableConverter.convert should not have thrown here");
            return;
        }

        assertEquals(inputTable, result);
    }

    // Test the convert table with just the type change.
    public void testUnchangedTableWithTypeChange() {
        String inp = "i";
        String outp = "o";
        VoltType itypes[] = {VoltType.TINYINT, VoltType.SMALLINT, VoltType.INTEGER,
            VoltType.FLOAT, VoltType.DECIMAL
        };
        Object max_vals[] = {Byte.MAX_VALUE, Short.MAX_VALUE, Integer.MAX_VALUE,
            Double.MAX_VALUE, new BigDecimal("99999999999999999999999999.999999999999")
        };
        System.out.println("All Tests Should succeed.");
        //You should be able to convert anything after you.
        for (int i = 0; i < itypes.length; i++) {
            for (int j = i; j < itypes.length - i; j++) {
                String in = inp + i + j;
                String out = outp + i + j;
                runNumberConversion(in, out, itypes[i], itypes[j], false, max_vals[i]);
            }
        }
        System.out.println("All Tests Should Fail.");
        // All should fail
        for (int i = 1; i < itypes.length - 1; i++) {
            for (int j = i - 1; j >= 0; j--) {
                String in = inp + "x" + i + j;
                String out = outp + "x" + i + j;
                runNumberConversion(in, out, itypes[i], itypes[j], true, max_vals[i]);
            }
        }

        // Converting from decimal to double should succeed (this is assumed to be lossy anyway)
        int i = itypes.length - 1;
        int j = i - 1;
        String in = inp + "x" + i + j;
        String out = outp + "x" + i + j;
        runNumberConversion(in, out, itypes[i], itypes[j], false, max_vals[i]);

        // These out-of-range conversions should fail
        for (j = i - 2; j >= 0; j--) {
            in = inp + "x" + i + j;
            out = outp + "x" + i + j;
            runNumberConversion(in, out, itypes[i], itypes[j], true, max_vals[i]);
        }
    }

    //Run conversions for numbers.
    private void runNumberConversion(String in, String out, VoltType tin, VoltType tout, boolean expectfailure, Object maxval) {

        System.out.println("Testing : Input table: " + in + " Output Table: " + out
                + " In Type: " + tin.name() + " Out Type: " + tout.name()
                + " Class: " + tout.classFromType().getName());
        m_catalogCreator.addTable(in, false);
        m_catalogCreator.addColumnToTable(in, "HAS_DEFAULT",
                tin, true,
                "INPUT_NUMBER",
                tin);

        m_catalogCreator.addTable(out, false);
        m_catalogCreator.addColumnToTable(out, "HAS_DEFAULT",
                tout, true,
                "OUTPUT_NUMBER",
                tout);

        Table catalogTable = m_catalogCreator.getTable(out);
        VoltTable inputTable = CatalogUtil.getVoltTable(m_catalogCreator.getTable(in));

        //Add 10 Rows.
        for (int i = 0; i < 10; i++) {
            inputTable.addRow(maxval);
        }

        VoltTable result = null;
        boolean failed = false;
        try {
            result = SavedTableConverter.convertTable(inputTable,
                    catalogTable, false, false, false, true);
            result.resetRowPosition();
            assertEquals(10, result.getRowCount());
        } catch (Exception e) {
            failed = true;
        }
        assertTrue((failed == expectfailure));
    }

    // dropping a column fills in the remaining values, all correctly
    public void testDroppedColumn() {
        VoltTable inputTable =
            new VoltTable(new ColumnInfo("HAS_DEFAULT", VoltType.INTEGER),
                          new ColumnInfo("HAS_NULLABLE_STRING",
                                         VoltType.STRING),
                          new ColumnInfo("HAS_NULLABLE_FLOAT",
                                         VoltType.FLOAT),
                          new ColumnInfo("HAS_NADA", VoltType.FLOAT),
                          new ColumnInfo("GOES_AWAY", VoltType.INTEGER));

        for (int i = 0; i < 10; i++) {
            inputTable.addRow(i, "name_" + i, new Double(i), new Double(i), i);
        }

        VoltTable result = null;
        try {
            result = SavedTableConverter.convertTable(inputTable,
                                                      m_catalogTable,
                                                      false, false, false, true);
        } catch (Exception e) {
            e.printStackTrace();
            fail("SavedTableConverter.convert should not have thrown here");
            return;
        }

        assertEquals(4, result.getColumnCount());
        for (int i = 0; i < 10; i++) {
            assertEquals(i, result.fetchRow(i).getLong("HAS_DEFAULT"));
            assertEquals("name_" + i,
                         result.fetchRow(i).getString("HAS_NULLABLE_STRING"));
            assertEquals(new Double(i),
                         result.fetchRow(i).getDouble("HAS_NULLABLE_FLOAT"));
            assertEquals(new Double(i),
                         result.fetchRow(i).getDouble("HAS_NADA"));
        }
    }

    // adding a column with a default value adds the column filled with default
    public void testAddDefaultColumn() {
        VoltTable inputTable =
            new VoltTable(new ColumnInfo("HAS_NULLABLE_STRING", VoltType.STRING),
                          new ColumnInfo("HAS_NULLABLE_FLOAT", VoltType.FLOAT),
                          new ColumnInfo("HAS_NADA", VoltType.FLOAT));

        for (int i = 0; i < 10; i++) {
            inputTable.addRow("name_" + i, new Double(i), new Double(i));
        }

        VoltTable result = null;
        try {
            result = SavedTableConverter.convertTable(inputTable,
                                                      m_catalogTable,
                                                      false, false, false, true);
        } catch (Exception e) {
            e.printStackTrace();
            fail("SavedTableConverter.convert should not have thrown here");
            return;
        }

        assertEquals(4, result.getColumnCount());
        for (int i = 0; i < 10; i++) {
            assertEquals(DEFAULT_INT,
                    result.fetchRow(i).getLong("HAS_DEFAULT"));
            assertEquals("name_" + i,
                         result.fetchRow(i).getString("HAS_NULLABLE_STRING"));
            assertEquals(new Double(i),
                    result.fetchRow(i).getDouble("HAS_NULLABLE_FLOAT"));
            assertEquals(new Double(i),
                         result.fetchRow(i).getDouble("HAS_NADA"));
        }
    }

    // adding columns with no default but nullable adds the
    // columns filled with null
    public void testAddNullColumn() {
        VoltTable inputTable =
            new VoltTable(new ColumnInfo("HAS_DEFAULT", VoltType.INTEGER),
                          new ColumnInfo("HAS_NADA", VoltType.FLOAT));

        for (int i = 0; i < 10; i++) {
            inputTable.addRow(i, new Double(i));
        }

        VoltTable result = null;
        try {
            result = SavedTableConverter.convertTable(inputTable,
                                                      m_catalogTable,
                                                      false, false, false, true);
        } catch (Exception e) {
            e.printStackTrace();
            fail("SavedTableConverter.convert should not have thrown here");
            return;
        }

        assertEquals(4, result.getColumnCount());
        for (int i = 0; i < 10; i++) {
            VoltTableRow row = result.fetchRow(i);
            assertEquals(i, row.getLong("HAS_DEFAULT"));
            assertEquals(null, row.getString("HAS_NULLABLE_STRING"));
            assertTrue(row.wasNull());
            assertEquals(VoltType.NULL_FLOAT,
                         row.getDouble("HAS_NULLABLE_FLOAT"));
            assertTrue(row.wasNull());
            assertEquals(new Double(i), row.getDouble("HAS_NADA"));
        }
    }

    // adding a column with no default and non-nullable results in fail
    public void testNoDefaultNoNullBoom() {
        VoltTable inputTable =
            new VoltTable(new ColumnInfo("HAS_DEFAULT", VoltType.INTEGER),
                          new ColumnInfo("HAS_NULLABLE_STRING",
                                         VoltType.STRING),
                          new ColumnInfo("HAS_NULLABLE_FLOAT",
                                         VoltType.FLOAT));

        for (int i = 0; i < 10; i++) {
            inputTable.addRow(i, "name_" + i, new Double(i));
        }

        try {
            SavedTableConverter.convertTable(inputTable, m_catalogTable, false, false, false, true);
        } catch (Exception e) {
            assertTrue(true);
            return;
        }
        fail("SavedTableConverter should have thrown an exception");
    }

    public void testDRPassiveToActive() {
        VoltTable inputTableWithoutDRCol =
                new VoltTable(new ColumnInfo("HAS_DEFAULT", VoltType.INTEGER),
                        new ColumnInfo("HAS_NULLABLE_STRING", VoltType.STRING),
                        new ColumnInfo("HAS_NULLABLE_FLOAT", VoltType.FLOAT),
                        new ColumnInfo("HAS_NADA", VoltType.FLOAT));

        assertFalse(SavedTableConverter.needsConversion(inputTableWithoutDRCol, m_catalogTable, false, false, false, true));
        assertTrue(SavedTableConverter.needsConversion(inputTableWithoutDRCol, m_catalogTable, true, false, false, true));

        for (int i = 0; i < 10; i++) {
            inputTableWithoutDRCol.addRow(i, Integer.toString(i), new Double(i), new Double(i));
        }

        VoltTable result = null;
        try {
            result = SavedTableConverter.convertTable(inputTableWithoutDRCol, m_catalogTable, true, false, false, true);
        } catch (Exception e) {
            e.printStackTrace();
            fail("SavedTableConverter.convert should not have thrown here");
        }

        assertEquals(5, result.getColumnCount());
        for (int i = 0; i < 10; i++) {
            VoltTableRow row = result.fetchRow(i);
            assertEquals(i, row.getLong("HAS_DEFAULT"));
            assertEquals(Integer.toString(i), row.getString("HAS_NULLABLE_STRING"));
            assertEquals(new Double(i), row.getDouble("HAS_NULLABLE_FLOAT"));
            assertEquals(new Double(i), row.getDouble("HAS_NADA"));
            assertEquals(VoltType.NULL_BIGINT, row.getLong(4));
            assertEquals(VoltType.NULL_BIGINT, row.getLong(CatalogUtil.DR_HIDDEN_COLUMN_NAME));
        }
    }

    public void testDRActiveToPassive() {
        VoltTable inputTableDifferent =
                new VoltTable(new ColumnInfo("HAS_DEFAULT", VoltType.INTEGER),
                        new ColumnInfo("HAS_NADA", VoltType.FLOAT),
                        new ColumnInfo(CatalogUtil.DR_HIDDEN_COLUMN_NAME, VoltType.BIGINT));

        VoltTable inputTableIdentical =
                new VoltTable(new ColumnInfo("HAS_DEFAULT", VoltType.INTEGER),
                        new ColumnInfo("HAS_NULLABLE_STRING", VoltType.STRING),
                        new ColumnInfo("HAS_NULLABLE_FLOAT", VoltType.FLOAT),
                        new ColumnInfo("HAS_NADA", VoltType.FLOAT),
                        new ColumnInfo(CatalogUtil.DR_HIDDEN_COLUMN_NAME, VoltType.BIGINT));

        // Missing HAS_NULLABLE_STRING and HAS_NULLABLE_FLOAT
        assertTrue(SavedTableConverter.needsConversion(inputTableDifferent, m_catalogTable, false, false, false, true));
        assertTrue(SavedTableConverter.needsConversion(inputTableDifferent, m_catalogTable, true, false, false, true));
        // Extra DR Column in input
        assertTrue(SavedTableConverter.needsConversion(inputTableIdentical, m_catalogTable, false, false, false, true));

        for (int i = 0; i < 10; i++) {
            inputTableIdentical.addRow(i, Integer.toString(i), new Double(i), new Double(i), new Long(i));
        }

        VoltTable result = null;
        try {
            result = SavedTableConverter.convertTable(inputTableIdentical, m_catalogTable, false, false, false, true);
        } catch (Exception e) {
            e.printStackTrace();
            fail("SavedTableConverter.convert should not have thrown here");
        }

        assertEquals(4, result.getColumnCount());
        for (int i = 0; i < 10; i++) {
            VoltTableRow row = result.fetchRow(i);
            assertEquals(i, row.getLong("HAS_DEFAULT"));
            assertEquals(Integer.toString(i), row.getString("HAS_NULLABLE_STRING"));
            assertEquals(new Double(i), row.getDouble("HAS_NULLABLE_FLOAT"));
            assertEquals(new Double(i), row.getDouble("HAS_NADA"));
        }
    }

    public void testDRActiveToActiveMissing2Cols() {
        VoltTable inputTable =
                new VoltTable(new ColumnInfo("HAS_DEFAULT", VoltType.INTEGER),
                        new ColumnInfo("HAS_NADA", VoltType.FLOAT),
                        new ColumnInfo(CatalogUtil.DR_HIDDEN_COLUMN_NAME, VoltType.BIGINT));

        assertTrue(SavedTableConverter.needsConversion(inputTable, m_catalogTable, true, false, false, true));

        for (int i = 0; i < 10; i++) {
            inputTable.addRow(i, new Double(i), new Long(i));
        }

        VoltTable result = null;
        try {
            result = SavedTableConverter.convertTable(inputTable, m_catalogTable, true, false, false, true);
        } catch (Exception e) {
            e.printStackTrace();
            fail("SavedTableConverter.convert should not have thrown here");
        }

        assertEquals(5, result.getColumnCount());
        for (int i = 0; i < 10; i++) {
            VoltTableRow row = result.fetchRow(i);
            assertEquals(i, row.getLong("HAS_DEFAULT"));
            assertEquals(null, row.getString("HAS_NULLABLE_STRING"));
            assertTrue(row.wasNull());
            assertEquals(VoltType.NULL_FLOAT,
                    row.getDouble("HAS_NULLABLE_FLOAT"));
            assertTrue(row.wasNull());
            assertEquals(new Double(i), row.getDouble("HAS_NADA"));
            assertEquals(i, row.getLong(4));
            assertEquals(i, row.getLong(CatalogUtil.DR_HIDDEN_COLUMN_NAME));
        }
    }

    public void testDRActiveToActiveIdentical() {
        VoltTable inputTableWithDRCol =
                new VoltTable(new ColumnInfo("HAS_DEFAULT", VoltType.INTEGER),
                        new ColumnInfo("HAS_NULLABLE_STRING", VoltType.STRING),
                        new ColumnInfo("HAS_NULLABLE_FLOAT", VoltType.FLOAT),
                        new ColumnInfo("HAS_NADA", VoltType.FLOAT),
                        new ColumnInfo(CatalogUtil.DR_HIDDEN_COLUMN_NAME, VoltType.BIGINT));

        assertTrue(SavedTableConverter.needsConversion(inputTableWithDRCol, m_catalogTable, false, false, false, true));
        assertFalse(SavedTableConverter.needsConversion(inputTableWithDRCol, m_catalogTable, true, false, false, true));
    }

    public void testViewWithoutCountStar() {
        VoltTable inputTableWithViewCol =
                new VoltTable(new ColumnInfo("HAS_DEFAULT", VoltType.INTEGER),
                        new ColumnInfo("HAS_NULLABLE_STRING", VoltType.STRING),
                        new ColumnInfo("HAS_NULLABLE_FLOAT", VoltType.FLOAT),
                        new ColumnInfo("HAS_NADA", VoltType.FLOAT),
                        new ColumnInfo(CatalogUtil.VIEW_HIDDEN_COLUMN_NAME, VoltType.BIGINT));
        assertTrue(SavedTableConverter.needsConversion(inputTableWithViewCol, m_catalogTable, false, false, false, true));
        assertFalse(SavedTableConverter.needsConversion(inputTableWithViewCol, m_catalogTable, false, true, false, true));
    }

    public void testDRPassiveMigrateToActiveNoMigrate() {
        VoltTable inputTableWithoutDRColWithMigrate =
                new VoltTable(new ColumnInfo("HAS_DEFAULT", VoltType.INTEGER),
                        new ColumnInfo("HAS_NULLABLE_STRING", VoltType.STRING),
                        new ColumnInfo("HAS_NULLABLE_FLOAT", VoltType.FLOAT),
                        new ColumnInfo("HAS_NADA", VoltType.FLOAT),
                        new ColumnInfo(CatalogUtil.MIGRATE_HIDDEN_COLUMN_NAME, VoltType.BIGINT));

        assertFalse(SavedTableConverter.needsConversion(inputTableWithoutDRColWithMigrate, m_catalogTable, false, false, true, true));
        assertTrue(SavedTableConverter.needsConversion(inputTableWithoutDRColWithMigrate, m_catalogTable, false, false, true, false));

        for (int i = 0; i < 10; i++) {
            inputTableWithoutDRColWithMigrate.addRow(i, Integer.toString(i), new Double(i), new Double(i), i);
        }

        VoltTable result = null;
        try {
            result = SavedTableConverter.convertTable(inputTableWithoutDRColWithMigrate, m_catalogTable, true, false, false, true);
        } catch (Exception e) {
            e.printStackTrace();
            fail("SavedTableConverter.convert should not have thrown here");
        }

        assertEquals(5, result.getColumnCount());
        for (int i = 0; i < 10; i++) {
            VoltTableRow row = result.fetchRow(i);
            assertEquals(i, row.getLong("HAS_DEFAULT"));
            assertEquals(Integer.toString(i), row.getString("HAS_NULLABLE_STRING"));
            assertEquals(new Double(i), row.getDouble("HAS_NULLABLE_FLOAT"));
            assertEquals(new Double(i), row.getDouble("HAS_NADA"));
            assertEquals(VoltType.NULL_BIGINT, row.getLong(4));
            assertEquals(VoltType.NULL_BIGINT, row.getLong(CatalogUtil.DR_HIDDEN_COLUMN_NAME));
        }

        inputTableWithoutDRColWithMigrate.resetRowPosition();
        VoltTable result2 = null;
        try {
            result2 = SavedTableConverter.convertTable(inputTableWithoutDRColWithMigrate, m_catalogTable, true, false, false, false);
        } catch (Exception e) {
            e.printStackTrace();
            fail("SavedTableConverter.convert should not have thrown here");
        }

        assertEquals(5, result2.getColumnCount());
        for (int i = 0; i < 10; i++) {
            VoltTableRow row = result2.fetchRow(i);
            assertEquals(i, row.getLong("HAS_DEFAULT"));
            assertEquals(Integer.toString(i), row.getString("HAS_NULLABLE_STRING"));
            assertEquals(new Double(i), row.getDouble("HAS_NULLABLE_FLOAT"));
            assertEquals(new Double(i), row.getDouble("HAS_NADA"));
            assertEquals(VoltType.NULL_BIGINT, row.getLong(4));
            assertEquals(VoltType.NULL_BIGINT, row.getLong(CatalogUtil.DR_HIDDEN_COLUMN_NAME));
        }
    }

    public void testDRActiveNoMigrateToPassiveMigrate() {
        VoltTable inputTableDifferent =
                new VoltTable(new ColumnInfo("HAS_DEFAULT", VoltType.INTEGER),
                        new ColumnInfo("HAS_NADA", VoltType.FLOAT),
                        new ColumnInfo(CatalogUtil.DR_HIDDEN_COLUMN_NAME, VoltType.BIGINT));

        VoltTable inputTableIdentical =
                new VoltTable(new ColumnInfo("HAS_DEFAULT", VoltType.INTEGER),
                        new ColumnInfo("HAS_NULLABLE_STRING", VoltType.STRING),
                        new ColumnInfo("HAS_NULLABLE_FLOAT", VoltType.FLOAT),
                        new ColumnInfo("HAS_NADA", VoltType.FLOAT),
                        new ColumnInfo(CatalogUtil.DR_HIDDEN_COLUMN_NAME, VoltType.BIGINT));

        assertTrue(SavedTableConverter.needsConversion(inputTableDifferent, m_catalogTable, false, false, true, true));
        assertTrue(SavedTableConverter.needsConversion(inputTableDifferent, m_catalogTable, false, false, true, false));
        // Remove DR Column and Add Migrate Column
        assertTrue(SavedTableConverter.needsConversion(inputTableIdentical, m_catalogTable, false, false, true, true));
        assertTrue(SavedTableConverter.needsConversion(inputTableIdentical, m_catalogTable, false, false, true, false));
        // Add Migrate Column After DR Column
        assertTrue(SavedTableConverter.needsConversion(inputTableIdentical, m_catalogTable, true, false, true, true));
        assertTrue(SavedTableConverter.needsConversion(inputTableIdentical, m_catalogTable, true, false, true, false));

        for (int i = 0; i < 10; i++) {
            inputTableIdentical.addRow(i, Integer.toString(i), new Double(i), new Double(i), new Long(i));
        }

        VoltTable result = null;
        try {
            result = SavedTableConverter.convertTable(inputTableIdentical, m_catalogTable, false, false, true, true);
        } catch (Exception e) {
            e.printStackTrace();
            fail("SavedTableConverter.convert should not have thrown here");
        }

        assertEquals(5, result.getColumnCount());
        for (int i = 0; i < 10; i++) {
            VoltTableRow row = result.fetchRow(i);
            assertEquals(i, row.getLong("HAS_DEFAULT"));
            assertEquals(Integer.toString(i), row.getString("HAS_NULLABLE_STRING"));
            assertEquals(new Double(i), row.getDouble("HAS_NULLABLE_FLOAT"));
            assertEquals(new Double(i), row.getDouble("HAS_NADA"));
            assertEquals(VoltType.NULL_BIGINT, row.getLong(CatalogUtil.MIGRATE_HIDDEN_COLUMN_NAME));
        }

        inputTableIdentical.resetRowPosition();
        VoltTable result2 = null;
        try {
            result2 = SavedTableConverter.convertTable(inputTableIdentical, m_catalogTable, false, false, true, true);
        } catch (Exception e) {
            e.printStackTrace();
            fail("SavedTableConverter.convert should not have thrown here");
        }

        assertEquals(5, result2.getColumnCount());
        for (int i = 0; i < 10; i++) {
            VoltTableRow row = result2.fetchRow(i);
            assertEquals(i, row.getLong("HAS_DEFAULT"));
            assertEquals(Integer.toString(i), row.getString("HAS_NULLABLE_STRING"));
            assertEquals(new Double(i), row.getDouble("HAS_NULLABLE_FLOAT"));
            assertEquals(new Double(i), row.getDouble("HAS_NADA"));
            assertEquals(VoltType.NULL_BIGINT, row.getLong(CatalogUtil.MIGRATE_HIDDEN_COLUMN_NAME));
        }

    }

    public void testMigrateToMigrateRestore() {
        VoltTable inputTableDifferent =
                new VoltTable(new ColumnInfo("HAS_DEFAULT", VoltType.INTEGER),
                        new ColumnInfo("HAS_NADA", VoltType.FLOAT),
                        new ColumnInfo(CatalogUtil.MIGRATE_HIDDEN_COLUMN_NAME, VoltType.BIGINT));

        VoltTable inputTableIdentical =
                new VoltTable(new ColumnInfo("HAS_DEFAULT", VoltType.INTEGER),
                        new ColumnInfo("HAS_NULLABLE_STRING", VoltType.STRING),
                        new ColumnInfo("HAS_NULLABLE_FLOAT", VoltType.FLOAT),
                        new ColumnInfo("HAS_NADA", VoltType.FLOAT),
                        new ColumnInfo(CatalogUtil.MIGRATE_HIDDEN_COLUMN_NAME, VoltType.BIGINT));

        assertTrue(SavedTableConverter.needsConversion(inputTableDifferent, m_catalogTable, false, false, true, true));
        assertTrue(SavedTableConverter.needsConversion(inputTableDifferent, m_catalogTable, false, false, true, false));

        // Same Table schema with Recover and Restore
        assertFalse(SavedTableConverter.needsConversion(inputTableIdentical, m_catalogTable, false, false, true, true));
        assertTrue(SavedTableConverter.needsConversion(inputTableIdentical, m_catalogTable, false, false, true, false));

        for (int i = 0; i < 10; i++) {
            inputTableIdentical.addRow(i, Integer.toString(i), new Double(i), new Double(i), new Long(i));
        }

        VoltTable result = null;
        try {
            result = SavedTableConverter.convertTable(inputTableIdentical, m_catalogTable, false, false, true, false);
        } catch (Exception e) {
            e.printStackTrace();
            fail("SavedTableConverter.convert should not have thrown here");
        }

        assertEquals(5, result.getColumnCount());
        for (int i = 0; i < 10; i++) {
            VoltTableRow row = result.fetchRow(i);
            assertEquals(i, row.getLong("HAS_DEFAULT"));
            assertEquals(Integer.toString(i), row.getString("HAS_NULLABLE_STRING"));
            assertEquals(new Double(i), row.getDouble("HAS_NULLABLE_FLOAT"));
            assertEquals(new Double(i), row.getDouble("HAS_NADA"));
            assertEquals(VoltType.NULL_BIGINT, row.getLong(CatalogUtil.MIGRATE_HIDDEN_COLUMN_NAME));
        }
    }

    private MockVoltDB m_catalogCreator;
    private Table m_catalogTable;
}
