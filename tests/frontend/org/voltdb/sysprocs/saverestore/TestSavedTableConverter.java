/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

public class TestSavedTableConverter extends TestCase
{
    private static final String TABLE_NAME = "TEST_TABLE";
    private static int DEFAULT_INT = 1234;

    @Override
    public void tearDown() throws Exception {
        m_catalogCreator.shutdown(null);
    }

    @Override
    public void setUp()
    {
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
    }

    // Test cases:
    // NON-TYPE-CAST-RELATED:
    // unchanged table copies correctly
    public void testUnchangedTable()
    {
        Table catalog_table = m_catalogCreator.getTable(TABLE_NAME);
        VoltTable input_table = null;

        input_table =
            CatalogUtil.getVoltTable(m_catalogCreator.getTable(TABLE_NAME));

        for (int i = 0; i < 10; i++)
        {
            input_table.addRow(i, "name_" + i, new Double(i), new Double(i));
        }

        VoltTable result = null;
        try
        {
            result = SavedTableConverter.convertTable(input_table,
                                                      catalog_table,
                                                      false);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            fail("SavedTableConverter.convert should not have thrown here");
            return;
        }

        assertEquals(input_table, result);
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

        Table catalog_table = m_catalogCreator.getTable(out);
        VoltTable input_table = null;

        input_table
                = CatalogUtil.getVoltTable(m_catalogCreator.getTable(in));

        //Add 10 Rows.
        for (int i = 0; i < 10; i++) {
            input_table.addRow(maxval);
        }

        VoltTable result = null;
        boolean failed = false;
        try {
            result = SavedTableConverter.convertTable(input_table,
                    catalog_table, false);
            result.resetRowPosition();
            assertEquals(10, result.getRowCount());
        } catch (Exception e) {
            failed = true;
        }
        assertTrue((failed == expectfailure));
    }

    // dropping a column fills in the remaining values, all correctly
    public void testDroppedColumn()
    {
        Table catalog_table = m_catalogCreator.getTable(TABLE_NAME);

        VoltTable input_table =
            new VoltTable(new ColumnInfo("HAS_DEFAULT", VoltType.INTEGER),
                          new ColumnInfo("HAS_NULLABLE_STRING",
                                         VoltType.STRING),
                          new ColumnInfo("HAS_NULLABLE_FLOAT",
                                         VoltType.FLOAT),
                          new ColumnInfo("HAS_NADA", VoltType.FLOAT),
                          new ColumnInfo("GOES_AWAY", VoltType.INTEGER));

        for (int i = 0; i < 10; i++)
        {
            input_table.addRow(i, "name_" + i, new Double(i), new Double(i), i);
        }

        VoltTable result = null;
        try
        {
            result = SavedTableConverter.convertTable(input_table,
                                                      catalog_table,
                                                      false);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            fail("SavedTableConverter.convert should not have thrown here");
            return;
        }

        assertEquals(4, result.getColumnCount());
        for (int i = 0; i < 10; i++)
        {
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
    public void testAddDefaultColumn()
    {
        Table catalog_table = m_catalogCreator.getTable(TABLE_NAME);

        VoltTable input_table =
            new VoltTable(new ColumnInfo("HAS_NULLABLE_STRING", VoltType.STRING),
                          new ColumnInfo("HAS_NULLABLE_FLOAT", VoltType.FLOAT),
                          new ColumnInfo("HAS_NADA", VoltType.FLOAT));

        for (int i = 0; i < 10; i++)
        {
            input_table.addRow("name_" + i, new Double(i), new Double(i));
        }

        VoltTable result = null;
        try
        {
            result = SavedTableConverter.convertTable(input_table,
                                                      catalog_table,
                                                      false);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            fail("SavedTableConverter.convert should not have thrown here");
            return;
        }

        assertEquals(4, result.getColumnCount());
        for (int i = 0; i < 10; i++)
        {
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
    public void testAddNullColumn()
    {
        Table catalog_table = m_catalogCreator.getTable(TABLE_NAME);

        VoltTable input_table =
            new VoltTable(new ColumnInfo("HAS_DEFAULT", VoltType.INTEGER),
                          new ColumnInfo("HAS_NADA", VoltType.FLOAT));

        for (int i = 0; i < 10; i++)
        {
            input_table.addRow(i, new Double(i));
        }

        VoltTable result = null;
        try
        {
            result = SavedTableConverter.convertTable(input_table,
                                                      catalog_table,
                                                      false);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            fail("SavedTableConverter.convert should not have thrown here");
            return;
        }

        assertEquals(4, result.getColumnCount());
        for (int i = 0; i < 10; i++)
        {
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
    public void testNoDefaultNoNullBoom()
    {
        Table catalog_table = m_catalogCreator.getTable(TABLE_NAME);

        VoltTable input_table =
            new VoltTable(new ColumnInfo("HAS_DEFAULT", VoltType.INTEGER),
                          new ColumnInfo("HAS_NULLABLE_STRING",
                                         VoltType.STRING),
                          new ColumnInfo("HAS_NULLABLE_FLOAT",
                                         VoltType.FLOAT));

        for (int i = 0; i < 10; i++)
        {
            input_table.addRow(i, "name_" + i, new Double(i));
        }

        try
        {
            SavedTableConverter.convertTable(input_table, catalog_table, false);
        }
        catch (Exception e)
        {
            assertTrue(true);
            return;
        }
        fail("SavedTableConverter should have thrown an exception");
    }

    public void testDRPassiveToActive() {
        Table catalog_table = m_catalogCreator.getTable(TABLE_NAME);

        VoltTable input_table_without_dr_col =
                new VoltTable(new ColumnInfo("HAS_DEFAULT", VoltType.INTEGER),
                        new ColumnInfo("HAS_NULLABLE_STRING", VoltType.STRING),
                        new ColumnInfo("HAS_NULLABLE_FLOAT", VoltType.FLOAT),
                        new ColumnInfo("HAS_NADA", VoltType.FLOAT));

        assertFalse(SavedTableConverter.needsConversion(input_table_without_dr_col, catalog_table, false));
        assertTrue(SavedTableConverter.needsConversion(input_table_without_dr_col, catalog_table, true));

        for (int i = 0; i < 10; i++)
        {
            input_table_without_dr_col.addRow(i, Integer.toString(i), new Double(i), new Double(i));
        }

        VoltTable result = null;
        try
        {
            result = SavedTableConverter.convertTable(input_table_without_dr_col, catalog_table, true);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            fail("SavedTableConverter.convert should not have thrown here");
        }

        assertEquals(5, result.getColumnCount());
        for (int i = 0; i < 10; i++)
        {
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
        Table catalog_table = m_catalogCreator.getTable(TABLE_NAME);

        VoltTable input_table_different =
                new VoltTable(new ColumnInfo("HAS_DEFAULT", VoltType.INTEGER),
                        new ColumnInfo("HAS_NADA", VoltType.FLOAT),
                        new ColumnInfo(CatalogUtil.DR_HIDDEN_COLUMN_NAME, VoltType.BIGINT));

        VoltTable input_table_identical =
                new VoltTable(new ColumnInfo("HAS_DEFAULT", VoltType.INTEGER),
                        new ColumnInfo("HAS_NULLABLE_STRING", VoltType.STRING),
                        new ColumnInfo("HAS_NULLABLE_FLOAT", VoltType.FLOAT),
                        new ColumnInfo("HAS_NADA", VoltType.FLOAT),
                        new ColumnInfo(CatalogUtil.DR_HIDDEN_COLUMN_NAME, VoltType.BIGINT));

        // Missing HAS_NULLABLE_STRING and HAS_NULLABLE_FLOAT
        assertTrue(SavedTableConverter.needsConversion(input_table_different, catalog_table, false));
        assertTrue(SavedTableConverter.needsConversion(input_table_different, catalog_table, true));
        // Extra DR Column in input
        assertTrue(SavedTableConverter.needsConversion(input_table_identical, catalog_table, false));

        for (int i = 0; i < 10; i++)
        {
            input_table_identical.addRow(i, Integer.toString(i), new Double(i), new Double(i), new Long(i));
        }

        VoltTable result = null;
        try
        {
            result = SavedTableConverter.convertTable(input_table_identical, catalog_table, false);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            fail("SavedTableConverter.convert should not have thrown here");
        }

        assertEquals(4, result.getColumnCount());
        for (int i = 0; i < 10; i++)
        {
            VoltTableRow row = result.fetchRow(i);
            assertEquals(i, row.getLong("HAS_DEFAULT"));
            assertEquals(Integer.toString(i), row.getString("HAS_NULLABLE_STRING"));
            assertEquals(new Double(i), row.getDouble("HAS_NULLABLE_FLOAT"));
            assertEquals(new Double(i), row.getDouble("HAS_NADA"));
        }
    }

    public void testDRActiveToActiveMissing2Cols() {
        Table catalog_table = m_catalogCreator.getTable(TABLE_NAME);

        VoltTable input_table =
                new VoltTable(new ColumnInfo("HAS_DEFAULT", VoltType.INTEGER),
                        new ColumnInfo("HAS_NADA", VoltType.FLOAT),
                        new ColumnInfo(CatalogUtil.DR_HIDDEN_COLUMN_NAME, VoltType.BIGINT));

        assertTrue(SavedTableConverter.needsConversion(input_table, catalog_table, true));

        for (int i = 0; i < 10; i++)
        {
            input_table.addRow(i, new Double(i), new Long(i));
        }

        VoltTable result = null;
        try
        {
            result = SavedTableConverter.convertTable(input_table, catalog_table, true);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            fail("SavedTableConverter.convert should not have thrown here");
        }

        assertEquals(5, result.getColumnCount());
        for (int i = 0; i < 10; i++)
        {
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
        Table catalog_table = m_catalogCreator.getTable(TABLE_NAME);

        VoltTable input_table_with_dr_col =
                new VoltTable(new ColumnInfo("HAS_DEFAULT", VoltType.INTEGER),
                        new ColumnInfo("HAS_NULLABLE_STRING", VoltType.STRING),
                        new ColumnInfo("HAS_NULLABLE_FLOAT", VoltType.FLOAT),
                        new ColumnInfo("HAS_NADA", VoltType.FLOAT),
                        new ColumnInfo(CatalogUtil.DR_HIDDEN_COLUMN_NAME, VoltType.BIGINT));

        assertTrue(SavedTableConverter.needsConversion(input_table_with_dr_col, catalog_table, false));
        assertFalse(SavedTableConverter.needsConversion(input_table_with_dr_col, catalog_table, true));
    }

    private MockVoltDB m_catalogCreator;
}
