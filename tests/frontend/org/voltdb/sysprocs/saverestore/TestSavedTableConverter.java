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

package org.voltdb.sysprocs.saverestore;

import org.voltdb.MockVoltDB;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltTableRow;
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
                                                      catalog_table);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            fail("SavedTableConverter.convert should not have thrown here");
            return;
        }

        assertEquals(input_table, result);
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
                                                      catalog_table);
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
                                                      catalog_table);
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
                                                      catalog_table);
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
            SavedTableConverter.convertTable(input_table, catalog_table);
        }
        catch (Exception e)
        {
            assertTrue(true);
            return;
        }
        fail("SavedTableConverter should have thrown an exception");
    }

    private MockVoltDB m_catalogCreator;
}
