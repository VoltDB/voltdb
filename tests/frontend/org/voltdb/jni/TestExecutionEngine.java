/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

package org.voltdb.jni;

import org.voltdb.VoltDB;
import org.voltdb.SysProcSelector;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.LoadCatalogToString;
import junit.framework.TestCase;

/**
 * Tests native execution engine JNI interface.
 */
public class TestExecutionEngine extends TestCase {

    public void testLoadCatalogs() throws Exception {
        Catalog catalog = new Catalog();
        catalog.execute(LoadCatalogToString.THE_CATALOG);
        engine.loadCatalog(catalog);
    }

    public void testMultiSiteInSamePhysicalNodeWithExecutionSite() throws Exception {
        // TODO
    }

    private void loadTestTables(Catalog catalog) throws Exception
    {
        int WAREHOUSE_TABLEID = catalog.getClusters().get("cluster").getDatabases().get("database").getTables().get("WAREHOUSE").getRelativeIndex();
        int STOCK_TABLEID = catalog.getClusters().get("cluster").getDatabases().get("database").getTables().get("STOCK").getRelativeIndex();

        VoltTable warehousedata = new VoltTable(
                new VoltTable.ColumnInfo("W_ID", VoltType.INTEGER),
                new VoltTable.ColumnInfo("W_NAME", VoltType.STRING)
        );
        for (int i = 0; i < 200; ++i) {
            warehousedata.addRow(i, "str" + i);
        }

        System.out.println(warehousedata.toString());

        engine.loadTable(WAREHOUSE_TABLEID, warehousedata, 0, 0, Long.MAX_VALUE);

        VoltTable stockdata = new VoltTable(
                new VoltTable.ColumnInfo("S_I_ID", VoltType.INTEGER),
                new VoltTable.ColumnInfo("S_W_ID", VoltType.INTEGER),
                new VoltTable.ColumnInfo("S_QUANTITY", VoltType.INTEGER)
        );
        for (int i = 0; i < 1000; ++i) {
            stockdata.addRow(i, i % 200, i * i);
        }
        engine.loadTable(STOCK_TABLEID, stockdata, 0, 0, Long.MAX_VALUE);
    }

    public void testLoadTable() throws Exception {
        Catalog catalog = new Catalog();
        catalog.execute(LoadCatalogToString.THE_CATALOG);
        engine.loadCatalog(catalog);

        int WAREHOUSE_TABLEID = catalog.getClusters().get("cluster").getDatabases().get("database").getTables().get("WAREHOUSE").getRelativeIndex();
        int STOCK_TABLEID = catalog.getClusters().get("cluster").getDatabases().get("database").getTables().get("STOCK").getRelativeIndex();

        loadTestTables(catalog);

        assertEquals(200, engine.serializeTable(WAREHOUSE_TABLEID).getRowCount());
        assertEquals(1000, engine.serializeTable(STOCK_TABLEID).getRowCount());
    }

    public void testGetStats() throws Exception {
        final Catalog catalog = new Catalog();
        catalog.execute(LoadCatalogToString.THE_CATALOG);
        engine.loadCatalog(catalog);

        final int WAREHOUSE_TABLEID = catalog.getClusters().get("cluster").getDatabases().get("database").getTables().get("WAREHOUSE").getRelativeIndex();
        final int STOCK_TABLEID = catalog.getClusters().get("cluster").getDatabases().get("database").getTables().get("STOCK").getRelativeIndex();
        final int locators[] = new int[] { WAREHOUSE_TABLEID, STOCK_TABLEID };
        final VoltTable results[] = engine.getStats(SysProcSelector.TABLE, locators);
        assertNotNull(results);
        assertEquals(1, results.length);
        assertNotNull(results[0]);
        final VoltTable resultTable = results[0];
        assertEquals(2, resultTable.getRowCount());
        while (resultTable.advanceRow()) {
            String tn = resultTable.getString("TABLE_NAME");
            assertTrue(tn.equals("WAREHOUSE") || tn.equals("STOCK"));
        }
    }

    private ExecutionEngine engine;
    private static final int CLUSTER_ID = 2;
    private static final int NODE_ID = 1;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        VoltDB.instance().readBuildInfo();
        engine = new ExecutionEngineJNI(null, CLUSTER_ID, NODE_ID);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        engine.release();
        engine = null;
    }
}
