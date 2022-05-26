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

package org.voltdb.jni;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.Pair;
import org.voltdb.ElasticHashinator;
import org.voltdb.PrivateVoltTableFactory;
import org.voltdb.StatsSelector;
import org.voltdb.TableStreamType;
import org.voltdb.TheHashinator.HashinatorConfig;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.catalog.Catalog;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.exceptions.ConstraintFailureException;
import org.voltdb.exceptions.EEException;
import org.voltdb.exceptions.ReplicatedTableException;
import org.voltdb.exceptions.SQLException;
import org.voltdb.expressions.HashRangeExpressionBuilder;
import org.voltdb.jni.ExecutionEngine.LoadTableCaller;
import org.voltdb.sysprocs.saverestore.SnapshotPredicates;
import org.voltdb.sysprocs.saverestore.HiddenColumnFilter;

import junit.framework.TestCase;

/**
 * Tests native execution engine JNI interface.
 */
public class TestExecutionEngine extends TestCase {

    public void testLoadCatalogs() throws Exception {
        initializeSourceEngine(1);
        sourceEngine.loadCatalog( 0, m_catalog.serialize());
        terminateSourceEngine();
    }

    public void testLoadBadCatalogs() throws Exception {
        initializeSourceEngine(1);
        /*
         * Tests if the intended EE exception will be thrown when bad catalog is
         * loaded. We are really expecting an ERROR message on the terminal in
         * this case.
         */
        String badCatalog = m_catalog.serialize().replaceFirst("set", "bad");
        try {
            sourceEngine.loadCatalog( 0, badCatalog);
        } catch (final EEException e) {
            terminateSourceEngine();
            return;
        }

        assertFalse(true);
    }

//    public void testMultiSiteInSamePhysicalNodeWithExecutionSite() throws Exception {
//        initializeSourceEngine(1);
//        // TODO
//        terminateSourceEngine();
//    }

    private void loadTestTables(Catalog catalog) throws Exception
    {
        int WAREHOUSE_TABLEID = warehouseTableId(catalog);
        int STOCK_TABLEID = stockTableId(catalog);

        VoltTable warehousedata = new VoltTable(
                new VoltTable.ColumnInfo("W_ID", VoltType.SMALLINT),
                new VoltTable.ColumnInfo("W_NAME", VoltType.STRING),
                new VoltTable.ColumnInfo("W_STREET_1", VoltType.STRING),
                new VoltTable.ColumnInfo("W_STREET_2", VoltType.STRING),
                new VoltTable.ColumnInfo("W_CITY", VoltType.STRING),
                new VoltTable.ColumnInfo("W_STATE", VoltType.STRING),
                new VoltTable.ColumnInfo("W_ZIP", VoltType.STRING),
                new VoltTable.ColumnInfo("W_TAX", VoltType.FLOAT),
                new VoltTable.ColumnInfo("W_YTD", VoltType.FLOAT)
        );
        for (int i = 0; i < 200; ++i) {
            warehousedata.addRow(i, "name" + i, "st1", "st2", "city", "ST", "zip", 0, 0);
        }

        System.out.println(warehousedata.toString());
        // Long.MAX_VALUE is a no-op don't track undo token
        sourceEngine.loadTable(WAREHOUSE_TABLEID, warehousedata, 0, 0, 0, 0, Long.MAX_VALUE, LoadTableCaller.DR);

        //Check that we can detect and handle the dups when loading the data twice
        byte results[] = sourceEngine.loadTable(WAREHOUSE_TABLEID, warehousedata, 0, 0, 0, 0, Long.MAX_VALUE,
                LoadTableCaller.DR);
        System.out.println("Printing dups");
        System.out.println(PrivateVoltTableFactory.createVoltTableFromBuffer(ByteBuffer.wrap(results), true));
        assertNotNull(results);

        VoltTable stockdata = new VoltTable(
                new VoltTable.ColumnInfo("S_I_ID", VoltType.INTEGER),
                new VoltTable.ColumnInfo("S_W_ID", VoltType.SMALLINT),
                new VoltTable.ColumnInfo("S_QUANTITY", VoltType.INTEGER),
                new VoltTable.ColumnInfo("S_DIST_01", VoltType.STRING),
                new VoltTable.ColumnInfo("S_DIST_02", VoltType.STRING),
                new VoltTable.ColumnInfo("S_DIST_03", VoltType.STRING),
                new VoltTable.ColumnInfo("S_DIST_04", VoltType.STRING),
                new VoltTable.ColumnInfo("S_DIST_05", VoltType.STRING),
                new VoltTable.ColumnInfo("S_DIST_06", VoltType.STRING),
                new VoltTable.ColumnInfo("S_DIST_07", VoltType.STRING),
                new VoltTable.ColumnInfo("S_DIST_08", VoltType.STRING),
                new VoltTable.ColumnInfo("S_DIST_09", VoltType.STRING),
                new VoltTable.ColumnInfo("S_DIST_10", VoltType.STRING),
                new VoltTable.ColumnInfo("S_YTD", VoltType.INTEGER),
                new VoltTable.ColumnInfo("S_ORDER_CNT", VoltType.INTEGER),
                new VoltTable.ColumnInfo("S_REMOTE_CNT", VoltType.INTEGER),
                new VoltTable.ColumnInfo("S_DATA", VoltType.STRING)
        );
        for (int i = 0; i < 1000; ++i) {
            stockdata.addRow(i, i % 200, i * i, "sdist1", "sdist2", "sdist3",
                             "sdist4", "sdist5", "sdist6", "sdist7", "sdist8",
                             "sdist9", "sdist10", 0, 0, 0, "sdata");
        }
        // Long.MAX_VALUE is a no-op don't track undo token
        sourceEngine.loadTable(STOCK_TABLEID, stockdata, 0, 0, 0, 0, Long.MAX_VALUE,
                LoadTableCaller.SNAPSHOT_THROW_ON_UNIQ_VIOLATION);
    }

    public void testLoadTable() throws Exception {
        initializeSourceEngine(1);
        sourceEngine.loadCatalog( 0, m_catalog.serialize());

        int WAREHOUSE_TABLEID = warehouseTableId(m_catalog);
        int STOCK_TABLEID = stockTableId(m_catalog);

        loadTestTables(m_catalog);

        assertEquals(200, sourceEngine.serializeTable(WAREHOUSE_TABLEID).getRowCount());
        assertEquals(1000, sourceEngine.serializeTable(STOCK_TABLEID).getRowCount());
        terminateSourceEngine();
    }

    // ENG-14346
    public void testLoadTableTooWideColumnCleanupOnError() throws Exception {
        initializeSourceEngine(1);
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.setUseDDLSchema(true);
        project.addLiteralSchema("create table test (msg VARCHAR(200 bytes));");
        Catalog catalog = project.compile("ENG-14346.jar", 1, 1, 0, null);
        assert(catalog != null);
        sourceEngine.loadCatalog(0, catalog.serialize());

        int TEST_TABLEID = catalog.getClusters().get("cluster").getDatabases().get("database").
                getTables().get("TEST").getRelativeIndex();
        VoltTable testTable = new VoltTable(new VoltTable.ColumnInfo("MSG", VoltType.STRING));
        // Assemble a very long string.
        testTable.addRow(String.join("", Collections.nCopies(15, "我能吞下玻璃而不伤身体。")));
        try {
            sourceEngine.loadTable(TEST_TABLEID, testTable, 0, 0, 0, 0, Long.MAX_VALUE,
                    LoadTableCaller.SNAPSHOT_THROW_ON_UNIQ_VIOLATION);
            fail("The loadTable() call is expected to fail, but did not.");
        }
        catch (SQLException ex) {
            assertTrue(ex.getMessage().contains("exceeds the size of the VARCHAR(200 BYTES) column"));
        }
        terminateSourceEngine();
    }

    private Pair<byte[], byte[]> verifyMultiSiteLoadTable(boolean returnUniqueViolations) throws Exception {
        int ITEM_TABLEID = itemTableId(m_twoSiteCatalog);

        VoltTable itemdata = new VoltTable(
                new VoltTable.ColumnInfo("I_ID", VoltType.INTEGER),
                new VoltTable.ColumnInfo("I_IM_ID", VoltType.INTEGER),
                new VoltTable.ColumnInfo("I_NAME", VoltType.STRING),
                new VoltTable.ColumnInfo("I_PRICE", VoltType.FLOAT),
                new VoltTable.ColumnInfo("I_DATA", VoltType.STRING)
        );
        for (int i = 0; i < 200; ++i) {
            itemdata.addRow(i, i, "name" + i, 1.0, "desc");
        }
        // make a conflict row
        itemdata.addRow(5, 5, "failedRow", 1.0, "failedRow");


        // Each EE needs its own thread for correct initialization.
        final AtomicReference<ExecutionEngine> source2Engine = new AtomicReference<ExecutionEngine>();
        final byte configBytes[] = ElasticHashinator.getConfigureBytes(1);
        final ExecutorService es = Executors.newSingleThreadExecutor();
        es.submit(new Runnable() {
            @Override
            public void run() {
                source2Engine.set(
                        new ExecutionEngineJNI(
                                CLUSTER_ID,
                                1,
                                1,
                                2,
                                NODE_ID,
                                "",
                                0,
                                64*1024,
                                false,
                                -1,
                                false,
                                100,
                                new HashinatorConfig(configBytes, 0, 0), false));
            }
        }).get();

        es.execute(new Runnable() {
            @Override
            public void run() {
                source2Engine.get().loadCatalog( 0, m_twoSiteCatalog.serialize());
            }
        });

        initializeSourceEngine(2);

        sourceEngine.loadCatalog( 0, m_twoSiteCatalog.serialize());

        Future<byte[]> ft = es.submit(new Callable<byte[]>() {
            @Override
            public byte[] call() throws Exception {
                try {
                    byte[] rslt = source2Engine.get().loadTable(ITEM_TABLEID, itemdata, 0, 0, 0, 0, Long.MAX_VALUE,
                            returnUniqueViolations ? LoadTableCaller.SNAPSHOT_REPORT_UNIQ_VIOLATIONS
                                    : LoadTableCaller.SNAPSHOT_THROW_ON_UNIQ_VIOLATION);
                    return rslt;
                }
                catch (ReplicatedTableException e) {
                    return null;
                }
                catch (Exception e) {
                    return new byte[] {(byte)0};
                }
            }
        });

        byte[] srcRslt = null;
        try {
            srcRslt = sourceEngine.loadTable(ITEM_TABLEID, itemdata, 0, 0, 0, 0,
                    Long.MAX_VALUE,
                    returnUniqueViolations ? LoadTableCaller.SNAPSHOT_REPORT_UNIQ_VIOLATIONS :
                        LoadTableCaller.SNAPSHOT_THROW_ON_UNIQ_VIOLATION);
        }
        catch (ConstraintFailureException e) {
            // srcRslt already null
        }
        catch (Exception e) {
            srcRslt = new byte[] {(byte)0};
        }
        byte[] src2Rslt = ft.get();

        es.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    source2Engine.get().release();
                }
                catch (EEException | InterruptedException e) {
                }
            }
        }).get();
        terminateSourceEngine();
        es.shutdown();
        return Pair.of(srcRslt, src2Rslt);
    }

    public void testMultiSiteLoadTableWithException() throws Exception {
        Pair<byte[], byte[]> loadResults = verifyMultiSiteLoadTable(false);
        assertTrue(loadResults.getFirst() == null && loadResults.getSecond() == null);
    }

    public void testMultiSiteLoadTableWithoutException() throws Exception {
        Pair<byte[], byte[]> loadResults = verifyMultiSiteLoadTable(true);
        assertTrue(loadResults.getFirst() != null && loadResults.getSecond() != null);
        assertTrue(loadResults.getFirst().length > 4 &&
                Arrays.equals(loadResults.getFirst(), loadResults.getSecond()));

    }

    private int warehouseTableId(Catalog catalog) {
        return catalog.getClusters().get("cluster").getDatabases().get("database").getTables().
                get("WAREHOUSE").getRelativeIndex();
    }

    private int stockTableId(Catalog catalog) {
        return catalog.getClusters().get("cluster").getDatabases().get("database").getTables().
                get("STOCK").getRelativeIndex();
    }

    private int itemTableId(Catalog catalog) {
        return catalog.getClusters().get("cluster").getDatabases().get("database").getTables().
                get("ITEM").getRelativeIndex();
    }

    public void testGetStats() throws Exception {
        initializeSourceEngine(1);
        sourceEngine.loadCatalog( 0, m_catalog.serialize());

        final int WAREHOUSE_TABLEID = m_catalog.getClusters().get("cluster").getDatabases().get("database").
                getTables().get("WAREHOUSE").getRelativeIndex();
        final int STOCK_TABLEID = m_catalog.getClusters().get("cluster").getDatabases().get("database").
                getTables().get("STOCK").getRelativeIndex();
        final int locators[] = new int[] { WAREHOUSE_TABLEID, STOCK_TABLEID };
        final VoltTable results[] = sourceEngine.getStats(StatsSelector.TABLE, locators, false, 0L);
        assertNotNull(results);
        assertEquals(1, results.length);
        assertNotNull(results[0]);
        final VoltTable resultTable = results[0];
        assertEquals(2, resultTable.getRowCount());
        while (resultTable.advanceRow()) {
            String tn = resultTable.getString("TABLE_NAME");
            assertTrue(tn.equals("WAREHOUSE") || tn.equals("STOCK"));
        }
        terminateSourceEngine();
    }

    public void testStreamIndex() throws Exception {
        // Each EE needs its own thread for correct initialization.
        final AtomicReference<ExecutionEngine> destinationEngine = new AtomicReference<ExecutionEngine>();
        final byte configBytes[] = ElasticHashinator.getConfigureBytes(1);
        final ExecutorService es = Executors.newSingleThreadExecutor();
        es.submit(new Runnable() {
            @Override
            public void run() {
                destinationEngine.set(
                        new ExecutionEngineJNI(
                                CLUSTER_ID,
                                1,
                                1,
                                2,
                                NODE_ID,
                                "",
                                0,
                                64*1024,
                                false,
                                -1,
                                false,
                                100,
                                new HashinatorConfig(configBytes, 0, 0), false));
            }
        }).get();

        es.execute(new Runnable() {
            @Override
            public void run() {
                destinationEngine.get().loadCatalog( 0, m_catalog.serialize());
            }
        });

        initializeSourceEngine(2);
        sourceEngine.loadCatalog( 0, m_catalog.serialize());

        int STOCK_TABLEID = stockTableId(m_catalog);

        loadTestTables(m_catalog);

        SnapshotPredicates predicates = new SnapshotPredicates(-1);
        predicates.addPredicate(new HashRangeExpressionBuilder()
                                        .put(0x00000000, 0x7fffffff)
                                        .build(0),
                                true);

        // Build the index
        sourceEngine.activateTableStream(STOCK_TABLEID, TableStreamType.ELASTIC_INDEX, HiddenColumnFilter.NONE,
                Long.MAX_VALUE, predicates.toBytes());

        // Humor serializeMore() by providing a buffer, even though it's not used.
        final BBContainer origin = DBBPool.allocateDirect(1024 * 1024 * 2);
        origin.b().clear();
        BBContainer container = new BBContainer(origin.b()){
            @Override
            public void discard() {
                checkDoubleFree();
                origin.discard();
            }
        };
        try {
            List<BBContainer> output = new ArrayList<BBContainer>();
            output.add(container);
            assertEquals(0, sourceEngine.tableStreamSerializeMore(STOCK_TABLEID, TableStreamType.ELASTIC_INDEX,
                    output).getSecond()[0]);
        } finally {
            container.discard();
        }
        es.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    destinationEngine.get().release();
                }
                catch (EEException | InterruptedException e) {
                }
            }
        }).get();
        terminateSourceEngine();
        es.shutdown();
    }

    private ExecutionEngine sourceEngine;
    private static final int CLUSTER_ID = 2;
    private static final int NODE_ID = 1;

    Catalog m_catalog;
    Catalog m_twoSiteCatalog;

    private void initializeSourceEngine(int engineCount) throws Exception {
        sourceEngine =
                new ExecutionEngineJNI(
                        CLUSTER_ID,
                        0,
                        0,
                        engineCount,
                        NODE_ID,
                        "",
                        0,
                        64*1024,
                        false,
                        -1,
                        false,
                        100,
                        new HashinatorConfig(ElasticHashinator.getConfigureBytes(1), 0, 0), true);
    }

    private void terminateSourceEngine() throws Exception {
        sourceEngine.release();
        sourceEngine = null;
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        VoltDB.instance().readBuildInfo();
        m_catalog = TPCCProjectBuilder.getTPCCSchemaCatalog();
        m_twoSiteCatalog = TPCCProjectBuilder.getTPCCSchemaCatalogMultiSite(2);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        System.gc();
        System.runFinalization();
    }
}
