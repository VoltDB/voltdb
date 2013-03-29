/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicReference;

import junit.framework.TestCase;

import org.voltcore.messaging.MockMailbox;
import org.voltcore.messaging.RecoveryMessage;
import org.voltcore.messaging.RecoveryMessageType;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.Pair;
import org.voltdb.LegacyHashinator;
import org.voltdb.RecoverySiteProcessor.MessageHandler;
import org.voltdb.RecoverySiteProcessorDestination;
import org.voltdb.RecoverySiteProcessorSource;
import org.voltdb.SysProcSelector;
import org.voltdb.TableStreamType;
import org.voltdb.TheHashinator.HashinatorType;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.catalog.Catalog;
import org.voltdb.exceptions.EEException;

/**
 * Tests native execution engine JNI interface.
 */
public class TestExecutionEngine extends TestCase {

    public void testLoadCatalogs() throws Exception {
        sourceEngine.loadCatalog( 0, m_catalog.serialize());
    }

    public void testLoadBadCatalogs() throws Exception {
        /*
         * Tests if the intended EE exception will be thrown when bad catalog is
         * loaded. We are really expecting an ERROR message on the terminal in
         * this case.
         */
        String badCatalog = m_catalog.serialize().replaceFirst("set", "bad");
        try {
            sourceEngine.loadCatalog( 0, badCatalog);
        } catch (final EEException e) {
            return;
        }

        assertFalse(true);
    }

    public void testMultiSiteInSamePhysicalNodeWithExecutionSite() throws Exception {
        // TODO
    }

    private void loadTestTables(ExecutionEngine engine, Catalog catalog) throws Exception
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
        engine.loadTable(WAREHOUSE_TABLEID, warehousedata, 0, 0);

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
        engine.loadTable(STOCK_TABLEID, stockdata, 0, 0);
    }

    public void testLoadTable() throws Exception {
        sourceEngine.loadCatalog( 0, m_catalog.serialize());

        int WAREHOUSE_TABLEID = warehouseTableId(m_catalog);
        int STOCK_TABLEID = stockTableId(m_catalog);

        loadTestTables( sourceEngine, m_catalog);

        assertEquals(200, sourceEngine.serializeTable(WAREHOUSE_TABLEID).getRowCount());
        assertEquals(1000, sourceEngine.serializeTable(STOCK_TABLEID).getRowCount());
    }

    public void testStreamTables() throws Exception {
        sourceEngine.loadCatalog( 0, m_catalog.serialize());

        // Each EE needs its own thread for correct initialization.
        final AtomicReference<ExecutionEngine> destinationEngine = new AtomicReference<ExecutionEngine>();
        final byte configBytes[] = LegacyHashinator.getConfigureBytes(1);
        Thread destEEThread = new Thread() {
            @Override
            public void run() {
                destinationEngine.set(
                        new ExecutionEngineJNI(
                                CLUSTER_ID,
                                NODE_ID,
                                0,
                                0,
                                "",
                                100,
                                HashinatorType.LEGACY,
                                configBytes,
                                null));
            }
        };
        destEEThread.start();
        destEEThread.join();

        destinationEngine.get().loadCatalog( 0, m_catalog.serialize());

        int WAREHOUSE_TABLEID = warehouseTableId(m_catalog);
        int STOCK_TABLEID = stockTableId(m_catalog);

        loadTestTables( sourceEngine, m_catalog);

        sourceEngine.activateTableStream( WAREHOUSE_TABLEID, TableStreamType.RECOVERY );
        sourceEngine.activateTableStream( STOCK_TABLEID, TableStreamType.RECOVERY );

        BBContainer origin = DBBPool.allocateDirect(1024 * 1024 * 2);
        try {
            origin.b.clear();
            long address = org.voltcore.utils.DBBPool.getBufferAddress(origin.b);
            BBContainer container = new BBContainer(origin.b, address){

                @Override
                public void discard() {
                }};

            int serialized = sourceEngine.tableStreamSerializeMore( container, WAREHOUSE_TABLEID, TableStreamType.RECOVERY);
            assertTrue(serialized > 0);
            container.b.limit(serialized);
            destinationEngine.get().processRecoveryMessage( container.b, container.address);


            serialized = sourceEngine.tableStreamSerializeMore( container, WAREHOUSE_TABLEID, TableStreamType.RECOVERY);
            assertEquals( 5, serialized);
            assertEquals( RecoveryMessageType.Complete.ordinal(), container.b.get());

            assertEquals( sourceEngine.tableHashCode(WAREHOUSE_TABLEID), destinationEngine.get().tableHashCode(WAREHOUSE_TABLEID));

            container.b.clear();
            serialized = sourceEngine.tableStreamSerializeMore( container, STOCK_TABLEID, TableStreamType.RECOVERY);
            assertTrue(serialized > 0);
            container.b.limit(serialized);
            destinationEngine.get().processRecoveryMessage( container.b, container.address);


            serialized = sourceEngine.tableStreamSerializeMore( container, STOCK_TABLEID, TableStreamType.RECOVERY);
            assertEquals( 5, serialized);
            assertEquals( RecoveryMessageType.Complete.ordinal(), container.b.get());
            assertEquals( STOCK_TABLEID, container.b.getInt());

            assertEquals( sourceEngine.tableHashCode(STOCK_TABLEID), destinationEngine.get().tableHashCode(STOCK_TABLEID));
        } finally {
            origin.discard();
        }
    }

    public void testRecoveryProcessors() throws Exception {
        final long sourceId = 0;
        final long sourceDataId = 1;
        final long destinationId = 32;
        final long destinationDataId = 33;
        final AtomicReference<Boolean> sourceCompleted = new AtomicReference<Boolean>(false);
        final AtomicReference<Boolean> destinationCompleted = new AtomicReference<Boolean>(false);
        final String serializedCatalog = m_catalog.serialize();

        int WAREHOUSE_TABLEID = warehouseTableId(m_catalog);
        int STOCK_TABLEID = stockTableId(m_catalog);

        final HashMap<Pair<String, Integer>, HashSet<Long>> tablesAndDestinations =
            new HashMap<Pair<String, Integer>, HashSet<Long>>();
        HashSet<Long> destinations = new HashSet<Long>();
        destinations.add(destinationId);
        tablesAndDestinations.put(Pair.of( "STOCK", STOCK_TABLEID), destinations);
        tablesAndDestinations.put(Pair.of( "WAREHOUSE", WAREHOUSE_TABLEID), destinations);

        final MockMailbox sourceMailbox = new MockMailbox();
        MockMailbox.registerMailbox(sourceId, sourceMailbox);

        final MockMailbox sourceDataMailbox = new MockMailbox();
        sourceDataMailbox.setHSId(sourceDataId);
        MockMailbox.registerMailbox(sourceDataId, sourceDataMailbox);

        final Runnable onSourceCompletion = new Runnable() {
            @Override
            public void run() {
                sourceCompleted.set(true);
            }
        };

        final MessageHandler mh = new MessageHandler() {

            @Override
            public void handleMessage(VoltMessage message, long txnId) {
                fail();
            }
        };

        final Runnable onDestinationCompletion = new Runnable() {
            @Override
            public void run() {
                destinationCompleted.set(true);
            }
        };

        final AtomicReference<ExecutionEngine> destinationReference= new AtomicReference<ExecutionEngine>();


        final HashMap<Pair<String, Integer>, Long> tablesAndSources =
            new HashMap<Pair<String, Integer>, Long>();
        tablesAndSources.put(Pair.of( "STOCK", STOCK_TABLEID), sourceId);
        tablesAndSources.put(Pair.of( "WAREHOUSE", WAREHOUSE_TABLEID), sourceId);

        final MockMailbox destinationMailbox = new MockMailbox();
        MockMailbox.registerMailbox(destinationId, destinationMailbox);

        final MockMailbox destinationDataMailbox = new MockMailbox();
        destinationDataMailbox.setHSId(destinationDataId);
        MockMailbox.registerMailbox(destinationDataId, destinationDataMailbox);
        final byte configBytes[] = LegacyHashinator.getConfigureBytes(1);

        Thread destinationThread = new Thread("Destination thread") {
            @Override
            public void run() {
                final ExecutionEngine destinationEngine =
                    new ExecutionEngineJNI(
                            CLUSTER_ID,
                            NODE_ID,
                            (int)destinationId,
                            (int)destinationId,
                            "",
                            100,
                            HashinatorType.LEGACY,
                            configBytes,
                            null);
                destinationReference.set(destinationEngine);
                destinationEngine.loadCatalog( 0, serializedCatalog);
                RecoverySiteProcessorDestination destinationProcess =
                    new RecoverySiteProcessorDestination(
                            tablesAndSources,
                            destinationEngine,
                            destinationMailbox,
                            destinationDataMailbox,
                            destinationId,
                            0,
                            onDestinationCompletion,
                            mh);
                /*
                 * Do a lot of craziness so we can intercept the mailbox calls
                 * and discard the buffer so it is returned to the source
                 */
                destinationProcess.doRecoveryWork(-1);
                destinationProcess.doRecoveryWork(0);
                assert(destinationCompleted.get());
            }
        };
        destinationThread.start();

        Thread.sleep(1000);

        final AtomicReference<ExecutionEngine> sourceReference = new AtomicReference<ExecutionEngine>();

        Thread sourceThread = new Thread("Source thread") {
            @Override
            public void run() {
                final ExecutionEngine sourceEngine =
                        new ExecutionEngineJNI(
                                CLUSTER_ID,
                                NODE_ID,
                                (int)sourceId,
                                (int)sourceId,
                                "",
                                100,
                                HashinatorType.LEGACY,
                                configBytes,
                                null);
                sourceReference.set(sourceEngine);
                sourceEngine.loadCatalog( 0, serializedCatalog);

                try {
                    loadTestTables( sourceEngine, m_catalog);
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

                VoltMessage message = sourceMailbox.recvBlocking();
                assertTrue(message != null);
                assertTrue(message instanceof RecoveryMessage);
                RecoveryMessage rm = (RecoveryMessage)message;
                final RecoverySiteProcessorSource sourceProcessor =
                        new RecoverySiteProcessorSource(
                                                        null,
                                                        rm.txnId(),
                                                        rm.sourceSite(),
                                                        rm.getHSId(),
                                                        tablesAndDestinations,
                                                        sourceEngine,
                                                        sourceMailbox,
                                                        sourceDataMailbox,
                                                        sourceId,
                                                        onSourceCompletion,
                                                        mh);
                sourceProcessor.doRecoveryWork(0);
            }
        };
        sourceThread.start();

        destinationThread.join();
        sourceThread.join();

        assertEquals( sourceReference.get().tableHashCode(STOCK_TABLEID), destinationReference.get().tableHashCode(STOCK_TABLEID));
        assertEquals( sourceReference.get().tableHashCode(WAREHOUSE_TABLEID), destinationReference.get().tableHashCode(WAREHOUSE_TABLEID));

        assertEquals(200, sourceReference.get().serializeTable(WAREHOUSE_TABLEID).getRowCount());
        assertEquals(1000, sourceReference.get().serializeTable(STOCK_TABLEID).getRowCount());
        assertEquals(200, destinationReference.get().serializeTable(WAREHOUSE_TABLEID).getRowCount());
        assertEquals(1000, destinationReference.get().serializeTable(STOCK_TABLEID).getRowCount());
    }

    private int warehouseTableId(Catalog catalog) {
        return catalog.getClusters().get("cluster").getDatabases().get("database").getTables().get("WAREHOUSE").getRelativeIndex();
    }

    private int stockTableId(Catalog catalog) {
        return catalog.getClusters().get("cluster").getDatabases().get("database").getTables().get("STOCK").getRelativeIndex();
    }

    public void testGetStats() throws Exception {
        sourceEngine.loadCatalog( 0, m_catalog.serialize());

        final int WAREHOUSE_TABLEID = m_catalog.getClusters().get("cluster").getDatabases().get("database").getTables().get("WAREHOUSE").getRelativeIndex();
        final int STOCK_TABLEID = m_catalog.getClusters().get("cluster").getDatabases().get("database").getTables().get("STOCK").getRelativeIndex();
        final int locators[] = new int[] { WAREHOUSE_TABLEID, STOCK_TABLEID };
        final VoltTable results[] = sourceEngine.getStats(SysProcSelector.TABLE, locators, false, 0L);
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

    private ExecutionEngine sourceEngine;
    private static final int CLUSTER_ID = 2;
    private static final long NODE_ID = 1;

    TPCCProjectBuilder m_project;
    Catalog m_catalog;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        VoltDB.instance().readBuildInfo("Test");
        sourceEngine =
                new ExecutionEngineJNI(
                        CLUSTER_ID,
                        NODE_ID,
                        0,
                        0,
                        "",
                        100,
                        HashinatorType.LEGACY,
                        LegacyHashinator.getConfigureBytes(1),
                        null);
        m_project = new TPCCProjectBuilder();
        m_catalog = m_project.createTPCCSchemaCatalog();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        sourceEngine.release();
        sourceEngine = null;
    }
}
