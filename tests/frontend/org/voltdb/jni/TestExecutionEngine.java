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

import junit.framework.TestCase;

import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicReference;

import org.voltdb.SysProcSelector;
import org.voltdb.VoltDB;
import org.voltdb.RecoverySiteProcessor.MessageHandler;
import org.voltdb.RecoverySiteProcessorSource;
import org.voltdb.RecoverySiteProcessorSource.OnRecoveringPartitionInitiate;
import org.voltdb.RecoverySiteProcessorDestination;
import org.voltdb.RecoverySiteProcessor.OnRecoveryCompletion;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.LoadCatalogToString;
import org.voltdb.exceptions.EEException;
import org.voltdb.messaging.RecoveryMessageType;
import org.voltdb.messaging.VoltMessage;
import org.voltdb.messaging.RecoveryMessage;
import org.voltdb.messaging.MockMailbox;
import org.voltdb.TableStreamType;
import org.voltdb.utils.DBBPool;
import org.voltdb.utils.DBBPool.BBContainer;
import org.voltdb.utils.Pair;

/**
 * Tests native execution engine JNI interface.
 */
public class TestExecutionEngine extends TestCase {

    public void testLoadCatalogs() throws Exception {
        Catalog catalog = new Catalog();
        catalog.execute(LoadCatalogToString.THE_CATALOG);
        sourceEngine.loadCatalog(catalog.serialize());
    }

    public void testLoadBadCatalogs() throws Exception {
        /*
         * Tests if the intended EE exception will be thrown when bad catalog is
         * loaded. We are really expecting an ERROR message on the terminal in
         * this case.
         */
        String badCatalog = LoadCatalogToString.THE_CATALOG.replaceFirst("set", "bad");
        try {
            sourceEngine.loadCatalog(badCatalog);
        } catch (final EEException e) {
            return;
        }

        assertFalse(true);
    }

    public void testMultiSiteInSamePhysicalNodeWithExecutionSite() throws Exception {
        // TODO
    }

    private void loadTestTables(Catalog catalog) throws Exception
    {
        final boolean allowELT = false;
        int WAREHOUSE_TABLEID = warehouseTableId(catalog);
        int STOCK_TABLEID = stockTableId(catalog);

        VoltTable warehousedata = new VoltTable(
                new VoltTable.ColumnInfo("W_ID", VoltType.INTEGER),
                new VoltTable.ColumnInfo("W_NAME", VoltType.STRING)
        );
        for (int i = 0; i < 200; ++i) {
            warehousedata.addRow(i, "str" + i);
        }

        System.out.println(warehousedata.toString());
        sourceEngine.loadTable(WAREHOUSE_TABLEID, warehousedata, 0, 0, Long.MAX_VALUE, allowELT);

        VoltTable stockdata = new VoltTable(
                new VoltTable.ColumnInfo("S_I_ID", VoltType.INTEGER),
                new VoltTable.ColumnInfo("S_W_ID", VoltType.INTEGER),
                new VoltTable.ColumnInfo("S_QUANTITY", VoltType.INTEGER)
        );
        for (int i = 0; i < 1000; ++i) {
            stockdata.addRow(i, i % 200, i * i);
        }
        sourceEngine.loadTable(STOCK_TABLEID, stockdata, 0, 0, Long.MAX_VALUE, allowELT);
    }

    public void testLoadTable() throws Exception {
        Catalog catalog = new Catalog();
        catalog.execute(LoadCatalogToString.THE_CATALOG);
        sourceEngine.loadCatalog(catalog.serialize());

        int WAREHOUSE_TABLEID = warehouseTableId(catalog);
        int STOCK_TABLEID = stockTableId(catalog);

        loadTestTables(catalog);

        assertEquals(200, sourceEngine.serializeTable(WAREHOUSE_TABLEID).getRowCount());
        assertEquals(1000, sourceEngine.serializeTable(STOCK_TABLEID).getRowCount());
    }

    public void testStreamTables() throws Exception {
        final Catalog catalog = new Catalog();
        catalog.execute(LoadCatalogToString.THE_CATALOG);
        sourceEngine.loadCatalog(catalog.serialize());
        ExecutionEngine destinationEngine = new ExecutionEngineJNI(null, CLUSTER_ID, NODE_ID, 0, 0, "");
        destinationEngine.loadCatalog(catalog.serialize());

        int WAREHOUSE_TABLEID = warehouseTableId(catalog);
        int STOCK_TABLEID = stockTableId(catalog);

        loadTestTables(catalog);

        sourceEngine.activateTableStream( WAREHOUSE_TABLEID, TableStreamType.RECOVERY );
        sourceEngine.activateTableStream( STOCK_TABLEID, TableStreamType.RECOVERY );

        BBContainer origin = DBBPool.allocateDirect(1024 * 1024 * 2);
        try {
            origin.b.clear();
            long address = org.voltdb.utils.DBBPool.getBufferAddress(origin.b);
            BBContainer container = new BBContainer(origin.b, address){

                @Override
                public void discard() {
                }};

            int serialized = sourceEngine.tableStreamSerializeMore( container, WAREHOUSE_TABLEID, TableStreamType.RECOVERY);
            assertTrue(serialized > 0);
            container.b.limit(serialized);
            byte data[] = new byte[serialized];
            container.b.get(data);
            container.b.clear();
            destinationEngine.processRecoveryMessage(data);


            serialized = sourceEngine.tableStreamSerializeMore( container, WAREHOUSE_TABLEID, TableStreamType.RECOVERY);
            assertEquals( 5, serialized);
            assertEquals( RecoveryMessageType.Complete.ordinal(), container.b.get());
            assertEquals( WAREHOUSE_TABLEID, container.b.getInt());

            assertEquals( sourceEngine.tableHashCode(WAREHOUSE_TABLEID), destinationEngine.tableHashCode(WAREHOUSE_TABLEID));

            container.b.clear();
            serialized = sourceEngine.tableStreamSerializeMore( container, STOCK_TABLEID, TableStreamType.RECOVERY);
            assertTrue(serialized > 0);
            container.b.limit(serialized);
            data = new byte[serialized];
            container.b.get(data);
            container.b.clear();
            destinationEngine.processRecoveryMessage(data);


            serialized = sourceEngine.tableStreamSerializeMore( container, STOCK_TABLEID, TableStreamType.RECOVERY);
            assertEquals( 5, serialized);
            assertEquals( RecoveryMessageType.Complete.ordinal(), container.b.get());
            assertEquals( STOCK_TABLEID, container.b.getInt());

            assertEquals( sourceEngine.tableHashCode(STOCK_TABLEID), destinationEngine.tableHashCode(STOCK_TABLEID));
        } finally {
            origin.discard();
        }
    }

    public void testRecoveryProcessors() throws Exception {
        final int sourceId = 0;
        final int destinationId = 32;
        final AtomicReference<Boolean> sourceCompleted = new AtomicReference<Boolean>(false);
        final AtomicReference<Boolean> destinationCompleted = new AtomicReference<Boolean>(false);
        final Catalog catalog = new Catalog();
        catalog.execute(LoadCatalogToString.THE_CATALOG);
        sourceEngine.loadCatalog(catalog.serialize());
        final ExecutionEngine destinationEngine = new ExecutionEngineJNI(null, CLUSTER_ID, NODE_ID, destinationId, destinationId, "");
        destinationEngine.loadCatalog(catalog.serialize());

        int WAREHOUSE_TABLEID = warehouseTableId(catalog);
        int STOCK_TABLEID = stockTableId(catalog);

        loadTestTables(catalog);

        HashMap<Pair<String, Integer>, HashSet<Integer>> tablesAndDestinations = new HashMap<Pair<String, Integer>, HashSet<Integer>>();
        HashSet<Integer> destinations = new HashSet<Integer>();
        destinations.add(destinationId);
        tablesAndDestinations.put(Pair.of( "STOCK", STOCK_TABLEID), destinations);
        tablesAndDestinations.put(Pair.of( "WAREHOUSE", WAREHOUSE_TABLEID), destinations);

        final MockMailbox sourceMailbox = new MockMailbox();
        MockMailbox.registerMailbox( sourceId, sourceMailbox);

        final Runnable onSourceCompletion = new Runnable() {
            @Override
            public void run() {
                sourceCompleted.set(true);
            }
        };

        OnRecoveringPartitionInitiate onInitiate = new OnRecoveringPartitionInitiate() {

            @Override
            public long pickTxnToStopAfter(long recoveringPartitionTxnId) {
                return 0;
            }

        };

        final MessageHandler mh = new MessageHandler() {

            @Override
            public void handleMessage(VoltMessage message) {
                fail();
            }
        };

        final RecoverySiteProcessorSource sourceProcessor =
            new RecoverySiteProcessorSource(
                    tablesAndDestinations,
                    sourceEngine,
                    sourceMailbox,
                    sourceId,
                    onSourceCompletion,
                    onInitiate,
                    mh);

        Thread sourceThread = new Thread() {
            @Override
            public void run() {
                VoltMessage message = sourceMailbox.recvBlocking();
                assertTrue(message != null);
                assertTrue(message instanceof RecoveryMessage);
                sourceProcessor.handleRecoveryMessage((RecoveryMessage)message);
            }
        };
        sourceThread.start();

        final HashMap<Pair<String, Integer>, Integer> tablesAndSources = new HashMap<Pair<String, Integer>, Integer>();
        tablesAndSources.put(Pair.of( "STOCK", STOCK_TABLEID), sourceId);
        tablesAndSources.put(Pair.of( "WAREHOUSE", WAREHOUSE_TABLEID), sourceId);

        final MockMailbox destinationMailbox = new MockMailbox();
        MockMailbox.registerMailbox( destinationId, destinationMailbox);

        final OnRecoveryCompletion onDestinationCompletion = new OnRecoveryCompletion() {
            @Override
            public void complete(long txnId) {
                destinationCompleted.set(true);
            }
        };

        Thread destinationThread = new Thread() {
            @Override
            public void run() {
                RecoverySiteProcessorDestination destinationProcess =
                    new RecoverySiteProcessorDestination(
                            tablesAndSources,
                            destinationEngine,
                            destinationMailbox,
                            destinationId,
                            onDestinationCompletion,
                            0,
                            mh);
                while (!destinationCompleted.get()) {
                    VoltMessage message = destinationMailbox.recvBlocking();
                    assertTrue(message != null);
                    assertTrue(message instanceof RecoveryMessage);
                    destinationProcess.handleRecoveryMessage((RecoveryMessage)message);
                    message.discard();
                }
            }
        };
        destinationThread.start();

        destinationThread.join();
        sourceThread.join();

        assertEquals( sourceEngine.tableHashCode(STOCK_TABLEID), destinationEngine.tableHashCode(STOCK_TABLEID));
        assertEquals( sourceEngine.tableHashCode(WAREHOUSE_TABLEID), destinationEngine.tableHashCode(WAREHOUSE_TABLEID));

        assertEquals(200, sourceEngine.serializeTable(WAREHOUSE_TABLEID).getRowCount());
        assertEquals(1000, sourceEngine.serializeTable(STOCK_TABLEID).getRowCount());
        assertEquals(200, destinationEngine.serializeTable(WAREHOUSE_TABLEID).getRowCount());
        assertEquals(1000, destinationEngine.serializeTable(STOCK_TABLEID).getRowCount());
    }

    private int warehouseTableId(Catalog catalog) {
        return catalog.getClusters().get("cluster").getDatabases().get("database").getTables().get("WAREHOUSE").getRelativeIndex();
    }

    private int stockTableId(Catalog catalog) {
        return catalog.getClusters().get("cluster").getDatabases().get("database").getTables().get("STOCK").getRelativeIndex();
    }

    public void testGetStats() throws Exception {
        final Catalog catalog = new Catalog();
        catalog.execute(LoadCatalogToString.THE_CATALOG);
        sourceEngine.loadCatalog(catalog.serialize());

        final int WAREHOUSE_TABLEID = catalog.getClusters().get("cluster").getDatabases().get("database").getTables().get("WAREHOUSE").getRelativeIndex();
        final int STOCK_TABLEID = catalog.getClusters().get("cluster").getDatabases().get("database").getTables().get("STOCK").getRelativeIndex();
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
    private static final int NODE_ID = 1;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        VoltDB.instance().readBuildInfo();
        sourceEngine = new ExecutionEngineJNI(null, CLUSTER_ID, NODE_ID, 0, 0, "");
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        sourceEngine.release();
        sourceEngine = null;
    }
}
