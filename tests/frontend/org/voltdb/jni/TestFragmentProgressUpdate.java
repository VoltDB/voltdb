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

package org.voltdb.jni;

import static org.mockito.Matchers.contains;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import junit.framework.TestCase;

import org.mockito.Mockito;
import org.voltcore.logging.VoltLogger;
import org.voltdb.LegacyHashinator;
import org.voltdb.ParameterSet;
import org.voltdb.TheHashinator.HashinatorConfig;
import org.voltdb.TheHashinator.HashinatorType;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.PlanFragment;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.planner.ActivePlanRepository;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.Encoder;

public class TestFragmentProgressUpdate extends TestCase {

    private long READ_ONLY_TOKEN = Long.MAX_VALUE;
    private long WRITE_TOKEN = 0;

    public void testFragmentProgressUpdate() throws Exception {
        m_ee.loadCatalog( 0, m_catalog.serialize());

        m_tableSize = 5001;
        m_longOpthreshold = 10000;
        m_warehousedata.clearRowData();

        for (int i = 0; i < m_tableSize; ++i) {
            m_warehousedata.addRow(i, "name" + i, "st1", "st2", "city", "ST", "zip", 0, 0);
        }

        m_ee.loadTable(WAREHOUSE_TABLEID, m_warehousedata, 0, 0, 0, false, false, WRITE_TOKEN);
        assertEquals(m_tableSize, m_ee.serializeTable(WAREHOUSE_TABLEID).getRowCount());
        System.out.println("Rows loaded to table "+m_ee.serializeTable(WAREHOUSE_TABLEID).getRowCount());

        Statement selectStmt = m_testProc.getStatements().getIgnoreCase("warehouse_select");
        PlanFragment selectBottomFrag = null;

        int i = 0;
        // this kinda assumes the right order
        for (PlanFragment f : selectStmt.getFragments()) {
            if (i != 0) selectBottomFrag = f;
            i++;
        }
        // populate plan cache
        ActivePlanRepository.clear();
        ActivePlanRepository.addFragmentForTest(
                CatalogUtil.getUniqueIdForFragment(selectBottomFrag),
                Encoder.decodeBase64AndDecompressToBytes(selectBottomFrag.getPlannodetree()),
                selectStmt.getSqltext());
        ParameterSet params = ParameterSet.emptyParameterSet();

        m_ee.executePlanFragments(
                1,
                new long[] { CatalogUtil.getUniqueIdForFragment(selectBottomFrag) },
                null,
                new ParameterSet[] { params },
                new String[] { selectStmt.getSqltext() },
                3, 3, 2, 42, Long.MAX_VALUE);
        // Like many fully successful operations, a single row fetch counts as 2 logical row operations,
        // one for locating the row and one for retrieving it.
        assertEquals(1, m_ee.m_callsFromEE);
        assertEquals(m_longOpthreshold, m_ee.m_lastTuplesAccessed);
        assertTrue(450000 < m_ee.m_currMemoryInBytes);
        assertTrue(550000 > m_ee.m_currMemoryInBytes);
        assertTrue(450000 < m_ee.m_peakMemoryInBytes);
        assertTrue(550000 > m_ee.m_peakMemoryInBytes);
        assertTrue(m_ee.m_peakMemoryInBytes >= m_ee.m_currMemoryInBytes);
    }

    public void testTwoUpdates() throws Exception {
        m_ee.loadCatalog( 0, m_catalog.serialize());

        m_tableSize = 10000;
        m_longOpthreshold = 10000;
        m_warehousedata.clearRowData();

        for (int i = 0; i < m_tableSize; ++i) {
            m_warehousedata.addRow(i, "name" + i, "st1", "st2", "city", "ST", "zip", 0, 0);
        }

        m_ee.loadTable(WAREHOUSE_TABLEID, m_warehousedata, 0, 0, 0, false, false, Long.MAX_VALUE);
        assertEquals(m_tableSize, m_ee.serializeTable(WAREHOUSE_TABLEID).getRowCount());
        System.out.println("Rows loaded to table "+m_ee.serializeTable(WAREHOUSE_TABLEID).getRowCount());

        Statement selectStmt = m_testProc.getStatements().getIgnoreCase("warehouse_select");
        PlanFragment selectBottomFrag = null;

        // delete 5000 records
        // I have no idea what's going on here, and just copy code from the above methods
        int i = 0;
        // this kinda assumes the right order
        for (PlanFragment f : selectStmt.getFragments()) {
            if (i != 0) selectBottomFrag = f;
            i++;
        }
        // populate plan cache
        ActivePlanRepository.clear();
        ActivePlanRepository.addFragmentForTest(
                CatalogUtil.getUniqueIdForFragment(selectBottomFrag),
                Encoder.decodeBase64AndDecompressToBytes(selectBottomFrag.getPlannodetree()),
                selectStmt.getSqltext());
        ParameterSet params = ParameterSet.emptyParameterSet();

        m_ee.executePlanFragments(
                1,
                new long[] { CatalogUtil.getUniqueIdForFragment(selectBottomFrag) },
                null,
                new ParameterSet[] { params },
                new String[] { selectStmt.getSqltext() },
                3, 3, 2, 42, Long.MAX_VALUE);

        // Like many fully successful operations, a single row fetch counts as 2 logical row operations,
        // one for locating the row and one for retrieving it.
        assertEquals(2, m_ee.m_callsFromEE);
        assertEquals(m_longOpthreshold * m_ee.m_callsFromEE, m_ee.m_lastTuplesAccessed);
        assertTrue(900000 < m_ee.m_currMemoryInBytes);
        assertTrue(1100000 > m_ee.m_currMemoryInBytes);
        assertTrue(900000 < m_ee.m_peakMemoryInBytes);
        assertTrue(1100000 > m_ee.m_peakMemoryInBytes);
        assertTrue(m_ee.m_peakMemoryInBytes >= m_ee.m_currMemoryInBytes);
        long previousPeakMemory = m_ee.m_peakMemoryInBytes;

        Statement deleteStmt = m_testProc.getStatements().getIgnoreCase("warehouse_del_half");
        assertNotNull(deleteStmt);
        PlanFragment deleteBottomFrag = null;

        int j = 0;
        // this kinda assumes the right order
        for (PlanFragment f : deleteStmt.getFragments()) {
            if (j != 0) deleteBottomFrag = f;
            j++;
        }
        // populate plan cache
        ActivePlanRepository.clear();
        ActivePlanRepository.addFragmentForTest(
                CatalogUtil.getUniqueIdForFragment(deleteBottomFrag),
                Encoder.decodeBase64AndDecompressToBytes(deleteBottomFrag.getPlannodetree()),
                deleteStmt.getSqltext());
        params = ParameterSet.emptyParameterSet();
        m_ee.executePlanFragments(
                1,
                new long[] { CatalogUtil.getUniqueIdForFragment(deleteBottomFrag) },
                null,
                new ParameterSet[] { params },
                new String[] { deleteStmt.getSqltext() },
                3, 3, 2, 42, WRITE_TOKEN);

        // populate plan cache
        ActivePlanRepository.clear();
        ActivePlanRepository.addFragmentForTest(
                CatalogUtil.getUniqueIdForFragment(selectBottomFrag),
                Encoder.decodeBase64AndDecompressToBytes(selectBottomFrag.getPlannodetree()),
                selectStmt.getSqltext());
        params = ParameterSet.emptyParameterSet();
        m_ee.executePlanFragments(
                1,
                new long[] { CatalogUtil.getUniqueIdForFragment(selectBottomFrag) },
                null,
                new ParameterSet[] { params },
                new String[] { selectStmt.getSqltext() },
                3, 3, 2, 42, Long.MAX_VALUE);
        assertTrue(m_ee.m_callsFromEE > 2);
        // here the m_lastTuplesAccessed is just the same as threshold, since we start a new fragment
        assertEquals(m_longOpthreshold, m_ee.m_lastTuplesAccessed);
        assertTrue(450000 < m_ee.m_currMemoryInBytes);
        assertTrue(550000 > m_ee.m_currMemoryInBytes);
        assertTrue(450000 < m_ee.m_peakMemoryInBytes);
        assertTrue(550000 > m_ee.m_peakMemoryInBytes);
        assertTrue(m_ee.m_peakMemoryInBytes >= m_ee.m_currMemoryInBytes);
        // Although it is true, but I don't think we should compare the memory usage here.
        //assertTrue(m_ee.m_currMemoryInBytes < previousMemoryInBytes);
        assertTrue(m_ee.m_peakMemoryInBytes < previousPeakMemory);
    }

    public void testPeakLargerThanCurr() throws Exception {
        m_ee.loadCatalog( 0, m_catalog.serialize());

        m_tableSize = 20000;
        m_longOpthreshold = 10000;
        m_warehousedata.clearRowData();

        for (int i = 0; i < m_tableSize; ++i) {
            m_warehousedata.addRow(i, "name" + i, "st1", "st2", "city", "ST", "zip", 0, 0);
        }

        m_ee.loadTable(WAREHOUSE_TABLEID, m_warehousedata, 0, 0, 0, false, false, WRITE_TOKEN);
        assertEquals(m_tableSize, m_ee.serializeTable(WAREHOUSE_TABLEID).getRowCount());
        System.out.println("Rows loaded to table "+m_ee.serializeTable(WAREHOUSE_TABLEID).getRowCount());

        Statement selectStmt = m_testProc.getStatements().getIgnoreCase("warehouse_join");
        PlanFragment selectBottomFrag = null;

        // delete 5000 records
        // I have no idea what's going on here, and just copy code from the above methods
        int i = 0;
        // this kinda assumes the right order
        for (PlanFragment f : selectStmt.getFragments()) {
            if (i != 0) selectBottomFrag = f;
            i++;
        }
        // populate plan cache
        ActivePlanRepository.clear();
        ActivePlanRepository.addFragmentForTest(
                CatalogUtil.getUniqueIdForFragment(selectBottomFrag),
                Encoder.decodeBase64AndDecompressToBytes(selectBottomFrag.getPlannodetree()),
                selectStmt.getSqltext());
        ParameterSet params = ParameterSet.emptyParameterSet();

        m_ee.executePlanFragments(
                1,
                new long[] { CatalogUtil.getUniqueIdForFragment(selectBottomFrag) },
                null,
                new ParameterSet[] { params },
                new String[] { selectStmt.getSqltext() },
                3, 3, 2, 42, Long.MAX_VALUE);

        // If want to see the stats, please uncomment the following line.
        // It is '8 393216 262144' on my machine.
        //System.out.println(m_ee.m_callsFromEE +" " + m_ee.m_peakMemoryInBytes + " "+ m_ee.m_currMemoryInBytes);
        assertEquals(m_longOpthreshold * m_ee.m_callsFromEE, m_ee.m_lastTuplesAccessed);
        assertTrue(m_ee.m_peakMemoryInBytes > m_ee.m_currMemoryInBytes);
    }

    @SuppressWarnings("deprecation")
    public void testProgressUpdateLogSqlStmt() throws Exception {
        // Set the log duration to be very short, to ensure that a message will be logged.
        m_ee.setInitialLogDurationForTest(1);

        verifyLongRunningQueries(50, 0, "item_crazy_join", true);
    }

    private void verifyLongRunningQueries(int scale, int timeout, String query, boolean readOnly) {
        m_tableSize = scale;

        m_ee.loadCatalog( 0, m_catalog.serialize());

        m_itemData.clearRowData();
        for (int i = 0; i < m_tableSize; ++i) {
            m_itemData.addRow(i, i + 50, "item" + i, (double)i / 2, "data" + i);
        }

        m_ee.loadTable(ITEM_TABLEID, m_itemData, 0, 0, 0, false, false, WRITE_TOKEN);
        assertEquals(m_tableSize, m_ee.serializeTable(ITEM_TABLEID).getRowCount());
        System.out.println("Rows loaded to table " + m_ee.serializeTable(ITEM_TABLEID).getRowCount());

        Statement queryStmt = m_testProc.getStatements().getIgnoreCase(query);
        String sqlText = queryStmt.getSqltext();

        PlanFragment frag = null;
        for (PlanFragment f : queryStmt.getFragments()) {
            frag = f;
        }

        // populate plan cache
        ActivePlanRepository.clear();
        ActivePlanRepository.addFragmentForTest(
                CatalogUtil.getUniqueIdForFragment(frag),
                Encoder.decodeBase64AndDecompressToBytes(frag.getPlannodetree()),
                sqlText);
        ParameterSet params = ParameterSet.emptyParameterSet();

        // Replace the normal logger with a mocked one, so we can verify the message
        VoltLogger mockedLogger = Mockito.mock(VoltLogger.class);
        ExecutionEngine.setVoltLoggerForTest(mockedLogger);

        m_ee.setTimeoutLatency(timeout);

        try {
            m_ee.executePlanFragments(
                    1,
                    new long[] { CatalogUtil.getUniqueIdForFragment(frag) },
                    null,
                    new ParameterSet[] { params },
                    new String[] { sqlText },
                    3, 3, 2, 42,
                    readOnly ? READ_ONLY_TOKEN : WRITE_TOKEN);
            if (readOnly && timeout > 0) {
                // only time out read queries
                fail();
            }
        } catch (Exception ex) {
            String msg = String.format("A SQL query was terminated after %.2f seconds "
                    + "because it exceeded the query timeout period.",
                    timeout/1000.0);
            assertEquals(msg, ex.getMessage());
        }

        verify(mockedLogger, atLeastOnce()).info(contains(
            String.format("Executing SQL statement is \"%s\".", sqlText)));
    }

    public void testTimingoutQueries() throws Exception {
        //
        // ReadOnly query
        //
        String procName = "item_crazy_join";

        verifyLongRunningQueries(200, 0, procName, true);
        tearDown(); setUp();

        verifyLongRunningQueries(200, 0, procName, true);
        tearDown(); setUp();

        verifyLongRunningQueries(300, 0, procName, true);
        tearDown(); setUp();


        //
        // Write query (negative)
        //
        procName = "item_big_del";
        m_ee.setInitialLogDurationForTest(1);

        verifyLongRunningQueries(50000, 0, procName, false);
        tearDown(); setUp();
        m_ee.setInitialLogDurationForTest(1);

        verifyLongRunningQueries(50000, 100, procName, false);
        tearDown(); setUp();
    }

    private ExecutionEngine m_ee;
    private static final int CLUSTER_ID = 2;
    private static final long NODE_ID = 1;
    private int m_tableSize;
    private int m_longOpthreshold;
    private VoltTable m_warehousedata;
    private VoltTable m_itemData;
    private Catalog m_catalog;
    private int WAREHOUSE_TABLEID;
    private int ITEM_TABLEID;
    private Procedure m_testProc;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        VoltDB.instance().readBuildInfo("Test");
        m_warehousedata = new VoltTable(
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
        m_itemData = new VoltTable(
                new VoltTable.ColumnInfo("I_ID", VoltType.INTEGER),
                new VoltTable.ColumnInfo("I_IM_ID", VoltType.INTEGER),
                new VoltTable.ColumnInfo("I_NAME", VoltType.STRING),
                new VoltTable.ColumnInfo("I_PRICE", VoltType.FLOAT),
                new VoltTable.ColumnInfo("I_DATA", VoltType.STRING)
                );
        TPCCProjectBuilder builder = new TPCCProjectBuilder();
        m_catalog = builder.createTPCCSchemaCatalog();
        Cluster cluster = m_catalog.getClusters().get("cluster");
        WAREHOUSE_TABLEID = m_catalog.getClusters().get("cluster").getDatabases().
                get("database").getTables().get("WAREHOUSE").getRelativeIndex();
        ITEM_TABLEID = m_catalog.getClusters().get("cluster").getDatabases().
                get("database").getTables().get("ITEM").getRelativeIndex();
        CatalogMap<Procedure> procedures = cluster.getDatabases().get("database").getProcedures();
        m_testProc = procedures.getIgnoreCase("FragmentUpdateTestProcedure");
        m_ee = new ExecutionEngineJNI(
                CLUSTER_ID,
                NODE_ID,
                0,
                0,
                "",
                100,
                new HashinatorConfig(HashinatorType.LEGACY,
                                     LegacyHashinator.getConfigureBytes(1),
                                     0,
                                     0));
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        m_ee.release();
        m_ee = null;
    }
}
