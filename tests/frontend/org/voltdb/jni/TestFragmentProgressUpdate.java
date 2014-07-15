/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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
    public void testJNIFragmentProgressUpdate() throws Exception {
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
        testFragmentProgressUpdate(m_ee);
    }

    // test we reset the statistic when start a new fragment
    public void testJNIFragmentProgressUpdate2() throws Exception {
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
        testTwoUpdates(m_ee);
    }

    // test sometimes the peak is larger than the current
    public void testJNIFragmentProgressUpdate3() throws Exception {
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
        testPeakLargerThanCurr(m_ee);
    }

    /*public void testIPCFragmentProgressUpdate() throws Exception {
        m_ee = new ExecutionEngineIPC(
                CLUSTER_ID,
                NODE_ID,
                0,
                0,
                "",
                100,
                BackendTarget.NATIVE_EE_IPC,
                10000,
                new HashinatorConfig(HashinatorType.LEGACY,
                        LegacyHashinator.getConfigureBytes(1),
                        0,
                        0));
        testFragmentProgressUpdate(m_ee);
    }*/

    void testFragmentProgressUpdate(ExecutionEngine ee) throws Exception {
        m_ee.loadCatalog( 0, m_catalog.serialize());

        m_tableSize = 5001;
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
                Encoder.decodeBase64AndDecompressToBytes(selectBottomFrag.getPlannodetree()));
        ParameterSet params = ParameterSet.emptyParameterSet();

        m_ee.executePlanFragments(
                1,
                new long[] { CatalogUtil.getUniqueIdForFragment(selectBottomFrag) },
                null,
                new ParameterSet[] { params },
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

    void testTwoUpdates(ExecutionEngine ee) throws Exception {
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
                Encoder.decodeBase64AndDecompressToBytes(selectBottomFrag.getPlannodetree()));
        ParameterSet params = ParameterSet.emptyParameterSet();

        m_ee.executePlanFragments(
                1,
                new long[] { CatalogUtil.getUniqueIdForFragment(selectBottomFrag) },
                null,
                new ParameterSet[] { params },
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
        long previousMemoryInBytes = m_ee.m_currMemoryInBytes;
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
                Encoder.decodeBase64AndDecompressToBytes(deleteBottomFrag.getPlannodetree()));
        params = ParameterSet.emptyParameterSet();
        m_ee.executePlanFragments(
                1,
                new long[] { CatalogUtil.getUniqueIdForFragment(deleteBottomFrag) },
                null,
                new ParameterSet[] { params },
                3, 3, 2, 42, Long.MAX_VALUE);

        // populate plan cache
        ActivePlanRepository.clear();
        ActivePlanRepository.addFragmentForTest(
                CatalogUtil.getUniqueIdForFragment(selectBottomFrag),
                Encoder.decodeBase64AndDecompressToBytes(selectBottomFrag.getPlannodetree()));
        params = ParameterSet.emptyParameterSet();
        m_ee.executePlanFragments(
                1,
                new long[] { CatalogUtil.getUniqueIdForFragment(selectBottomFrag) },
                null,
                new ParameterSet[] { params },
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

    void testPeakLargerThanCurr(ExecutionEngine ee) throws Exception {
        m_ee.loadCatalog( 0, m_catalog.serialize());

        m_tableSize = 20000;
        m_longOpthreshold = 10000;
        m_warehousedata.clearRowData();

        for (int i = 0; i < m_tableSize; ++i) {
            m_warehousedata.addRow(i, "name" + i, "st1", "st2", "city", "ST", "zip", 0, 0);
        }

        m_ee.loadTable(WAREHOUSE_TABLEID, m_warehousedata, 0, 0, 0, false, false, Long.MAX_VALUE);
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
                Encoder.decodeBase64AndDecompressToBytes(selectBottomFrag.getPlannodetree()));
        ParameterSet params = ParameterSet.emptyParameterSet();

        m_ee.executePlanFragments(
                1,
                new long[] { CatalogUtil.getUniqueIdForFragment(selectBottomFrag) },
                null,
                new ParameterSet[] { params },
                3, 3, 2, 42, Long.MAX_VALUE);

        // If want to see the stats, please uncomment the following line.
        // It is '8 393216 262144' on my machine.
        //System.out.println(m_ee.m_callsFromEE +" " + m_ee.m_peakMemoryInBytes + " "+ m_ee.m_currMemoryInBytes);
        assertEquals(m_longOpthreshold * m_ee.m_callsFromEE, m_ee.m_lastTuplesAccessed);
        assertTrue(m_ee.m_peakMemoryInBytes > m_ee.m_currMemoryInBytes);
    }

    private ExecutionEngine m_ee;
    private static final int CLUSTER_ID = 2;
    private static final long NODE_ID = 1;
    private int m_tableSize;
    private int m_longOpthreshold;
    private VoltTable m_warehousedata;
    private Catalog m_catalog;
    private int WAREHOUSE_TABLEID;
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
        TPCCProjectBuilder builder = new TPCCProjectBuilder();
        m_catalog = builder.createTPCCSchemaCatalog();
        Cluster cluster = m_catalog.getClusters().get("cluster");
        WAREHOUSE_TABLEID = m_catalog.getClusters().get("cluster").getDatabases().
                get("database").getTables().get("WAREHOUSE").getRelativeIndex();
        CatalogMap<Procedure> procedures = cluster.getDatabases().get("database").getProcedures();
        m_testProc = procedures.getIgnoreCase("FragmentUpdateTestProcedure");
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        m_ee.release();
        m_ee = null;
    }
}
