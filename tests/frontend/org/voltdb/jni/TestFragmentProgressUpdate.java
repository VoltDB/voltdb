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

import junit.framework.TestCase;

import org.voltdb.BackendTarget;
import org.voltdb.LegacyHashinator;
import org.voltdb.ParameterSet;
import org.voltdb.RunningProcedureContext;
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
                HashinatorType.LEGACY,
                LegacyHashinator.getConfigureBytes(1));
        testFragmentProgressUpdate(m_ee);
    }

//    public void testIPCFragmentProgressUpdate() throws Exception {
//        m_ee = new ExecutionEngineIPC(
//                CLUSTER_ID,
//                NODE_ID,
//                0,
//                0,
//                "",
//                100,
//                BackendTarget.NATIVE_EE_IPC,
//                10000,
//                HashinatorType.LEGACY,
//                LegacyHashinator.getConfigureBytes(1));
//        testFragmentProgressUpdate(m_ee);
//    }

    public void testFragmentProgressUpdate(ExecutionEngine ee) throws Exception {
        TPCCProjectBuilder builder = new TPCCProjectBuilder();
        Catalog catalog = builder.createTPCCSchemaCatalog();
        int WAREHOUSE_TABLEID = catalog.getClusters().get("cluster").getDatabases().
                get("database").getTables().get("WAREHOUSE").getRelativeIndex();

        m_ee.loadCatalog( 0, catalog.serialize());

        int tableSize = 10001;
        int longOpThreshold = 10000;
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
        for (int i = 0; i < tableSize; ++i) {
            warehousedata.addRow(i, "name" + i, "st1", "st2", "city", "ST", "zip", 0, 0);
        }

        m_ee.loadTable(WAREHOUSE_TABLEID, warehousedata, 0, 0, false, Long.MAX_VALUE);
        assertEquals(tableSize, m_ee.serializeTable(WAREHOUSE_TABLEID).getRowCount());
        System.out.println("Rows loaded to table "+m_ee.serializeTable(WAREHOUSE_TABLEID).getRowCount());

        Cluster cluster =  catalog.getClusters().get("cluster");
        CatalogMap<Procedure> procedures = cluster.getDatabases().get("database").getProcedures();
        Procedure selectProc = procedures.getIgnoreCase("SelectAll");
        Statement selectStmt = selectProc.getStatements().getIgnoreCase("warehouse");
        PlanFragment selectTopFrag = null;
        PlanFragment selectBottomFrag = null;

        int i = 0;
        // this kinda assumes the right order
        for (PlanFragment f : selectStmt.getFragments()) {
            if (i == 0) selectTopFrag = f;
            else selectBottomFrag = f;
            i++;
        }
        // populate plan cache
        ActivePlanRepository.clear();
        ActivePlanRepository.addFragmentForTest(
                CatalogUtil.getUniqueIdForFragment(selectBottomFrag),
                Encoder.base64Decode(selectBottomFrag.getPlannodetree()));
        ParameterSet params = ParameterSet.emptyParameterSet();

        m_ee.executePlanFragments(
                1,
                new long[] { CatalogUtil.getUniqueIdForFragment(selectBottomFrag) },
                null,
                new ParameterSet[] { params },
                3, 2, 42, Long.MAX_VALUE, new RunningProcedureContext());
        assertEquals(1, m_ee.m_callsFromEE);
        assertEquals(longOpThreshold, m_ee.m_lastTuplesAccessed);
    }

    private ExecutionEngine m_ee;
    private static final int CLUSTER_ID = 2;
    private static final long NODE_ID = 1;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        VoltDB.instance().readBuildInfo("Test");
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        m_ee.release();
        m_ee = null;
    }
}
