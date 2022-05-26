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

import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;

import java.util.Arrays;

import org.mockito.Mockito;
import org.voltcore.logging.VoltLogger;
import org.voltdb.ElasticHashinator;
import org.voltdb.ParameterSet;
import org.voltdb.TheHashinator.HashinatorConfig;
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
import org.voltdb.jni.ExecutionEngine.LoadTableCaller;
import org.voltdb.planner.ActivePlanRepository;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.CompressionService;

import junit.framework.TestCase;

public class TestFragmentProgressUpdate extends TestCase {

    private final long READ_ONLY_TOKEN = Long.MAX_VALUE;
    private final long WRITE_TOKEN = 0;

    /**
     * This local class is intended to make it easy to preserve default
     * EE settings for initial log duration (the time required for a fragment
     * to execute before an initial warning is written to the log),
     * and the timeout latency (time after which long-running read-only queries
     * will be canceled.)
     *
     * This avoids the situation where tests fail because a previous test tweaked
     * an EE setting.
     */
    @SuppressWarnings("deprecation")
    private class AutoEngineSettings implements AutoCloseable {

        private final int m_origTimeoutLatency;
        private final long m_origInitialLogDuration;

        AutoEngineSettings() {
            m_origTimeoutLatency = m_ee.getBatchTimeout();
            m_origInitialLogDuration = m_ee.getInitialLogDurationForTest();
        }

        public void setTimeoutLatency(int timeoutLatency) {
            m_ee.setBatchTimeout(timeoutLatency);
        }

        public void setInitialLogDuration(long initialLogDuration) {
            m_ee.setInitialLogDurationForTest(initialLogDuration);
        }

        // Sets execution engine settings back to what they were.
        @Override
        public void close() throws Exception {
            m_ee.setBatchTimeout(m_origTimeoutLatency);
            m_ee.setInitialLogDurationForTest(m_origInitialLogDuration);
        }

    }

    @SuppressWarnings("deprecation")
    public void testFragmentProgressUpdate() throws Exception {
        m_ee.loadCatalog( 0, m_catalog.serialize());

        int tableSize = 5001;
        int longOpthreshold = 10000;
        m_warehousedata.clearRowData();

        for (int i = 0; i < tableSize; ++i) {
            m_warehousedata.addRow(i, "name" + i, "st1", "st2", "city", "ST", "zip", 0, 0);
        }

        m_ee.loadTable(WAREHOUSE_TABLEID, m_warehousedata, 0, 0, 0, 0, WRITE_TOKEN,
                LoadTableCaller.SNAPSHOT_THROW_ON_UNIQ_VIOLATION);
        assertEquals(tableSize, m_ee.serializeTable(WAREHOUSE_TABLEID).getRowCount());
        System.out.println("Rows loaded to table "+m_ee.serializeTable(WAREHOUSE_TABLEID).getRowCount());

        Statement selectStmt = m_testProc.getStatements().getIgnoreCase("warehouse_select");
        PlanFragment selectBottomFrag = null;

        int i = 0;
        // this kinda assumes the right order
        for (PlanFragment f : selectStmt.getFragments()) {
            if (i != 0) {
                selectBottomFrag = f;
            }
            i++;
        }
        // populate plan cache
        ActivePlanRepository.clear();
        ActivePlanRepository.addFragmentForTest(
                CatalogUtil.getUniqueIdForFragment(selectBottomFrag),
                CompressionService.decodeBase64AndDecompressToBytes(selectBottomFrag.getPlannodetree()),
                selectStmt.getSqltext());
        ParameterSet params = ParameterSet.emptyParameterSet();

        m_ee.executePlanFragments(
                1,
                new long[] { CatalogUtil.getUniqueIdForFragment(selectBottomFrag) },
                null,
                new ParameterSet[] { params },
                null,
                new String[] { selectStmt.getSqltext() },
                null,
                null,
                3, 3, 2, 42, Long.MAX_VALUE, false);
        // Like many fully successful operations, a single row fetch counts as 2 logical row operations,
        // one for locating the row and one for retrieving it.
        assertEquals(1, m_ee.m_callsFromEE);
        assertEquals(longOpthreshold, m_ee.m_lastTuplesAccessed);
        assertTrue(450000 < m_ee.m_currMemoryInBytes);
        assertTrue(550000 > m_ee.m_currMemoryInBytes);
        assertTrue(450000 < m_ee.m_peakMemoryInBytes);
        assertTrue(550000 > m_ee.m_peakMemoryInBytes);
        assertTrue(m_ee.m_peakMemoryInBytes >= m_ee.m_currMemoryInBytes);
    }

    @SuppressWarnings("deprecation")
    public void testTwoUpdates() throws Exception {
        m_ee.loadCatalog( 0, m_catalog.serialize());

        int tableSize = 10000;
        int longOpthreshold = 10000;
        m_warehousedata.clearRowData();

        for (int i = 0; i < tableSize; ++i) {
            m_warehousedata.addRow(i, "name" + i, "st1", "st2", "city", "ST", "zip", 0, 0);
        }

        m_ee.loadTable(WAREHOUSE_TABLEID, m_warehousedata, 0, 0, 0, 0, Long.MAX_VALUE,
                LoadTableCaller.SNAPSHOT_THROW_ON_UNIQ_VIOLATION);
        assertEquals(tableSize, m_ee.serializeTable(WAREHOUSE_TABLEID).getRowCount());
        System.out.println("Rows loaded to table "+m_ee.serializeTable(WAREHOUSE_TABLEID).getRowCount());

        Statement selectStmt = m_testProc.getStatements().getIgnoreCase("warehouse_select");
        PlanFragment selectBottomFrag = null;

        // delete 5000 records
        // I have no idea what's going on here, and just copy code from the above methods
        int i = 0;
        // this kinda assumes the right order
        for (PlanFragment f : selectStmt.getFragments()) {
            if (i != 0) {
                selectBottomFrag = f;
            }
            i++;
        }
        // populate plan cache
        ActivePlanRepository.clear();
        ActivePlanRepository.addFragmentForTest(
                CatalogUtil.getUniqueIdForFragment(selectBottomFrag),
                CompressionService.decodeBase64AndDecompressToBytes(selectBottomFrag.getPlannodetree()),
                selectStmt.getSqltext());
        ParameterSet params = ParameterSet.emptyParameterSet();

        m_ee.executePlanFragments(
                1,
                new long[] { CatalogUtil.getUniqueIdForFragment(selectBottomFrag) },
                null,
                new ParameterSet[] { params },
                null,
                new String[] { selectStmt.getSqltext() },
                null,
                null,
                3, 3, 2, 42, Long.MAX_VALUE, false);

        // Like many fully successful operations, a single row fetch counts as 2 logical row operations,
        // one for locating the row and one for retrieving it.
        assertEquals(2, m_ee.m_callsFromEE);
        assertEquals(longOpthreshold * m_ee.m_callsFromEE, m_ee.m_lastTuplesAccessed);
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
            if (j != 0) {
                deleteBottomFrag = f;
            }
            j++;
        }
        // populate plan cache
        ActivePlanRepository.clear();
        ActivePlanRepository.addFragmentForTest(
                CatalogUtil.getUniqueIdForFragment(deleteBottomFrag),
                CompressionService.decodeBase64AndDecompressToBytes(deleteBottomFrag.getPlannodetree()),
                deleteStmt.getSqltext());
        params = ParameterSet.emptyParameterSet();
        m_ee.executePlanFragments(
                1,
                new long[] { CatalogUtil.getUniqueIdForFragment(deleteBottomFrag) },
                null,
                new ParameterSet[] { params },
                null,
                new String[] { selectStmt.getSqltext() },
                null,
                null,
                3, 3, 2, 42, WRITE_TOKEN, false);

        // populate plan cache
        ActivePlanRepository.clear();
        ActivePlanRepository.addFragmentForTest(
                CatalogUtil.getUniqueIdForFragment(selectBottomFrag),
                CompressionService.decodeBase64AndDecompressToBytes(selectBottomFrag.getPlannodetree()),
                selectStmt.getSqltext());
        params = ParameterSet.emptyParameterSet();
        m_ee.executePlanFragments(
                1,
                new long[] { CatalogUtil.getUniqueIdForFragment(selectBottomFrag) },
                null,
                new ParameterSet[] { params },
                null,
                new String[] { selectStmt.getSqltext() },
                null,
                null,
                3, 3, 2, 42, Long.MAX_VALUE, false);
        assertTrue(m_ee.m_callsFromEE > 2);
        // here the m_lastTuplesAccessed is just the same as threshold, since we start a new fragment
        assertEquals(longOpthreshold, m_ee.m_lastTuplesAccessed);
        assertTrue(450000 < m_ee.m_currMemoryInBytes);
        assertTrue(550000 > m_ee.m_currMemoryInBytes);
        assertTrue(450000 < m_ee.m_peakMemoryInBytes);
        assertTrue(550000 > m_ee.m_peakMemoryInBytes);
        assertTrue(m_ee.m_peakMemoryInBytes >= m_ee.m_currMemoryInBytes);
        // Although it is true, but I don't think we should compare the memory usage here.
        //assertTrue(m_ee.m_currMemoryInBytes < previousMemoryInBytes);
        assertTrue(m_ee.m_peakMemoryInBytes < previousPeakMemory);
    }

    @SuppressWarnings("deprecation")
    public void testPeakLargerThanCurr() throws Exception {
        m_ee.loadCatalog( 0, m_catalog.serialize());

        int tableSize = 20000;
        int longOpthreshold = 10000;
        m_warehousedata.clearRowData();

        for (int i = 0; i < tableSize; ++i) {
            m_warehousedata.addRow(i, "name" + i, "st1", "st2", "city", "ST", "zip", 0, 0);
        }

        m_ee.loadTable(WAREHOUSE_TABLEID, m_warehousedata, 0, 0, 0, 0, WRITE_TOKEN,
                LoadTableCaller.SNAPSHOT_THROW_ON_UNIQ_VIOLATION);
        assertEquals(tableSize, m_ee.serializeTable(WAREHOUSE_TABLEID).getRowCount());
        System.out.println("Rows loaded to table "+m_ee.serializeTable(WAREHOUSE_TABLEID).getRowCount());

        Statement selectStmt = m_testProc.getStatements().getIgnoreCase("warehouse_join");
        PlanFragment selectBottomFrag = null;

        // delete 5000 records
        // I have no idea what's going on here, and just copy code from the above methods
        int i = 0;
        // this kinda assumes the right order
        for (PlanFragment f : selectStmt.getFragments()) {
            if (i != 0) {
                selectBottomFrag = f;
            }
            i++;
        }
        // populate plan cache
        ActivePlanRepository.clear();
        ActivePlanRepository.addFragmentForTest(
                CatalogUtil.getUniqueIdForFragment(selectBottomFrag),
                CompressionService.decodeBase64AndDecompressToBytes(selectBottomFrag.getPlannodetree()),
                selectStmt.getSqltext());
        ParameterSet params = ParameterSet.emptyParameterSet();

        m_ee.executePlanFragments(
                1,
                new long[] { CatalogUtil.getUniqueIdForFragment(selectBottomFrag) },
                null,
                new ParameterSet[] { params },
                null,
                new String[] { selectStmt.getSqltext() },
                null,
                null,
                3, 3, 2, 42, READ_ONLY_TOKEN, false);

        // If want to see the stats, please uncomment the following line.
        // It is '8 393216 262144' on my machine.
        //System.out.println(m_ee.m_callsFromEE +" " + m_ee.m_peakMemoryInBytes + " "+ m_ee.m_currMemoryInBytes);
        assertEquals(longOpthreshold * m_ee.m_callsFromEE, m_ee.m_lastTuplesAccessed);
        assertTrue(m_ee.m_peakMemoryInBytes > m_ee.m_currMemoryInBytes);
    }

    public void testProgressUpdateLogSqlStmt() throws Exception {
        verifyLongRunningQueries(50, 0, "item_crazy_join", 5, true, SqlTextExpectation.SQL_STATEMENT);
    }

    public void testProgressUpdateLogNoSqlStmt() throws Exception {
        verifyLongRunningQueries(50, 0, "item_crazy_join", 5, true, SqlTextExpectation.NO_STATEMENT);
    }

    public void testProgressUpdateLogSqlStmtList() throws Exception {
        verifyLongRunningQueries(50, 0, "item_crazy_join", 5, true, SqlTextExpectation.STATEMENT_LIST);
    }

    public void testProgressUpdateLogSqlStmtRW() throws Exception {
        verifyLongRunningQueries(50, 0, "item_crazy_join", 5, false, SqlTextExpectation.SQL_STATEMENT);
    }

    public void testProgressUpdateLogNoSqlStmtRW() throws Exception {
        verifyLongRunningQueries(50, 0, "item_crazy_join", 5, false, SqlTextExpectation.NO_STATEMENT);
    }

    public void testProgressUpdateLogSqlStmtListRW() throws Exception {
        verifyLongRunningQueries(50, 0, "item_crazy_join", 5, false, SqlTextExpectation.STATEMENT_LIST);
    }

    private enum SqlTextExpectation {
            SQL_STATEMENT,
            NO_STATEMENT,
            STATEMENT_LIST
    }

    /**
     * A simple class that encapsulates the fragment ID and text for
     * a SQL statement, given the name of SQLStmt object.
     *
     * Will also put the plan fragment into the ActivePlanRepository.
     */
    private class FragIdAndText {
        private final String sqlText;
        private final long fragId;

        @SuppressWarnings("deprecation")
        FragIdAndText(String stmtName) {
            Statement stmt = m_testProc.getStatements().getIgnoreCase(stmtName);
            PlanFragment frag = null;
            for (PlanFragment f : stmt.getFragments()) {
                frag = f;
            }

            fragId = CatalogUtil.getUniqueIdForFragment(frag);
            sqlText = stmt.getSqltext();

            ActivePlanRepository.addFragmentForTest(
                    fragId,
                    CompressionService.decodeBase64AndDecompressToBytes(frag.getPlannodetree()),
                    sqlText);
        }

        long id() { return fragId; }
        String text() { return sqlText; }
    }

    /**
     * Build a set of inputs for the EE to execute a group of
     * fragments.  Uses the sizes of arrays to determine how many
     * fragments to run.  The first fragments will be quick, and
     * should not trigger a log message; the last one is specified by
     * its SQL statement name.
     * @param lastStmtName   Stmt whose fragment should be executed last
     * @param fragIds        Fragment IDs, populated by this method
     * @param paramSets      Param sets, populated by this method
     * @param sqlTexts       Statement text for fragments, populated by this method
     */
    private void createExecutionEngineInputs(
            String lastStmtName,
            long[] fragIds,
            ParameterSet[] paramSets,
            String[] sqlTexts) {

        ActivePlanRepository.clear();

        int numQuickFrags = fragIds.length - 1;
        FragIdAndText quickFragIdAndText = new FragIdAndText("quick_query");
        for (int i = 0; i < numQuickFrags; ++i) {
            fragIds[i] = quickFragIdAndText.id();
            paramSets[i] = ParameterSet.emptyParameterSet();
            sqlTexts[i] = quickFragIdAndText.text();
        }

        FragIdAndText lastFragIdAndText = new FragIdAndText(lastStmtName);
        fragIds[numQuickFrags] = lastFragIdAndText.id();
        paramSets[numQuickFrags] = ParameterSet.emptyParameterSet();
        sqlTexts[numQuickFrags] = lastFragIdAndText.text();
    }

    /**
     * This method inserts a bunch of rows into table Items,
     * and executes a set of fragments in the EE.
     * @param numRowsToInsert     how many rows to insert into Items
     * @param timeout             number of ms to wait before canceling RO fragments
     * @param stmtName            SQL stmt name for last fragment to execute
     * @param numFragsToExecute   Total number of frags to execute
     *                            (if greater than 1, will prepend quick-running fragments)
     * @param readOnly            Identify the set of fragments as read-only (may timeout)
     * @param sqlTextExpectation  Behavior to expect when verify SQL text in log message
     */
    @SuppressWarnings("deprecation")
    private void verifyLongRunningQueries(
            int numRowsToInsert,
            int timeout,
            String stmtName,
            int numFragsToExecute,
            boolean readOnly,
            SqlTextExpectation sqlTextExpectation) {

        m_ee.loadCatalog( 0, m_catalog.serialize());

        m_itemData.clearRowData();
        for (int i = 0; i < numRowsToInsert; ++i) {
            m_itemData.addRow(i, i + 50, "item" + i, (double)i / 2, "data" + i);
        }

        m_ee.loadTable(ITEM_TABLEID, m_itemData, 0, 0, 0, 0, WRITE_TOKEN, LoadTableCaller.SNAPSHOT_THROW_ON_UNIQ_VIOLATION);
        assertEquals(numRowsToInsert, m_ee.serializeTable(ITEM_TABLEID).getRowCount());
        System.out.println("Rows loaded to table " + m_ee.serializeTable(ITEM_TABLEID).getRowCount());

        long[] fragIds = new long[numFragsToExecute];
        ParameterSet[] paramSets = new ParameterSet[numFragsToExecute];
        String[] sqlTexts = new String[numFragsToExecute];
        boolean[] writeFrags = new boolean[numFragsToExecute];
        for (boolean writeFrag : writeFrags) { writeFrag = false; }
        createExecutionEngineInputs(stmtName, fragIds, paramSets, sqlTexts);

        // Replace the normal logger with a mocked one, so we can verify the message
        VoltLogger mockedLogger = Mockito.mock(VoltLogger.class);
        ExecutionEngine.setVoltLoggerForTest(mockedLogger);

        try (AutoEngineSettings aes = new AutoEngineSettings()) {
            // Set the log duration to be short to ensure that a message will be logged
            // for long-running queries
            aes.setInitialLogDuration(1);
            aes.setTimeoutLatency(timeout);

            // NOTE: callers of this method specify something other than SQL_STATEMENT
            // in order to prove that we don't crash if the sqlTexts array passed to
            // (Java class) ExecutionEngine is malformed.
            //
            // This issue was related to ENG-7610, which took down the EE.  It has since
            // been fixed, so (we hope) nothing like this will happen in the wild.
            switch (sqlTextExpectation) {
            case SQL_STATEMENT:
                // leave sqlTexts AS-IS
                break;
            case NO_STATEMENT:
                sqlTexts = null;
                break;
            case STATEMENT_LIST:
                // Leave off the last item, which is the one that needs to be
                // reported.
                sqlTexts = Arrays.copyOfRange(sqlTexts, 0, numFragsToExecute - 1);
                break;
            default:
                fail("Invalid value for sqlTextExpectation");
            }

            m_ee.setupProcedure(null);

            m_ee.executePlanFragments(
                    numFragsToExecute,
                    fragIds,
                    null,
                    paramSets,
                    null,
                    sqlTexts,
                    writeFrags,
                    null,
                    3, 3, 2, 42,
                    readOnly ? READ_ONLY_TOKEN : WRITE_TOKEN, false);
            if (readOnly && timeout > 0) {
                // only time out read queries
                fail();
            }
        } catch (Exception ex) {
            String msg = String.format("A SQL query was terminated after %.03f seconds "
                    + "because it exceeded",
                    timeout/1000.0);
            assertTrue(ex.getMessage().contains(msg));
        }

        String expectedSqlTextMsg = null;
        switch (sqlTextExpectation) {
        case SQL_STATEMENT:
            String sqlText = sqlTexts[numFragsToExecute - 1];
            expectedSqlTextMsg = String.format("Executing SQL statement is \"%s\".", sqlText);
            break;
        case NO_STATEMENT:
            expectedSqlTextMsg = "SQL statement text is not available.";
            break;
        case STATEMENT_LIST:
            expectedSqlTextMsg = "Unable to report specific SQL statement text "
                    + "for fragment task message index " + (numFragsToExecute - 1) + ". "
                    + "It MAY be one of these " + (numFragsToExecute - 1) + " items: "
                    + "\"SELECT W_ID FROM WAREHOUSE LIMIT 1;\", ";
            break;
        default:
            fail("Invalid value for sqlTextExpectation");
        }

        verify(mockedLogger, atLeastOnce()).info(contains(expectedSqlTextMsg));
    }

    /**
     * A simpler version of verifyLongRunningQueries that executes just one
     * fragment and assumes the SQL text will be correctly displayed.
     * @param numRowsToInsert     how many rows to insert into Items
     * @param timeout             number of ms to wait before canceling RO fragments
     * @param stmtName            SQL stmt name for last fragment to execute
     * @param readOnly            Identify the set of fragments as read-only (may timeout)
     */
    private void verifyLongRunningQueries(
            int numRowsToInsert,
            int timeout,
            String stmtName,
            boolean readOnly) {
        verifyLongRunningQueries(numRowsToInsert, timeout, stmtName, 1, readOnly, SqlTextExpectation.SQL_STATEMENT);
    }

    public void testTimingQueriyReadOnly200() throws Exception {
        verifyLongRunningQueries(200, 0, "item_crazy_join", true);
    }

    public void testTimingQueryReadOnly200_2() throws Exception {
        verifyLongRunningQueries(200, 0, "item_crazy_join", true);
    }

    public void testTimingQueryReadOnly300() throws Exception {
        verifyLongRunningQueries(300, 0, "item_crazy_join", true);
    }

    public void testTimingQueryWrite() throws Exception {
        verifyLongRunningQueries(50000, 0, "item_big_del", false);
    }

    public void testTimingoutQueryWrite() throws Exception {
        verifyLongRunningQueries(50000, 100, "item_big_del", false);
    }

    private ExecutionEngine m_ee;
    private VoltTable m_warehousedata;
    private VoltTable m_itemData;
    private Catalog m_catalog;
    private int WAREHOUSE_TABLEID;
    private int ITEM_TABLEID;
    private Procedure m_testProc;

    @Override
    protected void setUp() throws Exception {
        final int CLUSTER_ID = 2;
        final long NODE_ID = 1;

        super.setUp();
        VoltDB.instance().readBuildInfo();
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
        m_catalog = TPCCProjectBuilder.getTPCCSchemaCatalog();
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
                1,
                0,
                "",
                0,
                64*1024,
                false,
                -1,
                false,
                100,
                new HashinatorConfig(ElasticHashinator.getConfigureBytes(1),
                                     0,
                                     0),
                true);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        m_ee.release();
        m_ee = null;
    }
}
