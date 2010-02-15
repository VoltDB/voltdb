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

package org.voltdb.regressionsuites;

import java.io.IOException;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;

public class TestSystemProcedureSuite extends RegressionSuite {

    public TestSystemProcedureSuite(String name) {
        super(name);
    }

    public void testInvalidProcedureName() throws IOException {
        Client client = getClient();
        try {
            client.callProcedure("@SomeInvalidSysProcName", "1", "2");
        }
        catch (Exception e2) {
            assertEquals("Procedure @SomeInvalidSysProcName was not found", e2.getMessage());
            return;
        }
        fail("Expected exception.");
    }

    public void testLastCommittedTransaction() throws Exception {
        Client client = getClient();
        VoltTable results[] = null;
        results = client.callProcedure("@LastCommittedTransaction");

        assertTrue(results.length == 1);
        assertTrue(results[0].asScalarLong() == 0);
    }

    private final String m_loggingConfig =
        "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>" +
        "<!DOCTYPE log4j:configuration SYSTEM \"log4j.dtd\">" +
        "<log4j:configuration xmlns:log4j=\"http://jakarta.apache.org/log4j/\">" +
            "<appender name=\"Console\" class=\"org.apache.log4j.ConsoleAppender\">" +
                "<param name=\"Target\" value=\"System.out\" />" +
                "<layout class=\"org.apache.log4j.TTCCLayout\">" +
                "</layout>" +
            "</appender>" +
            "<appender name=\"Async\" class=\"org.apache.log4j.AsyncAppender\">" +
                "<param name=\"Blocking\" value=\"true\" />" +
                "<appender-ref ref=\"Console\" /> " +
            "</appender>" +
            "<root>" +
               "<priority value=\"info\" />" +
               "<appender-ref ref=\"Async\" />" +
            "</root>" +
        "</log4j:configuration>";

    public void testUpdateLoggingLocal() throws Exception {
        Client client = getClient();
        VoltTable results[] = null;
        results = client.callProcedure("@UpdateLogging", m_loggingConfig, 0L);
        for (VoltTable result : results) {
            assertEquals( 0, result.asScalarLong());
        }
    }

    public void testUpdateLoggingMultiNode() throws Exception {
        Client client = getClient();
        VoltTable results[] = null;
        results = client.callProcedure("@UpdateLogging", m_loggingConfig, 1L);
        for (VoltTable result : results) {
            assertEquals( 0, result.asScalarLong());
        }
    }

    public void testStatistics_Table() throws Exception {
        Client client = getClient();
        VoltTable results[] = null;
        results = client.callProcedure("@Statistics", "table");
        // one aggregate table returned
        assertTrue(results.length == 1);
        // with 10 rows per site. Can be two values depending on the test scenario of cluster vs. local.
        assertTrue(results[0].getRowCount() == 20 || results[0].getRowCount() == 40);

        System.out.println("Test statistics table: " + results[0].toString());
    }

    public void testStatistics_Procedure() throws Exception {
        Client client  = getClient();
        VoltTable results[] = null;
        results = client.callProcedure("@Statistics", "procedure");
        // one aggregate table returned
        assertTrue(results.length == 1);
        System.out.println("Test procedures table: " + results[0].toString());
    }

    public void testStatistics_Initiator() throws Exception {
        Client client  = getClient();
        VoltTable results[] = null;
        results = client.callProcedure("@Statistics", "INITIATOR");
        // one aggregate table returned
        assertTrue(results.length == 1);
        System.out.println("Test initiators table: " + results[0].toString());
        assertEquals(1, results[0].getRowCount());
        VoltTableRow resultRow = results[0].fetchRow(0);
        assertNotNull(resultRow);
        assertEquals("@Statistics", resultRow.getString(3));
        assertEquals( 1, resultRow.getLong(4));
    }

    public void testStatistics_InvalidSelector() throws IOException {
        Client client = getClient();
        boolean exceptionThrown = false;

        // No selector at all.
        try {
            client.callProcedure("@Statistics");
        }
        catch (Exception ex) {
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);
        exceptionThrown = false;

        // Invalid selector
        try {
            client.callProcedure("@Statistics", "garbage");
        }
        catch (Exception ex) {
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);
    }

    //public void testShutdown() {
    //    running @shutdown kills the JVM.
    //    not sure how to test this.
    // }

    public void testSystemInformation() throws IOException, ProcCallException {
        Client client = getClient();
        VoltTable results[] = client.callProcedure("@SystemInformation");
        assertEquals(1, results.length);
        System.out.println(results[0]);
    }

    // Pretty lame test but at least invoke the procedure.
    // "@Quiesce" is used more meaningfully in TestELTSuite.
    public void testQuiesce() throws IOException, ProcCallException {
        Client client = getClient();
        VoltTable results[] = client.callProcedure("@Quiesce");
        assertEquals(1, results.length);
        results[0].advanceRow();
        assertEquals(results[0].get(0, VoltType.STRING), "okay");
    }

    public void testLoadMulipartitionTable_InvalidTableName() throws IOException, ProcCallException {
        Client client = getClient();
        try {
            client.callProcedure("@LoadMultipartitionTable", "DOES_NOT_EXIST", null);
        } catch (ProcCallException ex) {
            assertTrue(true);
            return;
        }
        fail();
    }

    public void testLoadMultipartitionTable() throws IOException {
        Client client = getClient();
        // make a TPCC warehouse table
        VoltTable partitioned_table = new VoltTable(
                new VoltTable.ColumnInfo("W_ID", org.voltdb.VoltType.get((byte)3)),
                new VoltTable.ColumnInfo("W_NAME", org.voltdb.VoltType.get((byte)9)),
                new VoltTable.ColumnInfo("W_STREET_1", org.voltdb.VoltType.get((byte)9)),
                new VoltTable.ColumnInfo("W_STREET_2", org.voltdb.VoltType.get((byte)9)),
                new VoltTable.ColumnInfo("W_CITY", org.voltdb.VoltType.get((byte)9)),
                new VoltTable.ColumnInfo("W_STATE", org.voltdb.VoltType.get((byte)9)),
                new VoltTable.ColumnInfo("W_ZIP", org.voltdb.VoltType.get((byte)9)),
                new VoltTable.ColumnInfo("W_TAX",org.voltdb.VoltType.get((byte)8)),
                new VoltTable.ColumnInfo("W_YTD", org.voltdb.VoltType.get((byte)8))
        );

        for (int i = 1; i < 21; i++) {
            Object[] row = new Object[] {new Byte((byte) i),
                                         "name_" + i,
                                         "street1_" + i,
                                         "street2_" + i,
                                         "city_" + i,
                                         "ma",
                                         "zip_"  + i,
                                         new Double(i),
                                         new Double(i)};
            partitioned_table.addRow(row);
        }

        // make a TPCC item table
        VoltTable replicated_table =
            new VoltTable(new VoltTable.ColumnInfo("I_ID", VoltType.INTEGER),
                          new VoltTable.ColumnInfo("I_IM_ID", VoltType.INTEGER),
                          new VoltTable.ColumnInfo("I_NAME", VoltType.STRING),
                          new VoltTable.ColumnInfo("I_PRICE", VoltType.FLOAT),
                          new VoltTable.ColumnInfo("I_DATA", VoltType.STRING));

        for (int i = 1; i < 21; i++) {
            Object[] row = new Object[] {i,
                                         i,
                                         "name_" + i,
                                         new Double(i),
                                         "data_"  + i};

            replicated_table.addRow(row);
        }

        try {
            client.callProcedure("@LoadMultipartitionTable", "WAREHOUSE",
                                 partitioned_table);
            client.callProcedure("@LoadMultipartitionTable", "ITEM",
                                 replicated_table);
            VoltTable results[] = client.callProcedure("@Statistics", "table");

            int foundItem = 0;
            // to verify, each of the 2 sites should have 5 warehouses.
            int foundWarehouse = 0;

            System.out.println(results[0]);

            // Check that tables loaded correctly
            while(results[0].advanceRow()) {
                if (results[0].getString(2).equals("WAREHOUSE")) {
                    ++foundWarehouse;
                    //Different values depending on local cluster vs. single process hence ||
                    assertTrue(5 == results[0].getLong(4) || 10 == results[0].getLong(4));
                }
                if (results[0].getString(2).equals("ITEM"))
                {
                    ++foundItem;
                    //Different values depending on local cluster vs. single process hence ||
                    assertTrue(10 == results[0].getLong(4) || 20 == results[0].getLong(4));
                }
            }
            // make sure both warehouses were located
            //Different values depending on local cluster vs. single process hence ||
            assertTrue(2 == foundWarehouse || 4 == foundWarehouse);
            assertTrue(2 == foundItem || 4 == foundItem);
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
            fail();
        }
    }



    /**
     * Build a list of the tests to be run. Use the regression suite
     * helpers to allow multiple backends.
     * JUnit magic that uses the regression suite helper classes.
     */
    static public Test suite() {
        VoltServerConfig config = null;

        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestSystemProcedureSuite.class);

        // Not really using TPCC functionality but need a database.
        // The testLoadMultipartitionTable procedure assumes partitioning
        // on warehouse id.
        TPCCProjectBuilder project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        project.addStmtProcedure("InsertNewOrder", "INSERT INTO NEW_ORDER VALUES (?, ?, ?);", "NEW_ORDER.NO_W_ID: 2");

        config = new LocalSingleProcessServer("sysproc-twosites.jar", 2,
                                              BackendTarget.NATIVE_EE_JNI);
        config.compile(project);
        builder.addServerConfig(config);


        /*
         * Add a cluster configuration for sysprocs too
         */
        config = new LocalCluster("sysproc-cluster.jar", 2, 2, 1,
                                  BackendTarget.NATIVE_EE_JNI);
        config.compile(project);
        builder.addServerConfig(config);

        return builder;
    }
}


