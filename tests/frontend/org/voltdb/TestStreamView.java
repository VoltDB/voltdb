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

package org.voltdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.compiler.deploymentfile.ServerExportEnum;
import org.voltdb.export.ExportDataProcessor;
import org.voltdb.regressionsuites.RegressionSuite;
import org.voltdb.utils.MiscUtils;

/**
 */

public class TestStreamView
{
    private VoltDB.Configuration m_config;
    private ServerThread m_localServer;
    private Client m_client;

    @Test
    public void testStreamViewOnCatalogUpdate() throws NoConnectionsException, IOException, ProcCallException {
        String createStream = "CREATE STREAM ddata_stream "
                + "PARTITION ON COLUMN vin "
                + "EXPORT TO TARGET ddata_stream_target "
                + "( "
                + "     VIN VARCHAR(18) NOT NULL, "
                + "     PRICE INTEGER NOT NULL,"
                + "     COLLECT_DATE TIMESTAMP NOT NULL,"
                + ");";
        ClientResponse response = m_client.callProcedure("@AdHoc", createStream);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        String createStreamView = "CREATE VIEW message_count_by_vin AS "
                + "SELECT VIN, dayofweek(collect_date), count(*) how_many, sum(price), min(price), max(price) "
                + "FROM ddata_stream "
                + "GROUP BY VIN, dayofweek(collect_date);";
        response = m_client.callProcedure("@AdHoc", createStreamView);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());

        VoltTable[] results;
        Random r = new Random();
        // insert into stream make sure you get more blocks
        for (int i=0; i < 1000; i++) {
            String vin = String.valueOf(Math.abs(r.nextInt()));
            int price = Math.abs(r.nextInt(10_000));
            String insertStmt = "insert into ddata_stream VALUES ('%s', '%d', now);";
            String q = String.format(insertStmt, vin, price);
            results = m_client.callProcedure("@AdHoc", q).getResults();
            assertEquals(1, results[0].asScalarLong());
        }
        response = m_client.callProcedure("@AdHoc", "alter stream ddata_stream add column d int not null;");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = m_client.callProcedure("@AdHoc", "alter stream ddata_stream alter column d varchar(32) not null;");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = m_client.callProcedure("@AdHoc", "alter stream ddata_stream drop column d;");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        results = m_client.callProcedure("@AdHoc", "select * from message_count_by_vin;").getResults();
        assertTrue(results[0].m_rowCount > 1);
    }

    @Test
    public void testDeleteAll() throws Exception
    {
        // insert into stream
        String insertStmt = "insert into testexporttable1 VALUES ('VOLT', 10, 10, 100, now);";
        VoltTable[] results = m_client.callProcedure("@AdHoc", insertStmt).getResults();
        assertEquals(1, results[0].asScalarLong());

        insertStmt = "insert into testexporttable1 VALUES ('VOLT', 10, 11, 100, now);";
        results = m_client.callProcedure("@AdHoc", insertStmt).getResults();
        assertEquals(1, results[0].asScalarLong());

        insertStmt = "insert into testexporttable1 VALUES ('GOOG', 500, 510, 100, now);";
        results = m_client.callProcedure("@AdHoc", insertStmt).getResults();
        assertEquals(1, results[0].asScalarLong());

        String selectStmt = "SELECT count(*) FROM bidask_minmax;";
        results = m_client.callProcedure("@AdHoc", selectStmt).getResults();
        assertEquals(2, results[0].asScalarLong());

        String deleteStmt = "DELETE FROM bidask_minmax;";
        results = m_client.callProcedure("@AdHoc", deleteStmt).getResults();
        assertEquals(2, results[0].asScalarLong());

        selectStmt = "SELECT count(*) FROM bidask_minmax;";
        results = m_client.callProcedure("@AdHoc", selectStmt).getResults();
        assertEquals(0, results[0].asScalarLong());
    }

    @Test
    public void testTruncate() throws Exception
    {
        // insert into stream
        String insertStmt = "insert into testexporttable1 VALUES ('VOLT', 10, 10, 100, now);";
        VoltTable[] results = m_client.callProcedure("@AdHoc", insertStmt).getResults();
        assertEquals(1, results[0].asScalarLong());

        insertStmt = "insert into testexporttable1 VALUES ('VOLT', 10, 11, 100, now);";
        results = m_client.callProcedure("@AdHoc", insertStmt).getResults();
        assertEquals(1, results[0].asScalarLong());

        insertStmt = "insert into testexporttable1 VALUES ('GOOG', 500, 510, 100, now);";
        results = m_client.callProcedure("@AdHoc", insertStmt).getResults();
        assertEquals(1, results[0].asScalarLong());

        String selectStmt = "SELECT count(*) FROM bidask_minmax;";
        results = m_client.callProcedure("@AdHoc", selectStmt).getResults();
        assertEquals(2, results[0].asScalarLong());

        String truncateStmt = "TRUNCATE TABLE bidask_minmax;";
        try {
            results = m_client.callProcedure("@AdHoc", truncateStmt).getResults();
            fail();
        } catch(ProcCallException e) {
            // expected
        }
        assertEquals(2, results[0].asScalarLong());

        selectStmt = "SELECT count(*) FROM bidask_minmax;";
        results = m_client.callProcedure("@AdHoc", selectStmt).getResults();
        assertEquals(2, results[0].asScalarLong());
    }

    @Test
    public void testDeleteWherePartitionedColumn() throws Exception
    {
        // insert into stream
        String insertStmt = "insert into testexporttable1 VALUES ('VOLT', 10, 10, 100, now);";
        VoltTable[] results = m_client.callProcedure("@AdHoc", insertStmt).getResults();
        assertEquals(1, results[0].asScalarLong());

        insertStmt = "insert into testexporttable1 VALUES ('VOLT', 10, 11, 100, now);";
        results = m_client.callProcedure("@AdHoc", insertStmt).getResults();
        assertEquals(1, results[0].asScalarLong());

        insertStmt = "insert into testexporttable1 VALUES ('GOOG', 500, 510, 100, now);";
        results = m_client.callProcedure("@AdHoc", insertStmt).getResults();
        assertEquals(1, results[0].asScalarLong());

        String selectStmt = "SELECT count(*) FROM bidask_minmax;";
        results = m_client.callProcedure("@AdHoc", selectStmt).getResults();
        assertEquals(2, results[0].asScalarLong());

        selectStmt = "SELECT count(*) FROM bidask_minmax WHERE symbol='GOOG';";
        results = m_client.callProcedure("@AdHoc", selectStmt).getResults();
        assertEquals(1, results[0].asScalarLong());

        String deleteStmt = "DELETE FROM bidask_minmax WHERE symbol='GOOG';";
        results = m_client.callProcedure("@AdHoc", deleteStmt).getResults();
        assertEquals(1, results[0].asScalarLong());

        selectStmt = "SELECT count(*) FROM bidask_minmax;";
        results = m_client.callProcedure("@AdHoc", selectStmt).getResults();
        assertEquals(1, results[0].asScalarLong());
    }

    @Test
    public void testDeleteWhereNonPartitionedColumn() throws Exception
    {
        // insert into stream
        String insertStmt = "insert into testexporttable1 VALUES ('VOLT', 10, 10, 100, now);";
        VoltTable[] results = m_client.callProcedure("@AdHoc", insertStmt).getResults();
        assertEquals(1, results[0].asScalarLong());

        insertStmt = "insert into testexporttable1 VALUES ('VOLT', 10, 11, 100, now);";
        results = m_client.callProcedure("@AdHoc", insertStmt).getResults();
        assertEquals(1, results[0].asScalarLong());

        insertStmt = "insert into testexporttable1 VALUES ('GOOG', 500, 510, 100, now);";
        results = m_client.callProcedure("@AdHoc", insertStmt).getResults();
        assertEquals(1, results[0].asScalarLong());

        String selectStmt = "SELECT count(*) FROM bidask_minmax;";
        results = m_client.callProcedure("@AdHoc", selectStmt).getResults();
        assertEquals(2, results[0].asScalarLong());

        selectStmt = "SELECT count(*) FROM bidask_minmax WHERE txn_count=2;";
        results = m_client.callProcedure("@AdHoc", selectStmt).getResults();
        assertEquals(1, results[0].asScalarLong());

        String deleteStmt = "DELETE FROM bidask_minmax WHERE txn_count=2;";
        results = m_client.callProcedure("@AdHoc", deleteStmt).getResults();
        assertEquals(1, results[0].asScalarLong());

        selectStmt = "SELECT count(*) FROM bidask_minmax;";
        results = m_client.callProcedure("@AdHoc", selectStmt).getResults();
        assertEquals(1, results[0].asScalarLong());
    }

    @Test
    public void testUpdateWherePartitionedColumn() throws Exception
    {
        // insert into stream
        String insertStmt = "insert into testexporttable1 VALUES ('VOLT', 10, 10, 100, now);";
        VoltTable[] results = m_client.callProcedure("@AdHoc", insertStmt).getResults();
        assertEquals(1, results[0].asScalarLong());

        String selectStmt = "SELECT txn_count FROM bidask_minmax WHERE symbol='VOLT';";
        results = m_client.callProcedure("@AdHoc", selectStmt).getResults();
        assertEquals(1, results[0].asScalarLong());

        String updateStmt = "UPDATE bidask_minmax SET txn_count = 10 WHERE symbol='VOLT';";
        results = m_client.callProcedure("@AdHoc", updateStmt).getResults();
        assertEquals(1, results[0].asScalarLong());

        selectStmt = "SELECT txn_count FROM bidask_minmax WHERE symbol='VOLT';";
        results = m_client.callProcedure("@AdHoc", selectStmt).getResults();
        assertEquals(10, results[0].asScalarLong());
    }

    @Test
    public void testUpdateWhereNonPartitionedColumn() throws Exception
    {
        // insert into stream
        String insertStmt = "insert into testexporttable1 VALUES ('VOLT', 10, 10, 100, now);";
        VoltTable[] results = m_client.callProcedure("@AdHoc", insertStmt).getResults();
        assertEquals(1, results[0].asScalarLong());

        insertStmt = "insert into testexporttable1 VALUES ('VOLT', 10, 11, 100, now);";
        results = m_client.callProcedure("@AdHoc", insertStmt).getResults();
        assertEquals(1, results[0].asScalarLong());

        insertStmt = "insert into testexporttable1 VALUES ('GOOG', 500, 510, 100, now);";
        results = m_client.callProcedure("@AdHoc", insertStmt).getResults();
        assertEquals(1, results[0].asScalarLong());

        String selectStmt = "SELECT txn_count FROM bidask_minmax WHERE symbol='VOLT';";
        results = m_client.callProcedure("@AdHoc", selectStmt).getResults();
        assertEquals(2, results[0].asScalarLong());

        String updateStmt = "UPDATE bidask_minmax SET txn_count = 10 WHERE txn_count=2;";
        results = m_client.callProcedure("@AdHoc", updateStmt).getResults();
        assertEquals(1, results[0].asScalarLong());

        selectStmt = "SELECT txn_count FROM bidask_minmax WHERE symbol='VOLT';";
        results = m_client.callProcedure("@AdHoc", selectStmt).getResults();
        assertEquals(10, results[0].asScalarLong());
    }

    @Test
    public void testUpdateNoWhere() throws Exception
    {
        // insert into stream
        String insertStmt = "insert into testexporttable1 VALUES ('VOLT', 10, 10, 100, now);";
        VoltTable[] results = m_client.callProcedure("@AdHoc", insertStmt).getResults();
        assertEquals(1, results[0].asScalarLong());

        insertStmt = "insert into testexporttable1 VALUES ('VOLT', 10, 11, 100, now);";
        results = m_client.callProcedure("@AdHoc", insertStmt).getResults();
        assertEquals(1, results[0].asScalarLong());

        insertStmt = "insert into testexporttable1 VALUES ('GOOG', 500, 510, 100, now);";
        results = m_client.callProcedure("@AdHoc", insertStmt).getResults();
        assertEquals(1, results[0].asScalarLong());

        String selectStmt = "SELECT txn_count FROM bidask_minmax WHERE symbol='VOLT';";
        results = m_client.callProcedure("@AdHoc", selectStmt).getResults();
        assertEquals(2, results[0].asScalarLong());

        selectStmt = "SELECT txn_count FROM bidask_minmax WHERE symbol='GOOG';";
        results = m_client.callProcedure("@AdHoc", selectStmt).getResults();
        assertEquals(1, results[0].asScalarLong());

        String updateStmt = "UPDATE bidask_minmax SET txn_count = 10;";
        results = m_client.callProcedure("@AdHoc", updateStmt).getResults();
        assertEquals(2, results[0].asScalarLong());

        selectStmt = "SELECT txn_count FROM bidask_minmax WHERE symbol='VOLT';";
        results = m_client.callProcedure("@AdHoc", selectStmt).getResults();
        assertEquals(10, results[0].asScalarLong());

        selectStmt = "SELECT txn_count FROM bidask_minmax WHERE symbol='GOOG';";
        results = m_client.callProcedure("@AdHoc", selectStmt).getResults();
        assertEquals(10, results[0].asScalarLong());
    }

    @Test
    public void testDeleteAndValidateReupdate() throws Exception
    {
        VoltTable[] results;
        //Do twice.
        for (int c = 0; c < 2;c++) {
            // insert into stream make sure you get more blocks
            for (int i=0; i < 100000; i++) {
                String insertStmt = "insert into testexporttable1 VALUES ('VOLT%d', %d, %d, 100, now);";
                String q = String.format(insertStmt, i, i, i);
                results = m_client.callProcedure("@AdHoc", q).getResults();
                assertEquals(1, results[0].asScalarLong());
            }

            String deleteStmt = "DELETE FROM bidask_minmax;";
            results = m_client.callProcedure("@AdHoc", deleteStmt).getResults();
            assertEquals(100000, results[0].asScalarLong());
            results = m_client.callProcedure("@AdHoc", "select count(*) from bidask_minmax").getResults();
            assertEquals(0, results[0].asScalarLong());

            // Make sure view still updated on inserts.
            String insertStmt = "insert into testexporttable1 VALUES ('VOLT', 10, 11, 100, now);";
            results = m_client.callProcedure("@AdHoc", insertStmt).getResults();
            assertEquals(1, results[0].asScalarLong());
            insertStmt = "insert into testexporttable1 VALUES ('VOLT', 10, 10, 100, now);";
            results = m_client.callProcedure("@AdHoc", insertStmt).getResults();
            assertEquals(1, results[0].asScalarLong());
            insertStmt = "insert into testexporttable1 VALUES ('GOOG', 500, 510, 100, now);";
            results = m_client.callProcedure("@AdHoc", insertStmt).getResults();
            assertEquals(1, results[0].asScalarLong());
            String selectStmt = "SELECT count(*) FROM bidask_minmax;";
            results = m_client.callProcedure("@AdHoc", selectStmt).getResults();
            assertEquals(2, results[0].asScalarLong());

            // ENG-11832
            selectStmt = "SELECT max_ask, max_bid FROM bidask_minmax;";
            results = m_client.callProcedure("@AdHoc", selectStmt).getResults();
            RegressionSuite.assertContentOfTable(new Object[][] {{510.0, 500.0}, {11.0, 10.0}}, results[0]);
            results = m_client.callProcedure("@AdHoc", "UPDATE bidask_minmax SET max_ask = max_bid + 11").getResults();
            assertEquals(2, results[0].asScalarLong());
            results = m_client.callProcedure("@AdHoc", selectStmt).getResults();
            RegressionSuite.assertContentOfTable(new Object[][] {{511.0, 500.0}, {21.0, 10.0}}, results[0]);

            // Make sure we can update
            String updateStmt = "UPDATE bidask_minmax SET txn_count = 10 WHERE txn_count=2;";
            results = m_client.callProcedure("@AdHoc", updateStmt).getResults();
            assertEquals(1, results[0].asScalarLong());
            selectStmt = "SELECT txn_count FROM bidask_minmax WHERE symbol='VOLT';";
            results = m_client.callProcedure("@AdHoc", selectStmt).getResults();
            assertEquals(10, results[0].asScalarLong());

            // Delete everything VOLT & GOOG
            results = m_client.callProcedure("@AdHoc", deleteStmt).getResults();
            assertEquals(2, results[0].asScalarLong());
            results = m_client.callProcedure("@AdHoc", "select count(*) from bidask_minmax").getResults();
            assertEquals(0, results[0].asScalarLong());
        }
    }

    @Before
    public void setUp() throws Exception
    {
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addLiteralSchema(
           "CREATE STREAM testexporttable1 PARTITION ON COLUMN symbol EXPORT TO TARGET noop\n" +
             "(symbol VARCHAR(25) NOT NULL,\n" +
               "bid FLOAT NOT NULL,\n" +
               "ask FLOAT NOT NULL,\n" +
               "volume INTEGER NOT NULL,\n" +
               "txn_time TIMESTAMP NOT NULL);\n" +
           "CREATE VIEW bidask_minmax (symbol, txn_count, volume, max_bid, max_ask, min_bid, min_ask)\n" +
           "AS SELECT symbol, count(*), sum(volume), max(bid), max(ask), min(bid), min(ask)\n" +
           "FROM testexporttable1 GROUP BY symbol;" +
           "CREATE INDEX bidask_minmax_idx on bidask_minmax ( ABS(min_ask) );");

        Properties props = new Properties();
        project.addExport(true, ServerExportEnum.CUSTOM, "org.voltdb.exportclient.NoOpExporter", props, "noop");
        project.setUseDDLSchema(true);

        boolean compiled = project.compile(Configuration.getPathToCatalogForTest("test-stream-view.jar"), 1, 1, 0);
        assertTrue(compiled);
        MiscUtils.copyFile(project.getPathToDeployment(), Configuration.getPathToCatalogForTest("test-stream-view.xml"));

        m_config = new VoltDB.Configuration();
        m_config.m_pathToCatalog = Configuration.getPathToCatalogForTest("test-stream-view.jar");
        m_config.m_pathToDeployment = Configuration.getPathToCatalogForTest("test-stream-view.xml");
        m_localServer = new ServerThread(m_config);
        m_localServer.start();
        m_localServer.waitForInitialization();

        m_client = ClientFactory.createClient();
        m_client.createConnection("localhost:" + m_config.m_adminPort);
    }

    @After
    public void tearDown() throws Exception {
        if (m_client != null) {
            m_client.close();
        }
        if (m_localServer != null) {
            m_localServer.shutdown();
        }
    }
}
