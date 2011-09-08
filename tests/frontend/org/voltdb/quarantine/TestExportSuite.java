/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

package org.voltdb.quarantine;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.util.ArrayList;

import org.voltdb.BackendTarget;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NullCallback;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.compiler.VoltProjectBuilder.GroupInfo;
import org.voltdb.compiler.VoltProjectBuilder.ProcedureInfo;
import org.voltdb.compiler.VoltProjectBuilder.UserInfo;
import org.voltdb.export.ExportTestClient;
import org.voltdb.exportclient.ExportClientException;
import org.voltdb.exportclient.ExportToFileClient;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.MultiConfigSuiteBuilder;
import org.voltdb.regressionsuites.RegressionSuite;
import org.voltdb.regressionsuites.TestSQLTypesSuite;
import org.voltdb.regressionsuites.VoltServerConfig;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.SnapshotVerifier;
import org.voltdb.utils.VoltFile;
import org.voltdb_testprocs.regressionsuites.sqltypesprocs.Insert;
import org.voltdb_testprocs.regressionsuites.sqltypesprocs.InsertAddedTable;
import org.voltdb_testprocs.regressionsuites.sqltypesprocs.InsertBase;
import org.voltdb_testprocs.regressionsuites.sqltypesprocs.RollbackInsert;
import org.voltdb_testprocs.regressionsuites.sqltypesprocs.Update_Export;

/**
 *  End to end Export tests using the RawProcessor and the ExportSinkServer.
 *
 *  Note, this test reuses the TestSQLTypesSuite schema and procedures.
 *  Each table in that schema, to the extent the DDL is supported by the
 *  DB, really needs an Export round trip test.
 */

public class TestExportSuite extends RegressionSuite {

    private ExportTestClient m_tester;

    /** Shove a table name and pkey in front of row data */
    private Object[] convertValsToParams(String tableName, final int i,
            final Object[] rowdata)
    {
        final Object[] params = new Object[rowdata.length + 2];
        params[0] = tableName;
        params[1] = i;
        for (int ii=0; ii < rowdata.length; ++ii)
            params[ii+2] = rowdata[ii];
        return params;
    }

    /** Push pkey into expected row data */
    private Object[] convertValsToRow(final int i, final char op,
            final Object[] rowdata) {
        final Object[] row = new Object[rowdata.length + 2];
        row[0] = (byte)(op == 'I' ? 1 : 0);
        row[1] = i;
        for (int ii=0; ii < rowdata.length; ++ii)
            row[ii+2] = rowdata[ii];
        return row;
    }

    /** Push pkey into expected row data */
    @SuppressWarnings("unused")
    private Object[] convertValsToLoaderRow(final int i, final Object[] rowdata) {
        final Object[] row = new Object[rowdata.length + 1];
        row[0] = i;
        for (int ii=0; ii < rowdata.length; ++ii)
            row[ii+1] = rowdata[ii];
        return row;
    }

    private void quiesce(final Client client)
    throws Exception
    {
        client.drain();
        client.callProcedure("@Quiesce");
    }

    private void quiesceAndVerify(final Client client, ExportTestClient tester)
    throws Exception
    {
        quiesce(client);
        tester.work();
        assertTrue(tester.allRowsVerified());
        assertTrue(tester.verifyExportOffsets());
    }

    private void quiesceAndVerifyRetryWorkOnIOException(final Client client, ExportTestClient tester)
    throws Exception
    {
        quiesce(client);
        while (true) {
            try {
                tester.work();
            } catch (ExportClientException e) {
                boolean success = reconnect(tester);
                assertTrue(success);
                System.out.println(e.toString());
                continue;
            }
            break;
        }
        assertTrue(tester.allRowsVerified());
        assertTrue(tester.verifyExportOffsets());
    }

    private void quiesceAndVerifyFalse(final Client client, ExportTestClient tester)
    throws Exception
    {
        quiesce(client);
        tester.work();
        assertFalse(tester.allRowsVerified());
    }

    @Override
    public void setUp() throws Exception
    {
        m_username = "default";
        m_password = "password";
        VoltFile.recursivelyDelete(new File("/tmp/" + System.getProperty("user.name")));
        File f = new File("/tmp/" + System.getProperty("user.name"));
        f.mkdirs();
        super.setUp();

        callbackSucceded = true;
        m_tester = new ExportTestClient(getServerConfig().getNodeCount());
        m_tester.addCredentials("export", "export");
        try {
            m_tester.connect();
        } catch (ExportClientException e) {
            e.printStackTrace();
            assertTrue(false);
        }
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        m_tester.disconnect();
        assertTrue(callbackSucceded);
    }

    private boolean callbackSucceded = true;
    class RollbackCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse clientResponse) {
            if (clientResponse.getStatus() != ClientResponse.USER_ABORT) {
                callbackSucceded = false;
                System.err.println(clientResponse.getException());
            }
        }
    }

    private boolean reconnect(ExportTestClient client) throws ExportClientException {
        for (int ii = 0; ii < 3; ii++) {
            m_tester.disconnect();
            m_tester.reserveVerifiers();
            boolean success = client.connect();
            if (success) return true;
        }
        return false;
    }

    //
    // Only notify the verifier of the first set of rows. Expect that the rows after will be truncated
    // when the snapshot is restored
    // @throws Exception
    //
    public void testExportEnterExitAdminMode() throws Exception {
        System.out.println("testExportEnterExitAdminMode");
        m_username = "admin";
        m_password = "admin";
        final Object[] rowdata = TestSQLTypesSuite.m_midValues;
        Client client = getClient();
        for (int i=0; i < 40; i++) {
            m_tester.addRow( m_tester.m_generationsSeen.first(), "NO_NULLS", i, convertValsToRow(i, 'I', rowdata));
            final Object[] params = convertValsToParams("NO_NULLS", i, rowdata);
            client.callProcedure("Insert", params);
        }

        quiesce(client);

        VoltTable stats = client.callProcedure("@Statistics", "table", (byte)0).getResults()[0];
        while (stats.advanceRow()) {
            if (stats.getString("TABLE_NAME").equals("NO_NULLS")) {
                if (stats.getLong("PARTITION_ID") == 0) {
                    assertEquals(14, stats.getLong("TUPLE_COUNT"));
                    assertEquals(3, stats.getLong("TUPLE_ALLOCATED_MEMORY"));
                } else if (stats.getLong("PARTITION_ID") == 1) {
                    assertEquals(13, stats.getLong("TUPLE_COUNT"));
                    assertEquals(2, stats.getLong("TUPLE_ALLOCATED_MEMORY"));
                } else if (stats.getLong("PARTITION_ID") == 2) {
                    assertEquals(13, stats.getLong("TUPLE_COUNT"));
                    assertEquals(2, stats.getLong("TUPLE_ALLOCATED_MEMORY"));
                } else {
                    fail();
                }
            }
        }

        ExportToFileClient exportClient =
          new ExportToFileClient(
              ',',
              "testnonce",
              new File("/tmp/" + System.getProperty("user.name")),
              60,
              "yyyyMMddHHmmss",
              null,
              0,
              false,
              false,
              false,
              0);
        exportClient.addServerInfo(new InetSocketAddress("localhost", VoltDB.DEFAULT_PORT));
        exportClient.addCredentials("export", "export");
        final Thread currentThread = Thread.currentThread();
        new Thread() {
            @Override
            public void run() {
                try {
                    Thread.sleep(3000);
                    currentThread.interrupt();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }.start();

        boolean threwException = false;
        try {
            exportClient.run();
        } catch (ExportClientException e) {
            assertTrue(e.getCause() instanceof InterruptedException);
            threwException = true;
        }
        assertTrue(threwException);

        ClientConfig adminConfig = new ClientConfig("admin", "admin");
        final Client adminClient = org.voltdb.client.ClientFactory.createClient(adminConfig);
        adminClient.createConnection("localhost", VoltDB.DEFAULT_ADMIN_PORT);
        adminClient.callProcedure("@Pause");

        new Thread() {
            @Override
            public void run() {
                try {
                    Thread.sleep(2000);
                    adminClient.callProcedure("@Resume");
                    Thread.sleep(3000);
                    currentThread.interrupt();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }.start();

        threwException = false;
        try {
            exportClient.run();
        } catch (ExportClientException e) {
            assertTrue(e.getCause() instanceof InterruptedException);
            threwException = true;
        }
        assertTrue(threwException);

        File tempDir = new File("/tmp/" + System.getProperty("user.name"));
        File outfile = null;
        for (File f : tempDir.listFiles()) {
            if (f.getName().contains("testnonce") && f.getName().endsWith(".csv")) {
                outfile = f;
                break;
            }
        }
        assertNotNull(outfile);

        FileInputStream fis = new FileInputStream(outfile);
        InputStreamReader isr = new InputStreamReader(fis, "UTF-8");
        BufferedReader br = new BufferedReader(isr);

        ArrayList<String> lines = new ArrayList<String>();

        String nextLine = null;
        while ((nextLine = br.readLine()) != null) {
            lines.add(nextLine);
        }

        assertEquals(lines.size(), 40);

        ArrayList<String[]> splitLines = new ArrayList<String[]>();

        for (String line : lines) {
            line.split(",");
        }

        for (String split[] : splitLines) {
            assertEquals(12, split.length);
            assertTrue(Integer.valueOf(split[2]) < 4);
            assertTrue(Integer.valueOf(split[3]) < 3);
            Integer siteId = Integer.valueOf(split[4]);
            assertTrue(siteId == 101 || siteId == 102 ||
                    siteId == 201 || siteId == 202 || siteId == 301 || siteId == 302);
            assertTrue(split[split.length - 1].equals(rowdata[rowdata.length -1].toString()));
        }
    }

    //
    // Only notify the verifier of the first set of rows. Expect that the rows after will be truncated
    // when the snapshot is restored
    // @throws Exception
    //
    public void testExportSnapshotTruncatesExportData() throws Exception {
        System.out.println("testExportSnapshotTruncatesExportData");
        Client client = getClient();
        for (int i=0; i < 40; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            m_tester.addRow( m_tester.m_generationsSeen.first(), "NO_NULLS", i, convertValsToRow(i, 'I', rowdata));
            final Object[] params = convertValsToParams("NO_NULLS", i, rowdata);
            client.callProcedure("Insert", params);
        }

        client.callProcedure("@SnapshotSave", "/tmp/" + System.getProperty("user.name"), "testnonce", (byte)1);

        VoltTable stats = client.callProcedure("@Statistics", "table", (byte)0).getResults()[0];
        while (stats.advanceRow()) {
            if (stats.getString("TABLE_NAME").equals("NO_NULLS")) {
                if (stats.getLong("PARTITION_ID") == 0) {
                    assertEquals(14, stats.getLong("TUPLE_COUNT"));
                    assertEquals(3, stats.getLong("TUPLE_ALLOCATED_MEMORY"));
                } else if (stats.getLong("PARTITION_ID") == 1) {
                    assertEquals(13, stats.getLong("TUPLE_COUNT"));
                    assertEquals(2, stats.getLong("TUPLE_ALLOCATED_MEMORY"));
                } else if (stats.getLong("PARTITION_ID") == 2) {
                    assertEquals(13, stats.getLong("TUPLE_COUNT"));
                    assertEquals(2, stats.getLong("TUPLE_ALLOCATED_MEMORY"));
                } else {
                    fail();
                }
            }
        }

        for (int i=40; i < 50; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            final Object[] params = convertValsToParams("NO_NULLS", i, rowdata);
            client.callProcedure("Insert", params);
        }
        quiesce(client);

        m_config.shutDown();
        m_config.startUp(false);

        client = getClient();

        client.callProcedure("@SnapshotRestore", "/tmp/" + System.getProperty("user.name"), "testnonce");

        // must still be able to verify the export data.
        quiesceAndVerifyRetryWorkOnIOException(client, m_tester);

        stats = client.callProcedure("@Statistics", "table", (byte)0).getResults()[0];
        while (stats.advanceRow()) {
            if (stats.getString("TABLE_NAME").equals("NO_NULLS")) {
                if (stats.getLong("PARTITION_ID") == 0) {
                    assertEquals(0, stats.getLong("TUPLE_COUNT"));
                    assertEquals(0, stats.getLong("TUPLE_ALLOCATED_MEMORY"));
                } else if (stats.getLong("PARTITION_ID") == 1) {
                    assertEquals(0, stats.getLong("TUPLE_COUNT"));
                    assertEquals(0, stats.getLong("TUPLE_ALLOCATED_MEMORY"));
                } else if (stats.getLong("PARTITION_ID") == 2) {
                    assertEquals(0, stats.getLong("TUPLE_COUNT"));
                    assertEquals(0, stats.getLong("TUPLE_ALLOCATED_MEMORY"));
                } else {
                    fail();
                }
            }
        }
    }

  //
  //  Test that the file client can actually works
  //
  public void testExportToFileClient() throws Exception {
      System.out.println("testExportToFileClient");
      final Client client = getClient();
      final Object[] rowdata = TestSQLTypesSuite.m_midValues;
      for (int i=0; i < 10; i++) {
          m_tester.addRow( m_tester.m_generationsSeen.last(), "NO_NULLS", i, convertValsToRow(i, 'I', rowdata));
          final Object[] params = convertValsToParams("NO_NULLS", i, rowdata);
          client.callProcedure("Insert", params);
      }
      client.drain();
      quiesce(client);
      ExportToFileClient exportClient =
          new ExportToFileClient(
              ',',
              "testnonce",
              new File("/tmp/" + System.getProperty("user.name")),
              60,
              "yyyyMMddHHmmss",
              null,
              0,
              false,
              false,
              false,
              0);
      exportClient.addServerInfo(new InetSocketAddress("localhost", VoltDB.DEFAULT_PORT));
      exportClient.addCredentials("export", "export");
      final Thread currentThread = Thread.currentThread();
      new Thread() {
          @Override
          public void run() {
              try {
                  Thread.sleep(3000);
                  currentThread.interrupt();
              } catch (Exception e) {
                  e.printStackTrace();
              }
          }
      }.start();

      boolean threwException = false;
      try {
          exportClient.run();
      } catch (ExportClientException e) {
          assertTrue(e.getCause() instanceof InterruptedException);
          threwException = true;
      }
      assertTrue(threwException);

      File tempDir = new File("/tmp/" + System.getProperty("user.name"));
      File outfile = null;
      for (File f : tempDir.listFiles()) {
          if (f.getName().contains("testnonce") && f.getName().endsWith(".csv")) {
              outfile = f;
              break;
          }
      }
      assertNotNull(outfile);

      FileInputStream fis = new FileInputStream(outfile);
      InputStreamReader isr = new InputStreamReader(fis, "UTF-8");
      BufferedReader br = new BufferedReader(isr);

      ArrayList<String> lines = new ArrayList<String>();

      String nextLine = null;
      while ((nextLine = br.readLine()) != null) {
          lines.add(nextLine);
      }

      assertEquals(lines.size(), 10);

      ArrayList<String[]> splitLines = new ArrayList<String[]>();

      for (String line : lines) {
          line.split(",");
      }

      for (String split[] : splitLines) {
          assertEquals(12, split.length);
          assertTrue(Integer.valueOf(split[2]) < 4);
          assertTrue(Integer.valueOf(split[3]) < 3);
          Integer siteId = Integer.valueOf(split[4]);
          assertTrue(siteId == 101 || siteId == 102 ||
                  siteId == 201 || siteId == 202 || siteId == 301 || siteId == 302);
          assertTrue(split[split.length - 1].equals(rowdata[rowdata.length -1].toString()));
      }
  }

    public void testExportSnapshotPreservesSequenceNumber() throws Exception {
        System.out.println("testExportSnapshotPreservesSequenceNumber");
        Client client = getClient();
        for (int i=0; i < 10; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            m_tester.addRow( m_tester.m_generationsSeen.first(), "NO_NULLS", i, convertValsToRow(i, 'I', rowdata));
            final Object[] params = convertValsToParams("NO_NULLS", i, rowdata);
            client.callProcedure("Insert", params);
        }
        quiesce(client);

        client.callProcedure("@SnapshotSave", "/tmp/" + System.getProperty("user.name"), "testnonce", (byte)1);

        m_config.shutDown();
        m_config.startUp(false);

        client = getClient();

        client.callProcedure("@SnapshotRestore", "/tmp/" + System.getProperty("user.name"), "testnonce");

        // There will be 1 disconnect for the
        for (int ii = 0; m_tester.m_generationsSeen.size() < 2 ||
             m_tester.m_verifiers.get(m_tester.m_generationsSeen.last()).size() < 6; ii++) {
            Thread.sleep(500);
            boolean threwException = false;
            try {
                m_tester.work(1000);
            } catch (ExportClientException e) {
                boolean success = reconnect(m_tester);
                assertTrue(success);
                System.out.println(e.toString());
                threwException = true;
            }
            if (ii < 2) {
                assertTrue(threwException);
            }
        }

        for (int i=10; i < 20; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            m_tester.addRow( m_tester.m_generationsSeen.last(), "NO_NULLS", i, convertValsToRow(i, 'I', rowdata));
            final Object[] params = convertValsToParams("NO_NULLS", i, rowdata);
            client.callProcedure("Insert", params);
        }
        client.drain();

        // must still be able to verify the export data.
        quiesceAndVerifyRetryWorkOnIOException(client, m_tester);
    }

    public void testExportClientIsBootedAfterRejoin() throws Exception {
        System.out.println("testExportClientIsBootedAfterRejoin");
        Client client = getClient();
        for (int i=0; i < 10; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            m_tester.addRow( m_tester.m_generationsSeen.last(), "NO_NULLS", i, convertValsToRow(i, 'I', rowdata));
            final Object[] params = convertValsToParams("NO_NULLS", i, rowdata);
            client.callProcedure("Insert", params);
        }
        client.drain();

        //  Kill a host and then process the failure
        ((LocalCluster)m_config).shutDownSingleHost(1);
        Thread.sleep(500);
        boolean threwException = false;
        try {
            m_tester.work(1000);
        } catch (ExportClientException e) {
            boolean success = reconnect(m_tester);
            assertTrue(success);
            System.out.println(e.toString());
            threwException = true;
        }
        assertTrue(threwException);

        //Reconnect, then do work and expect failure, after a recovery
        ((LocalCluster)m_config).recoverOne(1, null, "admin:admin@localhost");
        Thread.sleep(500);
        threwException = false;
        try {
            m_tester.work(1000);
        } catch (ExportClientException e) {
            boolean success = reconnect(m_tester);
            assertTrue(success);
            System.out.println(e.toString());
            threwException = true;
        }
        assertTrue(threwException);

        // After being booted try to reconnect. This should work.
        Thread.sleep(1000);
        quiesceAndVerifyRetryWorkOnIOException(getClient(), m_tester);
    }

    //  Test Export of a DROPPED table.  Queues some data to a table.
    //  Then drops the table and restarts the server. Verifies that Export can successfully
    //  drain the dropped table. IE, drop table doesn't lose Export data.
    //
    public void testExportAndThenRejoinClearsExportOverflow() throws Exception {
        System.out.println("testExportAndThenRejoinClearsExportOverflow");
        Client client = getClient();
        for (int i=0; i < 10; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            m_tester.addRow( m_tester.m_generationsSeen.last(), "NO_NULLS", i, convertValsToRow(i, 'I', rowdata));
            final Object[] params = convertValsToParams("NO_NULLS", i, rowdata);
            client.callProcedure("Insert", params);
        }
        client.drain();

        ((LocalCluster)m_config).shutDownSingleHost(1);
        File exportOverflowDir =
            new File(((LocalCluster)m_config).getSubRoots().get(1),
                    "/tmp/" + System.getProperty("user.name") + "/export_overflow");
        File files[] = exportOverflowDir.listFiles();
        long modifiedTimes[] = new long[files.length];
        int ii = 0;
        for (File f : files) {
            modifiedTimes[ii++] = f.lastModified();
        }

        ((LocalCluster)m_config).recoverOne(1, null, "admin:admin@localhost");
        Thread.sleep(500);
        File filesAfterRejoin[] = exportOverflowDir.listFiles();
        ii = 0;
        for (File f : files) {
            for (File f2 : filesAfterRejoin) {
                if (f.getPath().equals(f2.getPath()) && modifiedTimes[ii] == f2.lastModified()) {
                    fail("Files " + f + " still exists after rejoin in export overflow directory");
                }
            }
            ii++;
        }
    }

    // Test Export of an ADDED table.
    //
    public void testExportAndAddedTable() throws Exception {
        System.out.println("testExportAndAddedTable");
        final Client client = getClient();

        // add a new table
        final String newCatalogURL = Configuration.getPathToCatalogForTest("export-ddl-addedtable.jar");
        final String deploymentURL = Configuration.getPathToCatalogForTest("export-ddl-addedtable.xml");
        final ClientResponse callProcedure = client.updateApplicationCatalog(new File(newCatalogURL),
                                                                             new File(deploymentURL));
        assertTrue(callProcedure.getStatus() == ClientResponse.SUCCESS);

        // make a new tester and see if it gets the new advertisement!
        m_tester.disconnect();
        m_tester = new ExportTestClient(getServerConfig().getNodeCount());
        m_tester.connect();

        // verify that it exports
        for (int i=0; i < 10; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            m_tester.addRow( m_tester.m_generationsSeen.last(), "ADDED_TABLE", i, convertValsToRow(i, 'I', rowdata));
            final Object[]  params = convertValsToParams("ADDED_TABLE", i, rowdata);
            client.callProcedure("InsertAddedTable", params);
        }

        quiesceAndVerify(client, m_tester);
    }

    //  Test Export of a DROPPED table.  Queues some data to a table.
    //  Then drops the table and verifies that Export can successfully
    //  drain the dropped table. IE, drop table doesn't lose Export data.
    //
    public void testExportAndDroppedTable() throws Exception {
        System.out.println("testExportAndDroppedTable");
        Client client = getClient();
        for (int i=0; i < 10; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            m_tester.addRow( m_tester.m_generationsSeen.last(), "NO_NULLS", i, convertValsToRow(i, 'I', rowdata));
            final Object[] params = convertValsToParams("NO_NULLS", i, rowdata);
            client.callProcedure("Insert", params);
        }

        // now drop the no-nulls table
        final String newCatalogURL = Configuration.getPathToCatalogForTest("export-ddl-sans-nonulls.jar");
        final String deploymentURL = Configuration.getPathToCatalogForTest("export-ddl-sans-nonulls.xml");
        final ClientResponse callProcedure = client.updateApplicationCatalog(new File(newCatalogURL),
                                                                             new File(deploymentURL));
        assertTrue(callProcedure.getStatus() == ClientResponse.SUCCESS);

        client = getClient();

        m_tester.reserveVerifiers();

        // must still be able to verify the export data.
        quiesceAndVerifyRetryWorkOnIOException(client, m_tester);
    }

    //
    // Verify safe startup (we can connect clients and poll empty tables)
    //
    public void testExportSafeStartup() throws Exception
    {
        System.out.println("testExportSafeStartup");
        final Client client = getClient();
        quiesceAndVerify(client, m_tester);
    }

    public void testExportLocalServerTooMany() throws Exception
    {
        System.out.println("testExportLocalServerTooMany");
        final Client client = getClient();
        for (int i=0; i < 10; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            final Object[] params = convertValsToParams("ALLOW_NULLS", i, rowdata);
            client.callProcedure("Insert", params);
        }
        quiesceAndVerifyFalse(client, m_tester);
    }

    public void testExportLocalServerTooMany2() throws Exception
    {
        System.out.println("testExportLocalServerTooMany2");
        final Client client = getClient();
        for (int i=0; i < 10; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            // register only even rows with tester
            if ((i % 2) == 0)
            {
                m_tester.addRow( m_tester.m_generationsSeen.last(),
                        "ALLOW_NULLS", i, convertValsToRow(i, 'I', rowdata));
            }
            final Object[] params = convertValsToParams("ALLOW_NULLS", i, rowdata);
            client.callProcedure("Insert", params);
        }
        quiesceAndVerifyFalse(client, m_tester);
    }

    // Verify test infrastructure fails a test that sends too few rows
    public void testExportLocalServerTooFew() throws Exception
    {
        System.out.println("testExportLocalServerTooFew");
        final Client client = getClient();

        for (int i=0; i < 10; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            m_tester.addRow( m_tester.m_generationsSeen.last(), "ALLOW_NULLS", i, convertValsToRow(i, 'I', rowdata));
            final Object[] params = convertValsToParams("ALLOW_NULLS", i, rowdata);
            // Only do the first 7 inserts
            if (i < 7)
            {
                client.callProcedure("Insert", params);
            }
        }
        quiesceAndVerifyFalse(client, m_tester);
    }

    // Verify test infrastructure fails a test that sends mismatched data
    public void testExportLocalServerBadData() throws Exception
    {
        System.out.println("testExportLocalServerBadData");
        final Client client = getClient();

        for (int i=0; i < 10; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            // add wrong pkeys on purpose!
            m_tester.addRow( m_tester.m_generationsSeen.last(),
                    "ALLOW_NULLS", i + 10, convertValsToRow(i+10, 'I', rowdata));
            final Object[] params = convertValsToParams("ALLOW_NULLS", i, rowdata);
            client.callProcedure("Insert", params);
        }
        quiesceAndVerifyFalse(client, m_tester);
    }

    //
    // Sends ten tuples to an Export enabled VoltServer and verifies the receipt
    // of those tuples after a quiesce (shutdown). Base case.
    //
    public void testExportRoundTripStreamedTable() throws Exception
    {
        System.out.println("testExportRoundTripStreamedTable");
        final Client client = getClient();

        for (int i=0; i < 10; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            m_tester.addRow( m_tester.m_generationsSeen.last(),
                    "NO_NULLS", i, convertValsToRow(i, 'I', rowdata));
            final Object[] params = convertValsToParams("NO_NULLS", i, rowdata);
            client.callProcedure("Insert", params);
        }
        quiesceAndVerify(client, m_tester);
    }


    // Test that a table w/o Export enabled does not produce Export content
    public void testThatTablesOptIn() throws Exception
    {
        System.out.println("testThatTablesOptIn");
        final Client client = getClient();

        final Object params[] = new Object[TestSQLTypesSuite.COLS + 2];
        params[0] = "WITH_DEFAULTS";  // this table should not produce Export output

        // populate the row data
        for (int i=0; i < TestSQLTypesSuite.COLS; ++i) {
            params[i+2] = TestSQLTypesSuite.m_midValues[i];
        }

        for (int i=0; i < 10; i++) {
            params[1] = i; // pkey
            // do NOT add row to TupleVerfier as none should be produced
            client.callProcedure("Insert", params);
        }
        quiesceAndVerify(client, m_tester);
    }


    //
    // Sends many tuples to an Export enabled VoltServer and verifies the receipt
    // of each in the Export stream. Some procedures rollback (after a real insert).
    // Tests that streams are correct in the face of rollback.
    //
    public void testExportRollback() throws Exception {
        System.out.println("testExportRollback");
        final Client client = getClient();

        final double rollbackPerc = 0.15;
        long seed = 0;
        System.out.println("TestExportRollback seed " + seed);
        java.util.Random r = new java.util.Random(seed);

        // exportxxx: should pick more random data
        final Object[] rowdata = TestSQLTypesSuite.m_midValues;

        // roughly 10k rows is a full buffer it seems
        for (int pkey=0; pkey < 175000; pkey++) {
            if ((pkey % 1000) == 0) {
                System.out.println("Rollback test added " + pkey + " rows");
            }
            final Object[] params = convertValsToParams("ALLOW_NULLS", pkey, rowdata);
            double random = r.nextDouble();
            if (random <= rollbackPerc) {
                // note - do not update the el verifier as this rollsback
                boolean done;
                do {
                    done = client.callProcedure(new RollbackCallback(), "RollbackInsert", params);
                    if (done == false) {
                        client.backpressureBarrier();
                    }
                } while (!done);
            }
            else {
                m_tester.addRow( m_tester.m_generationsSeen.last(),
                        "ALLOW_NULLS", pkey, convertValsToRow(pkey, 'I', rowdata));
                // the sync call back isn't synchronous if it isn't explicitly blocked on...
                boolean done;
                do {
                    done = client.callProcedure(new NullCallback(), "Insert", params);
                    if (done == false) {
                        client.backpressureBarrier();
                    }
                } while (!done);
            }
        }
        client.drain();
        quiesceAndVerify(client, m_tester);
    }

    //
    // Verify that planner rejects updates to append-only tables
    //
    public void testExportUpdateAppendOnly() throws IOException {
        System.out.println("testExportUpdateAppendOnly");
        final Client client = getClient();
        boolean passed = false;
        try {
            client.callProcedure("@AdHoc", "Update NO_NULLS SET A_TINYINT=0 WHERE PKEY=0;");
        }
        catch (final ProcCallException e) {
            if (e.getMessage().contains("Illegal to update an export table.")) {
                passed = true;
            }
        }
        assertTrue(passed);
    }

    //
    // Verify that planner rejects reads of append-only tables.
    //
    public void testExportSelectAppendOnly() throws IOException {
        System.out.println("testExportSelectAppendOnly");
        final Client client = getClient();
        boolean passed = false;
        try {
            client.callProcedure("@AdHoc", "Select PKEY from NO_NULLS WHERE PKEY=0;");
        }
        catch (final ProcCallException e) {
            if (e.getMessage().contains("Illegal to read an export table.")) {
                passed = true;
            }
        }
        assertTrue(passed);
    }

    //
    //  Verify that planner rejects deletes of append-only tables
    //
    public void testExportDeleteAppendOnly() throws IOException {
        System.out.println("testExportDeleteAppendOnly");
        final Client client = getClient();
        boolean passed = false;
        try {
            client.callProcedure("@AdHoc", "DELETE from NO_NULLS WHERE PKEY=0;");
        }
        catch (final ProcCallException e) {
            if (e.getMessage().contains("Illegal to delete from an export table.")) {
                passed = true;
            }
        }
        assertTrue(passed);
    }

    /**
     * Multi-table test
     */
    public void testExportMultiTable() throws Exception
    {
        System.out.println("testExportMultiTable");
        final Client client = getClient();

        for (int i=0; i < 10; i++) {
            // add data to a first (persistent) table
            Object[] rowdata = TestSQLTypesSuite.m_midValues;
            m_tester.addRow( m_tester.m_generationsSeen.last(),
                    "ALLOW_NULLS", i, convertValsToRow(i, 'I', rowdata));
            Object[] params = convertValsToParams("ALLOW_NULLS", i, rowdata);
            client.callProcedure("Insert", params);

            // add data to a second (streaming) table.
            rowdata = TestSQLTypesSuite.m_defaultValues;
            m_tester.addRow( m_tester.m_generationsSeen.last(),
                    "NO_NULLS", i, convertValsToRow(i, 'I', rowdata));
            params = convertValsToParams("NO_NULLS", i, rowdata);
            client.callProcedure("Insert", params);
        }
        quiesceAndVerify(client, m_tester);
    }

    /*
     * Verify that snapshot can be enabled with a streamed table present
     */
    public void testExportPlusSnapshot() throws Exception {
        System.out.println("testExportPlusSnapshot");
        final Client client = getClient();
        for (int i=0; i < 10; i++) {
            // add data to a first (persistent) table
            Object[] rowdata = TestSQLTypesSuite.m_midValues;
            m_tester.addRow( m_tester.m_generationsSeen.last(),
                    "ALLOW_NULLS", i, convertValsToRow(i, 'I', rowdata));
            Object[] params = convertValsToParams("ALLOW_NULLS", i, rowdata);
            client.callProcedure("Insert", params);

            // add data to a second (streaming) table.
            rowdata = TestSQLTypesSuite.m_defaultValues;
            m_tester.addRow( m_tester.m_generationsSeen.last(),
                    "NO_NULLS", i, convertValsToRow(i, 'I', rowdata));
            params = convertValsToParams("NO_NULLS", i, rowdata);
            client.callProcedure("Insert", params);
        }
        // this blocks until the snapshot is complete
        client.callProcedure("@SnapshotSave", "/tmp/" + System.getProperty("user.name"), "testExportPlusSnapshot", (byte)1).getResults();

        // verify. copped from TestSaveRestoreSysprocSuite
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final PrintStream ps = new PrintStream(baos);
        final PrintStream original = System.out;
        new java.io.File("/tmp/" + System.getProperty("user.name")).mkdir();
        try {
            System.setOut(ps);
            final String args[] = new String[] {
                    "testExportPlusSnapshot",
                    "--dir",
                    "/tmp/" + System.getProperty("user.name")
            };
            SnapshotVerifier.main(args);
            ps.flush();
            final String reportString = baos.toString("UTF-8");
            assertTrue(reportString.startsWith("Snapshot valid\n"));
        } catch (final UnsupportedEncodingException e) {}
        finally {
            System.setOut(original);
        }

        // verify the el data
        quiesceAndVerify(client, m_tester);
    }

    static final GroupInfo GROUPS[] = new GroupInfo[] {
        new GroupInfo("export", false, false),
        new GroupInfo("proc", true, true),
        new GroupInfo("admin", true, true)
    };

    static final UserInfo[] USERS = new UserInfo[] {
        new UserInfo("export", "export", new String[]{"export"}),
        new UserInfo("default", "password", new String[]{"proc"}),
        new UserInfo("admin", "admin", new String[]{"proc", "admin"})
    };

    /*
     * Test suite boilerplate
     */
    static final ProcedureInfo[] PROCEDURES = {
        new ProcedureInfo( new String[]{"proc"}, Insert.class),
        new ProcedureInfo( new String[]{"proc"}, InsertBase.class),
        new ProcedureInfo( new String[]{"proc"}, RollbackInsert.class),
        new ProcedureInfo( new String[]{"proc"}, Update_Export.class)
    };

    static final ProcedureInfo[] PROCEDURES2 = {
        new ProcedureInfo( new String[]{"proc"}, Update_Export.class)
    };

    static final ProcedureInfo[] PROCEDURES3 = {
        new ProcedureInfo( new String[]{"proc"}, InsertAddedTable.class)
    };

    public TestExportSuite(final String name) {
        super(name);
    }

    static public junit.framework.Test suite() throws Exception
    {
        VoltServerConfig config;

        final MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestExportSuite.class);

        VoltProjectBuilder project = new VoltProjectBuilder();
        project.setSecurityEnabled(true);
        project.addGroups(GROUPS);
        project.addUsers(USERS);
        project.addSchema(TestSQLTypesSuite.class.getResource("sqltypessuite-ddl.sql"));
        project.addSchema(TestSQLTypesSuite.class.getResource("sqltypessuite-nonulls-ddl.sql"));
        project.addExport("org.voltdb.export.processors.RawProcessor",
                true  /*enabled*/,
                java.util.Arrays.asList(new String[]{"export"}));
        // "WITH_DEFAULTS" is a non-exported persistent table
        project.setTableAsExportOnly("ALLOW_NULLS");
        project.setTableAsExportOnly("NO_NULLS");
        project.addPartitionInfo("NO_NULLS", "PKEY");
        project.addPartitionInfo("ALLOW_NULLS", "PKEY");
        project.addPartitionInfo("WITH_DEFAULTS", "PKEY");
        project.addPartitionInfo("WITH_NULL_DEFAULTS", "PKEY");
        project.addPartitionInfo("EXPRESSIONS_WITH_NULLS", "PKEY");
        project.addPartitionInfo("EXPRESSIONS_NO_NULLS", "PKEY");
        project.addPartitionInfo("JUMBO_ROW", "PKEY");
        project.addProcedures(PROCEDURES);

        // JNI, single server
        // Use the cluster only config. Multiple topologies with the extra catalog for the
        // Add drop tests is harder. Restrict to the single (complex) topology.
        //
        //        config = new LocalSingleProcessServer("export-ddl.jar", 2,
        //                                              BackendTarget.NATIVE_EE_JNI);
        //        config.compile(project);
        //        builder.addServerConfig(config);


        /*
         * compile the catalog all tests start with
         */
        config = new LocalCluster("export-ddl-cluster-rep.jar", 2, 3, 1,
                BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ALL_RUNNING, true);
        boolean compile = config.compile(project);
        assertTrue(compile);
        builder.addServerConfig(config);


        /*
         * compile a catalog without the NO_NULLS table for add/drop tests
         */
        config = new LocalCluster("export-ddl-sans-nonulls.jar", 2, 3, 1,
                BackendTarget.NATIVE_EE_JNI,  LocalCluster.FailureState.ALL_RUNNING, true);
        project = new VoltProjectBuilder();
        project.addGroups(GROUPS);
        project.addUsers(USERS);
        project.addSchema(TestSQLTypesSuite.class.getResource("sqltypessuite-ddl.sql"));
        project.addExport("org.voltdb.export.processors.RawProcessor",
                true,  //enabled
                java.util.Arrays.asList(new String[]{"export"}));
        // "WITH_DEFAULTS" is a non-exported persistent table
        project.setTableAsExportOnly("ALLOW_NULLS");

        // and then project builder as normal
        project.addPartitionInfo("ALLOW_NULLS", "PKEY");
        project.addPartitionInfo("WITH_DEFAULTS", "PKEY");
        project.addPartitionInfo("WITH_NULL_DEFAULTS", "PKEY");
        project.addPartitionInfo("EXPRESSIONS_WITH_NULLS", "PKEY");
        project.addPartitionInfo("EXPRESSIONS_NO_NULLS", "PKEY");
        project.addPartitionInfo("JUMBO_ROW", "PKEY");
        project.addProcedures(PROCEDURES2);
        compile = config.compile(project);
        MiscUtils.copyFile(project.getPathToDeployment(),
                Configuration.getPathToCatalogForTest("export-ddl-sans-nonulls.xml"));
        assertTrue(compile);

        /*
         * compile a catalog with an added table for add/drop tests
         */
        config = new LocalCluster("export-ddl-addedtable.jar", 2, 3, 1,
                BackendTarget.NATIVE_EE_JNI,  LocalCluster.FailureState.ALL_RUNNING, true);
        project = new VoltProjectBuilder();
        project.addGroups(GROUPS);
        project.addUsers(USERS);
        project.addSchema(TestSQLTypesSuite.class.getResource("sqltypessuite-ddl.sql"));
        project.addSchema(TestSQLTypesSuite.class.getResource("sqltypessuite-nonulls-ddl.sql"));
        project.addSchema(TestSQLTypesSuite.class.getResource("sqltypessuite-addedtable-ddl.sql"));
        project.addExport("org.voltdb.export.processors.RawProcessor",
                true  /*enabled*/,
                java.util.Arrays.asList(new String[]{"export"}));
        // "WITH_DEFAULTS" is a non-exported persistent table
        project.setTableAsExportOnly("ALLOW_NULLS");   // persistent table
        project.setTableAsExportOnly("ADDED_TABLE");   // persistent table
        project.setTableAsExportOnly("NO_NULLS");      // streamed table

        // and then project builder as normal
        project.addPartitionInfo("ALLOW_NULLS", "PKEY");
        project.addPartitionInfo("ADDED_TABLE", "PKEY");
        project.addPartitionInfo("WITH_DEFAULTS", "PKEY");
        project.addPartitionInfo("WITH_NULL_DEFAULTS", "PKEY");
        project.addPartitionInfo("EXPRESSIONS_WITH_NULLS", "PKEY");
        project.addPartitionInfo("EXPRESSIONS_NO_NULLS", "PKEY");
        project.addPartitionInfo("JUMBO_ROW", "PKEY");
        project.addPartitionInfo("NO_NULLS", "PKEY");
        project.addProcedures(PROCEDURES);
        project.addProcedures(PROCEDURES3);
        compile = config.compile(project);
        MiscUtils.copyFile(project.getPathToDeployment(),
                Configuration.getPathToCatalogForTest("export-ddl-addedtable.xml"));
        assertTrue(compile);


        return builder;
    }
}
