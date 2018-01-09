/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.export.ExportDataProcessor;
import org.voltdb.export.ExportTestExpectedData;
import org.voltdb.export.TestExportBaseSocketExport;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.MultiConfigSuiteBuilder;
import org.voltdb.regressionsuites.TestSQLTypesSuite;
import org.voltdb.utils.VoltFile;

/**
 * End to end Export tests using the injected custom export.
 *
 *  These tests all cover insert into export tables using
 *  INSERT INTO ... SELECT
 */

public class TestExportInsertIntoSelectSuite extends TestExportBaseSocketExport {
    private static final int k_factor = 0;
    static private final String EXPORT_TARGET_PART = "ALLOW_NULLS";
    static private final String EXPORT_TARGET_REPL = "ALLOW_NULLS_EXPORT_REPL";

    static private final String SOURCE_PART = "NO_NULLS";
    static private final String SOURCE_REPL = "NO_NULLS_REPL";

    static private List<Object[]> ROWS = new ArrayList<>();
    static {
        try {
            ROWS = generateRows();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
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

        startListener();
        m_verifier = new ExportTestExpectedData(m_serverSockets, m_isExportReplicated, true, k_factor+1);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        System.out.println("Shutting down client and server");
        closeClientAndServer();
    }

    // I had hoped to write a simple method to increment subclasses of Number.
    // I didn't think I would need to resort to reflection to do this.
    // Is there a better way?
    private static Number incrementBy(Number num, int val) throws Exception {
        long n = num.longValue() + val;
        Class<? extends Number> clazz = num.getClass();
        Object o = clazz.getMethod("valueOf", String.class).invoke(null, Long.toString(n));
        return (Number)o;
    }

    private static List<Object[]> generateRows() throws Exception {
        List<Object[]> rows = new ArrayList<>();

        final Object[] midValues = TestSQLTypesSuite.m_midValues;
        for (int i = 0; i < 10; ++i) {
            Object[] newRow = Arrays.copyOf(midValues, midValues.length);

            int numericColumns[] = {0, 1, 2, 3, 4}; // the 4 integral types and DOUBLE
            for (int col : numericColumns) {
                Number num = (Number)newRow[col];
                newRow[col] = incrementBy(num, i);
            }
            rows.add(newRow);
        }
        return rows;
    }

    private void doInsertIntoSelectTest(
            String exportTarget,
            String source,
            String procedure
            ) throws Exception {
        String diagnosticHeader = "[" + procedure + "] Inserting into " + exportTarget
                + " selecting from " + source + ": ";
        final Client client = getClient();
        boolean isReplicatedSource = source.contains("REPL");
        boolean isReplicatedTarget = exportTarget.contains("REPL");
        boolean isSinglePartitionProcedure = procedure.contains("SP");

        int i = 0;
        for (Object[] row : ROWS) {
            // Add the row to the set of rows we expect to be seen by export.
            // For replicated sources, we do the export on partition hash 0.
            m_verifier.addRow(client, exportTarget, (isReplicatedSource && isSinglePartitionProcedure) ? 0 : i,
                    convertValsToRow(i, 'I', row));

            // insert into source table
            final Object[] params = convertValsToParams(source, i, row);
            VoltTable vt = client.callProcedure("InsertMulti", params).getResults()[0];
            assertEquals(diagnosticHeader + "failed to insert into source",
                    1, vt.asScalarLong());
            ++i;
        }

        // Now insert into the export table.
        long numberOfInserts = 0;
        if ((! isReplicatedSource)  && isSinglePartitionProcedure) {

            // Use the run everywhere pattern for a partitioned source
            // read from a single-partition procedure.
            VoltTable partitionKeys = client.callProcedure("@GetPartitionKeys", "INTEGER")
                    .getResults()[0];
            while (partitionKeys.advanceRow()) {
                int key = (int)partitionKeys.getLong("PARTITION_KEY");
                ClientResponse cr = client.callProcedure(procedure, key, exportTarget, source);
                assertEquals(diagnosticHeader + "insert into select proc failed",
                        ClientResponse.SUCCESS, cr.getStatus());
                numberOfInserts += cr.getResults()[0].asScalarLong();
            }
        }
        else {
            ClientResponse cr = client.callProcedure(procedure, 0, exportTarget, source);
            assertEquals(diagnosticHeader + "insert into select proc failed",
                    ClientResponse.SUCCESS, cr.getStatus());
            numberOfInserts = cr.getResults()[0].asScalarLong();
        }
        assertEquals(ROWS.size(), numberOfInserts);
        waitForStreamedAllocatedMemoryZero(client);

        System.out.println("Again Seen Verifiers: " + m_verifier.m_seen_verifiers);

        assertEquals(ROWS.size(), m_verifier.getExportedDataCount());
        quiesceAndVerify(client,m_verifier);
    }

    static private enum AdHocKind {
        INFERRED_SP,
        INFERRED_MP
    }

    public void testReadFromStreamInsertIntoSelect() throws Exception {
        System.out.println("\n\n------------------------------------------");
        System.out.println("Testing current unsupported case reading from stream");
        Client client = getClient();
        verifyStmtFails(client,
                        "INSERT INTO " + EXPORT_TARGET_REPL + " "
                        + "SELECT * FROM " + SOURCE_PART + " ORDER BY PKEY",
                        "Illegal to read a stream.");
    }

    public TestExportInsertIntoSelectSuite(final String name) {
        super(name);
    }

    static public junit.framework.Test suite() throws Exception
    {
        m_isExportReplicated = true;
        System.setProperty(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.SocketExporter");
        String dexportClientClassName = System.getProperty("exportclass", "");
        System.out.println("Test System override export class is: " + dexportClientClassName);
        LocalCluster config;
        Map<String, String> additionalEnv = new HashMap<String, String>();
        additionalEnv.put(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.SocketExporter");

        final MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestExportInsertIntoSelectSuite.class);

        project = new VoltProjectBuilder();
        project.setSecurityEnabled(true, true);
        project.addRoles(GROUPS);
        project.addUsers(USERS);
        project.addSchema(TestSQLTypesSuite.class.getResource("sqltypessuite-export-ddl-with-target.sql"));
        project.addSchema(TestSQLTypesSuite.class.getResource("sqltypessuite-nonulls-export-ddl-with-target.sql"));

        // The partitioned export target
        wireupExportTableToSocketExport(EXPORT_TARGET_PART);

        // the replicated export target
        wireupExportTableToSocketExport(EXPORT_TARGET_REPL);

        // The partitioned source
        project.addPartitionInfo(SOURCE_PART, "PKEY");

        /*
         * compile the catalog all tests start with
         */
        config = new LocalCluster("export-ddl-cluster-rep.jar", 3, 1, k_factor,
                BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ALL_RUNNING, true, false, additionalEnv);
        config.setHasLocalServer(false);
        config.setMaxHeap(1024);
        boolean compile = config.compile(project);
        assertTrue(compile);
        builder.addServerConfig(config);

        return builder;
    }
}
