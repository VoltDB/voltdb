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

package org.voltdb.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Calendar;
import java.util.Random;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.JUnit4LocalClusterTest;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.sysprocs.saverestore.SystemTable;

import au.com.bytecode.opencsv_voltpatches.CSVReader;

public class TestSnapshotConverter extends JUnit4LocalClusterTest
{
    @Rule
    public final TemporaryFolder m_tempFolder = new TemporaryFolder();

    private LocalCluster m_cluster;

    @Before
    public void setup() throws IOException {
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addLiteralSchema(
                "CREATE TABLE T_SP(A2 VARCHAR(128), A1 INTEGER NOT NULL, A3 VARCHAR(64), A4 VARCHAR(64));"
                        + "CREATE TABLE T_MP MIGRATE TO TARGET T (A2 VARCHAR(128), A1 INTEGER NOT NULL, A3 VARCHAR(64), A4 VARCHAR(64));"
                        + "CREATE STREAM S(A2 VARCHAR(128), A1 INTEGER NOT NULL, A3 VARCHAR(64), A4 VARCHAR(64));"
                        + "CREATE VIEW V(A1, A2) AS SELECT A1, A2 FROM S GROUP BY A1, A2;");
        project.addPartitionInfo("T_SP", "A1");
        project.addPartitionInfo("S", "A1");
        if (MiscUtils.isPro()) {
            project.addDRTables(new String[] { "T_SP", "T_MP" });
            project.setXDCR();
        }

        m_cluster = new LocalCluster("testsnapshotstatus.jar", 8, 1, 0, BackendTarget.NATIVE_EE_JNI);
        assertTrue(m_cluster.compile(project));
        m_cluster.startCluster();
    }

    /*
     * Regression test for ENG-8609
     *
     * Test that hidden columns are filtered out when flag is passed to SnapshotConverter
     */
    @Test
    public void testSnapshotConverter() throws NoConnectionsException, IOException, ProcCallException {
        if (m_cluster.isValgrind()) {
            return;
        }

        Client client = m_cluster.createClient();
        int expectedLines = 10;
        Random r = new Random(Calendar.getInstance().getTimeInMillis());
        for (int i = 0; i < expectedLines; i++) {
            int id = r.nextInt();
            client.callProcedure("T_SP.insert", "Test String SP:" + i, id, "blab", "blab");
            client.callProcedure("T_MP.insert", "Test String MP:" + i, id, "blab", "blab");
            client.callProcedure("S.insert", "Test String S:" + i, id, "blab", "blab");
        }

        VoltTable[] results = null;
        File snapshotDir = m_tempFolder.newFolder("snapshot");
        String nonce = "NONCE";
        results = client.callProcedure("@SnapshotSave", snapshotDir.getPath(), nonce, 1).getResults();

        System.out.println(results[0]);
        results = client.callProcedure("@Statistics", "SnapshotStatus", 0).getResults();

        System.out.println(results[0]);
        // better be two rows
        assertEquals(3 + SystemTable.values().length, results[0].getRowCount());

        File unfiltered = m_tempFolder.newFolder("unfiltered");
        // start convert tables and views to csv
        String[] args = { "--table", "T_MP", "--table", "T_SP", "--table", "V", "--type", "CSV", "--dir",
                snapshotDir.getPath(), "--outdir", unfiltered.getPath(), nonce };
        SnapshotConverter.main(args);

        // this test will fail frequently with different lines before ENG-8609
        assertLineAndColumnCount(new File(unfiltered, "T_SP.csv"), expectedLines, MiscUtils.isPro() ? 5 : 4);
        assertLineAndColumnCount(new File(unfiltered, "T_MP.csv"), expectedLines, MiscUtils.isPro() ? 6 : 5);
        assertLineAndColumnCount(new File(unfiltered, "V.csv"), expectedLines, 3);

        // Now test with --filter-hidden and validate that hidden columns are removed from tables and views
        File filtered = m_tempFolder.newFolder("filtered");
        args = new String[] { "--table", "T_MP", "--table", "T_SP", "--table", "V", "--type", "CSV", "--dir",
                snapshotDir.getPath(), "--outdir", filtered.getPath(), "--filter-hidden", nonce };
        SnapshotConverter.main(args);

        assertLineAndColumnCount(new File(filtered, "T_SP.csv"), expectedLines, 4);
        assertLineAndColumnCount(new File(filtered, "T_MP.csv"), expectedLines, 4);
        assertLineAndColumnCount(new File(filtered, "V.csv"), expectedLines, 2);
    }

    public static void assertLineAndColumnCount(File csvFile, int expectedLines, int expectedColumns)
            throws IOException {
        int lines = 0, columns = -1;

        try (FileReader fileReader = new FileReader(csvFile); CSVReader csvReader = new CSVReader(fileReader)) {
            String[] line;
            while ((line = csvReader.readNext()) != null) {
                if (columns == -1) {
                    columns = line.length;
                } else {
                    assertEquals(columns, line.length);
                }
                ++lines;
            }
        }

        assertEquals(csvFile.getPath() + " lines", expectedLines, lines);
        assertEquals(csvFile.getPath() + " columns", expectedColumns, columns);
    }
}
