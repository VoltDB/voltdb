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

package org.voltdb.regressionsuites;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import org.json_voltpatches.JSONObject;
import org.junit.Test;
import org.voltdb.LocalClustersTestBase;
import org.voltdb.VoltTable;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;
import org.voltdb.sysprocs.saverestore.SystemTable;

public class TestRestoreSchema extends LocalClustersTestBase {
    /*
     * Test that doing a schema restore from a path that doesn't have a catalog jar and then a restore from the correct
     * snapshot works correctly
     *
     * See ENG-18697
     */
    @Test
    public void restoreFromNoCatalogFirst() throws Exception {
        cleanupAfterTest();
        int tableCount = 2, sitesPerHost = 4;
        configureClustersAndClients(Collections.singletonList(new ClusterConfiguration(sitesPerHost)), tableCount,
                tableCount);
        for (int i = 0; i < tableCount; ++i) {
            insertRandomRows(getClient(0), i, TableType.PARTITIONED, 100);
            insertRandomRows(getClient(0), i, TableType.REPLICATED, 100);
        }

        String snapshotPath = m_temporaryFolder.newFolder().getPath();
        JSONObject snapshotConfig = new JSONObject()
                .put(SnapshotUtil.JSON_URIPATH, "file://" + snapshotPath)
                .put(SnapshotUtil.JSON_NONCE, getMethodName()).put(SnapshotUtil.JSON_BLOCK, true);
        ClientResponse response = getClient(0).callProcedure("@SnapshotSave", snapshotConfig.toString());
        System.out.println(response.getResults()[0]);
        shutdownCluster(0);

        // Clear the catalog jar so that we start with an empty catalog
        getCluster(0).getTemplateCommandLine().jarFileName(null);

        startCluster(0, true, false);

        // Try it with a bogus path first
        snapshotConfig.put(SnapshotUtil.JSON_PATH, snapshotPath + "_bogus");
        try {
            getClient(0).callProcedure("@SnapshotRestore", snapshotConfig.toString());
            fail("Should not have been able to recover from killed host");
        } catch (ProcCallException e) {
            assertTrue(
                    e.getClientResponse().getStatusString() + '\n'
                            + Arrays.toString(e.getClientResponse().getResults()),
                    e.getClientResponse().getStatusString().startsWith("Unable to access schema and procedure file"));
        }

        // Now do it with the correct path
        snapshotConfig.put(SnapshotUtil.JSON_PATH, snapshotPath);
        try {
            response = getClient(0).callProcedure("@SnapshotRestore", snapshotConfig.toString());
        } catch (ProcCallException e) {
            response = e.getClientResponse();
            fail(response.getStatusString() + '\n' + Arrays.toString(response.getResults()));
        }

        assertEquals(response.getResults()[0].toString(), 2 * tableCount * sitesPerHost + SystemTable.values().length*sitesPerHost,
                response.getResults()[0].getRowCount());

        for (int i = 0; i < tableCount; ++i) {
            assertEquals(100, getCount(getTableName(i, TableType.PARTITIONED)));
            assertEquals(100, getCount(getTableName(i, TableType.REPLICATED)));
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testRestoreCSVFormat() throws Exception {
        cleanupAfterTest();
        int tableCount = 2, sitesPerHost = 4;
        configureClustersAndClients(Collections.singletonList(new ClusterConfiguration(sitesPerHost)), tableCount,
                tableCount);
        for (int i = 0; i < tableCount; ++i) {
            insertRandomRows(getClient(0), i, TableType.PARTITIONED, 100);
            insertRandomRows(getClient(0), i, TableType.REPLICATED, 100);
        }

        String snapshotPath = m_temporaryFolder.newFolder().getPath();
        JSONObject snapshotConfig = new JSONObject()
                .put(SnapshotUtil.JSON_URIPATH, "file://" + snapshotPath)
                .put(SnapshotUtil.JSON_NONCE, getMethodName())
                .put(SnapshotUtil.JSON_BLOCK, true)
                .put(SnapshotUtil.JSON_FORMAT, "CSV");
        ClientResponse response = getClient(0).callProcedure("@SnapshotSave", snapshotConfig.toString());
        System.out.println(response.getResults()[0]);
        shutdownCluster(0);

        // Clear the catalog jar so that we start with an empty catalog
        getCluster(0).getTemplateCommandLine().jarFileName(null);

        startCluster(0, true, false);
        snapshotConfig.remove(SnapshotUtil.JSON_FORMAT);
        snapshotConfig.put(SnapshotUtil.JSON_PATH, snapshotPath);
        try {
            response = getClient(0).callProcedure("@SnapshotRestore", snapshotConfig.toString());
        } catch (ProcCallException e) {
            response = e.getClientResponse();
            fail(response.getStatusString() + '\n' + Arrays.toString(response.getResults()));
        }
        VoltTable result = response.getResults()[0];
        result.advanceRow();
        assertTrue("cannot recover/restore csv snapshots".equals(result.getString("ERR_MSG")));
    }
    private long getCount(String table) throws NoConnectionsException, IOException, ProcCallException {
        return getClient(0).callProcedure("@AdHoc", "SELECT count(*) FROM " + table).getResults()[0].asScalarLong();
    }
}
