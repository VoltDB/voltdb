/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.voltdb;

import java.io.File;
import java.io.FileFilter;
import java.nio.ByteBuffer;
import java.util.List;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltcore.logging.VoltLogger;
import org.voltcore.network.Connection;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.client.ClientResponse;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;
import org.voltdb.utils.VoltFile;

/**
 * Agent responsible for collecting SnapshotDelete info on this host.
 *
 */
public class SnapshotDeleteAgent extends OpsAgent
{
    private static final VoltLogger SNAP_LOG = new VoltLogger("SNAPSHOT");

    public SnapshotDeleteAgent() {
        super("SnapshotDeleteAgent");
    }

    @Override
    protected void collectStatsImpl(Connection c, long clientHandle, OpsSelector selector,
            ParameterSet params) throws Exception
    {
        JSONObject obj = new JSONObject();
        obj.put("selector", "SNAPSHOTDELETE");
        String err = null;
        if (selector == OpsSelector.SNAPSHOTDELETE) {
            err = parseParams(params, obj);
        }
        else {
            err = "SnapshotDeleteAgent received non-SNAPSHOTDELETE selector: " + selector.name();
        }
        if (err != null) {
            // Maintain old @SnapshotDelete behavior.
            ColumnInfo[] result_columns = new ColumnInfo[1];
            result_columns[0] = new ColumnInfo("ERR_MSG", VoltType.STRING);
            VoltTable results[] = new VoltTable[] { new VoltTable(result_columns) };
            results[0].addRow(err);
            ClientResponseImpl errorResponse = new ClientResponseImpl(ClientResponse.SUCCESS,
                    ClientResponse.UNINITIALIZED_APP_STATUS_CODE, null, results, err);
            errorResponse.setClientHandle(clientHandle);
            ByteBuffer buf = ByteBuffer.allocate(errorResponse.getSerializedSize() + 4);
            buf.putInt(buf.capacity() - 4);
            errorResponse.flattenToBuffer(buf).flip();
            c.writeStream().enqueue(buf);
            return;
        }
        String subselector = obj.getString("subselector");

        PendingOpsRequest psr =
            new PendingOpsRequest(
                    selector,
                    subselector,
                    c,
                    clientHandle,
                    System.currentTimeMillis(),
                    obj);
        distributeOpsWork(psr, obj);
    }

    // Parse the provided parameter set object and fill in subselector and interval into
    // the provided JSONObject.  If there's an error, return that in the String, otherwise
    // return null.  Yes, ugly.  Bang it out, then refactor later.
    private String parseParams(ParameterSet params, JSONObject obj) throws Exception
    {
        if (params.size() != 2) {
            return "@SnapshotDelete expects 2 arguments, received " + params.size();
        }
        String[] paths = null;
        try {
            paths = (String[])(ParameterConverter.tryToMakeCompatible(
                        String[].class,
                        params.toArray()[0]));
        }
        catch (Exception e) {
            return e.getMessage();
        }

        if (paths == null || paths.length == 0) {
            return "No paths supplied";
        }

        for (String path : paths) {
            if (path == null || path.trim().isEmpty()) {
                return "A path was null or the empty string";
            }
        }

        String[] nonces = null;
        try {
            nonces = (String[])(ParameterConverter.tryToMakeCompatible(
                        String[].class,
                        params.toArray()[1]));
        }
        catch (Exception e) {
            return e.getMessage();
        }

        if (nonces == null || nonces.length == 0) {
            return "No nonces supplied";
        }

        for (String nonce : nonces) {
            if (nonce == null || nonce.trim().isEmpty()) {
                return "A nonce was null or the empty string";
            }
        }

        if (paths.length != nonces.length) {
            return "A path must be provided for every nonce";
        }

        // Dupe SNAPSHOTSCAN as the subselector in case we consolidate later
        obj.put("subselector", "SNAPSHOTDELETE");
        obj.put("interval", false);
        obj.put("paths", paths);
        obj.put("nonces", nonces);

        return null;
    }

    @Override
    protected void handleJSONMessage(JSONObject obj) throws Exception {
        VoltTable[] results = null;

        OpsSelector selector = OpsSelector.valueOf(obj.getString("selector").toUpperCase());
        if (selector == OpsSelector.SNAPSHOTDELETE) {
            results = dispatchSnapshotDelete(obj);
        }
        else {
            hostLog.warn("SnapshotDeleteAgent received a non-SNAPSHOTSCAN OPS selector: " + selector);
        }
        sendOpsResponse(results, obj);
    }

    private VoltTable[] dispatchSnapshotDelete(JSONObject obj) throws JSONException
    {
        VoltTable result = constructFragmentResultsTable();

        int length = obj.getJSONArray("paths").length();
        final String[] paths = new String[length];
        for (int i = 0; i < length; i++) {
            paths[i] = obj.getJSONArray("paths").getString(i);
        }
        length = obj.getJSONArray("nonces").length();
        final String[] nonces = new String[length];
        for (int i = 0; i < length; i++) {
            nonces[i] = obj.getJSONArray("nonces").getString(i);
        }

        new Thread("Async snapshot deletion thread") {
            @Override
            public void run() {
                StringBuilder sb = new StringBuilder();
                sb.append("Deleting files: ");
                for (int ii = 0; ii < paths.length; ii++) {
                    List<File> relevantFiles = retrieveRelevantFiles(paths[ii], nonces[ii]);
                    if (relevantFiles != null) {
                        for (final File f : relevantFiles) {
                            sb.append(f.getPath());
                            sb.append(',');
                            //long size = f.length();
                            f.delete();
                        }
                    }
                }
                SNAP_LOG.info(sb.toString());
            }
        }.start();

        return new VoltTable[] {result};
    }

    private final List<File> retrieveRelevantFiles(String filePath, String nonce) {
        final File path = new VoltFile(filePath);

        if (!path.exists()) {
            //m_errorString = "Provided search path does not exist: " + filePath;
            return null;
        }

        if (!path.isDirectory()) {
            //m_errorString = "Provided path exists but is not a directory: " + filePath;
            return null;
        }

        if (!path.canRead()) {
            if (!path.setReadable(true)) {
                //m_errorString = "Provided path exists but is not readable: " + filePath;
                return null;
            }
        }

        if (!path.canWrite()) {
            if (!path.setWritable(true)) {
                //m_errorString = "Provided path exists but is not writable: " + filePath;
                return null;
            }
        }

        return retrieveRelevantFiles(path, nonce);
    }

    private final List<File> retrieveRelevantFiles(File f, final String nonce) {
        assert(f.isDirectory());
        assert(f.canRead());
        assert(f.canWrite());
        return java.util.Arrays.asList(f.listFiles(new FileFilter() {

            @Override
            public boolean accept(File pathname) {
                if (pathname.isDirectory()) {
                    return false;
                }

                if (!pathname.getName().endsWith(".vpt") &&
                    !pathname.getName().endsWith(".digest") &&
                    !pathname.getName().endsWith(".jar") &&
                    !pathname.getName().endsWith(SnapshotUtil.HASH_EXTENSION) &&
                    !pathname.getName().endsWith(SnapshotUtil.COMPLETION_EXTENSION)) {
                    return false;
                }

                if (pathname.getName().startsWith(nonce)) {
                    return true;
                }
                return false;
            }
        }));
    }

    private VoltTable constructFragmentResultsTable() {
        ColumnInfo[] result_columns = new ColumnInfo[9];
        int ii = 0;
        result_columns[ii++] = new ColumnInfo(VoltSystemProcedure.CNAME_HOST_ID, VoltSystemProcedure.CTYPE_ID);
        result_columns[ii++] = new ColumnInfo("HOSTNAME", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("PATH", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("NONCE", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("NAME", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("SIZE", VoltType.BIGINT);
        result_columns[ii++] = new ColumnInfo("DELETED", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("RESULT", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("ERR_MSG", VoltType.STRING);
        return new VoltTable(result_columns);
    }
}
