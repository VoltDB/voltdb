/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
import org.voltdb.sysprocs.saverestore.SnapshotPathType;
import org.voltdb.utils.VoltSnapshotFile;

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
        if (params.size() < 2) {
            return "@SnapshotDelete expects 2 or 3 arguments, received " + params.size();
        }
        String[] paths = null;
        Object paramList[] = params.toArray();
        try {
            paths = (String[])(ParameterConverter.tryToMakeCompatible(
                        String[].class,
                        paramList[0]));
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
                        paramList[1]));
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
            if (paths.length > nonces.length) {
                return "A path must be provided for every nonce";
            } else {
                return "A nonce must be provided for every path";
            }
        }

        String stype = SnapshotPathType.SNAP_PATH.toString();
        if (params.size() > 2) {
            stype = (String)(ParameterConverter.tryToMakeCompatible(
                        String.class,
                        paramList[2]));
        }
        // Dupe SNAPSHOTSCAN as the subselector in case we consolidate later
        obj.put("subselector", "SNAPSHOTDELETE");
        obj.put("interval", false);
        obj.put("paths", paths);
        obj.put(SnapshotUtil.JSON_PATH_TYPE, stype);
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
        final SnapshotPathType stype = SnapshotPathType.valueOf(obj.getString(SnapshotUtil.JSON_PATH_TYPE));

        new Thread("Async snapshot deletion thread") {
            @Override
            public void run() {
                StringBuilder sb = new StringBuilder();
                sb.append("Deleting files: ");
                int delCount = 0;
                for (int ii = 0; ii < paths.length; ii++) {
                    //When user calls @SnapshotDelete, 'stype' will be set to SNAP_PATH so we will delete what user requested.
                    //If its SNAP_AUTO then its coming from periodic delete task so we use the configured snapshot path.
                    String path = SnapshotUtil.getRealPath(stype, paths[ii]);
                    List<File> relevantFiles = retrieveRelevantFiles(path, nonces[ii]);
                    if (relevantFiles != null) {
                        for (final File f : relevantFiles) {
                            sb.append(f.getPath()).append(' ');
                            f.delete();
                            delCount++;
                        }
                    }
                }
                if (delCount == 0) {
                    sb.append("none");
                }
                SNAP_LOG.info(sb.toString());
            }
        }.start();

        return new VoltTable[] {result};
    }

    // Foe auto snaps, the nonce has the prefix and a timestamp
    private final List<File> retrieveRelevantFiles(String filePath, String nonce) {
        final File path = new VoltSnapshotFile(filePath);

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

                String filename = pathname.getName();
                if (!filename.endsWith(".vpt") &&
                    !filename.endsWith(".digest") &&
                    !filename.endsWith(".jar") &&
                    !filename.endsWith(SnapshotUtil.HASH_EXTENSION) &&
                    !filename.endsWith(SnapshotUtil.COMPLETION_EXTENSION)) {
                    return false;
                }

                if (filename.startsWith(nonce)) {
                    return true;
                }
                return false;
            }
        }));
    }

    private VoltTable constructFragmentResultsTable() {
        return new VoltTable(new ColumnInfo(
                VoltSystemProcedure.CNAME_HOST_ID, VoltSystemProcedure.CTYPE_ID)
                , new ColumnInfo("HOSTNAME", VoltType.STRING)
                , new ColumnInfo("PATH", VoltType.STRING)
                , new ColumnInfo("PATHTYPE", VoltType.STRING)
                , new ColumnInfo("NONCE", VoltType.STRING)
                , new ColumnInfo("NAME", VoltType.STRING)
                , new ColumnInfo("SIZE", VoltType.BIGINT)
                , new ColumnInfo("DELETED", VoltType.STRING)
                , new ColumnInfo("RESULT", VoltType.STRING)
                , new ColumnInfo("ERR_MSG", VoltType.STRING));
    }
}
