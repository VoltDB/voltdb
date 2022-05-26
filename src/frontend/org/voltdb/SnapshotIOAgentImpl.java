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
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;
import org.voltdb.messaging.SnapshotCheckRequestMessage;
import org.voltdb.messaging.SnapshotCheckResponseMessage;
import org.voltdb.sysprocs.saverestore.SnapshotPathType;
import org.voltdb.sysprocs.saverestore.SnapshotRequestConfig;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;
import org.voltdb.utils.VoltSnapshotFile;

import com.google_voltpatches.common.util.concurrent.ListenableFuture;
import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;

/**
 * A agent that does non-core snapshot IO work on each node, e.g. pre-snapshot
 * scan, post-snapshot file creation, etc.
 */
public class SnapshotIOAgentImpl extends SnapshotIOAgent {

    private static final VoltLogger SNAP_LOG = new VoltLogger("SNAPSHOT");

    private final HostMessenger m_messenger;
    private final ListeningExecutorService m_es =
            CoreUtils.getCachedSingleThreadExecutor("SnapshotIOAgentImpl", 60 * 1000);

    public SnapshotIOAgentImpl(HostMessenger hostMessenger, long hsId)
    {
        super(hostMessenger, hsId);
        m_messenger = hostMessenger;
    }

    @Override
    public void deliver(VoltMessage message)
    {
        if (message instanceof SnapshotCheckRequestMessage) {
            handleFileScanRequest((SnapshotCheckRequestMessage) message);
        }
    }

    @Override
    public void shutdown() throws InterruptedException
    {
        m_es.shutdown();
        m_es.awaitTermination(365, TimeUnit.DAYS);
    }

    @Override
    public <T> ListenableFuture<T> submit(Callable<T> work)
    {
        return m_es.submit(work);
    }

    private void handleFileScanRequest(final SnapshotCheckRequestMessage message)
    {
        m_es.execute(new Runnable() {
            @Override
            public void run()
            {
                try {
                    JSONObject json = new JSONObject(message.getRequestJson());
                    String nonce = json.getString(SnapshotUtil.JSON_NONCE);
                    SnapshotPathType stype = SnapshotPathType.valueOf(json.getString(SnapshotUtil.JSON_PATH_TYPE));
                    String path = json.optString(SnapshotUtil.JSON_PATH, null);
                    if (stype == SnapshotPathType.SNAP_PATH && path == null) {
                        //Send error response scan of PATH requested but no PATH
                        VoltTable result = SnapshotUtil.constructNodeResultsTable();
                        result.addRow(m_messenger.getHostId(), m_messenger.getHostname(), "", "FAILURE", "SNAPSHOT SCAN REQUEST WITH NO PATH");
                        send(message.m_sourceHSId, new SnapshotCheckResponseMessage(path, stype, nonce, result));
                        return;
                    } else {
                        path = SnapshotUtil.getRealPath(stype, path);
                        json.put(SnapshotUtil.JSON_PATH, path);
                    }
                    JSONObject data = json.has(SnapshotUtil.JSON_DATA) ? new JSONObject(json.getString(SnapshotUtil.JSON_DATA)) : null;

                    VoltTable response = checkSnapshotFeasibility(path, nonce, data,
                            SnapshotFormat.getEnumIgnoreCase(json.optString(SnapshotUtil.JSON_FORMAT, SnapshotFormat.NATIVE.toString())));
                    send(message.m_sourceHSId, new SnapshotCheckResponseMessage(path, stype, nonce, response));
                } catch (JSONException e) {
                    SNAP_LOG.warn("Failed to parse snapshot request", e);
                }
            }
        });
    }

    /**
     * Check if a snapshot can be initiated. It does the following two things:
     * - Check if a snapshot is still in progress on the local node.
     * - Check if files can be created successfully locally.
     *
     * @return An empty table if both checks pass, a non-empty table contains error message if fail.
     */
    private VoltTable checkSnapshotFeasibility(String file_path, String file_nonce, JSONObject data, SnapshotFormat format)
    {
        VoltTable result = SnapshotUtil.constructNodeResultsTable();
        SNAP_LOG.trace("Checking feasibility of save with path and nonce: " + file_path + ", " + file_nonce);

        // Check if a snapshot is already in progress
        if (SnapshotSiteProcessor.isSnapshotInProgress()) {
            result.addRow(m_messenger.getHostId(), m_messenger.getHostname(), "", "FAILURE", "SNAPSHOT IN PROGRESS");
            return result;
        }

        SnapshotRequestConfig config = null;
        try {
            config = new SnapshotRequestConfig(data, VoltDB.instance().getCatalogContext().database);
        } catch (IllegalArgumentException e) {
            // if we get exception here, it means table specified in snapshot doesn't exist in database.
            result.addRow(m_messenger.getHostId(), m_messenger.getHostname(), "", "FAILURE", e.getMessage());
            return result;
        }

        // Check if the snapshot file can be created successfully.
        if (format.isFileBased()) {
            // Check snapshot directory no matter table exists or not. If not, try to create the directory.
            File parent = new VoltSnapshotFile(file_path);
            if (!parent.exists() && !parent.mkdirs()) {
                result.addRow(m_messenger.getHostId(), m_messenger.getHostname(), "", "FAILURE",
                        "FILE LOCATION UNWRITABLE: failed to create parent directory " + parent.getPath());
                return result;
            }
            if (!parent.isDirectory() || !parent.canRead() || !parent.canWrite() || !parent.canExecute()) {
                result.addRow(m_messenger.getHostId(), m_messenger.getHostname(), "", "FAILURE",
                        "FILE LOCATION UNWRITABLE: " + parent);
                return result;
            }

            boolean sameNonceSnapshotExist = false;
            Map<String, String> tableToErrorMsg = new HashMap<>();
            for (SnapshotTableInfo table : SnapshotUtil
                    .getTablesToSave(VoltDB.instance().getCatalogContext().database)) {
                String errMsg = null;
                File saveFilePath =
                        SnapshotUtil.constructFileForTable(table, file_path, file_nonce, format,
                                m_messenger.getHostId());
                SNAP_LOG.trace("Host ID " + m_messenger.getHostId() +
                        " table: " + table.getName() +
                               " to path: " + saveFilePath);
                if (saveFilePath.exists()) {
                    sameNonceSnapshotExist = true;
                    //snapshot with same nonce exists, there is no need to check for other tables.
                    break;
                }
                try {
                    /*
                     * Sanity check that the file can be created
                     * and then delete it so empty files aren't
                     * orphaned if another part of the snapshot
                     * test fails.
                     */
                    if (saveFilePath.createNewFile()) {
                        saveFilePath.delete();
                    }
                } catch (IOException ex) {
                    errMsg = "FILE CREATION OF " + saveFilePath +
                               " RESULTED IN IOException: " + CoreUtils.throwableToString(ex);
                }
                if (errMsg != null) {
                    tableToErrorMsg.put(table.getName(), errMsg);
                }
            }

            // Add error message only for tables included in current snapshot.
            // If there exist any previous snapshot with same nonce, add sameNonceErrorMsg for each table,
            // or add corresponding error message otherwise.
            final String sameNonceErrorMsg = "SNAPSHOT FILE WITH SAME NONCE ALREADY EXISTS";
            for (SnapshotTableInfo table : config.tables) {
                if (sameNonceSnapshotExist) {
                    result.addRow(m_messenger.getHostId(), m_messenger.getHostname(), table.getName(), "FAILURE",
                            sameNonceErrorMsg);
                } else if (tableToErrorMsg.containsKey(table.getName())) {
                    result.addRow(m_messenger.getHostId(), m_messenger.getHostname(), table.getName(), "FAILURE",
                            tableToErrorMsg.get(table.getName()));
                }
            }
        }
        return result;
    }
}
