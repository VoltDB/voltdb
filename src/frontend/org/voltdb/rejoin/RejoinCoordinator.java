/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

package org.voltdb.rejoin;

import org.apache.zookeeper_voltpatches.KeeperException;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONStringer;
import org.voltcore.messaging.HostMessenger;
import org.voltdb.ClientInterface;
import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Database;
import org.voltdb.client.ClientResponse;
import org.voltdb.iv2.Cartographer;
import org.voltdb.messaging.LocalMailbox;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;

import java.util.Collection;
import java.util.Map;

/**
 * Coordinates the sites to perform rejoin
 */
public abstract class RejoinCoordinator extends LocalMailbox {
    protected final HostMessenger m_messenger;

    /*
     * m_handler is called when a SnapshotUtil.requestSnapshot response occurs.
     * This callback runs on the snapshot daemon thread.
     */
    protected SnapshotUtil.SnapshotResponseHandler m_handler =
            new SnapshotUtil.SnapshotResponseHandler() {
        @Override
        public void handleResponse(ClientResponse resp)
        {
            if (resp == null) {
                VoltDB.crashLocalVoltDB("Failed to initiate rejoin snapshot",
                        false, null);
            } else if (resp.getStatus() != ClientResponseImpl.SUCCESS) {
                VoltDB.crashLocalVoltDB("Failed to initiate rejoin snapshot: "
                        + resp.getStatusString(), false, null);
            }

            VoltTable[] results = resp.getResults();
            if (SnapshotUtil.didSnapshotRequestSucceed(results)) {
                String appStatus = resp.getAppStatusString();
                if (appStatus == null) {
                    VoltDB.crashLocalVoltDB("Rejoin snapshot request failed: "
                            + resp.getStatusString(), false, null);
                }
                else {
                    // success is buried down here...
                    return;
                }
            } else {
                VoltDB.crashLocalVoltDB("Snapshot request for rejoin failed",
                        false, null);
            }
        }
    };

    public RejoinCoordinator(HostMessenger hostMessenger) {
        super(hostMessenger, hostMessenger.generateMailboxId(null));
        m_messenger = hostMessenger;
    }

    public void setClientInterface(ClientInterface ci) {}

    /**
     * Starts the rejoin process.
     */
    public abstract boolean startJoin(Database catalog, Cartographer cartographer)
            throws KeeperException, InterruptedException, JSONException;

    /**
     * Discard the mailbox.
     */
    public void close() {
        m_messenger.removeMailbox(getHSId());
    }

    protected String makeSnapshotNonce(String type, long HSId)
    {
        return type + "_" + HSId + "_" + System.currentTimeMillis();
    }

    protected String makeSnapshotRequest(Map<Long, Long> sourceToDests,
                                         Collection<Integer> tableIds)
    {
        try {
            JSONStringer jsStringer = new JSONStringer();
            jsStringer.object();

            jsStringer.key("streamPairs");
            jsStringer.object();
            for (Map.Entry<Long, Long> entry : sourceToDests.entrySet()) {
                jsStringer.key(Long.toString(entry.getKey())).value(Long.toString(entry.getValue()));
            }
            jsStringer.endObject();

            jsStringer.key("tableIds");
            jsStringer.array();
            if (tableIds != null) {
                for (int id : tableIds) {
                    jsStringer.value(id);
                }
            }
            jsStringer.endArray();

            jsStringer.endObject();
            return jsStringer.toString();
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Failed to serialize to JSON", true, e);
        }
        // unreachable;
        return null;
    }
}
