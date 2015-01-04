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

package org.voltdb.rejoin;

import org.apache.zookeeper_voltpatches.KeeperException;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltcore.messaging.HostMessenger;
import org.voltdb.VoltDB;
import org.voltdb.catalog.Database;
import org.voltdb.messaging.LocalMailbox;
import org.voltdb.sysprocs.saverestore.SnapshotRequestConfig;
import org.voltdb.utils.VoltFile;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Coordinates the sites to perform rejoin
 */
public abstract class JoinCoordinator extends LocalMailbox {
    protected final HostMessenger m_messenger;

    public JoinCoordinator(HostMessenger hostMessenger) {
        super(hostMessenger, hostMessenger.generateMailboxId(null));
        m_messenger = hostMessenger;
    }

    public void initialize(int kfactor)
        throws JSONException, KeeperException, InterruptedException, ExecutionException {}
    public void setPartitionsToHSIds(Map<Integer, Long> partsToHSIds) {}
    public JSONObject getTopology() {
        throw new UnsupportedOperationException("getTopology is only supported for elastic join");
    }

    /**
     * Starts the rejoin process.
     */
    public abstract boolean startJoin(Database catalog);

    /**
     * Discard the mailbox.
     */
    public void close() {
        m_messenger.removeMailbox(getHSId());
    }

    protected static void clearOverflowDir(String voltroot)
    {
        // clear overflow dir in case there are files left from previous runs
        try {
            File overflowDir = new File(voltroot, "join_overflow");
            if (overflowDir.exists()) {
                VoltFile.recursivelyDelete(overflowDir);
            }
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Fail to clear join overflow directory", false, e);
        }
    }

    public static String makeSnapshotNonce(String type, long HSId)
    {
        return type + "_" + HSId + "_" + System.currentTimeMillis();
    }

    public static String makeSnapshotRequest(SnapshotRequestConfig config)
    {
        try {
            JSONStringer jsStringer = new JSONStringer();
            jsStringer.object();
            config.toJSONString(jsStringer);
            jsStringer.endObject();
            return jsStringer.toString();
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Failed to serialize to JSON", true, e);
        }
        // unreachable;
        return null;
    }
}
