/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb;

import java.util.LinkedList;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.voltcore.agreement.ZKUtil;

/**
 * VoltZK provides constants for all voltdb-registered
 * ZooKeeper paths.
 */
public class VoltZK {
    private static final String root = "/db";

    public static final String catalogbytes = "/db/catalogbytes";
    public static final String topology = "/db/topology";
    public static final String unfaulted_hosts = "/db/unfaulted_hosts";

    // configuration (ports, interfaces, ...)
    public static final String cluster_metadata = "/db/cluster_metadata";

    // mailboxes
    public static final String mailboxes = "/db/mailboxes";
    public static final String mailboxes_asyncplanners = "/db/mailboxes/asyncplanners";
    public static final String mailboxes_clientinterfaces = "/db/mailboxes/clientinterfaces";
    public static final String mailboxes_clientinterfaces_ci = "/db/mailboxes/clientinterfaces/ci";
    public static final String mailboxes_executionsites = "/db/mailboxes/executionsites";
    public static final String mailboxes_executionsites_site = "/db/mailboxes/executionsites/site";
    public static final String mailboxes_initiators = "/db/mailboxes/initiators";
    public static final String mailboxes_initiators_initiator = "/db/mailboxes/initiators/initiator";

    // snapshot and command log
    public static final String completed_snapshots = "/db/completed_snapshots";
    public static final String nodes_currently_snapshotting = "/db/nodes_currently_snapshotting";
    public static final String restore = "/db/restore";
    public static final String restore_barrier = "/db/restore_barrier";
    public static final String restore_snapshot_id = "/db/restore/snapshot_id";
    public static final String request_truncation_snapshot = "/db/request_truncation_snapshot";
    public static final String snapshot_truncation_master = "/db/snapshot_truncation_master";
    public static final String test_scan_path = "/db/test_scan_path";   // (test only)
    public static final String truncation_snapshot_path = "/db/truncation_snapshot_path";
    public static final String user_snapshot_request = "/db/user_snapshot_request";
    public static final String user_snapshot_response = "/db/user_snapshot_response";

    // Persistent nodes (mostly directories) to create on startup
    public static final String[] ZK_HIERARCHY = {
            root,
            mailboxes,
            mailboxes_executionsites,
            mailboxes_initiators,
            mailboxes_asyncplanners,
            mailboxes_clientinterfaces,
    };

    /**
     * Race to create the persistent nodes.
     */
    public static void createPersistentZKNodes(ZooKeeper zk) {
        LinkedList<ZKUtil.StringCallback> callbacks = new LinkedList<ZKUtil.StringCallback>();
        for (int i=0; i < VoltZK.ZK_HIERARCHY.length; i++) {
                ZKUtil.StringCallback cb = new ZKUtil.StringCallback();
                callbacks.add(cb);
                zk.create(VoltZK.ZK_HIERARCHY[i], null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, cb, null);

        }
        try {
            for (ZKUtil.StringCallback cb : callbacks) {
                cb.get();
            }
        } catch (org.apache.zookeeper_voltpatches.KeeperException.NodeExistsException e) {
            // this is an expected race.
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
        }
    }

}
