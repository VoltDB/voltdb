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

package org.voltcore.zk;

import java.util.LinkedList;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;

/**
 * CoreZK provides constants for all voltcore-registered
 * ZooKeeper paths.
 */
public class CoreZK {
    private static final String root = "/core";

    // unique instance ID for this cluster,
    // which is the JSON object:
    // -- coord: (int) original coordinator IP address as an int
    // -- timestamp: (long) the timestamp at which the first zookeeper node was created
    public static final String instance_id = "/core/instance_id";

    // hosts in the current system (ephemeral)
    public static final String hosts = "/core/hosts";
    public static final String hosts_host = "/core/hosts/host";
    public static final String readyhosts = "/core/readyhosts";
    public static final String readyhosts_host = "/core/readyhosts/host";

    // hosts since beginning of time (persistent)
    public static final String hostids = "/core/hostids";
    public static final String hostids_host = "/core/hostids/host";

    // Persistent nodes (mostly directories) to create on startup
    public static final String[] ZK_HIERARCHY = {
        root, hosts, readyhosts, hostids
    };

    /**
     * Creates the ZK directory nodes. Only the leader should do this.
     */
    public static void createHierarchy(ZooKeeper zk) {
        LinkedList<ZKUtil.StringCallback> callbacks = new LinkedList<ZKUtil.StringCallback>();
        for (String node : CoreZK.ZK_HIERARCHY) {
                ZKUtil.StringCallback cb = new ZKUtil.StringCallback();
                callbacks.add(cb);
                zk.create(node, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, cb, null);
        }
        try {
            for (ZKUtil.StringCallback cb : callbacks) {
                cb.get();
            }
        } catch (Exception e) {
            org.voltdb.VoltDB.crashLocalVoltDB(
                    e.getMessage(), false, e);
        }
    }

    /**
     * Given a ZK node name of the form PREFIX_SUFFIX (for example, the
     * format used by the LeaderElector which is HSID_SEQUENCENUM), return
     * the prefix. The prefix cannot have any underscores in it.
     */
    public static String getPrefixFromChildName(String childName) {
        return childName.split("_")[0];
    }

    /**
     * Given a ZK node name of the form PREFIX_SUFFIX (for example, the
     * format used by the LeaderElector which is HSID_SEQUENCENUM), return
     * the suffix. The suffix cannot have any underscores in it.
     */
    public static String getSuffixFromChildName(String childName) {
        final String[] parts = childName.split("_");
        return parts[parts.length - 1];
    }
}
