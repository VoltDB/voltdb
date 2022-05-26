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

package org.voltcore.zk;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.apache.zookeeper_voltpatches.data.Stat;
import org.voltdb.VoltZK;

import com.google_voltpatches.common.collect.Lists;

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
    public static final String readyjoininghosts = "/core/readyjoininghosts";

    // hosts since beginning of time (persistent)
    public static final String hostids = "/core/hostids";
    public static final String hostids_host = "/core/hostids/host";

    // root for rejoin nodes
    public static final String rejoin_node_blocker = "/core/rejoin_nodes_blocker";


    // Persistent nodes (mostly directories) to create on startup
    public static final String[] ZK_HIERARCHY = {
        root, hosts, readyhosts, readyjoininghosts, hostids
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
        return childName.substring(childName.lastIndexOf('_') + 1);
    }

    /**
     * Creates a rejoin blocker for the given rejoining host.
     * This prevents other hosts from rejoining at the same time.
     *
     * @param zk        ZooKeeper client
     * @param hostId    The rejoining host ID
     * @return -1 if the blocker is created successfully, or the host ID
     * if there is already another host rejoining.
     */
    public static int createRejoinNodeIndicator(ZooKeeper zk, int hostId)
    {
        try {
            zk.create(rejoin_node_blocker,
                      ByteBuffer.allocate(4).putInt(hostId).array(),
                      Ids.OPEN_ACL_UNSAFE,
                      CreateMode.PERSISTENT);
        } catch (KeeperException e) {
            if (e.code() == KeeperException.Code.NODEEXISTS) {
                try {
                    return ByteBuffer.wrap(zk.getData(rejoin_node_blocker, false, null)).getInt();
                } catch (KeeperException e1) {
                    if (e1.code() != KeeperException.Code.NONODE) {
                        org.voltdb.VoltDB.crashLocalVoltDB("Unable to get the current rejoining node indicator");
                    }
                } catch (InterruptedException e1) {}
            } else {
                org.voltdb.VoltDB.crashLocalVoltDB("Unable to create rejoin node Indicator", true, e);
            }
        } catch (InterruptedException e) {
            org.voltdb.VoltDB.crashLocalVoltDB("Unable to create rejoin node Indicator", true, e);
        }

        return -1;
    }

    /**
     * Removes the rejoin blocker if the current rejoin blocker contains the given host ID.
     * @return true if the blocker is removed successfully, false otherwise.
     */
    public static boolean removeRejoinNodeIndicatorForHost(ZooKeeper zk, int hostId)
    {
        try {
            Stat stat = new Stat();
            final int rejoiningHost = ByteBuffer.wrap(zk.getData(rejoin_node_blocker, false, stat)).getInt();
            if (hostId == rejoiningHost) {
                zk.delete(rejoin_node_blocker, stat.getVersion());
                return true;
            }
        } catch (KeeperException e) {
            if (e.code() == KeeperException.Code.NONODE ||
                e.code() == KeeperException.Code.BADVERSION) {
                // Okay if the rejoin blocker for the given hostId is already gone.
                return true;
            }
        } catch (InterruptedException e) {
            return false;
        }
        return false;
    }

    /**
     * Removes the join indicator for the given host ID.
     * @return true if the indicator is removed successfully, false otherwise.
     */
    public static boolean removeJoinNodeIndicatorForHost(ZooKeeper zk, int hostId)
    {
        try {
            Stat stat = new Stat();
            String path = ZKUtil.joinZKPath(readyjoininghosts, Integer.toString(hostId));
            zk.getData(path, false, stat);
            zk.delete(path, stat.getVersion());
            return true;
        } catch (KeeperException e) {
            if (e.code() == KeeperException.Code.NONODE ||
                    e.code() == KeeperException.Code.BADVERSION) {
                // Okay if the join indicator for the given hostId is already gone.
                return true;
            }
        } catch (InterruptedException e) {
            return false;
        }
        return false;
    }

    /**
     * Checks if the cluster suffered an aborted join or node shutdown and is still in the process of cleaning up.
     * @param zk    ZooKeeper client
     * @return true if the cluster is still cleaning up.
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static boolean isPartitionCleanupInProgress(ZooKeeper zk) throws KeeperException, InterruptedException
    {
        List<String> children = zk.getChildren(VoltZK.leaders_initiators, null);
        List<ZKUtil.ChildrenCallback> childrenCallbacks = Lists.newArrayList();
        for (String child : children) {
            ZKUtil.ChildrenCallback callback = new ZKUtil.ChildrenCallback();
            zk.getChildren(ZKUtil.joinZKPath(VoltZK.leaders_initiators, child), false, callback, null);
            childrenCallbacks.add(callback);
        }

        for (ZKUtil.ChildrenCallback callback : childrenCallbacks) {
            if (callback.get().isEmpty()) {
                return true;
            }
        }

        return false;
    }
}
