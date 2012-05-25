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
package org.voltcore.zk;

import java.util.concurrent.ExecutionException;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.WatchedEvent;
import org.apache.zookeeper_voltpatches.Watcher;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.voltcore.utils.CoreUtils;
import org.voltdb.VoltDB;
import org.voltdb.VoltZK;

public class LeaderElector {
    private final ZooKeeper zk;
    private final String dir;
    private final String prefix;
    private final byte[] data;
    private final LeaderNoticeHandler cb;
    private String node = null;

    private volatile String leader = null;
    private volatile boolean isLeader = false;
    private final ExecutorService es;

    private final Runnable eventHandler = new Runnable() {
        @Override
        public void run() {
            try {
                leader = watchNextLowerNode();
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB("Failed to get leader", false, e);
            }

            if (node != null && node.equals(leader)) {
                // become the leader
                isLeader = true;
                if (cb != null) {
                    cb.becomeLeader();
                }
            }
        }
    };

    private final Watcher watcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            es.submit(eventHandler);
        }
    };

    public LeaderElector(ZooKeeper zk, String dir, String prefix, byte[] data,
                         LeaderNoticeHandler cb) {
        this.zk = zk;
        this.dir = dir;
        this.prefix = prefix;
        this.data = data;
        this.cb = cb;
        es = Executors.newSingleThreadExecutor(CoreUtils.getThreadFactory("Leader elector-" + dir));
    }

    /**
     * Start leader election.
     *
     * Creates an ephemeral sequential node under the given directory and check
     * if we are the first one who created it.
     *
     * For details about the leader election algorithm, @see <a href=
     * "http://zookeeper.apache.org/doc/trunk/recipes.html#sc_leaderElection"
     * >Zookeeper Leader Election</a>
     *
     * @param block true for blocking operation, false for nonblocking
     * @throws Exception
     */
    public void start(boolean block) throws KeeperException, InterruptedException, ExecutionException
    {
        // create the election root node if it doesn't exist.
        try {
            zk.create(dir, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException e) {
            // expected on all nodes that don't start() first.
        }

        node = zk.create(ZKUtil.joinZKPath(dir, prefix + "_"), data,
                         Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        Future<?> task = es.submit(eventHandler);
        if (block) {
            task.get();
        }
    }

    public boolean isLeader() {
        return isLeader;
    }

    /**
     * Get the current leader node
     * @return
     */
    public String leader() {
        return leader;
    }

    public String getNode() {
        return node;
    }

    /**
     * Deletes the ephemeral node. Make sure that no future watches will fire.
     *
     * @throws InterruptedException
     * @throws KeeperException
     */
    public void shutdown() throws InterruptedException, KeeperException {
        zk.delete(node, -1);
        es.shutdown();
        es.awaitTermination(356, TimeUnit.DAYS);
    }

    /**
     * Set a watch on the node that comes before the specified node in the
     * directory.

     * @return The lowest sequential node
     * @throws Exception
     */
    private String watchNextLowerNode() throws Exception {
        /*
         * Iterate through the sorted list of children and find the given node,
         * then setup a watcher on the previous node if it exists, otherwise the
         * previous of the previous...until we reach the beginning, then we are
         * the lowest node.
         */
        List<String> children = zk.getChildren(dir, false);
        ZKUtil.sortSequentialNodes(children);
        String lowest = null;
        String previous = null;
        ListIterator<String> iter = children.listIterator();
        while (iter.hasNext()) {
            String child = ZKUtil.joinZKPath(dir, iter.next());
            if (lowest == null) {
                lowest = child;
                previous = child;
                continue;
            }

            if (child.equals(node)) {
                while (zk.exists(previous, watcher) == null) {
                    if (previous.equals(lowest)) {
                        /*
                         * If the leader disappeared, and we follow the leader, we
                         * become the leader now
                         */
                        lowest = child;
                        break;
                    } else {
                        // reverse the direction of iteration
                        previous = iter.previous();
                    }
                }
                break;
            }
            previous = child;
        }

        return lowest;
    }

    public static String electionDirForPartition(int partition) {
        return ZKUtil.path(VoltZK.leaders_initiators, "partition_" + partition);
    }

    public static int getPartitionFromElectionDir(String partitionDir) {
        return Integer.parseInt(partitionDir.substring("partition_".length()));
    }

    public static String getPrefixFromChildName(String childName) {
        return childName.split("_")[0];
    }
}
