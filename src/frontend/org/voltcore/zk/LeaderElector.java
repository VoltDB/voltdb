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

import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.WatchedEvent;
import org.apache.zookeeper_voltpatches.Watcher;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.voltcore.utils.CoreUtils;
import org.voltdb.VoltDB;

/**
 * Uses an ephemeral sequential node under a given zookeeper node and check if this instance has the lowest sequence id
 *
 * For details about the leader election algorithm,
 * <a href="https://zookeeper.apache.org/doc/current/recipes.html#sc_leaderElection" >Zookeeper Leader Election</a>
 *
 */
public class LeaderElector {
    // The root is always created as INITIALIZING until the first participant is added,
    // then it's changed to INITIALIZED.
    public static final byte INITIALIZING = 0;
    public static final byte INITIALIZED = 1;

    private final ZooKeeper zk;
    private final String dir;
    private final String prefix;
    private final byte[] data;
    private final LeaderNoticeHandler cb;
    private String node = null;

    private volatile boolean isLeader = false;
    private final ExecutorService es;
    private volatile boolean m_shutdown = false;

    private final Runnable electionEventHandler = new Runnable() {
        @Override
        public void run() {
            try {
                isLeader = watchNextLowerNode();
            } catch (KeeperException.SessionExpiredException | KeeperException.ConnectionLossException e) {
                // lost the full connection. some test cases do this...
                // means zk shutdown without the elector being shutdown.
                // ignore.
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB("Unexepected failure in LeaderElector.", true, e);
            }

            if (isLeader && cb != null) {
                cb.becomeLeader();
            }
        }
    };

    private class ElectionWatcher implements Watcher {

        @Override
        public void process(final WatchedEvent event) {
            try {
                if (!m_shutdown) {
                    es.submit(electionEventHandler);
                }
            } catch (RejectedExecutionException e) {
            }
        }
    }
    private final ElectionWatcher electionWatcher = new ElectionWatcher();

    public LeaderElector(ZooKeeper zk, String dir, String prefix, byte[] data,
                         LeaderNoticeHandler cb) {
        this.zk = zk;
        this.dir = dir;
        this.prefix = prefix;
        this.data = data;
        this.cb = cb;
        es = CoreUtils.getCachedSingleThreadExecutor("Leader elector-" + dir, 15000);
    }

    /**
     * Provide a way for clients to create nodes which comply with the leader election
     * format without participating in a leader election
     * @throws InterruptedException
     * @throws KeeperException
     */
    public static String createParticipantNode(ZooKeeper zk, String dir, String prefix, byte[] data)
        throws KeeperException, InterruptedException
    {
        createRootIfNotExist(zk, dir);

        String node = zk.create(ZKUtil.joinZKPath(dir, prefix + "_"), data,
                                Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        // Unlock the dir as initialized
        zk.setData(dir, new byte[] {INITIALIZED}, -1);

        return node;
    }

    public static void createRootIfNotExist(ZooKeeper zk, String dir)
        throws KeeperException, InterruptedException
    {
        // create the election root node if it doesn't exist.
        try {
            zk.create(dir, new byte[] {INITIALIZING}, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException e) {
            // expected on all nodes that don't start() first.
        }
    }

    /**
     * Start participation in a leader election.
     *
     * @param block true for blocking operation, false for nonblocking
     * @throws Exception
     */
    public void start(boolean block) throws KeeperException, InterruptedException, ExecutionException
    {
        Future<?> task = start();
        if (block) {
            task.get();
        }
    }

    public int getLeaderId() {
        if (node == null) {
            throw new IllegalStateException("LeaderElector must be started");
        }
        return Integer.parseInt(node.substring(node.lastIndexOf('_') + 1));
    }

    /**
     * Start participation in a leader election.
     *
     * @return {@link Future} which is completed after the leader election has been performed
     * @throws KeeperException      If there is an error creating election nodes
     * @throws InterruptedException If this thread was interrupted
     */
    public Future<?> start() throws KeeperException, InterruptedException {
        node = createParticipantNode(zk, dir, prefix, data);
        return es.submit(electionEventHandler);
    }

    public boolean isLeader() {
        return isLeader;
    }

    /**
     * Deletes the ephemeral node. Make sure that no future watches will fire.
     *
     * @throws InterruptedException
     * @throws KeeperException
     */
    synchronized public void shutdown() throws InterruptedException, KeeperException {
        m_shutdown = true;
        es.shutdown();
        es.awaitTermination(365, TimeUnit.DAYS);
    }

    /**
     * Set a watch on the node that comes before the specified node in the directory.
     *
     * @return {@code true} if this node is the lowest node and therefore leader
     * @throws Exception
     */
    private boolean watchNextLowerNode() throws KeeperException, InterruptedException {
        /*
         * Iterate through the sorted list of children and find the given node,
         * then setup a electionWatcher on the previous node if it exists, otherwise the
         * previous of the previous...until we reach the beginning, then we are
         * the lowest node.
         */
        List<String> children = zk.getChildren(dir, false);
        Collections.sort(children);
        ListIterator<String> iter = children.listIterator();
        String me = null;
        //Go till I find myself.
        while (iter.hasNext()) {
            me = ZKUtil.joinZKPath(dir, iter.next());
            if (me.equals(node)) {
                break;
            }
        }
        assert (me != null);
        //Back on me
        iter.previous();
        //Until we have previous nodes and we set a watch on previous node.
        while (iter.hasPrevious()) {
            //Proess my lower nodes and put a watch on whats live
            String previous = ZKUtil.joinZKPath(dir, iter.previous());
            if (zk.exists(previous, electionWatcher) != null) {
                return false;
            }
        }
        return true;
    }

    public static String electionDirForPartition(String path, int partition) {
        return ZKUtil.path(path, "partition_" + partition);
    }

    public static int getPartitionFromElectionDir(String partitionDir) {
        return Integer.parseInt(partitionDir.substring("partition_".length()));
    }

    public static String getPrefixFromChildName(String childName) {
        return childName.split("_")[0];
    }
}
