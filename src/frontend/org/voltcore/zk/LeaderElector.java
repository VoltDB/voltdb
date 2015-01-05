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

import com.google_voltpatches.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google_voltpatches.common.collect.Sets;
import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.WatchedEvent;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.voltcore.utils.CoreUtils;
import java.util.concurrent.TimeUnit;
import org.apache.zookeeper_voltpatches.Watcher;
import org.voltdb.VoltZK;

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
    private Set<String> knownChildren = null;

    private volatile String leader = null;
    private volatile boolean isLeader = false;
    private final ExecutorService es;
    private final AtomicBoolean m_done = new AtomicBoolean(false);

    private final Runnable electionEventHandler = new Runnable() {
        @Override
        public void run() {
            try {
                leader = watchNextLowerNode();
            } catch (KeeperException.SessionExpiredException e) {
                // lost the full connection. some test cases do this...
                // means zk shutdown without the elector being shutdown.
                // ignore.
                e.printStackTrace();
            } catch (KeeperException.ConnectionLossException e) {
                // lost the full connection. some test cases do this...
                // means shutdoown without the elector being
                // shutdown; ignore.
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (Exception e) {
                org.voltdb.VoltDB.crashLocalVoltDB(
                        "Unexepected failure in LeaderElector.", true, e);
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

    private final Runnable childrenEventHandler = new Runnable() {
        @Override
        public void run() {
            try {
                checkForChildChanges();
            } catch (KeeperException.SessionExpiredException e) {
                // lost the full connection. some test cases do this...
                // means zk shutdown without the elector being shutdown.
                // ignore.
                e.printStackTrace();
            } catch (KeeperException.ConnectionLossException e) {
                // lost the full connection. some test cases do this...
                // means shutdoown without the elector being
                // shutdown; ignore.
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (Exception e) {
                org.voltdb.VoltDB.crashLocalVoltDB(
                        "Unexepected failure in LeaderElector.", true, e);
            }
        }
    };

    private class ChildrenWatcher implements Watcher {

        @Override
        public void process(WatchedEvent event) {
            try {
                if (!m_done.get()) {
                    es.submit(childrenEventHandler);
                }
            } catch (RejectedExecutionException e) {
            }
        }
    }
    private final ChildrenWatcher childWatcher = new ChildrenWatcher();

    private class ElectionWatcher implements Watcher {

        @Override
        public void process(final WatchedEvent event) {
            try {
                if (!m_done.get()) {
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
        node = createParticipantNode(zk, dir, prefix, data);
        Future<?> task = es.submit(electionEventHandler);
        if (block) {
            task.get();
        }
        //Only do the extra work for watching children if a callback is registered
        if (cb != null) {
            task = es.submit(childrenEventHandler);
            if (block) {
                task.get();
            }
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
    synchronized public void shutdown() throws InterruptedException, KeeperException {
        m_done.set(true);
        es.shutdown();
        es.awaitTermination(365, TimeUnit.DAYS);
        zk.delete(node, -1);
    }

    /**
     * Set a watch on the node that comes before the specified node in the
     * directory.

     * @return The lowest sequential node
     * @throws Exception
     */
    private String watchNextLowerNode() throws KeeperException, InterruptedException {
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
        String lowest = null;
        //Until we have previous nodes and we set a watch on previous node.
        while (iter.hasPrevious()) {
            //Proess my lower nodes and put a watch on whats live
            String previous = ZKUtil.joinZKPath(dir, iter.previous());
            if (zk.exists(previous, electionWatcher) != null) {
                lowest = previous;
                break;
            }
        }
        //If we could not watch any lower node we are lowest and must become leader.
        if (lowest == null) {
            return node;
        }
        return lowest;
    }


    /*
     * Check for a change in present nodes
     */
    private void checkForChildChanges() throws KeeperException, InterruptedException {
        /*
         * Iterate through the sorted list of children and find the given node,
         * then setup a electionWatcher on the previous node if it exists, otherwise the
         * previous of the previous...until we reach the beginning, then we are
         * the lowest node.
         */
        Set<String> children = ImmutableSet.copyOf(zk.getChildren(dir, childWatcher));

        boolean topologyChange = false;
        boolean removed = false;
        boolean added = false;
        if (knownChildren != null) {
            if (!knownChildren.equals(children)) {
                removed = !Sets.difference(knownChildren, children).isEmpty();
                added = !Sets.difference(children, knownChildren).isEmpty();
                topologyChange = true;
            }
        }
        knownChildren = children;

        if (topologyChange && cb != null) {
            cb.noticedTopologyChange(added, removed);
        }
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
