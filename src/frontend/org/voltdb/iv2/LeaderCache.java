/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

package org.voltdb.iv2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.WatchedEvent;
import org.apache.zookeeper_voltpatches.Watcher;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.voltcore.utils.CoreUtils;
import org.voltcore.zk.ZKUtil;
import org.voltcore.zk.ZKUtil.ByteArrayCallback;
import org.voltdb.VoltZK;

import com.google_voltpatches.common.base.Charsets;
import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;

/**
 * Tracker monitors and provides snapshots of a single ZK node's
 * children. The children data objects must be JSONObjects.
 */
public class LeaderCache implements LeaderCacheReader, LeaderCacheWriter {

    public static class LeaderCallBackInfo {
        Long m_HSID;
        boolean m_isMigratePartitionLeaderRequested;

        public LeaderCallBackInfo(Long hsid, boolean isRequested) {
            m_HSID = hsid;
            m_isMigratePartitionLeaderRequested = isRequested;
        }

        @Override
        public String toString() {
            return "leader hsid: " + CoreUtils.hsIdToString(m_HSID) +
                    ( m_isMigratePartitionLeaderRequested ? ", MigratePartitionLeader requested" : "");
        }
    }

    /**
     * Callback is passed an immutable cache when a child (dis)appears/changes.
     * Callback runs in the LeaderCache's ES (not the zk trigger thread).
     */
    public abstract static class Callback
    {
        abstract public void run(ImmutableMap<Integer, LeaderCallBackInfo> cache);
    }

    /** Instantiate a LeaderCache of parent rootNode. The rootNode must exist. */
    public LeaderCache(ZooKeeper zk, String rootNode)
    {
        this(zk, rootNode, null);
    }

    public LeaderCache(ZooKeeper zk, String rootNode, Callback cb)
    {
        m_zk = zk;
        m_rootNode = rootNode;
        m_cb = cb;
    }

    /** Initialize and start watching the cache. */
    @Override
    public void start(boolean block) throws InterruptedException, ExecutionException {
        Future<?> task = m_es.submit(new ParentEvent(null));
        if (block) {
            task.get();
        }
    }

    /** Stop caring */
    @Override
    public void shutdown() throws InterruptedException {
        m_shutdown.set(true);
        m_es.shutdown();
        m_es.awaitTermination(356, TimeUnit.DAYS);
    }

    /**
     * Get a current snapshot of the watched root node's children. This snapshot
     * promises no cross-children atomicity guarantees.
     */
    @Override
    public ImmutableMap<Integer, Long> pointInTimeCache() {
        if (m_shutdown.get()) {
            throw new RuntimeException("Requested cache from shutdown LeaderCache.");
        }
        HashMap<Integer, Long> cacheCopy = new HashMap<Integer, Long>();
        for (Entry<Integer, LeaderCallBackInfo> e : m_publicCache.entrySet()) {
            cacheCopy.put(e.getKey(), e.getValue().m_HSID);
        }
        return ImmutableMap.copyOf(cacheCopy);
    }

    /**
     * Read a single key from the cache. Matches the semantics of put()
     */
    @Override
    public Long get(int partitionId) {
        return m_publicCache.get(partitionId).m_HSID;
    }

    /**
     * Create or update a new rootNode child
     */
    @Override
    public void put(int partitionId, long HSId) throws KeeperException, InterruptedException {
        put(partitionId, Long.toString(HSId));
    }

    /**
     * Create or update a new rootNode child
     */
    @Override
    public void put(int partitionId, String HSIdStr) throws KeeperException, InterruptedException {
        try {
            m_zk.create(ZKUtil.joinZKPath(m_rootNode, Integer.toString(partitionId)),
                    HSIdStr.getBytes(Charsets.UTF_8),
                    Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        catch (KeeperException.NodeExistsException e) {
            m_zk.setData(ZKUtil.joinZKPath(m_rootNode, Integer.toString(partitionId)),
                    HSIdStr.getBytes(Charsets.UTF_8), -1);
        }
    }

    // check if the cache contains the partition
    public boolean contain(int partitionId) {
        return m_publicCache.containsKey(partitionId);
    }

    private final ZooKeeper m_zk;
    private final AtomicBoolean m_shutdown = new AtomicBoolean(false);
    private final Callback m_cb; // the callback when the cache changes

    // the children of this node are observed.
    private final String m_rootNode;

    // All watch processing is run serially in this thread.
    private final ListeningExecutorService m_es = CoreUtils.getCachedSingleThreadExecutor("LeaderCache", 15000);

    // previous children snapshot for internal use.
    private Set<String> m_lastChildren = new HashSet<String>();

    // the cache exposed to the public. Start empty. Love it.
    private volatile ImmutableMap<Integer, LeaderCallBackInfo> m_publicCache = ImmutableMap.of();

    // parent (root node) sees new or deleted child
    private class ParentEvent implements Runnable {
        private final WatchedEvent m_event;
        public ParentEvent(WatchedEvent event) {
            m_event = event;
        }

        @Override
        public void run() {
            try {
                processParentEvent();
            } catch (Exception e) {
                // ignore post-shutdown session termination exceptions.
                if (!m_shutdown.get()) {
                    org.voltdb.VoltDB.crashLocalVoltDB("Unexpected failure in LeaderCache.", true, e);
                }
            }
        }
    }

    // child node sees modification or deletion
    private class ChildEvent implements Runnable {
        private final WatchedEvent m_event;
        public ChildEvent(WatchedEvent event) {
            m_event = event;
        }

        @Override
        public void run() {
            try {
                processChildEvent(m_event);
            } catch (Exception e) {
                // ignore post-shutdown session termination exceptions.
                if (!m_shutdown.get()) {
                    org.voltdb.VoltDB.crashLocalVoltDB("Unexpected failure in LeaderCache.", true, e);
                }
            }
        }
    }

    // Boilerplate to forward zookeeper watches to the executor service
    private final Watcher m_parentWatch = new Watcher() {
        @Override
        public void process(final WatchedEvent event) {
            try {
                if (!m_shutdown.get()) {
                    m_es.submit(new ParentEvent(event));
                }
            } catch (RejectedExecutionException e) {
                if (m_es.isShutdown()) {
                    return;
                } else {
                    org.voltdb.VoltDB.crashLocalVoltDB("Unexpected rejected execution exception", false, e);
                }
            }
        }
    };

    // Boilerplate to forward zookeeper watches to the executor service
    private final Watcher m_childWatch = new Watcher() {
        @Override
        public void process(final WatchedEvent event) {
            try {
                if (!m_shutdown.get()) {
                    m_es.submit(new ChildEvent(event));
                }
            } catch (RejectedExecutionException e) {
                if (m_es.isShutdown()) {
                    return;
                } else {
                    org.voltdb.VoltDB.crashLocalVoltDB("Unexpected rejected execution exception", false, e);
                }
            }
        }
    };

    // example zkPath string: /db/iv2masters/1
    private static int getPartitionIdFromZKPath(String zkPath)
    {
        String array[] = zkPath.split("/");
        return Integer.valueOf(array[array.length - 1]);
    }

    /**
     * Rebuild the point-in-time snapshot of the children objects
     * and set watches on new children.
     */
    private void processParentEvent() throws Exception {
        // get current children snapshot and reset this watch.
        Set<String> children = new TreeSet<String>(m_zk.getChildren(m_rootNode, m_parentWatch));
        // intersect to get newChildren and update m_lastChildren to the current set.
        Set<String> newChildren = new HashSet<String>(children);
        newChildren.removeAll(m_lastChildren);
        m_lastChildren = children;

        List<ByteArrayCallback> callbacks = new ArrayList<ByteArrayCallback>();
        for (String child : children) {
            ByteArrayCallback cb = new ByteArrayCallback();
            // set watches on new children.
            if(newChildren.contains(child)) {
                m_zk.getData(ZKUtil.joinZKPath(m_rootNode, child), m_childWatch, cb, null);
            } else {
                m_zk.getData(ZKUtil.joinZKPath(m_rootNode, child), false, cb, null);
            }

            callbacks.add(cb);
        }

        HashMap<Integer, LeaderCallBackInfo> cache = new HashMap<Integer, LeaderCallBackInfo>();
        for (ByteArrayCallback callback : callbacks) {
            try {
                byte payload[] = callback.getData();
                String data = new String(payload, "UTF-8");
                long HSId = VoltZK.getHSId(data);
                boolean isMigratePartitionLeader = VoltZK.isHSIdFromMigratePartitionLeaderRequest(data);
                Integer partitionId = getPartitionIdFromZKPath(callback.getPath());
                cache.put(partitionId, new LeaderCallBackInfo(HSId, isMigratePartitionLeader));
            } catch (KeeperException.NoNodeException e) {
                // child may have been deleted between the parent trigger and getData.
            }
        }

        m_publicCache = ImmutableMap.copyOf(cache);
        if (m_cb != null) {
            m_cb.run(m_publicCache);
        }
    }

    /**
     * Update a modified child and republish a new snapshot. This may indicate
     * a deleted child or a child with modified data.
     */
    private void processChildEvent(WatchedEvent event) throws Exception {
        HashMap<Integer, LeaderCallBackInfo> cacheCopy = new HashMap<Integer, LeaderCallBackInfo>(m_publicCache);
        ByteArrayCallback cb = new ByteArrayCallback();
        m_zk.getData(event.getPath(), m_childWatch, cb, null);
        try {
            // cb.getData() and cb.getPath() throw KeeperException
            byte payload[] = cb.getData();
            String data = new String(payload, "UTF-8");
            long HSId = VoltZK.getHSId(data);
            boolean isMigratePartitionLeader = VoltZK.isHSIdFromMigratePartitionLeaderRequest(data);

            Integer partitionId = getPartitionIdFromZKPath(cb.getPath());
            cacheCopy.put(partitionId, new LeaderCallBackInfo(HSId, isMigratePartitionLeader));
        } catch (KeeperException.NoNodeException e) {
            // rtb: I think result's path is the same as cb.getPath()?
            Integer partitionId = getPartitionIdFromZKPath(event.getPath());
            cacheCopy.remove(partitionId);
        }
        m_publicCache = ImmutableMap.copyOf(cacheCopy);
        if (m_cb != null) {
            m_cb.run(m_publicCache);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("LeaderCache, root node:").append(m_rootNode).append("\n");
        sb.append("public cache: partition id -> HSId -> isMigratePartitionLeader\n");
        for (Entry<Integer, LeaderCallBackInfo> entry: m_publicCache.entrySet()) {
            sb.append("             ").append(entry.getKey()).append(" -> ").append(entry.getValue())
            .append("\n");
        }

        return sb.toString();
    }
}
