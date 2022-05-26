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

import com.google_voltpatches.common.base.Charsets;
import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;

/**
 * Tracker monitors and provides snapshots of a single ZK node's
 * children. The children data objects must be JSONObjects.
 */
public class LeaderCache implements LeaderCacheReader, LeaderCacheWriter {

    protected final ZooKeeper m_zk;
    private final AtomicBoolean m_shutdown = new AtomicBoolean(false);
    protected final Callback m_cb; // the callback when the cache changes
    private final String m_whoami; // identify the owner of the leader cache

    // the children of this node are observed.
    protected final String m_rootNode;

    // All watch processing is run serially in this thread.
    private final ListeningExecutorService m_es;

    // previous children snapshot for internal use.
    protected Set<String> m_lastChildren = new HashSet<String>();

    // the cache exposed to the public. Start empty. Love it.
    protected volatile ImmutableMap<Integer, LeaderCallBackInfo> m_publicCache = ImmutableMap.of();


    public static final String migrate_partition_leader_suffix = "_migrated";

    public static class LeaderCallBackInfo {
        Long m_lastHSId;
        Long m_HSId;
        boolean m_isMigratePartitionLeaderRequested;

        public LeaderCallBackInfo(Long lastHSId, Long HSId, boolean isRequested) {
            m_lastHSId = lastHSId;
            m_HSId = HSId;
            m_isMigratePartitionLeaderRequested = isRequested;
        }

        @Override
        public String toString() {
            return "leader hsid: " + CoreUtils.hsIdToString(m_HSId) +
                    ( m_lastHSId != Long.MAX_VALUE ? " (previously " + CoreUtils.hsIdToString(m_lastHSId) + ")" : "" ) +
                    ( m_isMigratePartitionLeaderRequested ? ", MigratePartitionLeader requested" : "");
        }
    }

    /**
     * Generate a HSID string with BALANCE_SPI_SUFFIX information.
     * When this string is updated, we can tell the reason why HSID is changed.
     */
    public static String suffixHSIdsWithMigratePartitionLeaderRequest(Long HSId) {
        return Long.toString(Long.MAX_VALUE) + "/" + Long.toString(HSId) + migrate_partition_leader_suffix;
    }

    /**
     * Is the data string hsid written because of MigratePartitionLeader request?
     */
    public static boolean isHSIdFromMigratePartitionLeaderRequest(String HSIdInfo) {
        return HSIdInfo.endsWith(migrate_partition_leader_suffix);
    }

    public static LeaderCallBackInfo buildLeaderCallbackFromString(String HSIdInfo) {
        int nextHSIdOffset = HSIdInfo.indexOf("/");
        assert(nextHSIdOffset >= 0);
        long lastHSId = Long.parseLong(HSIdInfo.substring(0, nextHSIdOffset));
        boolean migratePartitionLeader = isHSIdFromMigratePartitionLeaderRequest(HSIdInfo);
        long nextHSId;
        if (migratePartitionLeader) {
            nextHSId = Long.parseLong(HSIdInfo.substring(nextHSIdOffset+1, HSIdInfo.length() - migrate_partition_leader_suffix.length()));
        }
        else {
            nextHSId = Long.parseLong(HSIdInfo.substring(nextHSIdOffset+1));
        }
        return new LeaderCallBackInfo(lastHSId, nextHSId, migratePartitionLeader);
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
    public LeaderCache(ZooKeeper zk, String from, String rootNode)
    {
        this(zk, from, rootNode, null);
    }

    public LeaderCache(ZooKeeper zk, String from, String rootNode, Callback cb)
    {
        m_zk = zk;
        m_whoami = from;
        m_rootNode = rootNode;
        m_cb = cb;
        m_es = CoreUtils.getCachedSingleThreadExecutor("LeaderCache-" + m_whoami, 15000);
    }

    /** Initialize and start watching the cache. */
    @Override
    public void start(boolean block) throws InterruptedException, ExecutionException {
        Future<?> task = m_es.submit(new ParentEvent(null));
        if (block) {
            task.get();
        }
    }

    /**
     * Initialized and start watching partition level cache, this function is blocking.
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void startPartitionWatch() throws InterruptedException, ExecutionException {
        Future<?> task = m_es.submit(new PartitionWatchEvent());
        task.get();
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
            cacheCopy.put(e.getKey(), e.getValue().m_HSId);
        }
        return ImmutableMap.copyOf(cacheCopy);
    }

    /**
     * Read a single key from the cache. Matches the semantics of put()
     */
    @Override
    public Long get(int partitionId) {
        LeaderCallBackInfo info = m_publicCache.get(partitionId);
        if (info == null) {
            return null;
        }

        return info.m_HSId;
    }

    public boolean isMigratePartitionLeaderRequested(int partitionId) {
        LeaderCallBackInfo info = m_publicCache.get(partitionId);
        if (info != null) {
            return info.m_isMigratePartitionLeaderRequested;
        }
        return false;
    }

    /**
     * Create or update a new rootNode child
     */
    @Override
    public void put(int partitionId, long HSId) throws KeeperException, InterruptedException {
        put(partitionId, Long.toString(Long.MAX_VALUE) + "/" + Long.toString(HSId));
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

    // parent (root node) sees new or deleted child
    private class ParentEvent implements Runnable {
        public ParentEvent(WatchedEvent event) {
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

    // Boilerplate to forward zookeeper watches to the executor service
    protected final Watcher m_parentWatch = new Watcher() {
        @Override
        public void process(final WatchedEvent event) {
            try {
                if (!m_shutdown.get()) {
                    m_es.submit(new ParentEvent(event));
                }
            } catch (RejectedExecutionException e) {
                if (!m_es.isShutdown()) {
                    org.voltdb.VoltDB.crashLocalVoltDB("Unexpected rejected execution exception", false, e);
                }
            }
        }
    };

    /**
     * Rebuild the point-in-time snapshot of the children objects
     * and set watches on new children.
     */
    protected void processParentEvent() throws Exception {
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
                byte payload[] = callback.get();
                // During initialization children node may contain no data.
                if (payload == null) {
                    continue;
                }
                String data = new String(payload, "UTF-8");
                LeaderCallBackInfo info = LeaderCache.buildLeaderCallbackFromString(data);
                Integer partitionId = getPartitionIdFromZKPath(callback.getPath());
                cache.put(partitionId, info);
            } catch (KeeperException.NoNodeException e) {
                // child may have been deleted between the parent trigger and getData.
            }
        }

        m_publicCache = ImmutableMap.copyOf(cache);
        if (m_cb != null) {
            m_cb.run(m_publicCache);
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
    protected final Watcher m_childWatch = new Watcher() {
        @Override
        public void process(final WatchedEvent event) {
            try {
                if (!m_shutdown.get()) {
                    m_es.submit(new ChildEvent(event));
                }
            } catch (RejectedExecutionException e) {
                if (!m_es.isShutdown()) {
                    org.voltdb.VoltDB.crashLocalVoltDB("Unexpected rejected execution exception", false, e);
                }
            }
        }
    };

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
            byte payload[] = cb.get();
            String data = new String(payload, "UTF-8");
            LeaderCallBackInfo info = LeaderCache.buildLeaderCallbackFromString(data);
            Integer partitionId = getPartitionIdFromZKPath(cb.getPath());
            cacheCopy.put(partitionId, info);
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

    // parent (root node) sees new or deleted child
    private class PartitionWatchEvent implements Runnable {
        public PartitionWatchEvent() {
        }

        @Override
        public void run() {
            try {
                processPartitionWatchEvent();
            } catch (Exception e) {
                // ignore post-shutdown session termination exceptions.
                if (!m_shutdown.get()) {
                    org.voltdb.VoltDB.crashLocalVoltDB("Unexpected failure in LeaderCache.", true, e);
                }
            }
        }
    }

    // Race to create partition-specific zk node and put a watch on it.
    private void processPartitionWatchEvent() throws KeeperException, InterruptedException {
        try {
            m_zk.create(m_rootNode, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            m_zk.getData(m_rootNode, m_childWatch, null);
        } catch (KeeperException.NodeExistsException e) {
            m_zk.getData(m_rootNode, m_childWatch, null);
        }
    }

    // example zkPath string: /db/iv2masters/1
    protected static int getPartitionIdFromZKPath(String zkPath)
    {
        return Integer.parseInt(zkPath.substring(zkPath.lastIndexOf('/') + 1));
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
