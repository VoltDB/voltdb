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

package org.voltdb.iv2;

import java.io.UnsupportedEncodingException;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.Future;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.WatchedEvent;
import org.apache.zookeeper_voltpatches.Watcher;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.voltcore.utils.CoreUtils;
import org.voltcore.zk.ZKUtil;
import org.voltcore.zk.ZKUtil.ByteArrayCallback;

import org.voltdb.VoltDB;

import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;

/**
 * Tracker monitors and provides snapshots of a single ZK node's
 * children. The children data objects must be JSONObjects.
 */
public class LeaderCache implements LeaderCacheReader, LeaderCacheWriter {

    //
    // API
    //

    /**
     * Callback is passed an immutable cache when a child (dis)appears/changes.
     * Callback runs in the LeaderCache's ES (not the zk trigger thread).
     */
    public abstract static class Callback
    {
        abstract public void run(ImmutableMap<Integer, Long> cache);
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
        return m_publicCache;
    }

    /**
     * Read a single key from the cache. Matches the semantics of put()
     */
    @Override
    public Long get(int partitionId) {
        return m_publicCache.get(partitionId);
    }

    /**
     * Create or update a new rootNode child
     */
    @Override
    public void put(int partitionId, long HSId) throws KeeperException, InterruptedException {
        try {
            try {
                m_zk.create(ZKUtil.joinZKPath(m_rootNode, Integer.toString(partitionId)),
                        Long.toString(HSId).getBytes("UTF-8"),
                        Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException e) {
                m_zk.setData(ZKUtil.joinZKPath(m_rootNode, Integer.toString(partitionId)),
                        Long.toString(HSId).getBytes("UTF-8"), -1);
            }
        }
        catch (UnsupportedEncodingException utf8) {
            VoltDB.crashLocalVoltDB("Invalid string encoding: UTF-8", true, utf8);
        }
    }

    //
    // Implementation
    //

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
    private volatile ImmutableMap<Integer, Long> m_publicCache = ImmutableMap.of();

    // parent (root node) sees new or deleted child
    private class ParentEvent implements Runnable {
        private final WatchedEvent m_event;
        public ParentEvent(WatchedEvent event) {
            m_event = event;
        }

        @Override
        public void run() {
            try {
                processParentEvent(m_event);
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

    private static int getPartitionIdFromZKPath(String zkPath)
    {
        return Integer.valueOf(zkPath.split("/")[zkPath.split("/").length - 1]);
    }

    /**
     * Rebuild the point-in-time snapshot of the children objects
     * and set watches on new children.
     *
     * @Param event may be null on the first initialization.
     */
    private void processParentEvent(WatchedEvent event) throws Exception {
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

        HashMap<Integer, Long> cache = new HashMap<Integer, Long>();
        for (ByteArrayCallback callback : callbacks) {
            try {
                byte payload[] = callback.getData();
                long HSId = Long.valueOf(new String(payload, "UTF-8"));
                cache.put(getPartitionIdFromZKPath(callback.getPath()), HSId);
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
        HashMap<Integer, Long> cacheCopy = new HashMap<Integer, Long>(m_publicCache);
        ByteArrayCallback cb = new ByteArrayCallback();
        m_zk.getData(event.getPath(), m_childWatch, cb, null);
        try {
            // cb.getData() and cb.getPath() throw KeeperException
            byte payload[] = cb.getData();
            long HSId = Long.valueOf(new String(payload, "UTF-8"));
            cacheCopy.put(getPartitionIdFromZKPath(cb.getPath()), HSId);
        } catch (KeeperException.NoNodeException e) {
            // rtb: I think result's path is the same as cb.getPath()?
            cacheCopy.remove(getPartitionIdFromZKPath(event.getPath()));
        }
        m_publicCache = ImmutableMap.copyOf(cacheCopy);
        if (m_cb != null) {
            m_cb.run(m_publicCache);
        }
    }
}
