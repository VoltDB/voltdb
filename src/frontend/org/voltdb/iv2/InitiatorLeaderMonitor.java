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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.WatchedEvent;
import org.apache.zookeeper_voltpatches.Watcher;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltcore.zk.LeaderElector;
import org.voltcore.zk.ZKUtil;
import org.voltcore.zk.ZKUtil.ChildrenCallback;
import org.voltdb.VoltDB;
import org.voltdb.VoltZK;

/**
 * Monitors initiator leaders for each partition. When a new partition is
 * created, it will automatically start monitoring that partition.
 */
public class InitiatorLeaderMonitor {
    private static final VoltLogger hostLog = new VoltLogger("HOST");

    private final ZooKeeper zk;
    private final ExecutorService es =
            CoreUtils.getCachedSingleThreadExecutor("Client Interface", 15000);
    private final Map<Integer, Long> initiatorLeaders =
            Collections.synchronizedMap(new HashMap<Integer, Long>());
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    private final Watcher partitionWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            if (shutdown.get() == false) {
                es.submit(handlePartitionChange);
            }
        }
    };
    private final Runnable handlePartitionChange = new Runnable() {
        @Override
        public void run() {
            try {
                watchPartitions();
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB(e.getMessage(), false, e);
            }
        }
    };

    private class LeaderWatcher implements Watcher {
        private final int partition;
        private final String path;
        public LeaderWatcher(int partition, String path) {
            this.partition = partition;
            this.path = path;
        }

        @Override
        public void process(WatchedEvent event) {
            if (shutdown.get() == false) {
                es.submit(new LeaderChangeHandler(partition, path));
            }
        }
    };
    private class LeaderChangeHandler implements Runnable {
        private final int partition;
        private final String path;
        public LeaderChangeHandler(int partition, String path) {
            this.partition = partition;
            this.path = path;
        }

        @Override
        public void run() {
            try {
                watchInitiatorLeader(partition, path);
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB("Failed to get initiator leaders", false, e);
            }
        }
    };

    /**
     * Need to call {@link #start()} to start monitoring the leaders
     *
     * @param zk ZooKeeper instance
     */
    public InitiatorLeaderMonitor(ZooKeeper zk) {
        this.zk = zk;
    }

    /**
     * Start monitoring the leaders. This is a blocking operation.
     *
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void start() throws InterruptedException, ExecutionException {
        Future<?> task = es.submit(handlePartitionChange);
        task.get();
    }

    public void shutdown() {
        shutdown.set(true);
    }

    /**
     * Get the initiator leader HSId for the given partition.
     *
     * @param partition
     *            partition ID
     * @return The leader HSId if it's found, or null if the leader is unknown
     */
    public Long getLeader(int partition) {
        return initiatorLeaders.get(partition);
    }

    /**
     * @throws KeeperException
     * @throws InterruptedException
     */
    private void watchPartitions() throws KeeperException, InterruptedException {
        HashSet<Integer> existingPartitions = new HashSet<Integer>(initiatorLeaders.keySet());
        List<String> partitions = zk.getChildren(VoltZK.leaders_initiators, partitionWatcher);
        Map<Integer, ChildrenCallback> callbacks = new HashMap<Integer, ChildrenCallback>();
        for (String partitionString : partitions) {
            int partition = LeaderElector.getPartitionFromElectionDir(partitionString);
            ChildrenCallback cb = new ChildrenCallback();

            if (!existingPartitions.contains(partition)) {
                String path = ZKUtil.joinZKPath(VoltZK.leaders_initiators, partitionString);
                zk.getChildren(path, new LeaderWatcher(partition, path), cb, null);
                callbacks.put(partition, cb);
            } else {
                existingPartitions.remove(partition);
            }
        }

        // now existingPartitions only contains partitions that just disappeared
        initiatorLeaders.keySet().removeAll(existingPartitions);

        for (Entry<Integer, ChildrenCallback> e : callbacks.entrySet()) {
            processInitiatorLeader(e.getKey(), e.getValue());
        }
    }

    private void watchInitiatorLeader(int partition, String path)
    throws KeeperException, InterruptedException {
        ChildrenCallback cb = new ChildrenCallback();
        zk.getChildren(path, new LeaderWatcher(partition, path), cb, null);
        processInitiatorLeader(partition, cb);
    }

    private void processInitiatorLeader(int partition, ChildrenCallback cb)
    throws KeeperException, InterruptedException {
        Object[] result = cb.get();
        @SuppressWarnings("unchecked")
        List<String> children = (List<String>) result[3];
        if (!children.isEmpty()) {
            Collections.sort(children);
            String leader = children.get(0);
            // The leader HSId is the first half of the string
            try {
                long HSId = Long.parseLong(leader.split("_")[0]);
                initiatorLeaders.put(partition, HSId);
            } catch (NumberFormatException e) {
                hostLog.error("Unable to get initiator leader HSId from node " + leader);
            }
        } else {
            // No leader for this partition
            initiatorLeaders.remove(partition);
        }
    }
}
