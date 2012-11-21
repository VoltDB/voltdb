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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException.NoNodeException;
import org.apache.zookeeper_voltpatches.WatchedEvent;
import org.apache.zookeeper_voltpatches.Watcher;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONObject;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;

import com.google.common.primitives.Longs;

public class SnapshotCompletionMonitor {
    @SuppressWarnings("unused")
    private static final VoltLogger LOG = new VoltLogger("LOGGING");
    final CopyOnWriteArrayList<SnapshotCompletionInterest> m_interests =
            new CopyOnWriteArrayList<SnapshotCompletionInterest>();
    private ZooKeeper m_zk;
    private final ExecutorService m_es = new ThreadPoolExecutor(1, 1,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>(),
            CoreUtils.getThreadFactory("SnapshotCompletionMonitor"),
            new java.util.concurrent.ThreadPoolExecutor.DiscardPolicy());

    private final Watcher m_newSnapshotWatcher = new Watcher() {
        @Override
        public void process(final WatchedEvent event) {
            switch (event.getType()) {
            case NodeChildrenChanged:
                m_es.execute(new Runnable() {
                    @Override
                    public void run() {
                        processSnapshotChildrenChanged(event);
                    }
                });
            default:
                break;
            }
        }
    };

    /*
     * For every snapshot, the local sites will log their partition specific txnids here
     * and when the snapshot completes the completion monitor will grab the list of partition specific
     * txnids to pass to those who are interestewd
     */
    private final HashMap<Long, List<Long>> m_snapshotTxnIdsToPartitionTxnIds = new HashMap<Long, List<Long>>();

    public void registerPartitionTxnIdsForSnapshot(long snapshotTxnId, List<Long> partitionTxnIds) {
        synchronized (m_snapshotTxnIdsToPartitionTxnIds) {
            assert(!m_snapshotTxnIdsToPartitionTxnIds.containsKey(snapshotTxnId));
            m_snapshotTxnIdsToPartitionTxnIds.put(snapshotTxnId, partitionTxnIds);
        }
    }

    private TreeSet<String> m_lastKnownSnapshots = new TreeSet<String>();

    private void processSnapshotChildrenChanged(final WatchedEvent event) {
        try {
            TreeSet<String> children = new TreeSet<String>(m_zk.getChildren(
                    VoltZK.completed_snapshots, m_newSnapshotWatcher));
            TreeSet<String> newChildren = new TreeSet<String>(children);
            newChildren.removeAll(m_lastKnownSnapshots);
            m_lastKnownSnapshots = children;
            for (String newSnapshot : newChildren) {
                String path = VoltZK.completed_snapshots + "/" + newSnapshot;
                try {
                    byte data[] = m_zk.getData(path, new Watcher() {
                        @Override
                        public void process(final WatchedEvent event) {
                            switch (event.getType()) {
                            case NodeDataChanged:
                                m_es.execute(new Runnable() {
                                    @Override
                                    public void run() {
                                        processSnapshotDataChangedEvent(event);
                                    }
                                });
                                break;
                            default:
                                break;
                            }
                        }

                    }, null);
                    processSnapshotData(data);
                } catch (NoNodeException e) {
                }
            }
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Exception in snapshot completion monitor", true, e);
        }
    }

    private void processSnapshotDataChangedEvent(final WatchedEvent event) {
        try {
            byte data[] = m_zk.getData(event.getPath(), new Watcher() {
                @Override
                public void process(final WatchedEvent event) {
                    switch (event.getType()) {
                    case NodeDataChanged:
                        m_es.execute(new Runnable() {
                            @Override
                            public void run() {
                                processSnapshotDataChangedEvent(event);
                            }
                        });
                        break;
                    default:
                        break;
                    }
                }

            }, null);
            processSnapshotData(data);
        } catch (NoNodeException e) {
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Exception in snapshot completion monitor", true, e);
        }
    }

    private void processSnapshotData(byte data[]) throws Exception {
        if (data == null) {
            return;
        }
        JSONObject jsonObj = new JSONObject(new String(data, "UTF-8"));
        long txnId = jsonObj.getLong("txnId");
        JSONArray hosts = jsonObj.getJSONArray("hosts");
        int totalNodesFinished = jsonObj.getInt("finishedHosts");
        String nonce = jsonObj.getString("nonce");
        boolean truncation = jsonObj.getBoolean("isTruncation");
        // A truncation request ID is not always provided. It's used for
        // snapshots triggered indirectly via ZooKeeper so that the
        // triggerer can recognize the snapshot when it finishes.
        String truncReqId = jsonObj.optString("truncReqId");

        if (hosts.length() == totalNodesFinished) {
            long partitionTxnIds[] = null;
            synchronized (m_snapshotTxnIdsToPartitionTxnIds) {
                List<Long> partitionTxnIdsList = m_snapshotTxnIdsToPartitionTxnIds.get(txnId);
                if (partitionTxnIdsList != null) {
                    partitionTxnIds = Longs.toArray(partitionTxnIdsList);
                } else {
                    partitionTxnIds = new long[0];
                }
            }
            Iterator<SnapshotCompletionInterest> iter = m_interests.iterator();
            while (iter.hasNext()) {
                SnapshotCompletionInterest interest = iter.next();
                interest.snapshotCompleted(nonce, txnId, Arrays.copyOf(partitionTxnIds,
                                           partitionTxnIds.length), truncation, truncReqId);
            }
        }
    }

    public void addInterest(final SnapshotCompletionInterest interest) {
        m_interests.add(interest);
    }

    public void removeInterest(final SnapshotCompletionInterest interest) {
        m_interests.remove(interest);
    }

    public void shutdown() throws InterruptedException {
        m_es.shutdown();
        m_es.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    }

    public void init(final ZooKeeper zk) {
        m_es.execute(new Runnable() {
            @Override
            public void run() {
                m_zk = zk;
                try {
                    m_zk.create(VoltZK.completed_snapshots, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } catch (Exception e){}
                try {
                    m_lastKnownSnapshots =
                        new TreeSet<String>(m_zk.getChildren(VoltZK.completed_snapshots, m_newSnapshotWatcher));
                } catch (Exception e) {
                    VoltDB.crashLocalVoltDB("Error initializing snapshot completion monitor", true, e);
                }
            }
        });
    }
}
