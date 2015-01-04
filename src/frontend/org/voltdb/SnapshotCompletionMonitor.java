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
package org.voltdb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
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
import org.json_voltpatches.JSONObject;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;
import org.voltdb.SnapshotCompletionInterest.SnapshotCompletionEvent;

import com.google_voltpatches.common.collect.ImmutableMap;

public class SnapshotCompletionMonitor {
    private static final VoltLogger SNAP_LOG = new VoltLogger("SNAPSHOT");
    final CopyOnWriteArrayList<SnapshotCompletionInterest> m_interests =
            new CopyOnWriteArrayList<SnapshotCompletionInterest>();
    private ZooKeeper m_zk;
    private final ExecutorService m_es = new ThreadPoolExecutor(1, 1,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>(),
            CoreUtils.getThreadFactory(null, "SnapshotCompletionMonitor", CoreUtils.SMALL_STACK_SIZE, false, null),
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
    private final HashMap<Long, Map<Integer, Long>> m_snapshotTxnIdsToPartitionTxnIds =
            new HashMap<Long, Map<Integer, Long>>();

    public void registerPartitionTxnIdsForSnapshot(long snapshotTxnId, Map<Integer, Long> partitionTxnIds) {
        SNAP_LOG.debug("Registering per partition txnids " + partitionTxnIds);
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
        int hostCount = jsonObj.getInt("hostCount");
        String path = jsonObj.getString("path");
        String nonce = jsonObj.getString("nonce");
        boolean truncation = jsonObj.getBoolean("isTruncation");
        boolean didSucceed = jsonObj.getBoolean("didSucceed");
        // A truncation request ID is not always provided. It's used for
        // snapshots triggered indirectly via ZooKeeper so that the
        // triggerer can recognize the snapshot when it finishes.
        String truncReqId = jsonObj.optString("truncReqId");

        if (hostCount == 0) {
            /*
             * Convert the JSON object containing the export sequence numbers for each
             * table and partition to a regular map
             */
            Map<String, Map<Integer, Pair<Long, Long>>> exportSequenceNumbers = null;
            final JSONObject exportSequenceJSON = jsonObj.getJSONObject("exportSequenceNumbers");
            final ImmutableMap.Builder<String, Map<Integer, Pair<Long, Long>>> builder =
                    ImmutableMap.builder();
            @SuppressWarnings("unchecked")
            final Iterator<String> tableKeys = exportSequenceJSON.keys();
            while (tableKeys.hasNext()) {
                final String tableName = tableKeys.next();
                final JSONObject tableSequenceNumbers = exportSequenceJSON.getJSONObject(tableName);
                ImmutableMap.Builder<Integer, Pair<Long, Long>> tableBuilder = ImmutableMap.builder();
                @SuppressWarnings("unchecked")
                final Iterator<String> partitionKeys = tableSequenceNumbers.keys();
                while (partitionKeys.hasNext()) {
                    final String partitionString = partitionKeys.next();
                    final Integer partitionId = Integer.valueOf(partitionString);
                    JSONObject sequenceNumbers = tableSequenceNumbers.getJSONObject(partitionString);
                    final Long ackOffset = sequenceNumbers.getLong("ackOffset");
                    final Long sequenceNumber = sequenceNumbers.getLong("sequenceNumber");
                    tableBuilder.put(partitionId, Pair.of(ackOffset, sequenceNumber));
                }
                builder.put(tableName, tableBuilder.build());
            }
            exportSequenceNumbers = builder.build();

            Map<Integer, Long> partitionTxnIdsMap = ImmutableMap.of();
            synchronized (m_snapshotTxnIdsToPartitionTxnIds) {
                Map<Integer, Long> partitionTxnIdsList = m_snapshotTxnIdsToPartitionTxnIds.get(txnId);
                if (partitionTxnIdsList != null) {
                    partitionTxnIdsMap = ImmutableMap.copyOf(partitionTxnIdsList);
                }
            }

            Iterator<SnapshotCompletionInterest> iter = m_interests.iterator();
            while (iter.hasNext()) {
                SnapshotCompletionInterest interest = iter.next();
                try {
                    interest.snapshotCompleted(
                            new SnapshotCompletionEvent(
                                path,
                                nonce,
                                txnId,
                                partitionTxnIdsMap,
                                truncation,
                                didSucceed,
                                truncReqId,
                                exportSequenceNumbers));
                } catch (Exception e) {
                    SNAP_LOG.warn("Exception while executing snapshot completion interest", e);
                }
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
