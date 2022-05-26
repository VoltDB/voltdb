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
package org.voltdb;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collections;
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
import org.voltdb.DRConsumerDrIdTracker.DRSiteDrIdTracker;
import org.voltdb.SnapshotCompletionInterest.SnapshotCompletionEvent;
import org.voltdb.sysprocs.saverestore.SnapshotPathType;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;

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

    public static class ExportSnapshotTuple implements Serializable {
        private long m_ackOffset;
        private long m_sequenceNumber;
        private long m_generationId;

        public ExportSnapshotTuple(long uso, long seqNo, long timestamp) {
            m_ackOffset = uso;
            m_sequenceNumber = seqNo;
            m_generationId = timestamp;
        }

        public ExportSnapshotTuple() {
            this(0L, 0L, 0L);
        }

        public long getAckOffset() {
            return m_ackOffset;
        }

        public long getSequenceNumber() {
            return m_sequenceNumber;
        }

        public long getGenerationId() {
            return m_generationId;
        }

        /**
         * Serialize this {@code ExportSnapshotTuple} instance.
         *
         * @serialData The USO of export stream ({@code long}) (ack offset) and export sequence number ({@code long}) is emitted , followed by size of
         * generation id ({@code long}) (timestamp of most recent catalog update in snapshot).
         */
         private void writeObject(ObjectOutputStream out) throws IOException {
             out.writeLong(m_ackOffset);
             out.writeLong(m_sequenceNumber);
             out.writeLong(m_generationId);
         }

         private void readObject(ObjectInputStream in) throws IOException {
             m_ackOffset = in.readLong();
             m_sequenceNumber = in.readLong();
             m_generationId = in.readLong();
         }

         @Override
         public String toString() {
             return new StringBuilder(this.getClass().getSimpleName())
                     .append("[ack ")
                     .append(m_ackOffset)
                     .append(", seq ")
                     .append(m_sequenceNumber)
                     .append(", gen ")
                     .append(m_generationId)
                     .append("]")
                     .toString();
         }
    }

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

    private boolean isTerminus(long terminus) {
        return (terminus != 0L);
    }

    private void processSnapshotData(byte data[]) throws Exception {
        if (data == null) {
            return;
        }
        JSONObject jsonObj = new JSONObject(new String(data, "UTF-8"));
        long txnId = jsonObj.getLong("txnId");
        int hostCount = jsonObj.getInt("hostCount");
        String path = jsonObj.getString(SnapshotUtil.JSON_PATH);
        SnapshotPathType stype = SnapshotPathType.valueOf(jsonObj.getString(SnapshotUtil.JSON_PATH_TYPE));
        String nonce = jsonObj.getString(SnapshotUtil.JSON_NONCE);
        boolean truncation = jsonObj.getBoolean("isTruncation");
        long terminus = jsonObj.optLong(SnapshotUtil.JSON_TERMINUS, 0);
        boolean didSucceed = jsonObj.getBoolean("didSucceed");
        // A truncation request ID is not always provided. It's used for
        // snapshots triggered indirectly via ZooKeeper so that the
        // triggerer can recognize the snapshot when it finishes.
        String truncReqId = jsonObj.optString(SnapshotUtil.JSON_TRUNCATION_REQUEST_ID);

        if (hostCount == 0) {
            /*
             * Convert the JSON object containing the export sequence numbers for each
             * table and partition to a regular map
             */
            Map<String, Map<Integer, ExportSnapshotTuple>> exportSequenceNumbers = null;
            final JSONObject exportSequenceJSON = jsonObj.getJSONObject(ExtensibleSnapshotDigestData.EXPORT_SEQUENCE_NUMBER_ARR);
            final ImmutableMap.Builder<String, Map<Integer, ExportSnapshotTuple>> builder =
                    ImmutableMap.builder();
            final Iterator<String> tableKeys = exportSequenceJSON.keys();
            while (tableKeys.hasNext()) {
                final String tableName = tableKeys.next();
                final JSONObject tableSequenceNumbers = exportSequenceJSON.getJSONObject(tableName);
                ImmutableMap.Builder<Integer, ExportSnapshotTuple> tableBuilder = ImmutableMap.builder();
                final Iterator<String> partitionKeys = tableSequenceNumbers.keys();
                while (partitionKeys.hasNext()) {
                    final String partitionString = partitionKeys.next();
                    final Integer partitionId = Integer.valueOf(partitionString);
                    JSONObject sequenceNumbers = tableSequenceNumbers.getJSONObject(partitionString);
                    final Long ackOffset = sequenceNumbers.getLong(ExtensibleSnapshotDigestData.EXPORT_MERGED_USO);
                    final Long sequenceNumber = sequenceNumbers.getLong(ExtensibleSnapshotDigestData.EXPORT_MERGED_SEQNO);
                    final Long generationId = sequenceNumbers.getLong(ExtensibleSnapshotDigestData.EXPORT_MERGED_GENERATION_ID);
                    tableBuilder.put(partitionId, new ExportSnapshotTuple(ackOffset, sequenceNumber, generationId));
                }
                builder.put(tableName, tableBuilder.build());
            }
            exportSequenceNumbers = builder.build();

            long clusterCreateTime = jsonObj.optLong(ExtensibleSnapshotDigestData.DR_CLUSTER_CREATION_TIME, -1);
            Map<Integer, Long> drSequenceNumbers = new HashMap<>();
            JSONObject drTupleStreamJSON = jsonObj.getJSONObject(ExtensibleSnapshotDigestData.DR_TUPLE_STREAM_STATE_INFO);
            Iterator<String> partitionKeys = drTupleStreamJSON.keys();
            int drVersion = 0;
            while (partitionKeys.hasNext()) {
                String partitionIdString = partitionKeys.next();
                JSONObject stateInfo = drTupleStreamJSON.getJSONObject(partitionIdString);
                drVersion = (int)stateInfo.getLong(ExtensibleSnapshotDigestData.DR_VERSION);
                drSequenceNumbers.put(Integer.valueOf(partitionIdString), stateInfo.getLong(ExtensibleSnapshotDigestData.DR_ID));
            }

            Map<Integer, Long> partitionTxnIdsMap = ImmutableMap.of();
            synchronized (m_snapshotTxnIdsToPartitionTxnIds) {
                Map<Integer, Long> partitionTxnIdsList = m_snapshotTxnIdsToPartitionTxnIds.get(txnId);
                if (partitionTxnIdsList != null) {
                    partitionTxnIdsMap = ImmutableMap.copyOf(partitionTxnIdsList);
                }
            }

            /*
             * Collect all the last seen ids from the remote data centers so they can
             * be used by live rejoin to initialize a starting state for applying DR
             * data
             */
            Map<Integer, Map<Integer, Map<Integer, DRSiteDrIdTracker>>> drMixedClusterSizeConsumerState = new HashMap<>();
            JSONObject consumerPartitions = jsonObj.getJSONObject(ExtensibleSnapshotDigestData.DR_MIXED_CLUSTER_SIZE_CONSUMER_STATE);
            Iterator<String> cpKeys = consumerPartitions.keys();
            while (cpKeys.hasNext()) {
                final String consumerPartitionIdStr = cpKeys.next();
                final Integer consumerPartitionId = Integer.valueOf(consumerPartitionIdStr);
                JSONObject siteInfo = consumerPartitions.getJSONObject(consumerPartitionIdStr);
                drMixedClusterSizeConsumerState.put(consumerPartitionId, ExtensibleSnapshotDigestData.buildConsumerSiteDrIdTrackersFromJSON(siteInfo, false));
            }

            // Create a new DrProducerCatalogCommands because we do not want to modify the one being used
            JSONObject catalogCommands = jsonObj.getJSONObject(ExtensibleSnapshotDigestData.DR_CATALOG_COMMANDS);
            DrProducerCatalogCommands drCatalogCommands = new DrProducerCatalogCommands();
            drCatalogCommands.restore(catalogCommands);
            Map<Byte, String[]> replicableTables = drCatalogCommands
                    .calculateReplicableTables(VoltDB.instance().getCatalogContext().catalog);

            Iterator<SnapshotCompletionInterest> iter = m_interests.iterator();
            while (iter.hasNext()) {
                SnapshotCompletionInterest interest = iter.next();
                try {
                    interest.snapshotCompleted(
                            new SnapshotCompletionEvent(
                                path,
                                stype,
                                nonce,
                                txnId,
                                partitionTxnIdsMap,
                                truncation,
                                isTerminus(terminus),
                                didSucceed,
                                truncReqId,
                                exportSequenceNumbers,
                                Collections.unmodifiableMap(drSequenceNumbers),
                                Collections.unmodifiableMap(drMixedClusterSizeConsumerState),
                                drCatalogCommands.get(),
                                Collections.unmodifiableMap(replicableTables),
                                drVersion,
                                clusterCreateTime));
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
