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

package org.voltdb.dr2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.voltcore.utils.Pair;
import org.voltdb.StatsSource;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

import com.google_voltpatches.common.collect.ImmutableSet;

public class DRConsumerStatsBase {
    public static class DRConsumerClusterStatsBase extends StatsSource {

        public enum DRConsumerCluster {
            CLUSTER_ID                  (VoltType.INTEGER),
            REMOTE_CLUSTER_ID           (VoltType.INTEGER),
            STATE                       (VoltType.STRING),
            LAST_FAILURE                (VoltType.INTEGER);

            public final VoltType m_type;
            DRConsumerCluster(VoltType type) { m_type = type; }
        }

        private final static byte NO_FAILURE = 0;

        public DRConsumerClusterStatsBase() {
            super(false);
        }

        @Override
        protected void populateColumnSchema(ArrayList<VoltTable.ColumnInfo> columns) {
            for (DRConsumerCluster col : DRConsumerCluster.values()) {
                columns.add(new VoltTable.ColumnInfo(col.name(), col.m_type));
            }
        }

        @Override
        protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
            return ImmutableSet.of().iterator();
        }

        /**
         * Aggregates DRCONSUMERCLUSTER statistics reported by multiple nodes into
         * a one per-remote-cluster row. The last failure column should be the first
         * non-zero value across entire cluster.
         *
         * This method modifies the VoltTable in place.
         * @param stats Statistics from all cluster nodes. This will be modified in
         *              place. Cannot be null.
         * @return The same VoltTable as in the parameter.
         */
        public static VoltTable aggregateStats(VoltTable stats) {
            stats.resetRowPosition();
            if (stats.getRowCount() == 0) {
                return stats;
            }

            Map<String, Byte> failureMap = new TreeMap<>();
            Map<String, Pair<String, Byte>> rowMap = new TreeMap<>();
            while (stats.advanceRow()) {
                final byte clusterId = (byte) stats.getLong(DRConsumerCluster.CLUSTER_ID.name());
                final byte remoteClusterId = (byte) stats.getLong(DRConsumerCluster.REMOTE_CLUSTER_ID.name());
                String key = clusterId + ":" + remoteClusterId;

                // Remember the first non-zero failure per connection.
                final byte lastFailure = (byte) stats.getLong(DRConsumerCluster.LAST_FAILURE.name());
                Byte failure = failureMap.get(key);
                if (failure == null) {
                    failureMap.put(key, lastFailure);
                } else if (failure == NO_FAILURE && lastFailure != NO_FAILURE){
                    failureMap.put(key, lastFailure);
                }

                final String state = stats.getString(DRConsumerCluster.STATE.name());
                Pair<String, Byte> pair = rowMap.get(key);
                if (pair == null) {
                    rowMap.put(key, Pair.of(state, failureMap.get(key)));
                }
            }

            stats.clearRowData();
            for (Map.Entry<String, Pair<String, Byte>> e : rowMap.entrySet()) {
                String[] ids = e.getKey().split(":", 2);
                stats.addRow(Byte.parseByte(ids[0]), Byte.parseByte(ids[1]), e.getValue().getFirst(), e.getValue().getSecond());
            }
            return stats;
        }
    }

    public static class DRConsumerNodeStatsBase extends StatsSource {

        public enum DRConsumerNode {
            CLUSTER_ID                  (VoltType.INTEGER),
            REMOTE_CLUSTER_ID           (VoltType.INTEGER),
            STATE                       (VoltType.STRING),
            REPLICATION_RATE_1M         (VoltType.BIGINT),
            REPLICATION_RATE_5M         (VoltType.BIGINT),
            REMOTE_CREATION_TIMESTAMP   (VoltType.TIMESTAMP);

            public final VoltType m_type;
            DRConsumerNode(VoltType type) { m_type = type; }
        }

        public DRConsumerNodeStatsBase() {
            super(false);
        }

        @Override
        protected void populateColumnSchema(ArrayList<VoltTable.ColumnInfo> columns) {
            super.populateColumnSchema(columns, DRConsumerNode.class);
        }

        @Override
        protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
            return ImmutableSet.of().iterator();
        }
    }

    public static class DRConsumerPartitionStatsBase extends StatsSource {

        public enum DRConsumerPartition {
            CLUSTER_ID                  (VoltType.INTEGER),
            REMOTE_CLUSTER_ID           (VoltType.INTEGER),
            PARTITION_ID                (VoltType.INTEGER),
            IS_COVERED                  (VoltType.STRING),
            COVERING_HOST               (VoltType.STRING),
            LAST_RECEIVED_TIMESTAMP     (VoltType.TIMESTAMP),
            LAST_APPLIED_TIMESTAMP      (VoltType.TIMESTAMP),
            IS_PAUSED                   (VoltType.STRING),
            DUPLICATE_BUFFERS           (VoltType.BIGINT),
            IGNORED_BUFFERS             (VoltType.BIGINT),
            AVAILABLE_BYTES             (VoltType.INTEGER),
            AVAILABLE_BUFFERS           (VoltType.INTEGER),
            CONSUMER_LIMIT_TYPE         (VoltType.STRING);

            public final VoltType m_type;
            DRConsumerPartition(VoltType type) { m_type = type; }
        }

        public DRConsumerPartitionStatsBase() {
            super(false);
        }

        @Override
        protected void populateColumnSchema(ArrayList<VoltTable.ColumnInfo> columns) {
            super.populateColumnSchema(columns, DRConsumerPartition.class);
        }

        @Override
        protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
            return ImmutableSet.of().iterator();
        }
    }

}
