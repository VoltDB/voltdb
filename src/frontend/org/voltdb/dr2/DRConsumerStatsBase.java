/* This file is part of VoltDB.
 * Copyright (C) 2008-2020 VoltDB Inc.
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
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;

import com.google_voltpatches.common.collect.ImmutableSet;

public class DRConsumerStatsBase {

    public static interface Columns {
        // shared columns
        public static final String CLUSTER_ID = "CLUSTER_ID";
        public static final String REMOTE_CLUSTER_ID = "REMOTE_CLUSTER_ID";

        // column for both the cluster and node-level tables
        public static final String STATE = "STATE";

        // columns for the cluster-level table
        public static final String LAST_FAILURE = "LAST_FAILURE";

        // columns for the node-level table
        public static final String REPLICATION_RATE_1M = "REPLICATION_RATE_1M";
        public static final String REPLICATION_RATE_5M = "REPLICATION_RATE_5M";

        // columns for partition-level table
        public static final String IS_COVERED = "IS_COVERED";
        public static final String COVERING_HOST = "COVERING_HOST";
        public static final String LAST_RECEIVED_TIMESTAMP = "LAST_RECEIVED_TIMESTAMP";
        public static final String LAST_APPLIED_TIMESTAMP = "LAST_APPLIED_TIMESTAMP";
        public static final String IS_PAUSED = "IS_PAUSED";
        public static final String REMOTE_CREATION_TIMESTMAP = "REMOTE_CREATION_TIMESTMAP";
    }

    public static class DRConsumerClusterStatsBase extends StatsSource {
        private final static byte NO_FAILURE = 0;

        public DRConsumerClusterStatsBase() {
            super(false);
        }

        @Override
        protected void populateColumnSchema(ArrayList<VoltTable.ColumnInfo> columns) {
            columns.add(new ColumnInfo(Columns.CLUSTER_ID, VoltType.INTEGER));
            columns.add(new ColumnInfo(Columns.REMOTE_CLUSTER_ID, VoltType.INTEGER));
            columns.add(new ColumnInfo(Columns.STATE, VoltType.STRING));
            columns.add(new ColumnInfo(Columns.LAST_FAILURE, VoltType.INTEGER));
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
                final byte clusterId = (byte) stats.getLong(DRConsumerStatsBase.Columns.CLUSTER_ID);
                final byte remoteClusterId = (byte) stats.getLong(DRConsumerStatsBase.Columns.REMOTE_CLUSTER_ID);
                String key = clusterId + ":" + remoteClusterId;

                // Remember the first non-zero failure per connection.
                final byte lastFailure = (byte) stats.getLong(DRConsumerStatsBase.Columns.LAST_FAILURE);
                Byte failure = failureMap.get(key);
                if (failure == null) {
                    failureMap.put(key, lastFailure);
                } else if (failure == NO_FAILURE && lastFailure != NO_FAILURE){
                    failureMap.put(key, lastFailure);
                }

                final String state = stats.getString(DRConsumerStatsBase.Columns.STATE);
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
        public DRConsumerNodeStatsBase() {
            super(false);
        }

        @Override
        protected void populateColumnSchema(ArrayList<VoltTable.ColumnInfo> columns) {
            super.populateColumnSchema(columns);
            columns.add(new ColumnInfo(Columns.CLUSTER_ID, VoltType.INTEGER));
            columns.add(new ColumnInfo(Columns.REMOTE_CLUSTER_ID, VoltType.INTEGER));
            columns.add(new ColumnInfo(Columns.STATE, VoltType.STRING));
            columns.add(new ColumnInfo(Columns.REPLICATION_RATE_1M, VoltType.BIGINT));
            columns.add(new ColumnInfo(Columns.REPLICATION_RATE_5M, VoltType.BIGINT));
            columns.add(new ColumnInfo(Columns.REMOTE_CREATION_TIMESTMAP, VoltType.TIMESTAMP));
        }

        @Override
        protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
            return ImmutableSet.of().iterator();
        }
    }

    public static class DRConsumerPartitionStatsBase extends StatsSource {
        public DRConsumerPartitionStatsBase() {
            super(false);
        }

        @Override
        protected void populateColumnSchema(ArrayList<VoltTable.ColumnInfo> columns) {
            super.populateColumnSchema(columns);
            columns.add(new ColumnInfo(Columns.CLUSTER_ID, VoltType.INTEGER));
            columns.add(new ColumnInfo(Columns.REMOTE_CLUSTER_ID, VoltType.INTEGER));
            columns.add(new ColumnInfo(VoltSystemProcedure.CNAME_PARTITION_ID, VoltType.INTEGER));
            columns.add(new ColumnInfo(Columns.IS_COVERED, VoltType.STRING));
            columns.add(new ColumnInfo(Columns.COVERING_HOST, VoltType.STRING));
            columns.add(new ColumnInfo(Columns.LAST_RECEIVED_TIMESTAMP, VoltType.TIMESTAMP));
            columns.add(new ColumnInfo(Columns.LAST_APPLIED_TIMESTAMP, VoltType.TIMESTAMP));
            columns.add(new ColumnInfo(Columns.IS_PAUSED, VoltType.STRING));
        }

        @Override
        protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
            return ImmutableSet.of().iterator();
        }
    }

}
