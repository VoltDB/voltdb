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

package org.voltdb;

import static com.google_voltpatches.common.base.Predicates.equalTo;
import static com.google_voltpatches.common.base.Predicates.not;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.voltcore.utils.Pair;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.dr2.DRConsumerDispatcher;
import org.voltdb.dr2.DRStateMachine;

import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.ImmutableSet;
import com.google_voltpatches.common.collect.Maps;

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

    public static class DRConsumerClusterStats extends DRConsumerClusterStatsBase {
        private final byte m_localClusterId;
        private Map<Byte, Pair<DRConsumerDispatcher, Byte>> m_lastFailures = ImmutableMap.of();
        private final static byte NO_FAILURE = 0;

        public DRConsumerClusterStats(byte localClusterId) {
            m_localClusterId = localClusterId;
        }

        @Override
        protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
            if (interval) {
                throw new UnsupportedOperationException("Haven't implemented interval stats for DR replication");
            }
            return buildIterator();
        }

        private Iterator<Object> buildIterator() {
            final Iterator<Byte> iter = m_lastFailures.keySet().iterator();
            return new Iterator<Object>() {
                @Override
                public boolean hasNext() {
                    return iter.hasNext();
                }

                @Override
                public Object next() {
                    return iter.next();
                }
            };
        }

        @Override
        protected void updateStatsRow(Object rowKey, Object[] rowValues) {
            rowValues[columnNameToIndex.get(Columns.CLUSTER_ID)] = m_localClusterId;
            for (Map.Entry<Byte, Pair<DRConsumerDispatcher, Byte>> e : m_lastFailures.entrySet()) {
                if (rowKey == e.getKey()) {
                    DRConsumerDispatcher dispatcher = e.getValue().getFirst();
                    rowValues[columnNameToIndex.get(Columns.STATE)] =
                            dispatcher == null ? DRStateMachine.State.INITIALIZE.name() : dispatcher.getState().name();
                    rowValues[columnNameToIndex.get(Columns.REMOTE_CLUSTER_ID)] = e.getKey();
                    rowValues[columnNameToIndex.get(Columns.LAST_FAILURE)] = e.getValue().getSecond();
                    break;
                }
            }
        }

        public void updateDispatcher(Byte producerClusterId, DRConsumerDispatcher dispatcher) {
            Pair<DRConsumerDispatcher, Byte> pair = m_lastFailures.get(producerClusterId);
            if (pair == null || pair.getFirst() != dispatcher) {
                ImmutableMap.Builder<Byte, Pair<DRConsumerDispatcher, Byte>> builder = ImmutableMap.builder();
                builder.putAll(Maps.filterKeys(m_lastFailures, not(equalTo(producerClusterId))));
                builder.put(producerClusterId, new Pair<>(dispatcher, pair == null ? NO_FAILURE : pair.getSecond()));
                m_lastFailures = builder.build();
            }
        }

        public void update(Byte producerClusterId, Byte errorCode) {
            // Ignore uninitialized producer cluster id
            if (producerClusterId == -1) {
                return;
            }
            Pair<DRConsumerDispatcher, Byte> pair = m_lastFailures.get(producerClusterId);
            if (pair == null || pair.getSecond() != errorCode) {
                ImmutableMap.Builder<Byte, Pair<DRConsumerDispatcher, Byte>> builder = ImmutableMap.builder();
                builder.putAll(Maps.filterKeys(m_lastFailures, not(equalTo(producerClusterId))));
                builder.put(producerClusterId, new Pair<>(pair == null ? null : pair.getFirst(), errorCode));
                m_lastFailures = builder.build();
            }
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
            Map<String, Pair<DRStateMachine.State, Byte>> rowMap = new TreeMap<>();
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

                final DRStateMachine.State state = DRStateMachine.State.valueOf(stats.getString(DRConsumerStatsBase.Columns.STATE));
                Pair<DRStateMachine.State, Byte> pair = rowMap.get(key);
                if (pair == null) {
                    rowMap.put(key, Pair.of(state, failureMap.get(key)));
                }
            }

            stats.clearRowData();
            for (Map.Entry<String, Pair<DRStateMachine.State, Byte>> e : rowMap.entrySet()) {
                String[] ids = e.getKey().split(":", 2);
                stats.addRow(Byte.parseByte(ids[0]), Byte.parseByte(ids[1]), e.getValue().getFirst().toString(), e.getValue().getSecond());
            }
            return stats;
        }
    }

    public static class DRConsumerClusterStatsBase extends StatsSource {
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
