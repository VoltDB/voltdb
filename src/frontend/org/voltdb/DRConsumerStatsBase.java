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

import java.util.ArrayList;
import java.util.Iterator;

import org.voltdb.VoltTable.ColumnInfo;

import com.google_voltpatches.common.collect.ImmutableSet;
import com.google_voltpatches.common.collect.Iterators;

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
        public DRConsumerClusterStats(byte localClusterId) {
            m_localClusterId = localClusterId;
        }

        @Override
        protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
            return Iterators.<Object>singletonIterator(1);
        }

        @Override
        protected void updateStatsRow(Object rowKey, Object[] rowValues) {
            rowValues[columnNameToIndex.get(Columns.CLUSTER_ID)] = m_localClusterId;
            rowValues[columnNameToIndex.get(Columns.REMOTE_CLUSTER_ID)] = -1;
//            assert m_dispatcher.getState() != null;
            rowValues[columnNameToIndex.get(Columns.STATE)] = "";//m_dispatcher.getState().toString();
//            for (Map.Entry<AbstractDRClient, ProducerReport> e : m_lastProducerReport.entrySet()) {
//                ClusterInfo producerInfo = e.getKey().getActualProducerClusterInfo();
//                if (producerInfo != null && producerInfo.getClusterId() == m_remoteClusterId) {
//                    m_lastFailureCode = e.getValue().m_lastFailureCode.getValue();
//                    break;
//                }
//            }
            rowValues[columnNameToIndex.get(Columns.LAST_FAILURE)] = 0;
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
