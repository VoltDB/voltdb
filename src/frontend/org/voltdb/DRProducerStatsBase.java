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

import java.util.ArrayList;
import java.util.Iterator;

import com.google_voltpatches.common.collect.ImmutableSet;

public class DRProducerStatsBase {

    public static class DRProducerClusterStatsBase extends StatsSource {

        public enum DRProducerCluster {
            CLUSTER_ID                  (VoltType.SMALLINT),
            REMOTE_CLUSTER_ID           (VoltType.SMALLINT),
            STATE                       (VoltType.STRING),
            LASTFAILURE                 (VoltType.SMALLINT);

            public final VoltType m_type;
            DRProducerCluster(VoltType type) { m_type = type; }
        }

        public DRProducerClusterStatsBase() {
            super(false);
        }

        @Override
        protected void populateColumnSchema(ArrayList<VoltTable.ColumnInfo> columns) {
            for (DRProducerCluster col : DRProducerCluster.values()) {
                columns.add(new VoltTable.ColumnInfo(col.name(), col.m_type));
            };
        }

        @Override
        protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
            return ImmutableSet.of().iterator();
        }
    }

    public static class DRProducerNodeStatsBase extends StatsSource {

        public enum DRProducerNode {
            CLUSTER_ID                  (VoltType.SMALLINT),
            REMOTE_CLUSTER_ID           (VoltType.SMALLINT),
            STATE                       (VoltType.STRING),
            SYNCSNAPSHOTSTATE           (VoltType.STRING),
            ROWSINSYNCSNAPSHOT          (VoltType.BIGINT),
            ROWSACKEDFORSYNCSNAPSHOT    (VoltType.BIGINT),
            QUEUEDEPTH                  (VoltType.BIGINT),
            REMOTECREATIONTIMESTAMP     (VoltType.TIMESTAMP);

            public final VoltType m_type;
            DRProducerNode(VoltType type) { m_type = type; }
        }

        public DRProducerNodeStatsBase() {
            super(false);
        }

        @Override
        protected void populateColumnSchema(ArrayList<VoltTable.ColumnInfo> columns) {
            super.populateColumnSchema(columns, DRProducerNode.class);
        }

        @Override
        protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
            return ImmutableSet.of().iterator();
        }
    }

    public static class DRProducerPartitionStatsBase extends StatsSource {

        public enum DRProducerPartition {
            CLUSTER_ID                  (VoltType.SMALLINT),
            REMOTE_CLUSTER_ID           (VoltType.SMALLINT),
            PARTITION_ID                (VoltType.INTEGER),
            STREAMTYPE                  (VoltType.STRING),
            TOTALBYTES                  (VoltType.BIGINT),
            TOTALBYTESINMEMORY          (VoltType.BIGINT),
            TOTALBUFFERS                (VoltType.BIGINT),
            LASTQUEUEDDRID              (VoltType.BIGINT),
            LASTACKDRID                 (VoltType.BIGINT),
            LASTQUEUEDTIMESTAMP         (VoltType.TIMESTAMP),
            LASTACKTIMESTAMP            (VoltType.TIMESTAMP),
            ISSYNCED                    (VoltType.STRING),
            MODE                        (VoltType.STRING),
            QUEUE_GAP                   (VoltType.BIGINT),
            CONNECTION_STATUS           (VoltType.STRING),
            AVAILABLE_BYTES             (VoltType.INTEGER),
            AVAILABLE_BUFFERS           (VoltType.INTEGER),
            CONSUMER_LIMIT_TYPE         (VoltType.STRING);

            public final VoltType m_type;
            DRProducerPartition(VoltType type) { m_type = type; }
        }

        public DRProducerPartitionStatsBase() {
            super(false);
        }

        @Override
        protected void populateColumnSchema(ArrayList<VoltTable.ColumnInfo> columns) {
            super.populateColumnSchema(columns, DRProducerPartition.class);
        }

        @Override
        protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
            return ImmutableSet.of().iterator();
        }
    }

}
