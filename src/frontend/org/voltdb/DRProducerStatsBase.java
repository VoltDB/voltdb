/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

public class DRProducerStatsBase {

    //  DR_STATS_CONSUMER_CLUSTER_ID java property must equal "true"
    private static boolean m_addConsumerIdStat = Boolean.getBoolean("DR_STATS_CONSUMER_CLUSTER_ID");

    public static interface Columns {
        // column for both tables
        public static final String CONSUMER_CLUSTER_ID = "CONSUMERCLUSTERID";

        // columns for the node-level table
        public static final String STATE = "STATE";
        public static final String SYNC_SNAPSHOT_STATE = "SYNCSNAPSHOTSTATE";
        public static final String ROWS_IN_SYNC_SNAPSHOT = "ROWSINSYNCSNAPSHOT";
        public static final String ROWS_ACKED_FOR_SYNC_SNAPSHOT = "ROWSACKEDFORSYNCSNAPSHOT";
        public static final String QUEUE_DEPTH = "QUEUEDEPTH";

        // columns for partition-level table
        public static final String STREAM_TYPE = "STREAMTYPE";
        public static final String TOTAL_BYTES = "TOTALBYTES";
        public static final String TOTAL_BYTES_IN_MEMORY = "TOTALBYTESINMEMORY";
        public static final String TOTAL_BUFFERS = "TOTALBUFFERS";
        public static final String LAST_QUEUED_DRID = "LASTQUEUEDDRID";
        public static final String LAST_ACK_DRID = "LASTACKDRID";
        public static final String LAST_QUEUED_TIMESTAMP = "LASTQUEUEDTIMESTAMP";
        public static final String LAST_ACK_TIMESTAMP = "LASTACKTIMESTAMP";
        public static final String IS_SYNCED = "ISSYNCED";
        public static final String MODE = "MODE";
    }

    public static class DRProducerNodeStatsBase extends StatsSource {

        public DRProducerNodeStatsBase() {
            super(false);
        }

        @Override
        protected void populateColumnSchema(ArrayList<VoltTable.ColumnInfo> columns) {
            super.populateColumnSchema(columns);
            if (m_addConsumerIdStat) {
                columns.add(new ColumnInfo(Columns.CONSUMER_CLUSTER_ID, VoltType.SMALLINT));
            }
            columns.add(new ColumnInfo(Columns.STATE, VoltType.STRING));
            columns.add(new ColumnInfo(Columns.SYNC_SNAPSHOT_STATE, VoltType.STRING));
            columns.add(new ColumnInfo(Columns.ROWS_IN_SYNC_SNAPSHOT, VoltType.BIGINT));
            columns.add(new ColumnInfo(Columns.ROWS_ACKED_FOR_SYNC_SNAPSHOT, VoltType.BIGINT));
            columns.add(new ColumnInfo(Columns.QUEUE_DEPTH, VoltType.BIGINT));
        }

        @Override
        protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
            return ImmutableSet.of().iterator();
        }
    }

    public static class DRProducerPartitionStatsBase extends StatsSource {

        public DRProducerPartitionStatsBase() {
            super(false);
        }

        @Override
        protected void populateColumnSchema(ArrayList<VoltTable.ColumnInfo> columns) {
            super.populateColumnSchema(columns);
            if (m_addConsumerIdStat) {
                columns.add(new ColumnInfo(Columns.CONSUMER_CLUSTER_ID, VoltType.SMALLINT));
            }
            columns.add(new ColumnInfo(VoltSystemProcedure.CNAME_PARTITION_ID, VoltType.INTEGER));
            columns.add(new ColumnInfo(Columns.STREAM_TYPE, VoltType.STRING));
            columns.add(new ColumnInfo(Columns.TOTAL_BYTES, VoltType.BIGINT));
            columns.add(new ColumnInfo(Columns.TOTAL_BYTES_IN_MEMORY, VoltType.BIGINT));
            columns.add(new ColumnInfo(Columns.TOTAL_BUFFERS, VoltType.BIGINT));
            columns.add(new ColumnInfo(Columns.LAST_QUEUED_DRID, VoltType.BIGINT));
            columns.add(new ColumnInfo(Columns.LAST_ACK_DRID, VoltType.BIGINT));
            columns.add(new ColumnInfo(Columns.LAST_QUEUED_TIMESTAMP, VoltType.TIMESTAMP));
            columns.add(new ColumnInfo(Columns.LAST_ACK_TIMESTAMP, VoltType.TIMESTAMP));
            columns.add(new ColumnInfo(Columns.IS_SYNCED, VoltType.STRING));
            columns.add(new ColumnInfo(Columns.MODE, VoltType.STRING));
        }

        @Override
        protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
            return ImmutableSet.of().iterator();
        }
    }

}
