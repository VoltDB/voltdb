/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

public class ExportStatsBase extends StatsSource {

    public static class ExportStatsRow {
        public final int m_partitionId;
        public final int m_siteId;
        public final String m_sourceName;
        public final String m_exportTarget;
        public final String m_exportingRole;
        public final long m_tupleCount;
        public final long m_tuplesPending;
        public final long m_lastQueuedTimestamp;
        public final long m_lastAckedTimestamp;
        public final long m_averageLatency;
        public final long m_maxLatency;
        public final long m_queueGap;
        public final String m_status;

        public ExportStatsRow(int partitionId, int siteId, String sourceName, String exportTarget, String exportingRole,
                long tupleCount, long tuplesPending, long lastQueuedTimestamp, long lastAckedTimestamp,
                long averageLatency, long maxLatency, long queueGap, String status) {
            m_partitionId = partitionId;
            m_siteId = siteId;
            m_sourceName = sourceName;
            m_exportTarget = exportTarget;
            m_exportingRole = exportingRole;
            m_tupleCount = tupleCount;
            m_tuplesPending = tuplesPending > 0 ? tuplesPending : 0;
            m_lastQueuedTimestamp = lastQueuedTimestamp;
            m_lastAckedTimestamp = lastAckedTimestamp;
            m_averageLatency = averageLatency;
            m_maxLatency = maxLatency;
            m_queueGap = queueGap;
            m_status = status;
        }

        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(Columns.PARTITION_ID).append(":").append(m_partitionId).append(" ")
              .append(Columns.SITE_ID).append(":").append(m_siteId).append(" ")
              .append(Columns.SOURCE_NAME).append(":").append(m_sourceName).append(" ")
              .append(Columns.EXPORT_TARGET).append(":").append(m_exportTarget).append(" ")
              .append(Columns.ACTIVE).append(":").append(m_exportingRole).append(" ")
              .append(Columns.TUPLE_COUNT).append(":").append(m_tupleCount).append(" ")
              .append(Columns.TUPLE_PENDING).append(":").append(m_tuplesPending).append(" ")
              .append(Columns.LAST_QUEUED_TIMESTAMP).append(":").append(m_lastQueuedTimestamp).append(" ")
              .append(Columns.LAST_ACKED_TIMESTAMP).append(":").append(m_lastAckedTimestamp).append(" ")
              .append(Columns.AVERAGE_LATENCY).append(":").append(m_averageLatency).append(" ")
              .append(Columns.MAX_LATENCY).append(":").append(m_maxLatency).append(" ")
              .append(Columns.QUEUE_GAP).append(":").append(m_queueGap).append(" ")
              .append(Columns.STATUS).append(":").append(m_status);

            return sb.toString();
        }
    }


    static String keyName(String streamName, int partitionId) {
        return streamName + "-" + partitionId;
    }

    public static interface Columns {
        // column for both tables
        public static final String SITE_ID = "SITE_ID";
        public static final String PARTITION_ID = "PARTITION_ID";
        public static final String SOURCE_NAME = "SOURCE";
        public static final String EXPORT_TARGET = "TARGET";
        public static final String ACTIVE = "ACTIVE";
        public static final String TUPLE_COUNT = "TUPLE_COUNT";
        public static final String TUPLE_PENDING = "TUPLE_PENDING";
        public static final String LAST_QUEUED_TIMESTAMP = "LAST_QUEUED_TIMESTAMP";
        public static final String LAST_ACKED_TIMESTAMP = "LAST_ACKED_TIMESTAMP";
        public static final String AVERAGE_LATENCY = "AVERAGE_LATENCY";
        public static final String MAX_LATENCY = "MAX_LATENCY";
        public static final String QUEUE_GAP = "QUEUE_GAP";
        public static final String STATUS = "STATUS";
    }

    /* Constructor */
    public ExportStatsBase() {
        super(false);
    }

    // Check cluster.py and checkstats.py if order of the columns is changed,
    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        super.populateColumnSchema(columns);
        columns.add(new ColumnInfo(VoltSystemProcedure.CNAME_SITE_ID, VoltSystemProcedure.CTYPE_ID));
        columns.add(new ColumnInfo(Columns.PARTITION_ID, VoltType.BIGINT));
        columns.add(new ColumnInfo(Columns.SOURCE_NAME, VoltType.STRING));
        columns.add(new ColumnInfo(Columns.EXPORT_TARGET, VoltType.STRING));
        columns.add(new ColumnInfo(Columns.ACTIVE, VoltType.STRING));
        columns.add(new ColumnInfo(Columns.TUPLE_COUNT, VoltType.BIGINT));
        columns.add(new ColumnInfo(Columns.TUPLE_PENDING, VoltType.BIGINT));
        columns.add(new ColumnInfo(Columns.LAST_QUEUED_TIMESTAMP, VoltType.TIMESTAMP));
        columns.add(new ColumnInfo(Columns.LAST_ACKED_TIMESTAMP, VoltType.TIMESTAMP));
        columns.add(new ColumnInfo(Columns.AVERAGE_LATENCY, VoltType.BIGINT));
        columns.add(new ColumnInfo(Columns.MAX_LATENCY, VoltType.BIGINT));
        columns.add(new ColumnInfo(Columns.QUEUE_GAP, VoltType.BIGINT));
        columns.add(new ColumnInfo(Columns.STATUS, VoltType.STRING));
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(final boolean interval) {
        return ImmutableSet.of().iterator();
    }
}
