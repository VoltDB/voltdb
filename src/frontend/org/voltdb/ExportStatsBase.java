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
    }

    public enum Export {
        SITE_ID                     (VoltType.INTEGER),
        PARTITION_ID                (VoltType.BIGINT),
        SOURCE                      (VoltType.STRING),
        TARGET                      (VoltType.STRING),
        ACTIVE                      (VoltType.STRING),
        TUPLE_COUNT                 (VoltType.BIGINT),
        TUPLE_PENDING               (VoltType.BIGINT),
        LAST_QUEUED_TIMESTAMP       (VoltType.TIMESTAMP),
        LAST_ACKED_TIMESTAMP        (VoltType.TIMESTAMP),
        AVERAGE_LATENCY             (VoltType.BIGINT),
        MAX_LATENCY                 (VoltType.BIGINT),
        QUEUE_GAP                   (VoltType.BIGINT),
        STATUS                      (VoltType.STRING);

        public final VoltType m_type;
        Export(VoltType type) { m_type = type; }
    }


    static String keyName(String streamName, int partitionId) {
        return streamName + "-" + partitionId;
    }

    /* Constructor */
    public ExportStatsBase() {
        super(false);
    }

    // Check cluster.py and checkstats.py if order of the columns is changed,
    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        super.populateColumnSchema(columns, Export.class);
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(final boolean interval) {
        return ImmutableSet.of().iterator();
    }
}
