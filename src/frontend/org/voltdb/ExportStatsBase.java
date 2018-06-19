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

    public static enum ExportRole {
        MASTER,
        REPLICA,
    }

    public static class ExportStatsRow {
        public final int m_partitionId;
        public final int m_siteId;
        public final String m_streamName;
        public final String m_role;
        public final String m_exportTarget;
        public final long m_tupleCount;
        public final long m_tuplesPending;
        public final long m_averageLatency;
        public final long m_maxLatency;
        public final long m_gapCount;
        public final String m_status;

        public ExportStatsRow(int partitionId, int siteId, String streamName, String role, String exportTarget,
                long tupleCount, long tuplesPending, long averageLatency, long maxLatency, long gapCount, String status) {
            m_partitionId = partitionId;
            m_siteId = siteId;
            m_streamName = streamName;
            m_role = role;
            m_exportTarget = exportTarget;
            m_tupleCount = tupleCount;
            m_tuplesPending = tuplesPending > 0 ? tuplesPending : 0;
            m_averageLatency = averageLatency;
            m_maxLatency = maxLatency;
            m_gapCount = gapCount;
            m_status = status;
        }
    }


    static String keyName(String streamName, int partitionId) {
        return streamName + "-" + partitionId;
    }

    public static interface Columns {
        // column for both tables
        public static final String SITE_ID = "SITE_ID";
        public static final String PARTITION_ID = "PARTITION_ID";
        public static final String STREAM_NAME = "STREAM_NAME";
        public static final String ROLE = "ROLE";
        public static final String EXPORT_TARGET = "EXPORT_TARGET";
        public static final String TUPLE_COUNT = "TUPLE_COUNT";
        public static final String TUPLE_PENDING = "TUPLE_PENDING";
        public static final String AVERAGE_LATENCY = "AVERAGE_LATENCY";
        public static final String MAX_LATENCY = "MAX_LATENCY";
        public static final String GAP_COUNT = "GAP_COUNT";
        public static final String STATUS = "STATUS";
    }

    /* Constructor */
    public ExportStatsBase() {
        super(false);
    }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        super.populateColumnSchema(columns);
        columns.add(new ColumnInfo(VoltSystemProcedure.CNAME_SITE_ID, VoltSystemProcedure.CTYPE_ID));
        columns.add(new ColumnInfo(Columns.PARTITION_ID, VoltType.BIGINT));
        columns.add(new ColumnInfo(Columns.STREAM_NAME, VoltType.STRING));
        columns.add(new ColumnInfo(Columns.ROLE, VoltType.STRING));
        columns.add(new ColumnInfo(Columns.EXPORT_TARGET, VoltType.STRING));
        columns.add(new ColumnInfo(Columns.TUPLE_COUNT, VoltType.BIGINT));
        columns.add(new ColumnInfo(Columns.TUPLE_PENDING, VoltType.BIGINT));
        columns.add(new ColumnInfo(Columns.AVERAGE_LATENCY, VoltType.BIGINT));
        columns.add(new ColumnInfo(Columns.MAX_LATENCY, VoltType.BIGINT));
        columns.add(new ColumnInfo(Columns.GAP_COUNT, VoltType.BIGINT));
        columns.add(new ColumnInfo(Columns.STATUS, VoltType.STRING));
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(final boolean interval) {
        return ImmutableSet.of().iterator();
    }
}
