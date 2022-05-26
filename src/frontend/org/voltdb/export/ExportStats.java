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

package org.voltdb.export;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.voltdb.ExportStatsBase;
import org.voltdb.VoltDB;

/**
 * @author rdykiel
 *
 */
public class ExportStats extends ExportStatsBase {

    List<ExportStatsRow> m_stats;

    public ExportStats() {
        super();
    }

    @Override
    public Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        m_stats = VoltDB.getExportManager().getStats(interval);
        return buildIterator();
    }

    private Iterator<Object> buildIterator() {
        return new Iterator<Object>() {
            int index = 0;

            @Override
            public boolean hasNext() {
                return index < m_stats.size();
            }

            @Override
            public Object next() {
                if (index < m_stats.size()) {
                    return index++;
                }
                throw new NoSuchElementException();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

        };
    }

    @Override
    protected int updateStatsRow(Object rowKey, Object rowValues[]) {
        int offset = super.updateStatsRow(rowKey, rowValues);
        int rowIndex = (Integer) rowKey;
        assert (rowIndex >= 0);
        assert (rowIndex < m_stats.size());
        ExportStatsRow stat = m_stats.get(rowIndex);

        rowValues[offset + Export.SITE_ID.ordinal()] = stat.m_siteId;
        rowValues[offset + Export.PARTITION_ID.ordinal()] = stat.m_partitionId;
        rowValues[offset + Export.SOURCE.ordinal()] = stat.m_sourceName;
        rowValues[offset + Export.TARGET.ordinal()] = stat.m_exportTarget;
        rowValues[offset + Export.ACTIVE.ordinal()] = stat.m_exportingRole;
        rowValues[offset + Export.TUPLE_COUNT.ordinal()] = stat.m_tupleCount;
        rowValues[offset + Export.TUPLE_PENDING.ordinal()] = stat.m_tuplesPending;
        rowValues[offset + Export.LAST_QUEUED_TIMESTAMP.ordinal()] = stat.m_lastQueuedTimestamp;
        rowValues[offset + Export.LAST_ACKED_TIMESTAMP.ordinal()] = stat.m_lastAckedTimestamp;
        rowValues[offset + Export.AVERAGE_LATENCY.ordinal()] = stat.m_averageLatency;
        rowValues[offset + Export.MAX_LATENCY.ordinal()] = stat.m_maxLatency;
        rowValues[offset + Export.QUEUE_GAP.ordinal()] = stat.m_queueGap;
        rowValues[offset + Export.STATUS.ordinal()] = stat.m_status;

        return offset + Export.values().length;
    }

    public ExportStatsRow getStatsRow(Object rowKey) {
        int rowIndex = (Integer) rowKey;
        assert (rowIndex >= 0);
        assert (rowIndex < m_stats.size());
        ExportStatsRow stat = m_stats.get(rowIndex);
        return stat;
    }
}
