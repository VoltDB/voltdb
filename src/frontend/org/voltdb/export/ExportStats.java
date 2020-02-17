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

package org.voltdb.export;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.voltdb.ExportStatsBase;

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
        m_stats = ExportManagerInterface.instance().getStats(interval);
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
    protected void updateStatsRow(Object rowKey, Object rowValues[]) {
        super.updateStatsRow(rowKey, rowValues);
        int rowIndex = (Integer) rowKey;
        assert (rowIndex >= 0);
        assert (rowIndex < m_stats.size());
        ExportStatsRow stat = m_stats.get(rowIndex);
        rowValues[columnNameToIndex.get(Columns.SITE_ID)] = stat.m_siteId;
        rowValues[columnNameToIndex.get(Columns.PARTITION_ID)] = stat.m_partitionId;
        rowValues[columnNameToIndex.get(Columns.SOURCE_NAME)] = stat.m_sourceName;
        rowValues[columnNameToIndex.get(Columns.EXPORT_TARGET)] = stat.m_exportTarget;
        rowValues[columnNameToIndex.get(Columns.ACTIVE)] = stat.m_exportingRole;
        rowValues[columnNameToIndex.get(Columns.TUPLE_COUNT)] = stat.m_tupleCount;
        rowValues[columnNameToIndex.get(Columns.TUPLE_PENDING)] = stat.m_tuplesPending;
        rowValues[columnNameToIndex.get(Columns.LAST_QUEUED_TIMESTAMP)] = stat.m_lastQueuedTimestamp;
        rowValues[columnNameToIndex.get(Columns.LAST_ACKED_TIMESTAMP)] = stat.m_lastAckedTimestamp;
        rowValues[columnNameToIndex.get(Columns.AVERAGE_LATENCY)] = stat.m_averageLatency;
        rowValues[columnNameToIndex.get(Columns.MAX_LATENCY)] = stat.m_maxLatency;
        rowValues[columnNameToIndex.get(Columns.QUEUE_GAP)] = stat.m_queueGap;
        rowValues[columnNameToIndex.get(Columns.STATUS)] = stat.m_status;
    }

    public ExportStatsRow getStatsRow(Object rowKey) {
        int rowIndex = (Integer) rowKey;
        assert (rowIndex >= 0);
        assert (rowIndex < m_stats.size());
        ExportStatsRow stat = m_stats.get(rowIndex);
        return stat;
    }
}
