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
package org.voltdb.stats;

import java.util.ArrayList;
import java.util.Iterator;
import com.google_voltpatches.common.collect.Iterators;
import org.voltdb.StatsSource;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;

public class GcStats extends StatsSource {

    private int m_lastNewGenGcCount;
    private int m_lastNewGenGcTime;
    private int m_lastOldGenGcCount;
    private int m_lastOldGenGcTime;

    private int m_totalNewGenGcCount;
    private int m_totalNewGenGcTime;
    private int m_totalOldGenGcCount;
    private int m_totalOldGenGcTime;

    public enum GC implements StatsColumn {
        NEWGEN_GC_COUNT(VoltType.INTEGER),
        NEWGEN_AVG_GC_TIME(VoltType.BIGINT),
        OLDGEN_GC_COUNT(VoltType.INTEGER),
        OLDGEN_AVG_GC_TIME(VoltType.BIGINT);

        public final VoltType m_type;

        GC(VoltType type) {m_type = type;}

        @Override
        public VoltType getType() {
            return m_type;
        }
    }

    public GcStats() {
        super(false);
    }

    public synchronized void gcInspectorReport(boolean youngGenGC, int gcCount, long gcTime) {
        if (youngGenGC) {
            m_lastNewGenGcCount += gcCount;
            m_lastNewGenGcTime += gcTime;
        } else {
            m_lastOldGenGcCount += gcCount;
            m_lastOldGenGcTime += gcTime;
        }
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        return Iterators.singletonIterator(interval);
    }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        super.populateColumnSchema(columns, GC.values());
    }

    @Override
    protected synchronized int updateStatsRow(Object rowKey, Object[] rowValues) {
        int offset = super.updateStatsRow(rowKey, rowValues);

        Boolean intervalCollection = (Boolean) rowKey;
        if (intervalCollection) {
            calculateMetrics(
                    rowValues,
                    offset,
                    m_lastNewGenGcCount,
                    m_lastNewGenGcTime,
                    m_lastOldGenGcCount,
                    m_lastOldGenGcTime
            );

            m_totalNewGenGcCount += m_lastNewGenGcCount;
            m_lastNewGenGcCount = 0;
            m_totalOldGenGcCount += m_lastOldGenGcCount;
            m_lastOldGenGcCount = 0;
            m_totalNewGenGcTime += m_lastNewGenGcTime;
            m_lastNewGenGcTime = 0;
            m_totalOldGenGcTime += m_lastOldGenGcTime;
            m_lastOldGenGcTime = 0;
        } else {
            int totalNewGcCount = m_totalNewGenGcCount + m_lastNewGenGcCount;
            int totalOldGcCount = m_totalOldGenGcCount + m_lastOldGenGcCount;
            int totalNewGcTime = m_totalNewGenGcTime + m_lastNewGenGcTime;
            int totalOldGcTime = m_totalOldGenGcTime + m_lastOldGenGcTime;

            calculateMetrics(
                    rowValues,
                    offset,
                    totalNewGcCount,
                    totalNewGcTime,
                    totalOldGcCount,
                    totalOldGcTime
            );
        }

        return offset + GC.values().length;
    }

    private void calculateMetrics(
            Object[] rowValues,
            int offset,
            int newGcCount,
            int newGenGcTime,
            int oldGcCount,
            int oldGenGcTime
    ) {
        rowValues[offset + GC.NEWGEN_GC_COUNT.ordinal()] = newGcCount;
        rowValues[offset + GC.NEWGEN_AVG_GC_TIME.ordinal()] = newGcCount > 0 ? newGenGcTime / newGcCount : 0;
        rowValues[offset + GC.OLDGEN_GC_COUNT.ordinal()] = oldGcCount;
        rowValues[offset + GC.OLDGEN_AVG_GC_TIME.ordinal()] = oldGcCount > 0 ? oldGenGcTime / oldGcCount : 0;
    }
}
