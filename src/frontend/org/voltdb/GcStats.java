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

import org.voltdb.VoltTable.ColumnInfo;;

public class GcStats extends StatsSource {

    private int m_lastNewGenGcCount = 0;
    private int m_totalNewGenGcCount = 0;
    private int m_lastNewGenGcTime = 0;
    private int m_totalNewGenGcTime = 0;

    private int m_lastOldGenGcCount = 0;
    private int m_totalOldGenGcCount = 0;
    private int m_lastOldGenGcTime = 0;
    private int m_totalOldGenGcTime = 0;

    private boolean m_intervalCollection = false;

    public enum GC {
        NEWGEN_GC_COUNT             (VoltType.INTEGER),
        NEWGEN_AVG_GC_TIME          (VoltType.BIGINT),
        OLDGEN_GC_COUNT             (VoltType.INTEGER),
        OLDGEN_AVG_GC_TIME          (VoltType.BIGINT);

        public final VoltType m_type;
        GC(VoltType type) { m_type = type; }
    }

    public GcStats() {
        super(false);
    }

    public synchronized void gcInspectorReport(boolean youngGenGC, int gcCount, long avgGcTime) {
        if (youngGenGC) {
            m_lastNewGenGcCount += gcCount;
            m_lastNewGenGcTime += avgGcTime;
        }
        else {
            m_lastOldGenGcCount += gcCount;
            m_lastOldGenGcTime += avgGcTime;
        }
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        m_intervalCollection = interval;
        return new Iterator<Object>() {
            boolean returnRow = true;

            @Override
            public boolean hasNext() {
                return returnRow;
            }

            @Override
            public Object next() {
                if (returnRow) {
                    returnRow = false;
                    return new Object();
                } else {
                    return null;
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        super.populateColumnSchema(columns, GC.class);
    }

    @Override
    protected synchronized int updateStatsRow(Object rowKey, Object[] rowValues) {
        int offset = super.updateStatsRow(rowKey, rowValues);
        if (m_intervalCollection) {
            rowValues[offset + GC.NEWGEN_GC_COUNT.ordinal()] = m_lastNewGenGcCount;
            rowValues[offset + GC.NEWGEN_AVG_GC_TIME.ordinal()]
                    = m_lastNewGenGcCount > 0 ? m_lastNewGenGcTime / m_lastNewGenGcCount : 0;
            rowValues[offset + GC.OLDGEN_GC_COUNT.ordinal()] = m_lastOldGenGcCount;
            rowValues[offset + GC.OLDGEN_AVG_GC_TIME.ordinal()]
                    = m_lastOldGenGcCount > 0 ? m_lastOldGenGcTime / m_lastOldGenGcCount : 0;
            m_totalNewGenGcCount += m_lastNewGenGcCount;
            m_lastNewGenGcCount = 0;
            m_totalOldGenGcCount += m_lastOldGenGcCount;
            m_lastOldGenGcCount = 0;
            m_totalNewGenGcTime += m_lastNewGenGcTime;
            m_lastNewGenGcTime = 0;
            m_totalOldGenGcTime += m_lastOldGenGcCount;
            m_lastOldGenGcCount = 0;
        }
        else {
            int totalNewGcCount = m_totalNewGenGcCount + m_lastNewGenGcCount;
            int totalOldGcCount = m_totalOldGenGcCount + m_lastOldGenGcCount;
            rowValues[offset + GC.NEWGEN_GC_COUNT.ordinal()] = totalNewGcCount;
            rowValues[offset + GC.NEWGEN_AVG_GC_TIME.ordinal()]
                    = totalNewGcCount > 0 ? (m_totalNewGenGcTime + m_lastNewGenGcTime) / totalNewGcCount : 0;
            rowValues[offset + GC.OLDGEN_GC_COUNT.ordinal()] = totalOldGcCount;
            rowValues[offset + GC.OLDGEN_AVG_GC_TIME.ordinal()]
                    = totalOldGcCount > 0 ? (m_totalOldGenGcTime + m_lastOldGenGcTime) / totalOldGcCount : 0;
        }
        return offset + GC.values().length;
    }

}
