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
import java.util.Map;
import java.util.TreeMap;

import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.utils.PlatformProperties;
import org.voltdb.utils.SystemStatsCollector;

public class MemoryStats extends StatsSource {
    static class PartitionMemRow {
        long tupleCount = 0;
        long tupleDataMem = 0;
        long tupleAllocatedMem = 0;
        long indexMem = 0;
        long stringMem = 0;
        long pooledMem = 0;
    }
    Map<Long, PartitionMemRow> m_memoryStats = new TreeMap<Long, PartitionMemRow>();

    public enum Memory {
        RSS                     (VoltType.INTEGER),
        JAVAUSED                (VoltType.INTEGER),
        JAVAUNUSED              (VoltType.INTEGER),
        TUPLEDATA               (VoltType.BIGINT),
        TUPLEALLOCATED          (VoltType.BIGINT),
        INDEXMEMORY             (VoltType.BIGINT),
        STRINGMEMORY            (VoltType.BIGINT),
        TUPLECOUNT              (VoltType.BIGINT),
        POOLEDMEMORY            (VoltType.BIGINT),
        PHYSICALMEMORY          (VoltType.BIGINT),
        JAVAMAXHEAP             (VoltType.INTEGER);

        public final VoltType m_type;
        Memory(VoltType type) { m_type = type; }
    }

    public MemoryStats() {
        super(false);
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
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
        super.populateColumnSchema(columns, Memory.class);
    }

    @Override
    protected synchronized int updateStatsRow(Object rowKey, Object[] rowValues) {
        int offset = super.updateStatsRow(rowKey, rowValues);
        // sum up all of the site statistics
        PartitionMemRow totals = new PartitionMemRow();
        for (PartitionMemRow pmr : m_memoryStats.values()) {
            totals.tupleCount += pmr.tupleCount;
            totals.tupleDataMem += pmr.tupleDataMem;
            totals.tupleAllocatedMem += pmr.tupleAllocatedMem;
            totals.indexMem += pmr.indexMem;
            totals.stringMem += pmr.stringMem;
            totals.pooledMem += pmr.pooledMem;
        }

        // get system statistics
        int rss = 0; int javaused = 0; int javaunused = 0;
        SystemStatsCollector.Datum d = SystemStatsCollector.getRecentSample();
        if (d != null) {
            rss = (int) (d.rss / 1024);
            double javausedFloat = d.javausedheapmem + d.javausedsysmem;
            javaused = (int) (javausedFloat / 1024);
            javaunused = (int) ((d.javatotalheapmem + d.javatotalsysmem - javausedFloat) / 1024);
        }

        rowValues[offset + Memory.RSS.ordinal()] = rss;
        rowValues[offset + Memory.JAVAUSED.ordinal()] = javaused;
        rowValues[offset + Memory.JAVAUNUSED.ordinal()] = javaunused;
        rowValues[offset + Memory.TUPLEDATA.ordinal()] = totals.tupleDataMem;
        rowValues[offset + Memory.TUPLEALLOCATED.ordinal()] = totals.tupleAllocatedMem;
        rowValues[offset + Memory.INDEXMEMORY.ordinal()] = totals.indexMem;
        rowValues[offset + Memory.STRINGMEMORY.ordinal()] = totals.stringMem;
        rowValues[offset + Memory.TUPLECOUNT.ordinal()] = totals.tupleCount;
        rowValues[offset + Memory.POOLEDMEMORY.ordinal()] = totals.pooledMem / 1024;
        //in kb to make math simpler with other mem values.
        rowValues[offset + Memory.PHYSICALMEMORY.ordinal()] = PlatformProperties.getPlatformProperties().ramInMegabytes * 1024;
        rowValues[offset + Memory.JAVAMAXHEAP.ordinal()] = Runtime.getRuntime().maxMemory() / 1024;

        return offset + Memory.values().length;
    }

    public synchronized void eeUpdateMemStats(long siteId,
                                              long tupleCount,
                                              long tupleDataMem,
                                              long tupleAllocatedMem,
                                              long indexMem,
                                              long stringMem,
                                              long pooledMemory) {
        PartitionMemRow pmr = new PartitionMemRow();
        pmr.tupleCount = tupleCount;
        pmr.tupleDataMem = tupleDataMem;
        pmr.tupleAllocatedMem = tupleAllocatedMem;
        pmr.indexMem = indexMem;
        pmr.stringMem = stringMem;
        pmr.pooledMem = pooledMemory;
        m_memoryStats.put(siteId, pmr);
    }
}
