/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

/**
 * A class to track and generate statistics regarding task starvation.
 * A worker thread can instantiate one of these and report when starvation begins and ends.
 */
public class StarvationTracker extends SiteStatsSource {

    public StarvationTracker(long siteId) {
        super(siteId, false);
        m_lastStartTime = m_startTime = System.nanoTime();
    }

    /*
     * Keep track of the time since the tracker was created or in the interval case, the last time
     * the stat was checked
     */
    private long m_startTime;
    private long m_lastStartTime;

    private long m_count = 0;
    private long m_lastCount = 0;
    private long m_sumOfSquares = 0;
    private long m_lastSumOfSquares = 0;
    private long m_totalTime = 0;
    private long m_lastTotalTime = 0;
    private long m_max = 0;
    private long m_lastMax = 0;
    private long m_min = Long.MAX_VALUE;
    private long m_lastMin = Long.MAX_VALUE;

    private long m_starvationStartTime;

    private boolean m_interval;

    /**
     * Is there currently starvation
     */
    private boolean m_starved = false;
    public void beginStarvation() {
        if (m_starved) {
            return;
        }
        m_starved = true;
        m_starvationStartTime = System.nanoTime();
    }

    public void endStarvation() {
        if (!m_starved) {
            return;
        }
        m_starved = false;
        m_count++;
        long delta = System.nanoTime() - m_starvationStartTime;
        m_totalTime += delta;
        m_sumOfSquares += delta * delta / 1000000;
        m_max = Math.max(m_max, delta);
        m_lastMax = Math.max(m_lastMax, delta);
        m_min = Math.min(m_min, delta);
        m_lastMin = Math.min(m_lastMin, delta);
    }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        super.populateColumnSchema(columns);
        columns.add(new ColumnInfo("COUNT", VoltType.BIGINT));
        columns.add(new ColumnInfo("PERCENT", VoltType.FLOAT));
        columns.add(new ColumnInfo("AVG", VoltType.BIGINT));
        columns.add(new ColumnInfo("MIN", VoltType.BIGINT));
        columns.add(new ColumnInfo("MAX", VoltType.BIGINT));
        columns.add(new ColumnInfo("STDDEV", VoltType.BIGINT));
    }

    @Override
    protected void updateStatsRow(Object rowKey, Object rowValues[]) {
        if (m_interval) {
            final long now = System.nanoTime();
            final long totalTime = now - m_lastStartTime;
            final long count = m_count - m_lastCount;
            final long totalStarvedTime = m_totalTime - m_lastTotalTime;
            final long sumOfSquares = m_sumOfSquares - m_lastSumOfSquares;
            final long uSecs = totalStarvedTime / 1000;
            m_lastStartTime = now;
            m_lastSumOfSquares = m_sumOfSquares;
            m_lastTotalTime = m_totalTime;
            m_lastCount = m_count;
            m_lastMax = 0;
            m_lastMin = Long.MAX_VALUE;
            if (count > 0) {
                rowValues[columnNameToIndex.get("COUNT")] = count;
                rowValues[columnNameToIndex.get("PERCENT")] = totalStarvedTime / (totalTime / 100.0);
                rowValues[columnNameToIndex.get("AVG")] = (totalStarvedTime / count) / 1000;
                rowValues[columnNameToIndex.get("MIN")] = m_lastMin;
                rowValues[columnNameToIndex.get("MAX")] = m_lastMax;
                rowValues[columnNameToIndex.get("STDDEV")] = (long)Math.sqrt(sumOfSquares / count - uSecs * uSecs);
            } else {
                rowValues[columnNameToIndex.get("COUNT")] = 0L;
                rowValues[columnNameToIndex.get("PERCENT")] = 0L;
                rowValues[columnNameToIndex.get("AVG")] = 0L;
                rowValues[columnNameToIndex.get("MIN")] = 0L;
                rowValues[columnNameToIndex.get("MAX")] = 0L;
                rowValues[columnNameToIndex.get("STDDEV")] = 0L;
            }
        } else {
            final long totalTime = System.nanoTime() - m_startTime;
            if (m_count > 0) {
                final long uSecs = (m_totalTime / m_count) / 1000;
                rowValues[columnNameToIndex.get("COUNT")] = m_count;
                rowValues[columnNameToIndex.get("PERCENT")] = m_totalTime / (totalTime / 100.0);
                rowValues[columnNameToIndex.get("AVG")] = uSecs;
                rowValues[columnNameToIndex.get("MIN")] = m_min;
                rowValues[columnNameToIndex.get("MAX")] = m_max;
                rowValues[columnNameToIndex.get("STDDEV")] = (long)Math.sqrt(m_sumOfSquares / m_count - uSecs * uSecs);
            }
            else {
                rowValues[columnNameToIndex.get("COUNT")] = 0L;
                rowValues[columnNameToIndex.get("PERCENT")] = 0L;
                rowValues[columnNameToIndex.get("AVG")] = 0L;
                rowValues[columnNameToIndex.get("MIN")] = 0L;
                rowValues[columnNameToIndex.get("MAX")] = 0L;
                rowValues[columnNameToIndex.get("STDDEV")] = 0L;
            }
        }
        super.updateStatsRow(rowKey, rowValues);
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(final boolean interval) {
        m_interval = interval;
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
}
