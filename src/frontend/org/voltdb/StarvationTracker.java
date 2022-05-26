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

/**
 * A class to track and generate statistics regarding task starvation.
 * A worker thread can instantiate one of these and report when starvation begins and ends.
 */
public class StarvationTracker extends SiteStatsSource {

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

    // a.k.a starvation
    public enum Idletime {
        COUNT                       (VoltType.BIGINT),
        PERCENT                     (VoltType.FLOAT),
        AVG                         (VoltType.BIGINT),
        MIN                         (VoltType.BIGINT),
        MAX                         (VoltType.BIGINT),
        STDDEV                      (VoltType.BIGINT);

        public final VoltType m_type;
        Idletime(VoltType type) { m_type = type; }
    }

    public StarvationTracker(long siteId) {
        super(siteId, false);
        m_lastStartTime = m_startTime = System.nanoTime();
    }

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
        super.populateColumnSchema(columns, Idletime.class);
    }

    @Override
    protected int updateStatsRow(Object rowKey, Object rowValues[]) {
        int offset = super.updateStatsRow(rowKey, rowValues);
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
                rowValues[offset + Idletime.COUNT.ordinal()] = count;
                rowValues[offset + Idletime.PERCENT.ordinal()] = totalStarvedTime / (totalTime / 100.0);
                rowValues[offset + Idletime.AVG.ordinal()] = (totalStarvedTime / count) / 1000;
                rowValues[offset + Idletime.MIN.ordinal()] = m_lastMin;
                rowValues[offset + Idletime.MAX.ordinal()] = m_lastMax;
                rowValues[offset + Idletime.STDDEV.ordinal()] = (long)Math.sqrt(sumOfSquares / count - uSecs * uSecs);
            } else {
                rowValues[offset + Idletime.COUNT.ordinal()] = 0L;
                rowValues[offset + Idletime.PERCENT.ordinal()] = 0L;
                rowValues[offset + Idletime.AVG.ordinal()] = 0L;
                rowValues[offset + Idletime.MIN.ordinal()] = 0L;
                rowValues[offset + Idletime.MAX.ordinal()] = 0L;
                rowValues[offset + Idletime.STDDEV.ordinal()] = 0L;
            }
        } else {
            final long totalTime = System.nanoTime() - m_startTime;
            if (m_count > 0) {
                final long uSecs = (m_totalTime / m_count) / 1000;
                rowValues[offset + Idletime.COUNT.ordinal()] = m_count;
                rowValues[offset + Idletime.PERCENT.ordinal()] = m_totalTime / (totalTime / 100.0);
                rowValues[offset + Idletime.AVG.ordinal()] = uSecs;
                rowValues[offset + Idletime.MIN.ordinal()] = m_min;
                rowValues[offset + Idletime.MAX.ordinal()] = m_max;
                rowValues[offset + Idletime.STDDEV.ordinal()] = (long)Math.sqrt(m_sumOfSquares / m_count - uSecs * uSecs);
            }
            else {
                rowValues[offset + Idletime.COUNT.ordinal()] = 0L;
                rowValues[offset + Idletime.PERCENT.ordinal()] = 0L;
                rowValues[offset + Idletime.AVG.ordinal()] = 0L;
                rowValues[offset + Idletime.MIN.ordinal()] = 0L;
                rowValues[offset + Idletime.MAX.ordinal()] = 0L;
                rowValues[offset + Idletime.STDDEV.ordinal()] = 0L;
            }
        }
        return offset + Idletime.values().length;
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
