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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.voltcore.utils.Pair;
import org.voltdb.VoltTable.ColumnInfo;

public class IOStats extends StatsSource {
    private Map<Long, Pair<String, long[]>> m_ioStats = new HashMap<Long, Pair<String,long[]>>();

    public enum IoStats {
        CONNECTION_ID               (VoltType.BIGINT),
        CONNECTION_HOSTNAME         (VoltType.STRING),
        BYTES_READ                  (VoltType.BIGINT),
        MESSAGES_READ               (VoltType.BIGINT),
        BYTES_WRITTEN               (VoltType.BIGINT),
        MESSAGES_WRITTEN            (VoltType.BIGINT);

        public final VoltType m_type;
        IoStats(VoltType type) { m_type = type; }
    }

    /**
     * A dummy iterator that wraps an Iterator<Long> and provides the
     * Iterator<Object>
     */
    private class DummyIterator implements Iterator<Object> {
        private final Iterator<Long> i;

        private DummyIterator(Iterator<Long> i) {
            this.i = i;
        }

        @Override
        public boolean hasNext() {
            return i.hasNext();
        }

        @Override
        public Object next() {
            return i.next();
        }

        @Override
        public void remove() {
            i.remove();
        }
    }

    public IOStats() {
        super(false);
    }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        super.populateColumnSchema(columns, IoStats.class);
    }

    @Override
    protected int updateStatsRow(Object rowKey, Object[] rowValues) {
        int offset = super.updateStatsRow(rowKey, rowValues);
        final Pair<String, long[]> info = m_ioStats.get(rowKey);
        final long[] counters = info.getSecond();

        rowValues[offset + IoStats.CONNECTION_ID.ordinal()] = rowKey;
        rowValues[offset + IoStats.CONNECTION_HOSTNAME.ordinal()] = info.getFirst();
        rowValues[offset + IoStats.BYTES_READ.ordinal()] = counters[0];
        rowValues[offset + IoStats.MESSAGES_READ.ordinal()] = counters[1];
        rowValues[offset + IoStats.BYTES_WRITTEN.ordinal()] = counters[2];
        rowValues[offset + IoStats.MESSAGES_WRITTEN.ordinal()] = counters[3];
        return offset + IoStats.values().length;
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        try {
            m_ioStats = VoltDB.instance().getHostMessenger().getIOStats(interval);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return new DummyIterator(m_ioStats.keySet().iterator());
    }
}
