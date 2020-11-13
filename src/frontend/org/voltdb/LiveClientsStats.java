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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.voltcore.utils.Pair;
import org.voltdb.VoltTable.ColumnInfo;

// This StatsSource is a bit of a hackjob, in that it exists only to make StatsAgent
// happy.  The updating is never used, and the code in the Statistics sysproc
// gets the client stats from the client interfaces and constructs the results
// tables directly.  This is identical to what IOStats also does.  As
// near as I can tell, this is because both the VoltNetwork and now the ClientInterface
// have no catalog ID with which to associate these stats, so they just sort of hang out there
// oddly.
public class LiveClientsStats extends StatsSource
{
    private Map<Long, Pair<String, long[]>> m_clientStats = new HashMap<Long, Pair<String,long[]>>();

    public enum LiveClients {
        CONNECTION_ID                   (VoltType.BIGINT),
        CLIENT_HOSTNAME                 (VoltType.STRING),
        ADMIN                           (VoltType.TINYINT),
        OUTSTANDING_REQUEST_BYTES       (VoltType.BIGINT),
        OUTSTANDING_RESPONSE_MESSAGES   (VoltType.BIGINT),
        OUTSTANDING_TRANSACTIONS        (VoltType.BIGINT);

        public final VoltType m_type;
        LiveClients(VoltType type) { m_type = type; }
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

    public LiveClientsStats() {
        super(false);
    }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        super.populateColumnSchema(columns, LiveClients.class);
    }

    @Override
    protected int updateStatsRow(Object rowKey, Object[] rowValues) {
        int offset = super.updateStatsRow(rowKey, rowValues);
        final Pair<String, long[]> info = m_clientStats.get(rowKey);
        final long[] counters = info.getSecond();

        rowValues[offset + LiveClients.CONNECTION_ID.ordinal()] = rowKey;
        rowValues[offset + LiveClients.CLIENT_HOSTNAME.ordinal()] = info.getFirst();
        rowValues[offset + LiveClients.ADMIN.ordinal()] = counters[0];
        rowValues[offset + LiveClients.OUTSTANDING_REQUEST_BYTES.ordinal()] = counters[1];
        rowValues[offset + LiveClients.OUTSTANDING_RESPONSE_MESSAGES.ordinal()] = counters[2];
        rowValues[offset + LiveClients.OUTSTANDING_TRANSACTIONS.ordinal()] = counters[3];

        return offset + LiveClients.values().length;
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval)
    {
        m_clientStats = new HashMap<Long, Pair<String,long[]>>();
        ClientInterface ci = VoltDB.instance().getClientInterface();
        if (ci != null) {
            m_clientStats.putAll(ci.getLiveClientStats());
        }
        return new DummyIterator(m_clientStats.keySet().iterator());
    }
}
