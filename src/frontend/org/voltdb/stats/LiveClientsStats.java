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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.voltcore.utils.Pair;
import org.voltdb.ClientInterface;
import org.voltdb.StatsSource;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;

public class LiveClientsStats extends StatsSource {

    public enum LiveClients implements StatsColumn {
        CONNECTION_ID(VoltType.BIGINT),
        CLIENT_HOSTNAME(VoltType.STRING),
        ADMIN(VoltType.TINYINT),

        OUTSTANDING_REQUEST_BYTES(VoltType.BIGINT),
        OUTSTANDING_RESPONSE_MESSAGES(VoltType.BIGINT),
        OUTSTANDING_TRANSACTIONS(VoltType.BIGINT);

        private static final LiveClients[] values = LiveClients.values();

        public final VoltType m_type;

        LiveClients(VoltType type) {
            m_type = type;
        }

        public static LiveClients[] getValues() {
            return values;
        }

        @Override
        public VoltType getType() {
            return m_type;
        }
    }

    public LiveClientsStats() {
        super(false);
    }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        super.populateColumnSchema(columns, LiveClients.values());
    }

    @Override
    @SuppressWarnings("unchecked")
    protected int updateStatsRow(Object rowKey, Object[] rowValues) {
        int offset = super.updateStatsRow(rowKey, rowValues);

        Map.Entry<Long, Pair<String, long[]>> entry = (Map.Entry<Long, Pair<String, long[]>>) rowKey;
        Pair<String, long[]> info = entry.getValue();
        long[] counters = info.getSecond();

        rowValues[offset + LiveClients.CONNECTION_ID.ordinal()] = entry.getKey();
        rowValues[offset + LiveClients.CLIENT_HOSTNAME.ordinal()] = info.getFirst();
        rowValues[offset + LiveClients.ADMIN.ordinal()] = counters[0];

        rowValues[offset + LiveClients.OUTSTANDING_REQUEST_BYTES.ordinal()] = counters[1];
        rowValues[offset + LiveClients.OUTSTANDING_RESPONSE_MESSAGES.ordinal()] = counters[2];
        rowValues[offset + LiveClients.OUTSTANDING_TRANSACTIONS.ordinal()] = counters[3];

        return offset + LiveClients.getValues().length;
    }

    @SuppressWarnings({"unchecked", "RawUseOfParameterized"})
    private static <T> Iterator<T> cast(Iterator iterator) {
        return iterator;
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean ignored) {
        HashMap<Long, Pair<String, long[]>> clientStats = new HashMap<>();

        ClientInterface ci = VoltDB.instance().getClientInterface();
        if (ci != null) {
            clientStats.putAll(ci.getLiveClientStats());
        }

        return cast(clientStats.entrySet().iterator());
    }
}
