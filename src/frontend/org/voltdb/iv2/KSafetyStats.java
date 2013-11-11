/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

package org.voltdb.iv2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.atomic.AtomicStampedReference;

import org.voltdb.StatsSource;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;

import com.google.common.collect.ImmutableSortedMap;

public class KSafetyStats extends StatsSource {
    final AtomicStampedReference<NavigableMap<Integer, Integer>> m_kSafetyMap;

    public KSafetyStats() {
        super(false);
        NavigableMap<Integer, Integer> initial = ImmutableSortedMap.<Integer, Integer>of();
        m_kSafetyMap = new AtomicStampedReference<>(initial,0);
    }

    public AtomicStampedReference<NavigableMap<Integer, Integer>> getKSafetyMapReference() {
        return m_kSafetyMap;
    }

    public static interface Constants {
        public final static String PARTITION_ID = "PARTITION_ID";
        public final static String REPLICA_COUNT = "REPLICA_COUNT";
    }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        columns.add(new ColumnInfo(Constants.PARTITION_ID, VoltType.INTEGER));
        columns.add(new ColumnInfo(Constants.REPLICA_COUNT, VoltType.INTEGER));
    }

    @Override
    protected void updateStatsRow(Object rowKey, Object[] rowValues) {
        @SuppressWarnings("unchecked")
        Map.Entry<Integer, Integer> entry = (Map.Entry<Integer, Integer>)rowKey;
        rowValues[columnNameToIndex.get(Constants.PARTITION_ID)] = entry.getKey();
        rowValues[columnNameToIndex.get(Constants.REPLICA_COUNT)] = entry.getValue();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        @SuppressWarnings("rawtypes")
        Iterator iter = m_kSafetyMap.getReference().entrySet().iterator();
        return (Iterator<Object>)iter;
    }
}
