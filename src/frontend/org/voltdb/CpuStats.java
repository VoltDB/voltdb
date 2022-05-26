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

import org.voltdb.VoltTable.ColumnInfo;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.ArrayList;
import java.util.Iterator;

import static com.google_voltpatches.common.collect.Iterators.singletonIterator;

public class CpuStats extends StatsSource {

    //Note com.sun here
    com.sun.management.OperatingSystemMXBean m_osBean;

    public enum CPU {
        PERCENT_USED                (VoltType.BIGINT);

        public final VoltType m_type;
        CPU(VoltType type) { m_type = type; }
    }

    public CpuStats() {
        super(false);
        m_osBean = (com.sun.management.OperatingSystemMXBean)ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        return singletonIterator(new Object());
    }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        super.populateColumnSchema(columns, CPU.class);
    }

    @Override
    protected synchronized int updateStatsRow(Object rowKey, Object[] rowValues) {
        int offset = super.updateStatsRow(rowKey, rowValues);
        rowValues[offset + CPU.PERCENT_USED.ordinal()] = Math.round(m_osBean.getProcessCpuLoad() * 100);
        return offset + CPU.values().length;
    }
}
