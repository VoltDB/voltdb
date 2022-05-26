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

package org.voltdb.dtxn;

import java.util.ArrayList;

import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;

/** Source of @Statistics LATENCY_HISTOGRAM */
public class LatencyUncompressedHistogramStats extends LatencyHistogramStats {

    public enum LatencyHistogram {
        UNCOMPRESSED_HISTOGRAM       (VoltType.VARBINARY);

        public final VoltType m_type;
        LatencyHistogram(VoltType type) { m_type = type; }
    }

    public LatencyUncompressedHistogramStats(long siteId) {
        super(siteId);
    }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        super.populateColumnSchema(columns);
        columns.add(new ColumnInfo(LatencyHistogram.UNCOMPRESSED_HISTOGRAM.name(),
                LatencyHistogram.UNCOMPRESSED_HISTOGRAM.m_type));
    }

    @Override
    protected int updateStatsRow(Object rowKey, Object[] rowValues) {
        int offset = super.updateStatsRow(rowKey, rowValues);
        rowValues[offset + LatencyHistogram.UNCOMPRESSED_HISTOGRAM.ordinal()] = getSerializedCache();
        return offset + LatencyHistogram.values().length;
    }
}
