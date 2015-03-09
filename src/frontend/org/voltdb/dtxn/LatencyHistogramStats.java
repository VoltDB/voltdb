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

package org.voltdb.dtxn;

import java.util.ArrayList;

import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;

public class LatencyHistogramStats extends LatencyStats {

    public LatencyHistogramStats(long siteId) {
        super(siteId);
    }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        super.populateColumnSchema(columns);
        columns.add(new ColumnInfo("UNCOMPRESSED_HISTOGRAM", VoltType.VARBINARY));
    }

    @Override
    protected void updateStatsRow(Object rowKey, Object[] rowValues) {
        rowValues[columnNameToIndex.get("UNCOMPRESSED_HISTOGRAM")] = getSerializedCache();
        super.updateStatsRow(rowKey, rowValues);
    }
}
