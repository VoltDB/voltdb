/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

public class CommandLogStats extends StatsSource {

    private final CommandLog m_commandLog;

    public enum StatName {
        OUTSTANDING_BYTES,
        OUTSTANDING_TXNS,
        IN_USE_SEGMENT_COUNT,
        SEGMENT_COUNT,
        FSYNC_INTERVAL
    };

    public CommandLogStats(CommandLog commandLog) {
        super(false);
        m_commandLog = commandLog;
    }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        super.populateColumnSchema(columns);
        columns.add(new VoltTable.ColumnInfo(StatName.OUTSTANDING_BYTES.name(), VoltType.BIGINT));
        columns.add(new VoltTable.ColumnInfo(StatName.OUTSTANDING_TXNS.name(), VoltType.BIGINT));
        columns.add(new VoltTable.ColumnInfo(StatName.IN_USE_SEGMENT_COUNT.name(), VoltType.INTEGER));
        columns.add(new VoltTable.ColumnInfo(StatName.SEGMENT_COUNT.name(), VoltType.INTEGER));
        columns.add(new VoltTable.ColumnInfo(StatName.FSYNC_INTERVAL.name(), VoltType.INTEGER));
    }

    @Override
    protected void updateStatsRow(Object rowKey, Object[] rowValues) {
        m_commandLog.populateCommandLogStats(columnNameToIndex, rowValues);
        super.updateStatsRow(rowKey, rowValues);
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

}
