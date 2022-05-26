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

public class CommandLogStats extends StatsSource {

    private final CommandLog m_commandLog;

    public enum CommandLogCols {
        OUTSTANDING_BYTES           (VoltType.BIGINT),
        OUTSTANDING_TXNS            (VoltType.BIGINT),
        IN_USE_SEGMENT_COUNT        (VoltType.INTEGER),
        SEGMENT_COUNT               (VoltType.INTEGER),
        FSYNC_INTERVAL              (VoltType.INTEGER);

        public final VoltType m_type;
        CommandLogCols(VoltType type) { m_type = type; }
    }

    public CommandLogStats(CommandLog commandLog) {
        super(false);
        m_commandLog = commandLog;
    }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        super.populateColumnSchema(columns, CommandLogCols.class);
    }

    @Override
    protected int updateStatsRow(Object rowKey, Object[] rowValues) {
        int offset = super.updateStatsRow(rowKey, rowValues);
        m_commandLog.populateCommandLogStats(offset, rowValues);
        return offset + CommandLogCols.values().length;
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
