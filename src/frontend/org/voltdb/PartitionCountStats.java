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

package org.voltdb;

import java.util.ArrayList;
import java.util.Iterator;

import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.iv2.Cartographer;

public class PartitionCountStats extends StatsSource {
    private final String COLUMN_NAME = "PARTITION_COUNT";

    // IV2 asks the cartographer for partition count
    private final Cartographer m_cartographer;

    /** IV2 constructor */
    public PartitionCountStats(Cartographer cartographer) {
        super(false);
        m_cartographer = cartographer;
    }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        super.populateColumnSchema(columns);
        columns.add(new ColumnInfo(COLUMN_NAME, VoltType.INTEGER));
    }

    @Override
    protected void updateStatsRow(Object rowKey, Object[] rowValues) {
        rowValues[columnNameToIndex.get(COLUMN_NAME)] = m_cartographer.getPartitionCount();
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
