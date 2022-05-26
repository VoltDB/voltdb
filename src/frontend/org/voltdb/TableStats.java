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

public class TableStats extends SiteStatsSource {

    // Make sure that any changes to the EE schema are reflected here (sigh).
    public enum Table {
        PARTITION_ID            (VoltType.BIGINT),
        TABLE_NAME              (VoltType.STRING),
        TABLE_TYPE              (VoltType.STRING),
        TUPLE_COUNT             (VoltType.BIGINT),
        TUPLE_ALLOCATED_MEMORY  (VoltType.BIGINT),
        TUPLE_DATA_MEMORY       (VoltType.BIGINT),
        STRING_DATA_MEMORY      (VoltType.BIGINT),
        TUPLE_LIMIT             (VoltType.INTEGER),
        PERCENT_FULL            (VoltType.INTEGER),
        DR                      (VoltType.STRING),
        EXPORT                  (VoltType.STRING);

        public final VoltType m_type;
        Table(VoltType type) { m_type = type; }
    }

    public TableStats(long siteId) {
        super( siteId, true);
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        return null;
    }

    // Generally we fill in this schema from the EE, but we'll provide
    // this so that we can fill in an empty table before the EE has
    // provided us with a table.
    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        super.populateColumnSchema(columns, Table.class);
    }
}
