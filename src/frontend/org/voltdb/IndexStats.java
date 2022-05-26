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

public class IndexStats extends SiteStatsSource {

    public enum Index {
        PARTITION_ID                (VoltType.BIGINT),
        INDEX_NAME                  (VoltType.STRING),
        TABLE_NAME                  (VoltType.STRING),
        INDEX_TYPE                  (VoltType.STRING),
        IS_UNIQUE                   (VoltType.TINYINT),
        IS_COUNTABLE                (VoltType.TINYINT),
        ENTRY_COUNT                 (VoltType.BIGINT),
        MEMORY_ESTIMATE             (VoltType.BIGINT);

        public final VoltType m_type;
        Index(VoltType type) { m_type = type; }
    }

    public IndexStats(long siteId) {
        super(siteId, true);
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        return null;
    }

    // Generally we fill in this schema from the EE, but we'll provide
    // this so that we can fill in an empty table before the EE has
    // provided us with a table.  Make sure that any changes to the EE
    // schema are reflected here (sigh).
    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        super.populateColumnSchema(columns, Index.class);
    }
}
