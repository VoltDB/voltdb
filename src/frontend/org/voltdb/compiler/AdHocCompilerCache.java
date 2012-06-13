/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.compiler;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Keep a cache of the last 100 results from the adhoc planner.
 */
public class AdHocCompilerCache {

    static final int MAX_ENTRIES = 100;

    Map<String, AdHocPlannedStatement> m_cache = new LinkedHashMap<String, AdHocPlannedStatement>() {
        private static final long serialVersionUID = 1L;

        // This method is called just after a new entry has been added
        @Override
        public boolean removeEldestEntry(Map.Entry<String, AdHocPlannedStatement> eldest) {
            return size() > MAX_ENTRIES;
        }
    };

    public AdHocPlannedStatement get(String sql, boolean singlePartition) {
        AdHocPlannedStatement candidate = m_cache.get(sql);
        if (candidate == null) return null;
        if ((candidate.partitionParam != null) == singlePartition) return candidate;
        return null;
    }

    public void put (AdHocPlannedStatement result) {
        m_cache.put(result.sql, result);
    }
}
