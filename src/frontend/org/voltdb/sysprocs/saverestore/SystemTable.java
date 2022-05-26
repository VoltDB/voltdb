/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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
package org.voltdb.sysprocs.saverestore;

import java.util.Map;

import org.voltdb.SnapshotTableInfo;

import com.google_voltpatches.common.collect.ImmutableSortedMap;

/**
 * Enum of all of the system tables defined in the EE. The IDs have to be kept in sync between here and
 * SystemTableFactory.h but the the names do not need to be the same.
 */
public enum SystemTable {
    TOPICS_GROUP(-1, "_TOPICS_GROUP"),
    TOPICS_GROUP_MEMBER(-2, "_TOPICS_GROUP_MEMBER"),
    TOPICS_GROUP_OFFSET(-3, "_TOPICS_GROUP_OFFSET");

    private static final Map<String, SystemTable> s_nameToTable;

    private final int m_id;
    private final String m_name;

    static {
        // Use case insensitive order to allow case insensitive lookups
        ImmutableSortedMap.Builder<String, SystemTable> builder = ImmutableSortedMap
                .orderedBy(String.CASE_INSENSITIVE_ORDER);
        for (SystemTable st : values()) {
            builder.put(st.getName(), st);
        }
        s_nameToTable = builder.build();
    }

    /**
     * @param name of system table
     * @return {@link SnapshotTableInfo} for {@code name} or {@code null} if the system table does not exist
     */
    public static SnapshotTableInfo getTableInfo(String name) {
        SystemTable table = s_nameToTable.get(name);
        return table == null ? null : table.getTableInfo();
    }

    SystemTable(int id, String name) {
        m_id = id;
        m_name = name;
    }

    public int getId() {
        return m_id;
    }

    public String getName() {
        return m_name;
    }

    public SnapshotTableInfo getTableInfo() {
        return new SnapshotTableInfo(m_name, m_id);
    }
}
