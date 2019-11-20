/* This file is part of VoltDB.
 * Copyright (C) 2019 VoltDB Inc.
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

import org.voltdb.SnapshotTableInfo;

/**
 * Enum of all of the system tables defined in the EE. The IDs have to be kept in sync between here and
 * SystemTableFactory.h but the the names do not need to be the same.
 */
public enum SystemTable {
    KIPLING_GROUP(-1, "_KIPLING_GROUP"),
    KIPLING_GROUP_MEMBER(-2, "_KIPLING_GROUP_MEMBER"),
    KIPLING_GROUP_MEMBER_PROTOCOL(-3, "_KIPLING_GROUP_MEMBER_PROTOCOL"),
    KIPLING_GROUP_OFFSET(-4, "_KIPLING_GROUP_OFFSET");

    private final int m_id;
    private final String m_name;

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
