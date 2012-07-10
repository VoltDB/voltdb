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

package org.voltdb;

/**
 * A class identifying a table that should be snapshotted as well as the destination
 * for the resulting tuple blocks
 */
public class SnapshotTableTask
{
    final int m_tableId;
    final SnapshotDataTarget m_target;
    final SnapshotDataFilter m_filters[];
    final boolean m_isReplicated;
    final String m_name;

    public SnapshotTableTask(
            final int tableId,
            final SnapshotDataTarget target,
            final SnapshotDataFilter filters[],
            boolean isReplicated,
            final String tableName)
    {
        m_tableId = tableId;
        m_target = target;
        m_filters = filters;
        m_isReplicated = isReplicated;
        m_name = tableName;
    }

    public int getTableId() {
        return m_tableId;
    }

    @Override
    public String toString() {
        return ("SnapshotTableTask for " + m_name + " replicated " + m_isReplicated);
    }
}

