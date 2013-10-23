/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

import org.voltdb.catalog.Table;
import org.voltdb.expressions.AbstractExpression;

/**
 * A class identifying a table that should be snapshotted as well as the destination
 * for the resulting tuple blocks
 */
public class SnapshotTableTask
{
    final Table m_table;
    final SnapshotDataTarget m_target;
    final SnapshotDataFilter m_filters[];
    final AbstractExpression m_predicate;
    final boolean m_deleteTuples;

    public SnapshotTableTask(
            final Table table,
            final SnapshotDataTarget target,
            final SnapshotDataFilter filters[],
            final AbstractExpression predicate,
            final boolean deleteTuples)
    {
        m_table = table;
        m_target = target;
        m_filters = filters;
        m_predicate = predicate;
        m_deleteTuples = deleteTuples;
    }

    @Override
    public String toString()
    {
        return ("SnapshotTableTask for " + m_table.getTypeName() +
                " replicated " + m_table.getIsreplicated() +
                ", delete " + m_deleteTuples);
    }
}

